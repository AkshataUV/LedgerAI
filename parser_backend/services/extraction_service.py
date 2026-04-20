"""
services/extraction_service.py
──────────────────────────────
STEP 4 — Generate extraction code via LLM (Claude/OpenRouter/9router)
and execute it safely against document text.

This module is the thin orchestrator. All document-family-specific
prompts live in services/prompts/*.py.
"""

import re
import json
import logging
from typing import List, Dict, Any

from services.code_gen_client import get_code_gen_client
from services.prompts import get_prompt
from services.code_sandbox import execute_extraction_code, validate_code, clean_llm_code
from services.llm_parser import parse_with_llm  # Used for ground truth vetting
from services.validation_service import validate_transactions, extract_json_from_response

logger = logging.getLogger("ledgerai.extraction_service")


def _build_line_examples(ground_truth: list, pdf_text: str) -> str:
    """
    QC PANEL PORT: Searches for ground truth rows in raw text to provide 
    concrete 'Target -> Raw Line' mapping for the LLM.
    """
    lines = pdf_text.splitlines()
    examples = []
    
    for txn in ground_truth[:15]:
        date_str = txn.get("date", "")
        amount = txn.get("debit") or txn.get("credit")
        if not date_str or amount is None: continue
            
        amt_str = f"{abs(float(amount)):.2f}"
        for line in lines:
            if date_str in line and amt_str in line:
                examples.append(f"TARGET: {json.dumps(txn)}\nRAW LINE: \"{line.strip()}\"\n")
                break
        
    return "\n".join(examples) if examples else "No explicit line-mappings could be determined."


# ═══════════════════════════════════════════════════════════
# GENERATE EXTRACTION CODE VIA LLM
# ═══════════════════════════════════════════════════════════

def generate_extraction_logic_llm(
    identifier_json: dict,
    text_sample: str,
) -> str:
    """
    Generates extraction code using the family-specific prompt from services/prompts/.
    Uses Claude (via Anthropic/OpenRouter/9router) for better code quality.
    Returns validated Python code string containing extract_transactions().
    """
    document_family = identifier_json.get("document_family", "BANK_ACCOUNT_STATEMENT")

    prompt = get_prompt(document_family, identifier_json, text_sample)

    logger.info(
        "Generating extraction code: family=%s prompt_len=%d",
        document_family, len(prompt),
    )

    # Get the configured code generation client (Claude via Anthropic/OpenRouter/9router)
    code_gen_client = get_code_gen_client()

    # Generate code with retry logic built into the client
    content = code_gen_client.generate(prompt, max_retries=3)

    if not content:
        raise ValueError("LLM returned empty extraction code.")

    raw_output = _strip_markdown(content)

    # Validate AST before returning — reject dangerous code immediately
    validation_error = validate_code(raw_output)
    if validation_error:
        raise ValueError(f"Generated code failed security validation: {validation_error}")

    logger.info("Generated + validated code: %d chars.", len(raw_output))
    return raw_output


def generate_vetted_extraction_logic(
    identifier_json: dict,
    first_page_text: str,
    text_sample: str,
) -> str:
    """
    Self-healing orchestrator:
    1. Generates Ground Truth for Page 1 using LLM Parser.
    2. Generates Code.
    3. Run + Compare + Refine (max 2 retries).
    """
    logger.info("═══ VETTED GENERATION START ═══")
    
    # ── STEP A: GROUND TRUTH (PAGE 1) ──────────────────────────────────────────
    logger.info("[VETTING] Extracting ground truth for Page 1...")
    try:
        # We pass only the first page to parse_with_llm to keep it fast
        gt_response = parse_with_llm(first_page_text, identifier_json)
        ground_truth = extract_json_from_response(gt_response)
        
        if not ground_truth:
            logger.warning("[VETTING] Ground truth extraction returned 0. Vetting will be minimal.")
    except Exception as e:
        logger.error("[VETTING] Ground truth extraction FAILED: %s. Skipping vetting.", e)
        ground_truth = []

    # ── STEP B: INITIAL GENERATION ─────────────────────────────────────────────
    current_code = generate_extraction_logic_llm(identifier_json, text_sample)
    
    if not ground_truth:
        logger.info("[VETTING] No ground truth available. Returning raw generated code.")
        return current_code

    # ── STEP C: THE REPAIR LOOP ────────────────────────────────────────────────
    max_retries = 2
    for attempt in range(max_retries):
        logger.info("[VETTING] Test run: Attempt %d/%d", attempt + 1, max_retries + 1)
        
        try:
            code_results = extract_transactions_using_logic(first_page_text, current_code)
            
            # Compare count and data
            metrics = validate_transactions(code_results, ground_truth)
            accuracy = metrics.get("overall_accuracy", 0) if metrics else 0
            count_match = len(code_results) == len(ground_truth)
            too_long = any(len(str(r.get("details", ""))) > 100 for r in code_results)
            
            if accuracy >= 95 and count_match:
                logger.info("[VETTING] SUCCESS: Code matches ground truth (%.2f%% accuracy).", accuracy)
                return current_code
            
            # ── STEP D: REFINEMENT IF FAILED (QC-GRADE) ──────────────────────
            # Calculate diagnosis
            matched_count = metrics.get("matches", 0)
            diagnosis = f"Mismatches detected. Matched {matched_count}/{len(ground_truth)} rows. Accuracy: {accuracy:.1f}%"
            if len(code_results) == 0:
                diagnosis = f"CRITICAL: Code extracted ZERO transactions while LLM found {len(ground_truth)}."

            logger.warning("[VETTING] %s", diagnosis)
            
            # Build QC-style feedback block
            feedback_lines = [f"DIAGNOSIS: {diagnosis}\n"]
            
            # Map examples for the LLM
            line_examples = _build_line_examples(ground_truth, first_page_text)

            feedback = f"""
### VETTING FAILURE REPORT (QC-GRADE) ###
{diagnosis}

GROUND TRUTH (CORRECT REFERENCE):
{json.dumps(ground_truth, indent=2)}

YOUR PREVIOUS CODE'S OUTPUT (INCORRECT):
{json.dumps(code_results, indent=2)}

CONCRETE PATTERN EXAMPLES (Search for these lines in the Raw Text below):
{line_examples}

STRATEGY HINT:
In many Indian credit card PDFs, each transaction line has multiple numeric values. Usually:
- Right-most number is the CLOSING BALANCE.
- Second to right-most is the TRANSACTION AMOUNT.
Use this 'Anchor' logic to correctly identify the amount and avoid sidebar noise.

Please analyze the difference and update the logic to match exactly.
"""
            current_code = refine_extraction_logic_llm(current_code, feedback, text_sample)
            
        except Exception as e:
            logger.error("[VETTING] Execution error during vetting: %s. Attempting refinement...", e)
            error_feedback = f"The code you generated crashed during execution: {str(e)}"
            current_code = refine_extraction_logic_llm(current_code, error_feedback, text_sample)

    logger.info("[VETTING] Retries exhausted. Returning best available code.")
    return current_code


def vet_and_repair_existing_logic(
    statement_id: int,
    stored_code: str,
    identifier_json: dict,
    first_page_text: str,
    text_sample: str,
) -> str:
    """
    Vets existing UNDER_REVIEW code against Page 1 Ground Truth.
    If it fails, repairs it immediately and updates the DB.
    Returns the (potentially repaired) code.
    """
    logger.info("═══ EXISTING CODE VETTING START (statement_id=%s) ═══", statement_id)
    
    # ── STEP A: GROUND TRUTH (PAGE 1) ──────────────────────────────────────────
    logger.info("[VETTING] Extracting ground truth for Page 1...")
    try:
        gt_response = parse_with_llm(first_page_text, identifier_json)
        ground_truth = extract_json_from_response(gt_response)
        if not ground_truth:
             logger.info("[VETTING] No ground truth found on Page 1. Skipping repair.")
             return stored_code
    except Exception as e:
        logger.error("[VETTING] Ground truth failed: %s. Skipping repair.", e)
        return stored_code

    # ── STEP B & C: THE REPAIR LOOP ──────────────────────────────────────────
    current_code = stored_code
    max_retries = 2
    
    for attempt in range(max_retries + 1):
        try:
            logger.info("[VETTING] Test run: Attempt %d/%d", attempt + 1, max_retries + 1)
            code_results = extract_transactions_using_logic(first_page_text, current_code)
            
            # Compare count and data
            metrics = validate_transactions(code_results, ground_truth)
            accuracy = metrics.get("overall_accuracy", 0) if metrics else 0
            count_match = len(code_results) == len(ground_truth)
            
            # Pollution check
            avg_code_len = sum(len(str(t.get("details", ""))) for t in code_results) / len(code_results) if code_results else 0
            avg_gt_len   = sum(len(str(t.get("details", ""))) for t in ground_truth) / len(ground_truth) if ground_truth else 0
            too_long     = avg_code_len > (avg_gt_len * 2) and avg_code_len > 40
            
            if accuracy >= 95 and count_match and not too_long:
                logger.info("[VETTING] SUCCESS: Stored code is accurate (%.2f%% accuracy).", accuracy)
                if attempt > 0:
                    logger.info("[VETTING] REPAIR SUCCESS: Updating stored code in DB.")
                    from repository.statement_category_repo import update_extraction_logic
                    update_extraction_logic(statement_id, current_code)
                return current_code
            
            # ── TRIGGER REPAIR ──
            if attempt == max_retries:
                 logger.warning("[VETTING] Retries exhausted. Stored logic remains suboptimal.")
                 return current_code

            logger.warning("[VETTING] MISMATCH DETECTED: Attempt %d failed. Triggering repair...", attempt + 1)
            
            feedback = f"""
### REPAIR ATTEMPT {attempt + 1} FAILED ###
The existing parser logic does not match the Ground Truth for this specific document.

GROUND TRUTH (CORRECT DATA):
{json.dumps(ground_truth, indent=2)}

YOUR PREVIOUS CODE'S OUTPUT (INCORRECT DATA):
{json.dumps(code_results, indent=2)}

Please analyze the difference, review the raw text sample, and improve the logic code to be more dynamic and accurate.
"""
            current_code = refine_extraction_logic_llm(current_code, feedback, text_sample)
            
        except Exception as e:
            logger.error("[VETTING] Error during vetting attempt %d: %s", attempt + 1, e)
            if attempt == max_retries: return current_code
            current_code = refine_extraction_logic_llm(current_code, f"Code crashed: {str(e)}", text_sample)

    return current_code


def refine_extraction_logic_llm(
    current_logic: str,
    user_feedback: str,
    text_sample: str,
) -> str:
    """
    Takes existing extraction code and user feedback, and uses LLM to 'fix' or 'refine' it.
    This is used during retries when a user provides specific notes about errors.
    """
    logger.info("Refining extraction logic based on user feedback: %s", user_feedback)

    prompt = f"""

════════════════════════════════════════════
1. THE BUG REPORT (Specific failures)
════════════════════════════════════════════
{user_feedback}

════════════════════════════════════════════
2. THE CURRENT CODE (To be fixed)
════════════════════════════════════════════
{current_logic}

════════════════════════════════════════════
3. SAMPLE DOCUMENT TEXT (Reference)
════════════════════════════════════════════
{text_sample[:25000]}

════════════════════════════════════════════

1. **Transaction Starter**: A transaction ALWAYS starts with a date pattern (e.g., DD/MM/YY). 
2. **Multi-line Narration**: Once a transaction is detected, keep accumulating all text until NOTHING remains or the NEXT transaction date is found. Do NOT strip narration after the first line.
3. **Column Logic**: 
   - Identify the 'Withdrawal' and 'Deposit' columns correctly. 
   - Skip 'Chq/Ref No' and 'Value Date' columns—these are noise.
   - If the logic is capturing 03 or 21 from a date like 03/07/21 into an amount field, YOUR REGEX IS WRONG. Correct it.
4. **Data Cleaning**: 
   - Strip 'Chq./Ref.No.' digit strings from the 'details' field. 
   - Details must only contain the narration.
5. **Output Schema**: Return a list of dicts with:
   {{
     "date": "YYYY-MM-DD",
     "details": str,
     "debit": float|None,
     "credit": float|None,
     "balance": float|None,
     "confidence": float
   }}
6. **Generality**: Write code that handles the entire format dynamically. Do not hard-code row numbers. 

Return ONLY the code block. No explanation. 
"""


    code_gen_client = get_code_gen_client()
    content = code_gen_client.generate(prompt, max_retries=2)

    if not content:
        raise ValueError("LLM returned empty refinement.")

    fixed_code = _strip_markdown(content)
    
    # Validate
    val_err = validate_code(fixed_code)
    if val_err:
        raise ValueError(f"Refined code failed security validation: {val_err}")

    return fixed_code



# ═══════════════════════════════════════════════════════════
# EXECUTE EXTRACTION CODE
# ═══════════════════════════════════════════════════════════

def extract_transactions_using_logic(
    full_text: str,
    extraction_code: str,
) -> List[Dict]:
    """
    Execute LLM-generated Python code safely via code_sandbox,
    Returns cleaned transaction list.
    """
    try:
        # Gap 3 fix: use code_sandbox (AST-validated exec) not raw exec
        raw_transactions = execute_extraction_code(extraction_code, full_text)

        if not isinstance(raw_transactions, list):
            raise ValueError(
                f"Extraction returned {type(raw_transactions)}, expected List[Dict]."
            )
        
        logger.info("Code extraction success: %d transactions.", len(raw_transactions))
        return raw_transactions

    except Exception as e:
        logger.error("Code extraction failed: %s", e)
        raise RuntimeError(f"LLM extraction execution failed: {e}")


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def _strip_markdown(content: str) -> str:
    """
    Extract only the Python function from LLM output.

    Handles three output shapes:
      1. Wrapped in ```python ... ``` fences
      2. Step 1 analysis prose followed by bare function (two-step prompt output)
      3. Bare function only (old prompt style)

    In all cases returns only the text starting from
    'def extract_transactions' to end of output.
    """
    raw = content.strip()

    # Case 1 — markdown fences present: pull the block containing the function
    if "```" in raw:
        parts = raw.split("```")
        for part in parts:
            if "def extract_transactions" in part:
                raw = part.strip()
                if raw.lower().startswith("python"):
                    raw = raw[6:].strip()
                break

    # Cases 2 & 3 — find where the function starts and discard everything before it
    # This handles Step 1 prose sitting above the function
    fn_marker = "def extract_transactions"
    idx = raw.find(fn_marker)
    if idx > 0:
        # Content before function (Step 1 analysis) — strip it
        raw = raw[idx:]
    elif idx == -1:
        # Function not found — return as-is and let exec() raise a clear error
        logger.warning("_strip_markdown: 'def extract_transactions' not found in LLM output.")

    return raw.strip()