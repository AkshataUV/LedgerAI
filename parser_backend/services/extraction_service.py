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
from datetime import datetime

from services.code_gen_client import get_code_gen_client
from services.prompts import get_prompt
from services.code_sandbox import execute_extraction_code, validate_code
from services.validation_service import validate_transactions, extract_json_from_response

logger = logging.getLogger("ledgerai.extraction_service")


def _build_line_examples(ground_truth: list, pdf_text: str) -> str:
    lines = pdf_text.splitlines()
    # Precompute which lines look like transaction starters (start with a date)
    date_starter_pattern = re.compile(r'^\s*\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b')
    
    examples = []
    header = (
        "IMPORTANT: For EACH entry below, the TARGET is the EXACT JSON your code must produce. "
        "The RAW LINES are the actual text from the PDF that corresponds to that transaction. "
        "Your code must parse these RAW LINES and produce EXACTLY the TARGET JSON.\n"
        "────────────────────────────────────────────────────────────────────\n"
    )
    
    for txn in ground_truth[:15]:
        date_str = txn.get("date", "") # YYYY-MM-DD
        amount = txn.get("debit") or txn.get("credit")
        if not date_str or amount is None: continue
            
        # Try to extract Day and Month for flexible searching
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            d, m = dt.strftime("%d"), dt.strftime("%m")
            b = dt.strftime("%b") # Short month name
        except:
            d, m, b = None, None, None

        amt_val = abs(float(amount))
        amt_str_2f = f"{amt_val:.2f}"
        amt_str_commas = f"{amt_val:,.2f}"
        
        anchor_line_idx = None
        for idx, line in enumerate(lines):
            # Strict amount matching: must be surrounded by spaces or separators to avoid matching "20" in "2026"
            has_amt = (
                f" {amt_str_2f} " in f" {line} " or 
                f" {amt_str_commas} " in f" {line} " or 
                f" {int(amt_val)} " in f" {line} "
            )
            has_date = False
            if d and m:
                # Matches 01/04, 01-04, 01 Apr, 01-Apr
                if (f"{d}/{m}" in line or f"{d}-{m}" in line or 
                    f"{d} {b}" in line or f"{d}-{b}" in line or
                    f"{m}/{d}" in line or f"{m}-{d}" in line):
                    has_date = True
            
            if has_amt and has_date:
                anchor_line_idx = idx
                break
        
        if anchor_line_idx is None:
            continue

        # Collect continuation lines (lines after anchor that don't start with a date)
        raw_block = [lines[anchor_line_idx].strip()]
        for next_idx in range(anchor_line_idx + 1, min(anchor_line_idx + 8, len(lines))):
            next_line = lines[next_idx].strip()
            if not next_line:
                continue
            if date_starter_pattern.match(lines[next_idx]):
                break  # Next transaction started
            raw_block.append(next_line)
        
        raw_block_str = "\n  ".join(raw_block)
        examples.append(
            f"TARGET (must produce exactly):\n  {json.dumps(txn)}\n"
            f"RAW PDF LINES (parse these):\n  {raw_block_str}\n"
        )
        
    if not examples:
        return "No explicit line-mappings could be determined."
    
    return header + "\n".join(examples)

def _is_code_accurate_enough(code_results: list, ground_truth: list, metrics: dict) -> bool:
    """
    Returns True only when the extraction code is considered accurate enough to stop refining.
    Conditions — ALL must be met:
      1. Count match: same number of transactions as ground truth
      2. Date accuracy  ≥ 95%  (dates are correct)
      3. Amount accuracy ≥ 95%  (debits/credits are correct)
      4. Description accuracy ≥ 80%  (narrations are mostly correct — some variation acceptable)
      5. Overall accuracy ≥ 95%
    """
    if not metrics:
        return False
    count_ok   = len(code_results) == len(ground_truth)
    date_ok    = metrics.get("date_accuracy", 0)   >= 95
    amount_ok  = metrics.get("amount_accuracy", 0) >= 95
    desc_ok    = metrics.get("description_accuracy", 0) >= 80
    overall_ok = metrics.get("overall_accuracy", 0) >= 95
    return count_ok and date_ok and amount_ok and desc_ok and overall_ok


def _analyze_mismatches_deep(code_results: list, ground_truth: list) -> str:
    """
    Perform a granular row-by-row comparison between Code and Ground Truth.
    Returns a string analysis for the LLM.
    """
    analysis = []
    from services.validation_service import calculate_similarity
    
    matched_gt = set()
    matched_code = set()
    
    # 1. First pass: Match Date + Amount to find row-level parity
    for gt_idx, gt in enumerate(ground_truth[:30]):
        gt_date = str(gt.get("date", ""))
        gt_amt = float(gt.get("debit") or gt.get("credit") or 0)
        
        for c_idx, code in enumerate(code_results):
            if c_idx in matched_code: continue
            
            c_date = str(code.get("date", ""))
            c_amt = float(code.get("debit") or code.get("credit") or 0)
            
            if gt_date == c_date and abs(gt_amt - c_amt) < 0.05:
                matched_gt.add(gt_idx)
                matched_code.add(c_idx)
                
                # Check for Description Pollution
                gt_desc = str(gt.get("details", ""))
                c_desc = str(code.get("details", ""))
                sim = calculate_similarity(gt_desc, c_desc)
                if sim < 0.6:
                    analysis.append(f"POLLUTION DISCREPANCY in Row {gt_idx+1}: "
                                   f"Your code extracted: \"{c_desc}\", "
                                   f"but Ground Truth says: \"{gt_desc}\". "
                                   f"You are capturing extra noise (times, ids, line numbers).")
                break

    # 2. Identify missing rows
    for gt_idx, gt in enumerate(ground_truth[:20]):
        if gt_idx not in matched_gt:
            analysis.append(f"MISSING IN CODE: Ground Truth row {gt_idx+1} [Date: {gt.get('date')}, Amt: {gt.get('debit') or gt.get('credit')}] was NOT extracted.")

    # 3. Identify extra noise rows
    for c_idx, code in enumerate(code_results[:20]):
        if c_idx not in matched_code:
            analysis.append(f"EXTRA/NOISE IN CODE: Your code captured Row {c_idx+1} [Date: {code.get('date')}, Amt: {code.get('debit') or code.get('credit')}] which is likely a header, footer or summary line.")

    return "\n".join(analysis) if analysis else "No specific row-level discrepancies found."


def _diagnose_code_bugs(code_results: list, ground_truth: list, pdf_text: str) -> str:
    """
    Performs prescriptive analysis of the code output vs ground truth to identify
    specific code-level bugs that need to be fixed — not just symptoms.
    """
    from services.validation_service import calculate_similarity
    bugs = []

    for gt_idx, gt in enumerate(ground_truth[:20]):
        gt_date = str(gt.get("date", ""))
        gt_debit = gt.get("debit")
        gt_credit = gt.get("credit")
        gt_amt = float(gt_debit or gt_credit or 0)
        gt_desc = str(gt.get("details", ""))

        for c_idx, code in enumerate(code_results):
            c_date = str(code.get("date", ""))
            c_debit = code.get("debit")
            c_credit = code.get("credit")
            c_amt = float(c_debit or c_credit or 0)
            c_desc = str(code.get("details", ""))

            if gt_date != c_date:
                continue
            if abs(gt_amt - c_amt) > 1.0 and abs(c_amt) > 0:
                continue  # different transaction, skip

            # BUG: Floating-point amount (computed from balance diff instead of parsed)
            if abs(gt_amt - c_amt) > 0.001 and abs(gt_amt - c_amt) < 5.0:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] FLOATING POINT AMOUNT: "
                    f"Your code returned {c_amt} but correct is {gt_amt}. "
                    f"You are COMPUTING the amount from balance differences. "
                    f"FIX: Parse the Withdrawal/Deposit amount DIRECTLY from the raw line using regex, do NOT compute it arithmetically."
                )

            # BUG: Wrong debit/credit column assignment
            if gt_debit is not None and c_credit is not None and c_debit is None:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] WRONG COLUMN: Amount {gt_amt} should be DEBIT but your code put it in CREDIT. "
                    f"FIX: Re-check your column index for Withdrawal vs Deposit. Withdrawal is DEBIT, Deposit is CREDIT."
                )
            elif gt_credit is not None and c_debit is not None and c_credit is None:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] WRONG COLUMN: Amount {gt_amt} should be CREDIT but your code put it in DEBIT. "
                    f"FIX: Re-check your column index for Withdrawal vs Deposit. Withdrawal is DEBIT, Deposit is CREDIT."
                )

            # BUG: Missing amount (null credit where credit is expected)
            if gt_credit is not None and c_credit is None and c_debit is None:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] MISSING AMOUNT: Row has credit={gt_credit} but your code returned null for both debit and credit. "
                    f"FIX: Your regex or column-split logic is failing to capture the Deposit/Credit column. "
                    f"Check the raw PDF line: the amount {gt_credit} appears in the Deposit column, NOT the Withdrawal column."
                )

            # BUG: Noise separators in details (e.g. '//', '\\n', extra whitespace)
            if '//' in c_desc or '\\n' in c_desc:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] NOISE IN DETAILS: Your code added '//' or literal newlines in description: '{c_desc[:80]}'. "
                    f"FIX: Remove any custom separator characters before storing details. "
                    f"Join multi-line narration with a space, not '//'."
                )

            # BUG: Truncated or garbled narration
            sim = calculate_similarity(gt_desc, c_desc)
            if sim < 0.55:
                bugs.append(
                    f"BUG [Row {gt_idx+1}] GARBLED NARRATION: "
                    f"Expected: '{gt_desc[:80]}' but got: '{c_desc[:80]}'. "
                    f"FIX: Your narration assembly logic is splitting at wrong positions. "
                    f"Continuation lines (those NOT starting with a date) must be appended to the PREVIOUS transaction's narration with a space."
                )

            break  # matched this code row, move to next gt row

    # BUG: Same code output repeated despite different feedback (detect stale code)
    seen_details = [str(c.get("details", ""))[:40] for c in code_results]
    if len(set(seen_details)) == len(seen_details) and len(bugs) > 2:
        bugs.append(
            "CRITICAL: Multiple bugs detected that persist across retries. "
            "Do NOT make incremental patches — REWRITE the extract_transactions function from scratch "
            "using the RAW PDF LINES shown above as your guide."
        )

    return "\n".join(bugs) if bugs else "No specific code bugs detected."






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





def refine_extraction_logic_llm(
    current_logic: str,
    mismatch_analysis: str,
    text_sample: str,
    ground_truth: list = None,
    first_page_text: str = None,
    statement_id: int = None,
) -> str:
    """
    Takes existing extraction code and user feedback, and uses LLM to 'fix' or 'refine' it.
    This is used during retries when a user provides specific notes about errors.
    """
    logger.info("[REFINE] Starting refinement (max 3 retries).")

    best_code = current_logic
    best_accuracy = 0.0
    
    # ── Initial Evaluation ──
    if ground_truth and first_page_text:
        try:
            init_results = extract_transactions_using_logic(first_page_text, current_logic)
            init_metrics = validate_transactions(init_results, ground_truth)
            best_accuracy = init_metrics.get("overall_accuracy", 0) if init_metrics else 0
            logger.info(
                "[REFINE] Initial: %d txns | acc=%.1f%% | date=%.1f%% | amt=%.1f%% | desc=%.1f%%",
                len(init_results),
                best_accuracy,
                (init_metrics or {}).get("date_accuracy", 0),
                (init_metrics or {}).get("amount_accuracy", 0),
                (init_metrics or {}).get("description_accuracy", 0),
            )

            if _is_code_accurate_enough(init_results, ground_truth, init_metrics):
                logger.info("[REFINE] ✓ Already accurate — skipping refinement.")
                return current_logic
        except Exception:
            pass

    current_mismatch = mismatch_analysis

    for attempt in range(1, 4):
        logger.info("[REFINE] LLM refinement — attempt %d/3 (best_acc: %.1f%%)", attempt, best_accuracy)

        # ── Build raw PDF context (first 2 pages sent to LLM every attempt) ──
        raw_text_section = first_page_text or text_sample or ""
        # Trim to first 8000 chars to stay within context limits
        if len(raw_text_section) > 8000:
            raw_text_section = raw_text_section[:8000] + "\n... [truncated]"

        # For 0-extraction case, the bug report already has raw text embedded.
        # For other cases we also show raw text in section 3.
        prompt = f"""
════════════════════════════════════════════
1. THE BUG REPORT (Specific failures)
════════════════════════════════════════════
{current_mismatch}

════════════════════════════════════════════
2. THE CURRENT CODE (To be fixed)
════════════════════════════════════════════
{best_code}

════════════════════════════════════════════
3. RAW PDF TEXT — Pages 1-2 (what the code receives as input)
════════════════════════════════════════════
This is the ACTUAL text your function will parse. Use this to understand
the exact format: date patterns, column separators, transaction boundaries.
\"\"\"
{raw_text_section}
\"\"\"

════════════════════════════════════════════

RULES:
1. **You are a code debugger, not just a code writer.** Read the BUG REPORT above VERY carefully. Each bug is a specific, actionable fix.
2. **Parse amounts DIRECTLY from the raw line** using regex. NEVER compute them from balance differences.
3. **Column identification**: Find the header row (containing 'Withdrawal', 'Deposit', 'Dr', 'Cr', 'Debit', 'Credit') and use POSITIONAL splitting to identify columns.
4. **Multi-line narration**: Lines that do NOT start with a date pattern are CONTINUATIONS of the previous transaction. Append them with a single space.
5. **Debit vs Credit**: If the raw line has only ONE numeric value before balance, check which column it belongs to.
6. **Details Cleaning**: Strip only pure numeric reference codes (16+ digit strings). KEEP all descriptive text.
7. **Date normalization**: PDF dates like '03/01/26' = 2026-01-03. Handle DD/MM/YY and DD/MM/YYYY.
8. **Output Schema**: Return a list of dicts with:
   {{
     "date": "YYYY-MM-DD",
     "details": str,
     "debit": float|None,
     "credit": float|None,
     "balance": float|None,
     "confidence": float
   }}
9. **Generality**: Code must work for ALL pages and ALL similar documents, not just this sample. Use format-level patterns, not hardcoded row numbers.

Return ONLY the fixed Python function. No explanation.
"""

        try:
            code_gen_client = get_code_gen_client()
            content = code_gen_client.generate(prompt, max_retries=2)

            if not content:
                logger.warning("[REFINE] LLM returned empty content on attempt %d.", attempt)
                continue

            candidate_code = _strip_markdown(content)
            val_err = validate_code(candidate_code)
            if val_err:
                logger.warning("[REFINE] Candidate code failed security validation on attempt %d: %s", attempt, val_err)
                continue

            # ── LIVE VALIDATION ──
            if ground_truth and first_page_text:
                try:
                    code_results = extract_transactions_using_logic(first_page_text, candidate_code)
                    metrics      = validate_transactions(code_results, ground_truth)
                    accuracy     = metrics.get("overall_accuracy", 0) if metrics else 0
                    count_match  = len(code_results) == len(ground_truth)
                    matched_count = (metrics or {}).get("matched_transactions", 0)

                    logger.info(
                        "[REFINE] Attempt %d result: %d extracted / %d expected | accuracy=%.1f%%",
                        attempt, len(code_results), len(ground_truth), accuracy,
                    )

                    # Update best version if accuracy improved
                    if accuracy > best_accuracy or (accuracy == best_accuracy and count_match and accuracy > 0):
                        logger.info(
                            "[REFINE] ✓ IMPROVEMENT: acc %.1f%% → %.1f%% | date=%.1f%% | amt=%.1f%% | desc=%.1f%%",
                            best_accuracy, accuracy,
                            (metrics or {}).get("date_accuracy", 0),
                            (metrics or {}).get("amount_accuracy", 0),
                            (metrics or {}).get("description_accuracy", 0),
                        )
                        best_code = candidate_code
                        best_accuracy = accuracy

                        if _is_code_accurate_enough(code_results, ground_truth, metrics):
                            logger.info("[REFINE] ✓ EARLY EXIT — all fields accurate. Stopping.")
                            if statement_id:
                                try:
                                    from repository.statement_category_repo import update_extraction_logic
                                    update_extraction_logic(statement_id, best_code)
                                except Exception: pass
                            return best_code
                    else:
                        logger.warning(
                            "[REFINE] ❌ REGRESSION: Candidate accuracy (%.1f%%) <= best (%.1f%%). Rolling back.",
                            accuracy, best_accuracy,
                        )

                    # ── Compute feedback components (always needed) ────────
                    raw_preview   = "\n".join(first_page_text.splitlines()[:60])
                    mismatch_rows = _analyze_mismatches_deep(code_results, ground_truth)
                    bug_rows      = _diagnose_code_bugs(code_results, ground_truth, first_page_text)

                    # Special case: code extracted NOTHING — show raw PDF so LLM can see the format
                    if len(code_results) == 0:
                        current_mismatch = f"""
### ATTEMPT {attempt} CRITICAL FAILURE — Your code extracted 0 transactions!

Your code returned an EMPTY list []. Look at the RAW PDF TEXT below and write code that parses it correctly.

RAW PDF TEXT (parse this to find the {len(ground_truth)} transactions):
\"\"\"
{raw_preview}
\"\"\"

EXPECTED TRANSACTIONS (you must extract exactly these):
{json.dumps(ground_truth[:20], indent=2)}

INSTRUCTIONS:
- Locate where the transaction table starts in the raw text
- Identify the date pattern, amount columns, and description column
- Write regex/string-split code to capture each transaction row
- Lines without a date pattern are CONTINUATIONS of the previous transaction
- STOP parsing at page footers (Total, Page X of Y, addresses, T&C)
"""
                    elif len(code_results) > len(ground_truth):
                        current_mismatch = f"""
### ATTEMPT {attempt} FAILED — Over-extracted: {len(code_results)} extracted / {len(ground_truth)} expected | Matched {matched_count} | Accuracy: {accuracy:.1f}%

Your code is extracting TOO MANY rows — it is capturing header/footer/summary lines as transactions.

PRESCRIPTIVE BUG DIAGNOSIS:
{bug_rows}

DEEP MISMATCH ANALYSIS:
{mismatch_rows}

RAW PDF TEXT (first 60 lines — check which lines are NOT transactions):
\"\"\"
{raw_preview}
\"\"\"

GROUND TRUTH (only these {len(ground_truth)} rows are real transactions):
{json.dumps(ground_truth[:30], indent=2)}

YOUR CODE'S OUTPUT (TOO MANY — contains noise rows):
{json.dumps(code_results[:30], indent=2)}
"""
                    else:
                        current_mismatch = f"""
### ATTEMPT {attempt} FAILED — {len(code_results)} extracted / {len(ground_truth)} expected | Matched {matched_count} | Accuracy: {accuracy:.1f}%

PRESCRIPTIVE BUG DIAGNOSIS:
{bug_rows}

DEEP MISMATCH ANALYSIS:
{mismatch_rows}

GROUND TRUTH (REFERENCE):
{json.dumps(ground_truth[:30], indent=2)}

YOUR CODE'S OUTPUT (INCORRECT):
{json.dumps(code_results[:30], indent=2)}
"""
                except Exception as exec_err:
                    logger.error("[REFINE] Code execution failed on attempt %d: %s", attempt, exec_err)
                    raw_preview = "\n".join(first_page_text.splitlines()[:60])
                    current_mismatch = f"""
ATTEMPT {attempt}: Code crashed with runtime error: {exec_err}

Fix this runtime error immediately. Your code must not crash.

RAW PDF TEXT (what your code will receive as input):
\"\"\"
{raw_preview}
\"\"\"

EXPECTED TRANSACTIONS:
{json.dumps(ground_truth[:20], indent=2)}
"""
            else:
                # No validation possible
                logger.info("[REFINE] No ground truth — returning refined code after first success.")
                return candidate_code # use the refined one if we can't test

        except Exception as e:
            logger.error("[REFINE] LLM call failed on attempt %d: %s", attempt, e)
            continue

    logger.info("[REFINE] All attempts complete. Returning best_code (acc: %.1f%%).", best_accuracy)
    if statement_id:
        try:
            from repository.statement_category_repo import update_extraction_logic
            update_extraction_logic(statement_id, best_code)
        except Exception: pass
    return best_code



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