import re
import json
import logging
from typing import Dict, List, Optional

from config import CLASSIFIER_MODEL
from services.llm_provider import call_llm
from repository.statement_category_repo import (
    get_all_matchable_formats,
    insert_statement_category,
)

logger = logging.getLogger("ledgerai.identifier_service")


# ════════════════════════════════════════════════════════════
# INSTITUTION NAME NORMALISATION  [UNTOUCHED]
# ════════════════════════════════════════════════════════════
_LEGAL_SUFFIX_RE = re.compile(
    r"\s*,?\s*\b(limited|ltd\.?|pvt\.?|private)\s*$",
    re.IGNORECASE,
)


def normalise_institution_name(raw: str) -> str:
    """
    Strip trailing legal registration suffixes and handle common prefixes.
    Returns UPPERCASE.
    Example: "The Federal Bank Ltd." -> "FEDERAL BANK"
    """
    if not raw or not raw.strip():
        return "UNKNOWN"
    
    name = raw.strip().upper()
    
    # Strip common leading articles like "THE "
    if name.startswith("THE "):
        name = name[4:].strip()
        
    # Strip trailing legal registration suffixes (Limited, Ltd, etc.)
    prev = None
    while prev != name:
        prev = name
        # We use the regex on the upper-cased name
        name = _LEGAL_SUFFIX_RE.sub("", name).strip().rstrip(",").strip()
        
    return name


# ════════════════════════════════════════════════════════════
# FIRST N PAGES EXTRACTION
# Own function — does not reuse any existing service function
# ════════════════════════════════════════════════════════════

def _get_first_pages_text(pages: List[str], max_pages: int = 3) -> str:
    """
    Concatenate text from the first `max_pages` pages only.
    Adds a lightweight page marker so the LLM can orient itself.

    Sending only the first 2-3 pages to the LLM keeps token usage low
    while still capturing all structural signals (title, column headers,
    account markers, regulatory IDs) that appear at the top of a statement.
    """
    chunks = []
    for i, page_text in enumerate(pages[:max_pages], start=1):
        text = page_text.strip()
        if text:
            chunks.append(f"--- PAGE {i} ---\n{text}")
    return "\n\n".join(chunks)


# ════════════════════════════════════════════════════════════
# FORMAT EXISTENCE CHECK
# Own function — does not reuse any existing service function
# ════════════════════════════════════════════════════════════

def _get_table_columns_fingerprint(identifier_json: dict) -> str:
    """
    Build a stable, order-independent fingerprint from the
    table_header_markers inside the identification JSON.

    Same columns regardless of order → same fingerprint → dedup hit.
    Different columns → different fingerprint → treat as a new format.

    Example:
      ["Date", "Particulars", "Debit", "Credit", "Balance"]
      → "balance|credit|date|debit|particulars"
    """
    headers = (
        identifier_json
        .get("identity_markers", {})
        .get("transaction_table_identity", {})
        .get("table_header_markers", [])
    )
    normalised = sorted(
        re.sub(r"\s+", "", h.lower())
        for h in headers
        if isinstance(h, str) and h.strip()
    )
    return "|".join(normalised)


def _format_name_token_similarity(name_a: str, name_b: str) -> float:
    """
    Jaccard token-overlap similarity between two format name strings.
    Strips the trailing version suffix (_V\\d+), lowercases, and splits
    on underscores / spaces before comparing.

    Returns 0.0–1.0.  1.0 = identical token sets.

    Why this prevents the SAVINGS vs INSTANTSAVINGS false-dedup bug:
      "BANK_STATEMENT_HDFC_SAVINGS_V1"
        vs "BANK_STATEMENT_HDFC_INSTANTSAVINGS_V1"
        tokens_a = {bank, statement, hdfc, savings}
        tokens_b = {bank, statement, hdfc, instantsavings}
        intersection = {bank, statement, hdfc}   → 3
        union        = {bank, statement, hdfc, savings, instantsavings} → 5
        similarity   = 3/5 = 0.60  → below default threshold 0.65 → NOT a match

      "BANK_STATEMENT_HDFC_SAVINGS_V1"
        vs "BANK_STATEMENT_HDFC_SAVINGS_V1"
        similarity = 1.0  → match
    """
    def _tokenise(name: str) -> set:
        s = re.sub(r"[_\s]v\d+$", "", name.strip(), flags=re.IGNORECASE)
        return set(re.split(r"[_\s]+", s.lower())) - {""}

    tokens_a = _tokenise(name_a)
    tokens_b = _tokenise(name_b)
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union        = tokens_a | tokens_b
    return len(intersection) / len(union)


def check_format_exists(
    new_identifier_json: dict,
    format_name_similarity_threshold: float = 0.65,
) -> Optional[Dict]:
    """
    Check whether a format equivalent to `new_identifier_json` already
    exists in the statement_categories table.

    All THREE criteria must match for a dedup hit:
      1. institution_name  — normalised exact match
      2. format_name       — Jaccard token similarity
                             >= format_name_similarity_threshold
      3. table columns     — column fingerprint exact match

    Returns the matching statement_categories row dict, or None.

    Does NOT call find_existing_identifier or any other service function.
    Reads the DB directly via get_all_matchable_formats.
    """
    # Normalise institution name from the new document
    raw_inst      = new_identifier_json.get("institution_name") or ""
    new_norm_inst = normalise_institution_name(raw_inst)

    if not new_norm_inst or new_norm_inst == "UNKNOWN":
        logger.debug("check_format_exists: institution unknown — skip")
        return None

    # Column fingerprint for the new document
    new_col_fp = _get_table_columns_fingerprint(new_identifier_json)
    if not new_col_fp:
        logger.debug("check_format_exists: no table columns in new doc — skip")
        return None

    # Format name from the LLM-generated "id" field
    new_format_name = new_identifier_json.get("id") or ""

    # Fetch all stored formats from DB
    try:
        all_rows: List[Dict] = get_all_matchable_formats()
    except Exception as exc:
        logger.warning("check_format_exists: DB fetch failed — %s", exc)
        return None

    for row in all_rows:

        # ── 1. Institution match ──────────────────────────────────────────────
        stored_raw_inst  = row.get("institution_name") or ""
        stored_norm_inst = normalise_institution_name(stored_raw_inst)
        if stored_norm_inst != new_norm_inst:
            continue

        # ── 2. Format name similarity ─────────────────────────────────────────
        stored_format_name = row.get("format_name") or ""
        sim = _format_name_token_similarity(stored_format_name, new_format_name)
        if sim < format_name_similarity_threshold:
            logger.debug(
                "check_format_exists: fmt_sim=%.2f < %.2f "
                "('%s' vs '%s') — skip",
                sim, format_name_similarity_threshold,
                stored_format_name, new_format_name,
            )
            continue

        # ── 3. Table column fingerprint ───────────────────────────────────────
        stored_id_json = row.get("statement_identifier", {})
        if isinstance(stored_id_json, str):
            try:
                stored_id_json = json.loads(stored_id_json)
            except Exception:
                continue

        stored_col_fp = _get_table_columns_fingerprint(stored_id_json)
        if stored_col_fp != new_col_fp:
            logger.debug(
                "check_format_exists: column mismatch for statement_id=%s "
                "(stored='%s' | new='%s') — skip",
                row.get("statement_id"), stored_col_fp, new_col_fp,
            )
            continue

        # All three matched
        logger.info(
            "check_format_exists: HIT — statement_id=%s "
            "(institution='%s'  fmt_sim=%.2f  columns='%s')",
            row.get("statement_id"), new_norm_inst, sim, new_col_fp,
        )
        return row

    logger.info(
        "check_format_exists: no match for institution='%s' format='%s'",
        new_norm_inst, new_format_name,
    )
    return None


# ════════════════════════════════════════════════════════════
# CLASSIFY DOCUMENT — GENERATE IDENTIFICATION JSON  (LLM)
# ════════════════════════════════════════════════════════════

def classify_document_llm(pages: List[str]) -> Dict:
    """
    Generate the identification marker JSON for a new document.

    The identification prompt is defined inline as a local variable.
    Sends only the first 2-3 pages to the LLM to conserve tokens —
    structural signals (title, column headers, account/entity patterns)
    are always present within the first pages of a financial statement.

    Args:
        pages: Per-page text list produced by the page-split logic in
               processing_engine.py.

    Returns:
        Parsed identification JSON dict with institution_name normalised.
    """
    # ── Build page text (first 2-3 pages only) ───────────────────────────────
    first_pages_text = _get_first_pages_text(pages, max_pages=3)

    prompt = f"""
You are a financial document structure analyst. Your task is to analyze a financial statement PDF and generate a comprehensive identification marker JSON that captures all unique structural, textual, and formatting patterns that distinguish this specific statement type.

══════════════════════════════════════════════════════════════════════════════
ANALYSIS WORKFLOW
══════════════════════════════════════════════════════════════════════════════

STEP 1: DOCUMENT CLASSIFICATION
- Identify the document family: BANK_STATEMENT | CREDIT_CARD | WALLET | LOAN | INVESTMENT | INSURANCE | TAX | OTHER
- Identify the document subtype: Savings, Current, Platinum Card, Gold Card, Mutual Fund, Demat, etc.
- Identify the issuing institution name
- Assign confidence score (0.0-1.0) based on clarity of identification

STEP 2: EXTRACT ISSUER IDENTITY MARKERS
- Bank/Institution name patterns (exact strings that appear)
- Regulatory identifiers:
  * IFSC code pattern (if bank statement)
  * SWIFT code pattern (if applicable)
  * IBAN pattern (if applicable)
  * GSTIN (if applicable)
  * Any other regulatory IDs visible

STEP 3: EXTRACT DOCUMENT STRUCTURE IDENTITY
- Document title phrase (exact text, e.g., "ACCOUNT STATEMENT", "CREDIT CARD STATEMENT")
- Document reference number pattern (statement number, reference ID format)
- Generation phrase patterns (e.g., "Generated on", "Statement Date")

STEP 4: EXTRACT PERIOD IDENTITY MARKERS
- Statement period format (e.g., "01-Jan-2024 to 31-Jan-2024")
- Statement date format
- Billing cycle patterns (for credit cards)
- Tax period patterns (for investment/tax statements)

STEP 5: EXTRACT ENTITY IDENTITY MARKERS
Capture regex patterns for:
- Account number (full or masked format)
- Card number (masked, e.g., XXXX XXXX XXXX 1234)
- Loan account number
- Customer ID / CIF number
- Wallet ID (for payment wallets)
- Merchant ID (if applicable)
- PAN number
- BO ID / DP ID (for demat/investment accounts)

STEP 6: EXTRACT TRANSACTION TABLE IDENTITY
- List ALL column headers exactly as they appear (e.g., ["Date", "Description", "Debit", "Credit", "Balance"])
- Count minimum columns in transaction table
- Note if running balance column exists (true/false)
- Note if debit/credit style is used vs. single amount column (true/false)

STEP 7: EXTRACT FINANCIAL SUMMARY IDENTITY
Capture regex patterns that extract:
- Total outstanding amount (credit cards)
- Minimum amount due
- EMI amount (for loans)
- Credit limit (for credit cards/overdraft)
- Drawing power (for overdraft accounts)
- Portfolio value (for investment accounts)
- Total tax (for tax statements)

STEP 8: EXTRACT FOOTER IDENTITY
- List footer text patterns that consistently appear (disclaimers, contact info, etc.)

STEP 9: DEFINE EXCLUSION MARKERS
List patterns that should EXCLUDE lines from being treated as transactions:
- Page headers/footers (e.g., "Page 1 of 5")
- Section headers (e.g., "Transaction Details", "Summary")
- Disclaimer text
- Total/subtotal lines (e.g., "Total Debits", "Closing Balance")
- Empty or separator lines

STEP 10: DEFINE PARSING HINTS
- layout_type: SINGLE_COLUMN | TWO_COLUMN_PDF | MULTI_SECTION
- summary_section_labels: Labels that mark summary lines, not transactions (e.g., ["Opening Balance", "Closing Balance", "Total Credits"])
- transaction_boundary_signals: Signals that mark start of transaction (typically ["DATE"])
- ref_no_pattern: Regex to match and strip ONLY the raw reference code/number from descriptions. CRITICAL: Do NOT include words like "Ref No" or "ID" in the pattern unless they are truly noise—prefer keeping descriptive labels.
- page_break_pattern: Pattern for page numbering (e.g., "Page \\\\d+ of \\\\d+")
- details_strip_patterns: Regex patterns to remove raw alphanumeric ID hashes or long reference numbers (e.g., 12-digit UPI refs) from narrations. CRITICAL: NEVER include words like "UPI", "Ref No", "NEFT", "RTGS", "IMPS", or "ID" in these patterns. ONLY target the variable numbers/codes, preserving the descriptive labels.
- known_summary_amounts: Exact amount strings that are summary values, never transactions

══════════════════════════════════════════════════════════════════════════════
STATEMENT ID VERSIONING RULE
══════════════════════════════════════════════════════════════════════════════
ID format: [document_family]_[institution_abbreviation]_[document_subtype]_V[version_number]

Examples:
- BANK_STATEMENT_HDFC_SAVINGS_V1
- CREDIT_CARD_ICICI_PLATINUM_V1
- WALLET_PAYTM_MAIN_V1
- LOAN_SBI_HOME_V1
- INVESTMENT_ZERODHA_DEMAT_V1

══════════════════════════════════════════════════════════════════════════════
REGEX PATTERN RULES
══════════════════════════════════════════════════════════════════════════════
- Use Python regex syntax
- Escape special characters properly (\\\\d, \\\\s, \\\\., etc.)
- Make patterns specific but flexible enough to handle minor variations
- Use named groups where helpful: (?P<account>\\\\d{{10,16}})
- For dates, match actual format seen (e.g., "\\\\d{{2}}-[A-Z][a-z]{{2}}-\\\\d{{4}}" for "01-Jan-2024")
- For amounts, match format with commas/decimals: "[\\\\d,]+\\\\.\\\\d{{2}}"
- Return null if a field is not applicable to this statement type

══════════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT
══════════════════════════════════════════════════════════════════════════════
Return ONLY valid JSON matching this exact structure:

{{
  "id": "[document_family]_[institution]_[subtype]_V1",
  "document_family": "BANK_STATEMENT|CREDIT_CARD|WALLET|LOAN|INVESTMENT|INSURANCE|TAX|OTHER",
  "document_subtype": "<e.g., Savings, Current, Platinum Card>",
  "institution_name": "<detected institution>",
  "country": "India",
  "confidence_score": 0.95,

  "exclusion_markers": {{
    "patterns": ["pattern1", "pattern2", "..."]
  }},

  "parsing_hints": {{
    "layout_type": "SINGLE_COLUMN|TWO_COLUMN_PDF|MULTI_SECTION",
    "summary_section_labels": ["label1", "label2"],
    "transaction_boundary_signals": ["DATE"],
    "ref_no_pattern": "<regex or null>",
    "page_break_pattern": "Page \\\\d+ of \\\\d+",
    "details_strip_patterns": ["pattern1", "pattern2"],
    "known_summary_amounts": ["amount1", "amount2"]
  }},

  "identity_markers": {{
    "issuer_identity": {{
      "issuer_name": {{ "rule": "keyword", "patterns": ["exact name"] }},
      "regulatory_identifiers": {{
        "ifsc": {{ "rule": "regex", "pattern": "<regex or null>" }},
        "swift": {{ "rule": "regex", "pattern": "<regex or null>" }},
        "iban": {{ "rule": "regex", "pattern": "<regex or null>" }},
        "gstin": {{ "rule": "regex", "pattern": "<regex or null>" }},
        "other": []
      }}
    }},
    "document_structure_identity": {{
      "document_title_phrase": {{ "rule": "keyword", "patterns": ["EXACT TITLE"] }},
      "document_reference_number": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "generation_phrase": {{ "rule": "keyword", "patterns": ["Generated on", "Statement Date"] }}
    }},
    "period_identity": {{
      "statement_period": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "statement_date": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "billing_cycle": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "tax_period": {{ "rule": "regex", "pattern": "<regex or null>" }}
    }},
    "entity_identity": {{
      "account_number": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "masked_card_number": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "loan_account_number": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "customer_id": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "wallet_id": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "merchant_id": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "pan": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "bo_id": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "dp_id": {{ "rule": "regex", "pattern": "<regex or null>" }}
    }},
    "transaction_table_identity": {{
      "table_header_markers": ["Column1", "Column2", "Column3"],
      "minimum_column_count": 4,
      "presence_of_running_balance": true,
      "debit_credit_style": true
    }},
    "financial_summary_identity": {{
      "total_outstanding": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "minimum_due": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "emi_amount": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "credit_limit": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "drawing_power": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "portfolio_value": {{ "rule": "regex", "pattern": "<regex or null>" }},
      "total_tax": {{ "rule": "regex", "pattern": "<regex or null>" }}
    }},
    "footer_identity": {{
      "footer_markers": ["footer text pattern 1", "footer text pattern 2"]
    }}
  }}
}}

══════════════════════════════════════════════════════════════════════════════
CRITICAL OUTPUT RULES
══════════════════════════════════════════════════════════════════════════════
✓ Return ONLY the JSON object
✓ No markdown code blocks (no ```json```)
✓ No explanations before or after
✓ No comments in the JSON
✓ All regex patterns must use double backslashes (\\\\d not \\d)
✓ Set null for fields not applicable to the document type
✓ confidence_score must be between 0.0 and 1.0

BEGIN ANALYSIS OF THE PROVIDED FINANCIAL STATEMENT NOW.

Analyze this financial statement and generate identification markers:

{first_pages_text}
"""

    raw = call_llm(
        prompt=prompt,
        model=CLASSIFIER_MODEL,
        temperature=0
    )

    # ── Clean and parse the LLM JSON response ────────────────────────────────
    def _clean_json(s: str) -> str:
        s = re.sub(r"```(?:json)?", "", s).strip()   # strip markdown fences
        start = s.find("{")
        end   = s.rfind("}")
        if start != -1 and end != -1:
            s = s[start:end + 1]
        s = re.sub(r",\s*([\]}])", r"\1", s)          # trailing commas
        s = re.sub(r":\s*True\b",  ": true",  s)      # Python bool → JSON bool
        s = re.sub(r":\s*False\b", ": false", s)
        s = re.sub(r":\s*None\b",  ": null",  s)
        if s.count("{") > s.count("}"):                # auto-close open braces
            s += "}" * (s.count("{") - s.count("}"))
        return s

    try:
        identifier = json.loads(_clean_json(raw))
    except Exception as e:
        logger.error(
            "classify_document_llm: JSON parse failed — %s | raw_preview=%s",
            e, raw[:500],
        )
        m = re.search(r"(\{.*\})", raw, re.DOTALL)
        if m:
            try:
                identifier = json.loads(_clean_json(m.group(1)))
            except Exception:
                raise ValueError(f"LLM returned invalid JSON: {e}")
        else:
            raise ValueError(f"LLM returned no JSON-like content: {e}")

    # ── Ensure parsing_hints exists with safe defaults ────────────────────────
    if "parsing_hints" not in identifier:
        logger.warning("classify_document_llm: parsing_hints missing — injecting defaults")
        identifier["parsing_hints"] = {
            "layout_type":                  "SINGLE_COLUMN",
            "summary_section_labels":       [],
            "transaction_boundary_signals": ["DATE"],
            "ref_no_pattern":               None,
            "page_break_pattern":           r"Page \d+ of \d+",
            "details_strip_patterns":       [],
            "known_summary_amounts":        [],
        }
    else:
        ph = identifier["parsing_hints"]
        ph.setdefault("layout_type",                  "SINGLE_COLUMN")
        ph.setdefault("summary_section_labels",       [])
        ph.setdefault("transaction_boundary_signals", ["DATE"])
        ph.setdefault("ref_no_pattern",               None)
        ph.setdefault("page_break_pattern",           r"Page \d+ of \d+")
        ph.setdefault("details_strip_patterns",       [])
        ph.setdefault("known_summary_amounts",        [])

    # ── Normalise institution_name ────────────────────────────────────────────
    raw_inst = identifier.get("institution_name") or "Unknown"
    identifier["institution_name"] = normalise_institution_name(raw_inst)

    logger.info(
        "classify_document_llm: family=%s  institution=%s (raw=%r)  "
        "layout=%s  id=%s",
        identifier.get("document_family"),
        identifier.get("institution_name"),
        raw_inst,
        identifier.get("parsing_hints", {}).get("layout_type"),
        identifier.get("id"),
    )

    return identifier


# ════════════════════════════════════════════════════════════
# SAVE NEW FORMAT  [UNTOUCHED]
# ════════════════════════════════════════════════════════════

def derive_statement_type(identifier_json: dict) -> str:
    family   = identifier_json.get("document_family", "UNKNOWN")
    type_map = {
        "BANK_ACCOUNT_STATEMENT":          "BANK_STATEMENT",
        "CREDIT_CARD_STATEMENT":           "CREDIT_CARD",
        "LOAN_STATEMENT":                  "LOAN",
        "WALLET_STATEMENT":                "WALLET",
        "INVESTMENT_STATEMENT":            "INVESTMENT",
        "DEMAT_STATEMENT":                 "DEMAT",
        "TAX_LEDGER_STATEMENT":            "TAX_LEDGER",
        "PAYMENT_GATEWAY_SETTLEMENT":      "PAYMENT_GATEWAY",
        "OVERDRAFT_CASH_CREDIT_STATEMENT": "OD_CC",
    }
    return type_map.get(family, family)


def save_new_statement_format(
    format_name: str,
    identifier_json: dict,
    extraction_logic: str,
    threshold: float = 65.0,
) -> int:
    statement_type  = derive_statement_type(identifier_json)
    document_family = identifier_json.get("document_family", "UNKNOWN")

    # Always normalise before save — safety net for any caller that bypasses
    # classify_document_llm (e.g. tests or future code paths).
    raw_name         = identifier_json.get("institution_name") or "Unknown"
    institution_name = normalise_institution_name(raw_name)

    # Write normalised name back so the stored identifier_json is consistent
    # with the institution_name column value.
    identifier_json = {**identifier_json, "institution_name": institution_name}

    # ── Dedup guard ───────────────────────────────────────────────────────────
    existing = check_format_exists(identifier_json)
    if existing:
        logger.info(
            "save_new_statement_format: dedup hit — returning existing statement_id=%s "
            "(institution=%s  family=%s)",
            existing["statement_id"], institution_name, document_family,
        )
        return existing["statement_id"]

    # Extract IFSC from the identity markers if present
    # (the LLM stores it in identity_markers.issuer_identity.regulatory_identifiers.ifsc)
    ifsc_code = None
    try:
        ifsc_pattern = (
            identifier_json
            .get("identity_markers", {})
            .get("issuer_identity", {})
            .get("regulatory_identifiers", {})
            .get("ifsc", {})
            .get("pattern")
        )
        if ifsc_pattern:
            import re as _re
            # The pattern is a regex — extract the IFSC prefix (first 4 letters)
            m = _re.search(r"([A-Z]{4})", str(ifsc_pattern))
            if m:
                ifsc_code = m.group(1)
    except Exception:
        pass

    logger.info(
        "Saving NEW format: name=%s  type=%s  institution=%s  ifsc=%s",
        format_name, statement_type, institution_name, ifsc_code,
    )
    return insert_statement_category(
        statement_type=statement_type,
        format_name=format_name,
        institution_name=institution_name,
        identifier_json=identifier_json,
        extraction_logic=extraction_logic,
        ifsc_code=ifsc_code,
        threshold=threshold,
    )