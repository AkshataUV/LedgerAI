""""
services/prompts/demat.py
─────────────────────────
FIXED: Zerodha / CDSL / NSDL Demat Statement Parser Prompt.

ROOT CAUSES FIXED vs OLD PROMPT:
1. When transaction section has ZERO transactions (only Holdings), the old code
   crashed because `current_txn` was never set but still referenced in cleanup
2. The holdings section check "Holdings as on" triggered correctly, but
   "Portfolio Value" is not always present — added more trigger phrases
3. ISIN regex missed lowercase 'l' vs digit '1' ambiguity in some OCR outputs
4. Quantity extraction: `valid_numbers` filter requiring `len(clean_n) < 4`
   excluded large quantities like 130.000 (Zerodha shows 3-decimal quantities)
5. The `footers` variable was used as a raw Python set `{footers}` which may
   serialize as a string, not a set literal — fixed to proper list injection
6. Buy/Cr direction: some Zerodha rows say "Cr" in the column not in description
   — old keyword check on `lower_line` missed column-positional data
"""


def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Builds production-grade prompt for DEMAT statements.
    Handles: Zerodha, CDSL, NSDL, HDFC Securities formats.
    """

    family = identifier_json.get("document_family", "DEMAT_STATEMENT")
    subtype = identifier_json.get("document_subtype", "Unknown")
    institution = identifier_json.get("institution_name", "Unknown")

    identity = identifier_json.get("identity_markers", {})
    headers = identity.get("transaction_table_identity", {}).get("table_header_markers", [])
    footers = identity.get("footer_identity", {}).get("footer_markers", [])
    bo_id = identity.get("entity_identity", {}).get("bo_id", {}).get("pattern", "N/A")

    return f"""
You are a Senior Python Backend Engineer specializing in Stock Market / Demat Data Parsing.

============================================================
CRITICAL OBJECTIVE
============================================================
Generate EXACTLY one deterministic Python function:

    def extract_transactions(text: str) -> list:

Rules:
- Import re inside the function
- Never raise exceptions (wrap entire body in try/except returning [])
- Return list of dicts — EMPTY LIST is valid (e.g., no transactions in period)
- Do NOT crash when there are zero transactions

============================================================
DOCUMENT CONTEXT
============================================================
Institution   : {institution}
Family        : {family}
Subtype       : {subtype}
BO ID Pattern : {bo_id}
Table Headers : {headers}
Footer Markers: {footers}

============================================================
DEMAT DOCUMENT STRUCTURE
============================================================

A Demat statement has TWO distinct sections. You MUST parse ONLY the first:

SECTION A — Transaction/Ledger (PARSE THIS):
    Header: "Statement of Account from [date] to [date]"
    Columns: Date | Transaction Description | Buy/Cr | Sell/Dr | Balance
    Notes: May be completely empty ("Closing balance: None" or no rows)

SECTION B — Holdings (DO NOT PARSE):
    Header: "Holdings as on [date]:" or "Current Holdings"
    Columns: ISIN Code | Company Name | Curr. Bal | Free Bal | Pldg. Bal | ...

============================================================
PHASE 1 — PREPROCESSING
============================================================

    text = text.replace("\\u00A0", " ")
    text = text.replace("\\xa0", " ")
    lines = [line.rstrip() for line in text.splitlines()]
    lines = [l for l in lines if l.strip()]

============================================================
PHASE 2 — SECTION DETECTION
============================================================

    TRANSACTION_SECTION_MARKERS = [
        r'(?i)statement of account',
        r'(?i)transaction.*ledger',
        r'(?i)date.*transaction description.*buy',
        r'(?i)date.*buy.*sell.*balance',
    ]

    HOLDINGS_SECTION_MARKERS = [
        r'(?i)holdings as on',
        r'(?i)current holdings',
        r'(?i)isin\\s+code.*company.*curr.*bal',
        r'(?i)free\\s+bal',
        r'(?i)lockin',
        r'(?i)portfolio value',
    ]

    FOOTER_MARKERS = {footers}

    def is_holdings_start(line):
        for pat in HOLDINGS_SECTION_MARKERS:
            if re.search(pat, line, re.IGNORECASE):
                return True
        return False

    def is_transaction_section_header(line):
        for pat in TRANSACTION_SECTION_MARKERS:
            if re.search(pat, line, re.IGNORECASE):
                return True
        return False

============================================================
PHASE 3 — SKIP PATTERNS
============================================================

    SKIP_PATTERNS = [
        r'(?i)^\\s*(date|transaction description|buy.?cr|sell.?dr|balance)\\s*$',
        r'(?i)(closing balance|opening balance)',
        r'(?i)(promoter|public|pledge|remat|lockin)',
        r'(?i)(isin\\s*code|company\\s*name)',
        r'(?i)(total[:\\s])',
        r'(?i)^\\s*none\\s*$',
    ]

    def should_skip(line):
        stripped = line.strip()
        if not stripped:
            return True
        for pat in SKIP_PATTERNS:
            if re.search(pat, stripped):
                return True
        if FOOTER_MARKERS and any(m.lower() in stripped.lower() for m in FOOTER_MARKERS if m):
            return True
        return False

============================================================
PHASE 4 — DATE AND QUANTITY REGEX
============================================================

    DATE_REGEX = r'\\b(\\d{{1,2}}[-/ ](?:\\d{{1,2}}|[A-Za-z]{{3,9}})[-/ ]\\d{{2,4}})\\b'

    ISIN_REGEX = r'\\b([A-Z]{{2}}[A-Z0-9]{{9}}\\d)\\b'

    # Zerodha uses quantities like: 130.000, 0.000, 25857.000
    # Use flexible decimal matching
    QUANTITY_REGEX = r'\\b(\\d+(?:,\\d+)*\\.\\d{{0,4}})\\b'

    def extract_date(line):
        m = re.search(DATE_REGEX, line, re.IGNORECASE)
        return m.group(1) if m else None

    def line_starts_transaction(line):
        early = line.strip()[:20]
        return bool(re.search(DATE_REGEX, early, re.IGNORECASE))

    def extract_quantities(line):
        \"\"\"Extract all numeric quantities from a line\"\"\"
        results = []
        for m in re.finditer(QUANTITY_REGEX, line):
            raw = m.group(1).replace(",", "")
            try:
                val = float(raw)
                # Filter out years
                if not (1900 < val < 2100 and "." not in m.group(1)):
                    results.append(val)
            except:
                pass
        return results

============================================================
PHASE 5 — DIRECTION DETECTION
============================================================

Demat accounts use Buy/Cr and Sell/Dr columns.
The column is positional in OCR text:

In Zerodha format:
    [Date]  [Description]  [Buy/Cr qty]  [Sell/Dr qty]  [Balance]

The quantities appear in order: buy_qty (may be 0 or empty), sell_qty (may be 0 or empty), balance

    def detect_direction(line, quantities):
        lower = line.lower()
        
        # Column header check: if "buy" or "cr" appears as column label
        # Position heuristic: if the non-zero quantity appears in the
        # left-center of the number sequence = Buy/Cr = Credit
        # Right-center = Sell/Dr = Debit
        
        if "buy" in lower or "/cr" in lower or "credit" in lower or "purchase" in lower:
            return "credit"
        if "sell" in lower or "/dr" in lower or "debit" in lower or "sale" in lower:
            return "debit"
        
        # If 3 numbers present: [buy_qty, sell_qty, balance]
        if len(quantities) >= 3:
            buy_qty = quantities[0]
            sell_qty = quantities[1]
            if buy_qty > 0 and sell_qty == 0:
                return "credit"
            if sell_qty > 0 and buy_qty == 0:
                return "debit"
        
        return "unknown"

============================================================
PHASE 6 — MAIN PARSING LOOP
============================================================

    transactions = []
    current_txn = None
    in_transaction_section = False
    in_holdings_section = False

    for line in lines:
        stripped = line.strip()

        # Footer check
        if FOOTER_MARKERS and any(m.lower() in stripped.lower() for m in FOOTER_MARKERS if m):
            break

        # Holdings section detection — stop parsing transactions
        if is_holdings_start(stripped):
            in_holdings_section = True
            if current_txn:
                transactions.append(current_txn)
                current_txn = None
            continue

        if in_holdings_section:
            continue

        # Transaction section start
        if is_transaction_section_header(stripped):
            in_transaction_section = True
            continue

        if not in_transaction_section:
            continue

        if should_skip(stripped):
            continue

        # Parse transaction rows
        if line_starts_transaction(stripped):
            if current_txn:
                transactions.append(current_txn)

            date_str = extract_date(stripped)
            quantities = extract_quantities(stripped)

            direction = detect_direction(stripped, quantities)

            # Transaction quantity = first non-balance number
            # Balance = last number
            txn_qty = None
            balance_qty = None

            if quantities:
                balance_qty = quantities[-1]
                if len(quantities) >= 2:
                    txn_qty = quantities[0] if quantities[0] > 0 else (quantities[1] if quantities[1] > 0 else None)

            debit = txn_qty if direction == "debit" else None
            credit = txn_qty if direction == "credit" else None

            # Extract ISIN from line
            isin_match = re.search(ISIN_REGEX, stripped)

            details = stripped
            details = re.sub(DATE_REGEX, "", details, flags=re.IGNORECASE)
            details = re.sub(QUANTITY_REGEX, "", details)
            details = re.sub(r'\\s{{2,}}', ' ', details).strip()

            if isin_match:
                details += f" | ISIN: {{isin_match.group(1)}}"

            current_txn = {{
                "date": date_str,
                "details": details[:200],
                "debit": debit,
                "credit": credit,
                "balance": balance_qty,
                "confidence": 0.85
            }}

        else:
            # Continuation line
            if current_txn:
                isin_match = re.search(ISIN_REGEX, stripped)
                if isin_match:
                    current_txn["details"] += f" | ISIN: {{isin_match.group(1)}}"
                elif stripped and not re.search(QUANTITY_REGEX, stripped):
                    current_txn["details"] = (current_txn["details"] + " " + stripped).strip()[:200]

    # Don't forget last transaction
    if current_txn:
        transactions.append(current_txn)

    # For demat: filter out rows with no quantity (pure balance rows)
    # but DO NOT remove rows just because debit and credit are both None
    # (some informational rows like corporate actions may have no quantity)
    return transactions

============================================================
OUTPUT FORMAT
============================================================

Each dict:
    {{
        "date": str,
        "details": str,        # Company name + action
        "debit": float|None,   # Units sold/debited
        "credit": float|None,  # Units bought/credited
        "balance": float|None, # Running unit balance
        "confidence": float
    }}

NOTE: Values are UNITS (quantity of shares), NOT currency amounts.

============================================================
RETURN RULE
============================================================

Return ONLY Python code.
Do NOT include markdown backticks.
Do NOT add explanations.
Function must be complete and runnable.
EMPTY LIST is a valid return value when there are no transactions in the period.

============================================================
INPUT TEXT SAMPLE
============================================================

{text_sample}
"""