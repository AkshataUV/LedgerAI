"""
services/prompts/wallet.py
──────────────────────────
FIXED: Universal Indian Wallet/UPI Statement Parser Prompt.

ROOT CAUSES FIXED vs OLD PROMPT:
1. Paytm: Date "25 Feb" and Time "1:42 PM" are on SEPARATE lines —
   old prompt required date+time on SAME line for block start
2. Paytm: Amount "-Rs.19" is at END of the date line, not in a later line —
   old prompt's `valid_amounts[-1]` logic (taking last amount) missed this
   because year "2026" was confusingly parsed as an amount
3. Google Pay: "13 Jan, 2026" → the comma after month caused DATE_REGEX \\b
   word boundary to fail because comma is a word boundary character
4. Google Pay: "Paid by HDFC Bank 8323" line was consuming the amount line
   before it was read in some parsers
5. Both: Footer detection was crashing when footer_markers list was empty []
6. Paytm: Date format "25 Feb" has NO year — regex required year (\\d{{2,4}})
   making it non-optional, causing zero matches
7. Amount filter `float(clean) != 2026` was hardcoded — breaks for other years
"""


def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Builds a production-grade prompt for Wallet/UPI statement parsing.
    Handles: Paytm, Google Pay, PhonePe.
    """

    family = identifier_json.get("document_family", "WALLET_STATEMENT")
    subtype = identifier_json.get("document_subtype", "Unknown")
    institution = identifier_json.get("institution_name", "Unknown")

    identity = identifier_json.get("identity_markers", {})
    footers = identity.get("footer_identity", {}).get("footer_markers", [])

    return f"""
You are a Senior Python Backend Engineer specializing in UPI and Wallet Statement Parsing.

============================================================
CRITICAL OBJECTIVE
============================================================
Generate EXACTLY one deterministic Python function:

    def extract_transactions(text: str) -> list:

Rules:
- Import re inside the function
- Never raise exceptions (wrap entire body in try/except returning [])
- Return list of dicts, empty list if nothing found

============================================================
DOCUMENT CONTEXT
============================================================
Institution   : {institution}
Family        : {family}
Subtype       : {subtype}
Footer Markers: {footers}

============================================================
WALLET FORMAT DETECTION
============================================================

Detect wallet type from text at the very start:

    def detect_wallet_type(text):
        lower = text.lower()
        if "paytm" in lower:
            return "PAYTM"
        elif "google pay" in lower or "gpay" in lower:
            return "GPAY"
        elif "phonepe" in lower:
            return "PHONEPE"
        return "GENERIC"

============================================================
PHASE 1 — PREPROCESSING
============================================================

    text = text.replace("\\u00A0", " ")
    text = text.replace("\\xa0", " ")
    lines = [line.rstrip() for line in text.splitlines()]
    # Remove completely blank lines but preserve structure
    lines = [l for l in lines if l.strip()]

============================================================
PHASE 2 — DATE REGEX (CRITICAL FIXES)
============================================================

CRITICAL: Year must be OPTIONAL (Paytm shows "25 Feb" without year).
CRITICAL: Comma after month must be allowed (GPay shows "13 Jan, 2026").
CRITICAL: No \\b after optional year — use lookahead instead.

    # Matches ALL of:
    # "25 Feb"           (Paytm — no year)
    # "25 Feb 2026"      (with year)
    # "13 Jan, 2026"     (GPay — comma after month)
    # "27 JAN'26"        (apostrophe year)
    # "01-Jan-2026"      (hyphen format)
    # "01/01/2026"       (numeric)

    DATE_REGEX = (
        r'\\b(\\d{{1,2}}'                          # Day: 1-2 digits
        r'[-/\\s]'                               # Separator
        r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|'
        r'May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|'
        r'Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
        r'[,\\'\\s-]*'                           # Optional comma/apostrophe/space
        r'(?:\\d{{2,4}})?)'                      # Optional year
    )

    # Also handle pure numeric dates: 01/01/2026, 01-01-2026
    NUMERIC_DATE_REGEX = r'\\b(\\d{{1,2}}[-/]\\d{{1,2}}[-/]\\d{{2,4}})\\b'

    def extract_date(line):
        m = re.search(DATE_REGEX, line, re.IGNORECASE)
        if m:
            return m.group(1).strip().rstrip(",")
        m2 = re.search(NUMERIC_DATE_REGEX, line)
        if m2:
            return m2.group(1)
        return None

    def line_has_date(line):
        stripped = line.strip()
        # Date must appear within first 20 characters
        early = stripped[:20]
        has_month = bool(re.search(DATE_REGEX, early, re.IGNORECASE))
        has_numeric = bool(re.search(NUMERIC_DATE_REGEX, early))
        return has_month or has_numeric

============================================================
PHASE 3 — MONEY REGEX
============================================================

    # Matches all these patterns:
    # -Rs.19  |  + Rs.908  |  ₹871.26  |  Rs.1,133  |  -Rs.1,133
    # 871.26  |  1,030.00  |  1,133.00

    MONEY_REGEX = r'([+-]?\\s*(?:₹|Rs\\.?|INR)?\\s*)(\\d{{1,3}}(?:,\\d{{2,3}})*(?:\\.\\d{{1,2}})?)'

    def parse_money(text):
        \"\"\"
        Returns list of (sign_str, float_value) tuples.
        sign_str is '+', '-', or '' (unknown)
        \"\"\"
        results = []
        for m in re.finditer(MONEY_REGEX, text, re.IGNORECASE):
            prefix = (m.group(1) or "").strip()
            raw_num = m.group(2).replace(",", "")
            try:
                val = float(raw_num)
                # Filter out years and very small noise
                if val < 1 or (val > 1900 and val < 2100 and "." not in m.group(2)):
                    continue  # Skip years like 2026
                sign = "-" if "-" in prefix else ("+" if "+" in prefix else "")
                results.append((sign, val))
            except:
                pass
        return results

============================================================
PHASE 4 — PAYTM SPECIFIC PARSER
============================================================

Paytm layout (CRITICAL OBSERVATION from document):
- Line 1: "25 Feb" (date only, sometimes with time on same or next line)
- Line 2: "1:42 PM"  (time — continuation)
- Line 3: "Recharge of Jio Mobile 8767769936" (description)
- Line 4: "UPI ID: paybil3066@ptybl on paytm" (UPI details)
- Line 5: "UPI Ref No: 605676248340" (ref — continuation)
- Line 6: "Order ID: 26669679732" (order — continuation)
- **AMOUNT**: appears at the END of the date line itself: "25 Feb  - Rs.19"
  OR: appears as the LAST item in the block

    def parse_paytm_block(block_lines):
        if not block_lines:
            return None
        
        full_text = " ".join(block_lines)
        
        # Extract date from first line
        date_str = extract_date(block_lines[0])
        if not date_str:
            return None
        
        # Extract time if present
        time_match = re.search(r'\\b(\\d{{1,2}}:\\d{{2}}\\s*[AaPp][Mm])\\b', full_text)
        time_str = time_match.group(1) if time_match else ""
        
        # Extract amount — search entire block
        all_money = parse_money(full_text)
        if not all_money:
            return None
        
        # Get last valid money value (the transaction amount)
        sign, amount = all_money[-1]
        
        # Determine direction
        lower = full_text.lower()
        if sign == "-":
            debit = amount
            credit = None
        elif sign == "+":
            debit = None
            credit = amount
        elif any(k in lower for k in ["received from", "cashback", "refund", "money received"]):
            debit = None
            credit = amount
        else:
            debit = amount
            credit = None
        
        # Build clean details
        details = full_text
        details = re.sub(DATE_REGEX, "", details, flags=re.IGNORECASE)
        details = re.sub(MONEY_REGEX, "", details, flags=re.IGNORECASE)
        details = re.sub(r'\\b(UPI\\s*Ref\\s*No|Order\\s*ID|UPI\\s*ID)[:\\s]+\\S+', '', details, flags=re.IGNORECASE)
        details = re.sub(r'\\d{{1,2}}:\\d{{2}}\\s*[AaPp][Mm]', '', details)
        details = re.sub(r'\\s{{2,}}', ' ', details).strip()
        
        return {{
            "date": date_str + (" " + time_str if time_str else ""),
            "details": details[:200],
            "debit": debit,
            "credit": credit,
            "balance": None,
            "confidence": 0.90
        }}

============================================================
PHASE 5 — GPAY SPECIFIC PARSER
============================================================

Google Pay layout (CRITICAL OBSERVATION):
- Line 1: "13 Jan, 2026"  (date — starts block)
- Line 2: "06:04 PM"      (time — continuation)
- Line 3: "Paid to REDBUS INDIA PRIVATE LIMITED" (description)
- Line 4: "₹871.26"  (AMOUNT — its own line!)
- Line 5: "UPI Transaction ID: 117135833950" (ref)
- Line 6: "Paid by HDFC Bank 8323" (bank info)

    def parse_gpay_block(block_lines):
        if not block_lines:
            return None
        
        full_text = " ".join(block_lines)
        
        date_str = extract_date(block_lines[0])
        if not date_str:
            return None
        
        all_money = parse_money(full_text)
        if not all_money:
            return None
        
        sign, amount = all_money[-1]
        
        lower = full_text.lower()
        if sign == "+":
            debit = None
            credit = amount
        elif any(k in lower for k in ["received from", "cashback", "refund"]):
            debit = None
            credit = amount
        else:
            debit = amount
            credit = None
        
        # Clean details
        details = full_text
        details = re.sub(DATE_REGEX, "", details, flags=re.IGNORECASE)
        details = re.sub(MONEY_REGEX, "", details, flags=re.IGNORECASE)
        details = re.sub(r'UPI\\s*Transaction\\s*ID[:\\s]+\\S+', '', details, flags=re.IGNORECASE)
        details = re.sub(r'Paid\\s*by\\s+[\\w\\s]+\\d{{4}}', '', details, flags=re.IGNORECASE)
        details = re.sub(r'\\d{{1,2}}:\\d{{2}}\\s*[AaPp][Mm]', '', details)
        details = re.sub(r'\\s{{2,}}', ' ', details).strip()
        
        return {{
            "date": date_str,
            "details": details[:200],
            "debit": debit,
            "credit": credit,
            "balance": None,
            "confidence": 0.90
        }}

============================================================
PHASE 6 — SKIP PATTERNS (Lines that start but are NOT transactions)
============================================================

    SKIP_LINE_PATTERNS = [
        r'(?i)(passbook|payment history|payments? made|payments? received)',
        r'(?i)(total money paid|total money received)',
        r'(?i)(self transfer|payments? you might)',
        r'(?i)(date\\s*&?\\s*time|transaction details|notes|amount)',  # Header row
        r'(?i)(account|state bank|hdfc|icici)',  # Bank name lines
    ]

    FOOTER_MARKERS = {footers}

    def should_skip(line):
        stripped = line.strip()
        if not stripped:
            return True
        for pat in SKIP_LINE_PATTERNS:
            if re.search(pat, stripped):
                return True
        if FOOTER_MARKERS and any(m.lower() in stripped.lower() for m in FOOTER_MARKERS if m):
            return True
        return False

============================================================
PHASE 7 — MAIN LOOP
============================================================

    wallet_type = detect_wallet_type(text)
    parse_block = parse_paytm_block if wallet_type == "PAYTM" else parse_gpay_block

    transactions = []
    current_block = []
    in_data_section = False

    for line in lines:
        if should_skip(line):
            continue

        # Check footer
        if FOOTER_MARKERS and any(m.lower() in line.lower() for m in FOOTER_MARKERS if m):
            break

        if line_has_date(line):
            # Process previous block
            if current_block:
                txn = parse_block(current_block)
                if txn and (txn["debit"] is not None or txn["credit"] is not None):
                    transactions.append(txn)

            # Start new block
            current_block = [line]
            in_data_section = True

        elif in_data_section and current_block:
            current_block.append(line)

    # Process last block
    if current_block:
        txn = parse_block(current_block)
        if txn and (txn["debit"] is not None or txn["credit"] is not None):
            transactions.append(txn)

    return transactions

============================================================
OUTPUT FORMAT
============================================================

Each dict:
    {{
        "date": str,
        "details": str,
        "debit": float|None,
        "credit": float|None,
        "balance": None,       # Wallets don't show running balance
        "confidence": float
    }}

============================================================
RETURN RULE
============================================================

Return ONLY Python code.
Do NOT include markdown backticks.
Do NOT add explanations.
Function must be complete and runnable.

============================================================
INPUT TEXT SAMPLE (Study this to understand exact layout)
============================================================

{text_sample}
"""