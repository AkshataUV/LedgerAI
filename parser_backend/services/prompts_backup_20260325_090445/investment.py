def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Production-grade prompt for Indian Mutual Fund statement transaction extraction.
    Handles: Edelweiss MF, Nippon India MF, HDFC MF, SBI MF, ICICI Pru MF,
             Axis MF, Kotak MF, DSP MF, Mirae Asset, Franklin Templeton,
             CAMS / KFintech consolidated statements, and all major Indian MF formats.

    v2 — Fixes validated against real Edelweiss + Nippon India output:
      - Eliminated garbage rows from Summary/footer section (Current Value, Current Cost, etc.)
      - Fixed date=None rows caused by summary table lines without valid transaction dates
      - Eliminated barcode/reference string false positives (e.g. "5202.21.11-RELIAM-LPS")
      - Eliminated Gross/Stamp rows (only Net rows emitted for clean accounting)
      - Fixed raw unparsed text in details field
      - Validated Nippon India column order (NAV correctly extracted)
    """

    family = identifier_json.get("document_family", "INVESTMENT_STATEMENT")
    subtype = identifier_json.get("document_subtype", "Unknown")
    institution = identifier_json.get("institution_name", "Unknown")

    identity = identifier_json.get("identity_markers", {})
    headers = identity.get("transaction_table_identity", {}).get("table_header_markers", [])
    footers = identity.get("footer_identity", {}).get("footer_markers", [])

    return f"""
You are a Senior Python Backend Engineer specializing in WealthTech / Mutual Fund Data Parsing.

============================================================
CRITICAL OBJECTIVE
============================================================
Generate EXACTLY one self-contained Python function:

    def extract_transactions(text: str) -> list:

Rules:
- ALL helper functions defined INSIDE extract_transactions
- ALL imports (re, etc.) at the top of the function body
- NEVER raise exceptions — wrap entire body in try/except returning []
- Return list of dicts; empty list if nothing found

============================================================
DOCUMENT CONTEXT
============================================================
Institution   : {institution}
Family        : {family}
Subtype       : {subtype}
Table Headers : {headers}
Footer Markers: {footers}

============================================================
SECTION 1 — UNDERSTAND THE TWO REAL FORMATS
============================================================

You will encounter exactly two formats in Indian MF statements.
Study the verified examples in Section 9 carefully before writing any code.

────────────────────────────────────────────────────────────
FORMAT A — Edelweiss / CAMS Tabular  (3-row SIP groups)
────────────────────────────────────────────────────────────
Table columns (9 total, left-to-right):
  Date | Nav Date | Transaction Type | Amount(₹) | NAV(₹) | Load(₹) | Price(₹) | Units | Balance Units

Each SIP cycle produces EXACTLY 3 consecutive lines:

  Line 1 — Gross row (HAS leading date, amount only, NO nav/units):
    "10/10/2025  10/10/2025  Gross Systematic Investment    4,170.00"

  Line 2 — Stamp Duty row (NO leading date, tiny amount <5):
    "            10/10/2025  Stamp Duty                         0.21"

  Line 3 — Net row (HAS leading date, FULL data including NAV + Units):
    "10/10/2025  10/10/2025  Net Systematic Investment - 448258372  4,169.79  101.6410  0.0000  101.6410  41.025  41.025"

EMIT RULE FOR FORMAT A:
  ✅ EMIT:   Net rows only  (contain "Net Systematic" or have nav + units present)
  ❌ SKIP:   Gross rows     (contain "Gross Systematic" — these are pre-stamp-duty totals)
  ❌ SKIP:   Stamp Duty rows (contain "Stamp Duty" — these are sub-rows of the Net row)

Reason: The Net amount (4,169.79) already nets out the stamp duty. Emitting all 3 rows
creates duplicates. For audit purposes, stamp duty is noted in the details field.

────────────────────────────────────────────────────────────
FORMAT B — Nippon India / KFintech Narrative  (1-row SIP)
────────────────────────────────────────────────────────────
Table columns (7 total, left-to-right):
  NAV Date | Description | Amount(₹) | Revised Cost*(₹) | NAV(₹) | Number of Units | Balance Units

Each SIP = ONE line containing everything:
  "13/10/2025  Sys. Investment (Gross - Rs 4165.00, Stamp duty Rs 0.21) (1/241)  4,164.79  4192.1425  0.983   0.963"

Column breakdown for that line:
  NAV Date        = 13/10/2025
  Description     = Sys. Investment (Gross - Rs 4165.00, Stamp duty Rs 0.21) (1/241)
  Amount(₹)       = 4,164.79   ← NET amount, OUTSIDE parentheses
  Revised Cost(₹) = 4192.1425  ← 4dp, column 5 — this is the COST BASIS NAV, NOT transaction NAV
  NAV(₹)          = [may be same column or omitted in some versions]
  Number of Units = 0.983       ← 3dp
  Balance Units   = 0.963       ← 3dp

EMIT RULE FOR FORMAT B:
  ✅ EMIT: Every line that starts with a date (each = 1 complete transaction)
  Amount to use: the 2dp comma-formatted value OUTSIDE parentheses = 4,164.79

============================================================
SECTION 2 — FORMAT DETECTION
============================================================

Detect format from the first 60 lines of text:

  def detect_format(lines):
      sample = " ".join(lines[:60])
      # FORMAT B signals
      if (re.search(r'(?i)revised\\s*cost', sample) or
          re.search(r'(?i)number\\s*of\\s*units', sample) or
          re.search(r'Gross\\s*-\\s*(Rs|₹)', sample) or
          re.search(r'(?i)(kfintech|nippon)', sample)):
          return "FORMAT_B"
      # FORMAT A signals
      if (re.search(r'(?i)nav\\s*date', sample) or
          re.search(r'(?i)gross\\s*systematic', sample) or
          re.search(r'(?i)edelweiss', sample)):
          return "FORMAT_A"
      # Default
      return "FORMAT_A"

============================================================
SECTION 3 — PREPROCESSING
============================================================

  text = text.replace("\\u00A0", " ").replace("\\xa0", " ")
  text = text.replace("₹", " ").replace("Rs.", " ").replace("Rs ", " ").replace("INR", " ")
  lines = [line.rstrip() for line in text.splitlines()]
  lines = [l for l in lines if l.strip()]

============================================================
SECTION 4 — TRANSACTION SECTION BOUNDARY DETECTION  ← CRITICAL NEW RULE
============================================================

THE MOST IMPORTANT RULE TO PREVENT GARBAGE ROWS:

Edelweiss and CAMS statements have TWO distinct sections:
  Section 1 — SUMMARY TABLE  (scheme-level totals: Current Value, Current Cost, Units, NAV as on)
  Section 2 — TRANSACTION TABLE  (individual dated rows with SIP details)

The summary section contains lines like:
  "Edelweiss Mid Cap Fund - Regular Plan Growth  12,510.00  12,446.35  -5.97%  122.132  101.9090  0.00"
  "Current value : (₹) 12,446.35    Current Cost : (₹) 12,510.00"
  "NAV as on 10 Dec 2025 (Rs.) 101.909"

These lines look like transaction rows (they have amounts, 4dp values, 3dp values) but they
are NOT transactions. They have NO valid transaction date at the start.

RULE: A line is a valid transaction candidate ONLY if:
  1. It starts with a date (DD/MM/YYYY) within the first 12 characters, AND
  2. The date year is a plausible transaction year (2015–2035), AND
  3. The line contains a transaction keyword (see TRANSACTION_KEYWORDS below), OR
     contains a net amount + nav + units pattern (FORMAT A Net row)

TRANSACTION_KEYWORDS (case-insensitive):
  "systematic investment", "gross systematic", "net systematic",
  "stamp duty", "redemption", "switch", "purchase", "dividend",
  "lump sum", "sip", "sys. investment", "sys investment",
  "swp", "idcw", "additional purchase", "nfo"

Lines WITHOUT a leading date are NEVER valid transaction starts.
Lines with a leading date but NO transaction keyword AND NO nav/units pattern → SKIP.

  TRANSACTION_KW_RE = re.compile(
      r'(?i)(systematic\\s*investment|gross\\s*systematic|net\\s*systematic|'
      r'stamp\\s*duty|redemption|switch\\s*(in|out)?|purchase|dividend|'
      r'lump\\s*sum|\\bsip\\b|sys\\.?\\s*investment|swp|idcw|'
      r'additional\\s*purchase|nfo|new\\s*fund\\s*offer)',
      re.IGNORECASE
  )

  def is_transaction_line(line, fmt):
      # Must start with a date
      if not line_starts_with_date(line):
          return False
      # Must contain a transaction keyword OR (for FORMAT A Net rows) have nav+units
      if TRANSACTION_KW_RE.search(line):
          return True
      # FORMAT A Net rows without keyword: must have 4dp + 3dp values
      if fmt == "FORMAT_A":
          has_4dp = bool(re.search(r'\\b\\d{{1,6}}\\.\\d{{4}}\\b', line))
          has_3dp = bool(re.search(r'\\b\\d{{1,8}}\\.\\d{{3}}\\b', line))
          amount  = bool(re.search(r'\\b\\d{{1,3}}(?:,\\d{{2,3}})*\\.\\d{{2}}\\b', line))
          if has_4dp and has_3dp and amount:
              return True
      return False

============================================================
SECTION 5 — SKIP PATTERNS
============================================================

  NOISE_PATTERNS = [
      # ── Column header rows ──
      r'(?i)^\\s*(date|nav\\s*date|transaction\\s*type|amount|nav|load|price|units|balance\\s*units)\\s*$',
      r'(?i)(revised\\s*cost|number\\s*of\\s*units)\\s*$',

      # ── Edelweiss / CAMS summary rows ← CRITICAL ──
      r'(?i)(current\\s*value|current\\s*cost)',
      r'(?i)(nav\\s*as\\s*on)',
      r'(?i)(units\\s*pledged|balance\\s*units\\s*:)',
      r'(?i)(summary\\s*of\\s*investments)',
      r'(?i)(scheme\\s*details)',
      r'(?i)^\\s*total\\s*[:\\s]',
      r'(?i)(equity|debt|hybrid|liquid|elss)\\s+\\d{{1,3}}(?:,\\d{{2,3}})*\\.\\d{{2}}',  # Category summary rows

      # ── Account metadata ──
      r'(?i)(folio\\s*no|account\\s*statement|statement\\s*date|statement\\s*period)',
      r'(?i)(non\\s*transferable|transferable)',
      r'(?i)(nominee|kyc|fatca)',
      r'(?i)^\\s*(primary|2nd\\s*holder|3rd\\s*holder|guardian)',
      r'(?i)(pan\\s*/\\s*pekrn|kin\\s*no|pan\\s*status)',

      # ── Load / bank info ──
      r'(?i)(load\\s*structure|entry\\s*load|exit\\s*load)',
      r'(?i)(bank\\s*mandate|bank\\s*name|bank\\s*account|ifsc|micr|mode\\s*of\\s*payment)',

      # ── Nippon / KFintech specific ──
      r'(?i)(idcw\\s*earned|idcw\\s*paid|idcw\\s*reinvested)',
      r'(?i)(for\\s*subscriptions\\s*received|1%\\s*exit\\s*load)',
      r'(?i)(sub\\s*broker\\s*arn|agent.*advisor)',
      r'(?i)^\\s*\\*?form\\s*15',
      r'(?i)(city\\s*type|mode\\s*of\\s*holding|status\\s*:)',
      r'(?i)(long\\s*term\\s*capital|short\\s*term\\s*capital)',

      # ── Barcodes / reference strings (alphanumeric with dashes, no spaces) ← CRITICAL ──
      # e.g. "5202.21.11-RELIAM-LPS-FNOC-30yH" — these are print metadata
      r'^[A-Z0-9]{{4,}}-[A-Z]{{2,}}-',  # Starts with alnum chunk then dash-separated caps

      # ── Opening/Closing balance (comment out to include) ──
      r'(?i)(opening\\s*balance|closing\\s*balance)',
  ]

  FOOTER_MARKERS = {footers}

  def should_skip(line):
      stripped = line.strip()
      if not stripped or len(stripped) < 5:
          return True
      for pat in NOISE_PATTERNS:
          if re.search(pat, stripped):
              return True
      if FOOTER_MARKERS and any(
          m.lower() in stripped.lower() for m in FOOTER_MARKERS if m
      ):
          return True
      return False

============================================================
SECTION 6 — DATE HELPERS
============================================================

  DATE_REGEX = r'\\b(\\d{{1,2}}[-/](?:\\d{{1,2}}|[A-Za-z]{{3,9}})[-/]\\d{{2,4}})\\b'

  def extract_date(line):
      m = re.search(DATE_REGEX, line)
      return m.group(1) if m else None

  def line_starts_with_date(line):
      # Date must appear within the FIRST 12 characters of the stripped line
      early = line.strip()[:12]
      return bool(re.search(DATE_REGEX, early))

============================================================
SECTION 7 — FINANCIAL VALUE EXTRACTION
============================================================

  # Transaction amount: Indian format including lakh (1,70,000.00)
  # Handles: 4,169.79 / 1,70,000.00 / 100.00
  AMOUNT_REGEX = r'\\b((?:\\d{{1,2}},)?\\d{{1,3}}(?:,\\d{{2,3}})*\\.\\d{{2}})\\b'

  # 4-decimal values: NAV, Revised Cost, Load, Price
  FOUR_DP_REGEX = r'\\b(\\d{{1,6}}\\.\\d{{4}})\\b'

  # 3-decimal values: Units, Balance Units
  THREE_DP_REGEX = r'\\b(\\d{{1,8}}\\.\\d{{3}})\\b'

  def extract_amount(line):
      \"\"\"
      Extract net transaction amount (2dp, comma-formatted).
      CRITICAL: Strip parenthetical content first.
      Parentheses in Nippon lines contain embedded Gross/Stamp amounts
      that must NOT be used — the net amount is always OUTSIDE parentheses.
      \"\"\"
      clean = re.sub(r'\\([^)]*\\)', ' ', line)
      matches = re.findall(AMOUNT_REGEX, clean)
      candidates = []
      for m in matches:
          try:
              val = float(m.replace(",", ""))
              if val > 1.0:
                  candidates.append(val)
          except:
              pass
      return candidates[0] if candidates else None

  def extract_nav_units_format_a(line):
      \"\"\"
      FORMAT A Net row column order (after amount):
        Amount(2dp) | NAV(4dp) | Load(4dp) | Price(4dp) | Units(3dp) | BalUnits(3dp)
      
      NAV  = 1st valid 4dp value (range 1.0–100000)
      Load = 2nd 4dp value (usually 0.0000 — skip)
      Units = 1st 3dp value
      BalUnits = 2nd 3dp value
      \"\"\"
      four_dp = re.findall(FOUR_DP_REGEX, line)
      three_dp = re.findall(THREE_DP_REGEX, line)

      nav_str = None
      for v in four_dp:
          try:
              val = float(v)
              if 1.0 < val < 100000.0:
                  nav_str = v
                  break
          except:
              pass

      units_str     = three_dp[0] if len(three_dp) > 0 else None
      bal_units_str = three_dp[1] if len(three_dp) > 1 else None
      return nav_str, units_str, bal_units_str

  def extract_nav_units_format_b(line):
      \"\"\"
      FORMAT B column order after description (strip parens first):
        NetAmount(2dp) | RevisedCost(4dp) | NAV(4dp) | Units(3dp) | BalUnits(3dp)
      
      CRITICAL: RevisedCost is the FIRST 4dp value, NAV is the SECOND.
      However in many Nippon statements only ONE 4dp value appears (they merge columns).
      Rule:
        - If 2 valid 4dp values → NAV = second one
        - If 1 valid 4dp value  → NAV = that one (it IS the NAV column)
        - If 0 valid 4dp values → NAV = None
      \"\"\"
      clean = re.sub(r'\\([^)]*\\)', ' ', line)
      four_dp = re.findall(FOUR_DP_REGEX, clean)
      three_dp = re.findall(THREE_DP_REGEX, clean)

      valid_4dp = []
      for v in four_dp:
          try:
              val = float(v)
              if 1.0 < val < 100000.0:
                  valid_4dp.append(v)
          except:
              pass

      if len(valid_4dp) >= 2:
          nav_str = valid_4dp[1]   # Skip RevisedCost (index 0), take NAV (index 1)
      elif len(valid_4dp) == 1:
          nav_str = valid_4dp[0]   # Only column present — treat as NAV
      else:
          nav_str = None

      units_str     = three_dp[0] if len(three_dp) > 0 else None
      bal_units_str = three_dp[1] if len(three_dp) > 1 else None
      return nav_str, units_str, bal_units_str

============================================================
SECTION 8 — DIRECTION + DESCRIPTION HELPERS
============================================================

  CREDIT_KW = [
      "redemption", "redeem", "switch out", "swp", "systematic withdrawal",
      "dividend payout", "withdrawal", "repurchase", "idcw payout", "sale"
  ]

  def classify_direction(line, amount):
      lower = line.lower()
      for k in CREDIT_KW:
          if k in lower:
              return None, amount   # Credit: money back to investor
      return amount, None           # Default: Debit (purchase/SIP)

  def build_details(raw_line, nav_str, units_str, bal_units_str):
      \"\"\"
      Build clean human-readable description.
      - Remove date tokens
      - Remove SIP counters like (1/241)
      - Remove long numeric reference IDs (8+ digit strings)
      - Remove raw financial numbers (they're captured in dedicated fields)
      - Append nav/units as structured suffix
      \"\"\"
      desc = raw_line.strip()
      desc = re.sub(DATE_REGEX, '', desc)                    # Remove dates
      desc = re.sub(r'\\(\\d+/\\d+\\)', '', desc)           # Remove (1/241) etc.
      desc = re.sub(r'\\b\\d{{8,}}\\b', '', desc)           # Remove reference IDs
      desc = re.sub(r'\\([^)]*\\)', '', desc)               # Remove parenthetical content
      desc = re.sub(AMOUNT_REGEX, '', desc)                  # Remove 2dp amounts
      desc = re.sub(FOUR_DP_REGEX, '', desc)                 # Remove 4dp values
      desc = re.sub(THREE_DP_REGEX, '', desc)                # Remove 3dp values
      desc = re.sub(r'[-]{{2,}}', ' ', desc)                 # Replace multiple dashes
      desc = re.sub(r'\\s{{2,}}', ' ', desc).strip()
      desc = re.sub(r'^[-|\\s*]+', '', desc).strip()        # Strip leading punctuation

      extras = []
      if units_str:     extras.append(f"Units: {{units_str}}")
      if nav_str:       extras.append(f"NAV: {{nav_str}}")
      if bal_units_str: extras.append(f"Bal.Units: {{bal_units_str}}")
      if extras:
          desc = desc + " | " + ", ".join(extras)

      return desc[:250]

============================================================
SECTION 8b — FORMAT A PARSER  (Edelweiss / CAMS)
============================================================

  def parse_format_a(lines):
      \"\"\"
      Emits ONE transaction per SIP group = the Net Systematic Investment row.
      Gross rows and Stamp Duty rows are intentionally skipped.
      
      Why skip Gross + Stamp rows?
        - The Net amount already reflects the deduction of stamp duty
        - Emitting all 3 rows would triple-count each SIP
        - Gross row lacks NAV/Units so it's less useful
        - Stamp Duty is better noted as a field in the Net row details
      
      If you need full audit trail with Gross + Stamp separately,
      set EMIT_ALL_ROWS = True below.
      \"\"\"
      EMIT_ALL_ROWS = False   # ← Set True for full audit trail mode

      transactions = []
      last_date = None

      for line in lines:
          stripped = line.strip()
          if should_skip(stripped):
              continue

          # ── Stamp Duty sub-row (no leading date) ──────────────────────
          if not line_starts_with_date(stripped):
              if re.search(r'(?i)stamp\\s*duty', stripped) and last_date:
                  if EMIT_ALL_ROWS:
                      amount = extract_amount(stripped)
                      if amount and amount < 5.0:
                          transactions.append({{
                              "date": last_date,
                              "details": "Stamp Duty",
                              "debit": amount,
                              "credit": None,
                              "nav": None,
                              "units": None,
                              "balance_units": None,
                              "confidence": 0.99
                          }})
              continue

          # ── Line starts with a date ────────────────────────────────────
          if not is_transaction_line(stripped, "FORMAT_A"):
              continue   # Reject summary/footer lines that happen to have dates

          date_str = extract_date(stripped)
          if not date_str:
              continue
          last_date = date_str

          # ── Skip Gross rows (pre-stamp-duty totals) ───────────────────
          if re.search(r'(?i)gross\\s*systematic', stripped):
              if EMIT_ALL_ROWS:
                  amount = extract_amount(stripped)
                  if amount:
                      debit, credit = classify_direction(stripped, amount)
                      transactions.append({{
                          "date": date_str,
                          "details": "Gross Systematic Investment",
                          "debit": debit,
                          "credit": credit,
                          "nav": None,
                          "units": None,
                          "balance_units": None,
                          "confidence": 0.90
                      }})
              continue

          # ── Net row (or any other transaction type) ───────────────────
          amount = extract_amount(stripped)
          if not amount:
              continue

          nav_str, units_str, bal_units_str = extract_nav_units_format_a(stripped)
          debit, credit = classify_direction(stripped, amount)
          details = build_details(stripped, nav_str, units_str, bal_units_str)

          transactions.append({{
              "date": date_str,
              "details": details,
              "debit": debit,
              "credit": credit,
              "nav": float(nav_str) if nav_str else None,
              "units": float(units_str) if units_str else None,
              "balance_units": float(bal_units_str) if bal_units_str else None,
              "confidence": 0.97 if nav_str else 0.88
          }})

      return transactions

============================================================
SECTION 8c — FORMAT B PARSER  (Nippon India / KFintech)
============================================================

  def parse_format_b(lines):
      \"\"\"
      One transaction per line. Each line is fully self-contained.
      Amount is the net value OUTSIDE parentheses.
      \"\"\"
      transactions = []

      for line in lines:
          stripped = line.strip()
          if should_skip(stripped):
              continue
          if not line_starts_with_date(stripped):
              continue
          if not is_transaction_line(stripped, "FORMAT_B"):
              continue

          date_str = extract_date(stripped)
          if not date_str:
              continue

          amount = extract_amount(stripped)
          if not amount:
              continue

          nav_str, units_str, bal_units_str = extract_nav_units_format_b(stripped)
          debit, credit = classify_direction(stripped, amount)
          details = build_details(stripped, nav_str, units_str, bal_units_str)

          transactions.append({{
              "date": date_str,
              "details": details,
              "debit": debit,
              "credit": credit,
              "nav": float(nav_str) if nav_str else None,
              "units": float(units_str) if units_str else None,
              "balance_units": float(bal_units_str) if bal_units_str else None,
              "confidence": 0.97 if nav_str else 0.88
          }})

      return transactions

============================================================
SECTION 9 — VERIFIED GROUND-TRUTH EXAMPLES
     Study these carefully. Your regex MUST reproduce these outputs exactly.
============================================================

── FORMAT A Example 1: Gross row → SKIP ──
  INPUT:  "10/10/2025  10/10/2025  Gross Systematic Investment    4,170.00"
  EXPECTED: SKIPPED (contains "Gross Systematic")

── FORMAT A Example 2: Stamp Duty row → SKIP ──
  INPUT:  "            10/10/2025  Stamp Duty    0.21"
  EXPECTED: SKIPPED (no leading date)

── FORMAT A Example 3: Net row → EMIT ──
  INPUT:  "10/10/2025  10/10/2025  Net Systematic Investment - 448258372  4,169.79  101.6410  0.0000  101.6410  41.025  41.025"
  EXPECTED OUTPUT:
    date          = "10/10/2025"
    details       = "Net Systematic Investment | Units: 41.025, NAV: 101.6410, Bal.Units: 41.025"
    debit         = 4169.79
    credit        = None
    nav           = 101.641   (float of "101.6410")
    units         = 41.025
    balance_units = 41.025
    confidence    = 0.97

── FORMAT A Example 4: Summary line → SKIP (CRITICAL) ──
  INPUT:  "Edelweiss Mid Cap Fund - Regular Plan Growth  12,510.00  12,446.35  -5.97%  122.132  101.9090  0.00"
  EXPECTED: SKIPPED — no leading date within first 12 chars

── FORMAT A Example 5: Current Value line → SKIP (CRITICAL) ──
  INPUT:  "Current value : (₹) 12,446.35    Current Cost : (₹) 12,510.00"
  EXPECTED: SKIPPED — matches NOISE_PATTERN for "current value"

── FORMAT A Example 6: Barcode/reference string → SKIP (CRITICAL) ──
  INPUT:  "5202.21.11-RELIAM-LPS-FNOC-30yH"
  EXPECTED: SKIPPED — no leading date; also matches barcode NOISE_PATTERN

── FORMAT B Example 1: SIP row → EMIT ──
  INPUT:  "13/10/2025  Sys. Investment (Gross - Rs 4165.00, Stamp duty Rs 0.21) (1/241)  4,164.79  4192.1425  0.983   0.963"
  EXPECTED OUTPUT:
    date          = "13/10/2025"
    details       = "Sys. Investment | Units: 0.983, NAV: 4192.1425, Bal.Units: 0.963"
    debit         = 4164.79      ← NOT 4165.00 (gross inside parens)
    credit        = None
    nav           = 4192.1425    ← only one 4dp value present → treated as NAV
    units         = 0.983
    balance_units = 0.963
    confidence    = 0.97

── FORMAT B Example 2: 5th SIP row ──
  INPUT:  "10/02/2026  Sys. Investment (Gross - Rs 4165.00, Stamp duty Rs 0.21) (5/241)  4,164.79  4355.0021  0.956   4.931"
  EXPECTED OUTPUT:
    date          = "10/02/2026"
    debit         = 4164.79
    nav           = 4355.0021
    units         = 0.956
    balance_units = 4.931

============================================================
SECTION 10 — MAIN FUNCTION TEMPLATE
============================================================

def extract_transactions(text: str) -> list:
    try:
        import re

        # ── All helpers defined here (detect_format, should_skip,
        #    line_starts_with_date, extract_date, extract_amount,
        #    extract_nav_units_format_a, extract_nav_units_format_b,
        #    classify_direction, build_details,
        #    parse_format_a, parse_format_b) ──

        # ── Preprocessing ──
        text = text.replace("\\u00A0", " ").replace("\\xa0", " ")
        text = (text.replace("₹", " ").replace("Rs.", " ")
                    .replace("Rs ", " ").replace("INR", " "))
        lines = [l.rstrip() for l in text.splitlines() if l.strip()]

        # ── Format detection ──
        fmt = detect_format(lines)

        # ── Parse ──
        if fmt == "FORMAT_B":
            txns = parse_format_b(lines)
        else:
            txns = parse_format_a(lines)

        # ── Final guard: must have a monetary value ──
        txns = [t for t in txns if t.get("debit") or t.get("credit")]

        return txns

    except Exception:
        return []

============================================================
OUTPUT SCHEMA
============================================================

Each transaction dict MUST have these exact keys:
    {{
        "date":          str,          # "DD/MM/YYYY"
        "details":       str,          # Clean description + "| Units: X, NAV: X, Bal.Units: X"
        "debit":         float|None,   # Amount invested (SIP/purchase); None for redemptions
        "credit":        float|None,   # Amount redeemed; None for purchases
        "nav":           float|None,   # NAV at transaction date
        "units":         float|None,   # Units allotted/redeemed in this transaction
        "balance_units": float|None,   # Cumulative units after this transaction
        "confidence":    float         # 0.88 to 0.99
    }}

============================================================
RETURN RULE
============================================================
Return ONLY the Python function code.
Do NOT use markdown backticks.
Do NOT add any explanation outside the function.
ALL helpers must be defined INSIDE extract_transactions.
The outer try/except must catch ALL exceptions and return [].

============================================================
SECTION 11 — INPUT TEXT SAMPLE
(Study spacing and column positions carefully before writing regex)
============================================================

{text_sample}
"""