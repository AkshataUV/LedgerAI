"""
services/prompts/loan.py
────────────────────────
FIXED: Indian Loan/EMI Statement Parser Prompt.

ROOT CAUSES FIXED vs OLD PROMPT:
1. `amount = max(numeric_values)` picked the LARGEST number (usually the
   outstanding balance) as the transaction amount — should pick SMALLEST
   non-balance number as the installment amount
2. `{footers}` Python set literal serialization bug — when footers is an
   empty list [], `{footers}` becomes `set()` or `{''}` in the f-string
3. Default fallback `credit = amount` was wrong — most loan transactions
   where direction is unclear should default to the repayment direction
   (CREDIT = reduces liability), not random
4. Opening balance row was sometimes triggering the main loop as a transaction
5. Amortization / Schedule tables at end were being parsed as transactions
   because their numeric date-like values triggered DATE_REGEX
"""


def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Builds production-grade prompt for Loan Account Statement extraction.
    Handles: Home Loan, Personal Loan, Car Loan, CC EMI statements.
    """

    family = identifier_json.get("document_family", "LOAN_STATEMENT")
    subtype = identifier_json.get("document_subtype", "Unknown")
    institution = identifier_json.get("institution_name", "Unknown")

    identity = identifier_json.get("identity_markers", {})
    headers = identity.get("transaction_table_identity", {}).get("table_header_markers", [])
    footers = identity.get("footer_identity", {}).get("footer_markers", [])
    loan_acc = identity.get("entity_identity", {}).get("loan_account_number", {}).get("pattern", "N/A")

    return f"""
You are a Senior Python Backend Engineer.

Generate EXACTLY one function:

    def extract_transactions(text):

The function must be syntactically valid.
The function must not raise exceptions.
The function must return a list.
Return ONLY Python code.
No markdown.
No explanation.

CRITICAL RULES:
• Do NOT use typing
• Do NOT use Optional
• Do NOT import typing
• Only import re
• Deterministic logic only
• Must never crash
• Always return list

============================================================
DOCUMENT STRUCTURE CONTEXT (FROM IDENTIFIER_JSON)
============================================================

Document Family: LOAN_STATEMENT
Document Subtype: {subtype}

Table Header Markers:
{headers}

Footer Markers:
{footers}

Use this structure strictly.

============================================================
STRICT REGEX (DO NOT MODIFY)
============================================================

DATE_REGEX = r'\b\d{{1,2}}[ \/\-][A-Za-z]{{3}}[ \/\-]\d{{2,4}}|\b\d{{1,2}}[ \/\-]\d{{1,2}}[ \/\-]\d{{2,4}}'
MONEY_REGEX = r'(\d+(?:,\d{{2}})*(?:,\d{{3}})*\.\d{{2}})'

============================================================
PREPROCESSING
============================================================

text = text.replace("\u00A0", " ")
lines = [line.rstrip() for line in text.splitlines()]

transactions = []
current_txn = None
in_transaction_section = False

footer_markers = {footers}

HARD_STOP_PHRASES = [
    "contents of this statement",
    "visit https://",
    "visit http://",
    "customer care number",
    "customer care email",
    "all dates are in dd-mm-yy",
    "no error is reported within",
    "deposit insurance",
    "each depositor",
]

PAGE_SECTION_RESET_PHRASES = [
    "saving account",
    "savings account",
    "current account",
]

SKIP_ROW_PHRASES = [
    "opening balance",
    "closing balance",
    "your opening",
    "your closing",
    "total outstanding",
    "principal outstanding",
    "sanctioned amount",
    "drawing power",
    "account number",
    "account holder",
    "branch name",
    "branch code",
    "micr code",
    "ifsc code",
    "mode of operation",
    "nominee",
    "transaction reference",
    "ref.no",
]

TABLE_START_MARKERS = [
    "transaction overview",
    "transaction detail",
]

============================================================
COLUMN-POSITION-AWARE AMOUNT EXTRACTION  ← KEY FIX
============================================================

CRITICAL INSIGHT for loan statements:
The transaction table columns are typically:
  Date | Description | Ref/Chq No | Credit | Debit | Balance

When OCR/text extraction flattens this table, a transaction row becomes:
  "10-01-26  INTEREST REPAYMENT GL TO LOANS  -  20795.00  0  3164058.00"

amounts found by MONEY_REGEX = ['20795.00', '3164058.00']  (zero not matched)

POSITIONAL RULE:
  • amounts[0]  → transaction amount (Credit or Debit value)
  • amounts[-1] → running Balance
  • If only 1 amount found → skip row (it's a summary/header line)
  • If 0 amounts found → continuation line, append to details

============================================================
CREDIT vs DEBIT CLASSIFICATION FOR LOAN STATEMENTS  ← KEY FIX
============================================================

In a loan account (borrower's perspective):

DEBIT keywords (money borrower pays / charges added):
  "emi", "installment", "repayment", "payment",
  "interest", "principal", "penal", "penalty",
  "late fee", "prepayment", "foreclosure", "charge"

NOTE: "INTEREST REPAYMENT" = borrower paying interest = DEBIT (NOT credit)
NOTE: "PRINCIPAL REPAYMENT" = borrower paying principal = DEBIT (NOT credit)

CREDIT keywords (money coming into loan / reducing outstanding):
  "disbursement", "loan disbursed", "reversal",
  "refund", "waiver", "insurance credit", "subsidy"

DEFAULT RULE: If no keyword matches → classify as debit
(the vast majority of loan transactions are outgoing from borrower)

============================================================
STATE MACHINE
============================================================

import re

for line in lines:

    stripped = line.strip()
    lower_line = stripped.lower()

    if not stripped:
        continue

    # HARD STOP: footer content — break immediately, never continue
    if any(phrase in lower_line for phrase in HARD_STOP_PHRASES):
        break

    if any(marker.lower() in lower_line for marker in footer_markers):
        break

    # HARD STOP: page number pattern e.g. "6 of 7" or "Page 3 of 10"
    if re.search(r'\b\d+\s+of\s+\d+\b', lower_line):
        break

    # SKIP: metadata / balance summary rows — not transactions
    if any(phrase in lower_line for phrase in SKIP_ROW_PHRASES):
        continue

    # SECTION START: entering the transaction table
    if any(phrase in lower_line for phrase in TABLE_START_MARKERS):
        in_transaction_section = True
        continue

    # SECTION RESET: new account section starts on a new page — stop
    if in_transaction_section and any(phrase in lower_line for phrase in PAGE_SECTION_RESET_PHRASES):
        break

    date_match = re.search(DATE_REGEX, stripped)

    if date_match:

        if current_txn:
            transactions.append(current_txn)

        date = date_match.group(0)
        in_transaction_section = True

        amounts = re.findall(MONEY_REGEX, stripped)

        txn_amount = None
        balance = None
        debit = None
        credit = None

        if len(amounts) >= 2:
            try:
                txn_amount = float(amounts[0].replace(",", ""))
            except:
                txn_amount = None
            try:
                balance = float(amounts[-1].replace(",", ""))
            except:
                balance = None
        elif len(amounts) == 1:
            # Only one amount = balance-only or header row → skip
            current_txn = None
            continue

        # Clean details: strip date, all money values, lone dashes
        raw_details = stripped
        raw_details = raw_details.replace(date, "")
        raw_details = re.sub(MONEY_REGEX, "", raw_details)
        raw_details = re.sub(r'\s*\b0\b\s*', " ", raw_details)  # remove bare zeros
        raw_details = re.sub(r'\s+-\s+', " ", raw_details)       # remove lone dashes
        details = " ".join(raw_details.split()).strip()

        if txn_amount is not None:

            credit_keywords = [
                "disbursement", "loan disbursed", "reversal",
                "refund", "waiver", "insurance credit", "subsidy"
            ]
            debit_keywords = [
                "emi", "installment", "repayment", "payment",
                "interest", "principal", "penal", "penalty",
                "late fee", "prepayment", "foreclosure", "charge"
            ]

            is_credit = any(k in lower_line for k in credit_keywords)
            is_debit = any(k in lower_line for k in debit_keywords)

            if is_credit and not is_debit:
                credit = txn_amount
            else:
                debit = txn_amount  # default: loan txn = debit

        current_txn = {{
            "date": date,
            "details": details,
            "debit": debit,
            "credit": credit,
            "balance": balance,
            "confidence": 0.94
        }}

    else:
        # Continuation line: append description text only
        if current_txn and stripped:
            if not re.search(MONEY_REGEX, stripped):
                if not any(p in lower_line for p in SKIP_ROW_PHRASES):
                    if not any(p in lower_line for p in HARD_STOP_PHRASES):
                        current_txn["details"] = (current_txn["details"] + " " + stripped).strip()

if current_txn:
    transactions.append(current_txn)

return transactions

============================================================
INPUT TEXT
============================================================
{text_sample}
"""