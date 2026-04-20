def build_prompt(identifier_json: dict, text_sample: str) -> str:
    institution = identifier_json.get("institution_name", "Unknown")

    return f"""
You are a Python engineer. Write a Python function that extracts transactions
from an Indian credit card statement.

The function will be called with the raw extracted text of a real statement.
A sample of that text is provided below so you can observe its structure.

════════════════════════════════════════════
SAMPLE DOCUMENT TEXT
════════════════════════════════════════════

{text_sample}

════════════════════════════════════════════
YOUR TASK
════════════════════════════════════════════

Read the sample text above the way a human reads a bank statement.
You already understand what a credit card transaction is:
  - it has a date
  - it has a payee or description
  - it has an amount
  - it is either a debit (money spent) or a credit (payment/refund/waiver)

Using that understanding, write extract_transactions(text) so that when
called with any page of this statement it returns all transactions and
skips everything else (summaries, headers, offers, footers text, T&C text).

Do not write a rigid line-by-line regex parser.
Write code that finds transactions the same way you would find them by
reading — using context and meaning, not just pattern position.

def extract_transactions(text: str) -> list:
    \"\"\"
    Returns list of dicts — one per transaction, in document order:
    {{
        "date"      : str,          # DD/MM/YYYY — zero-padded, 4-digit year
        "details"   : str,          # payee/description exactly as it appears
                                    # in the statement — do not add or remove words
        "debit"     : float | None, # amount spent — None if this is a credit
        "credit"    : float | None, # amount received — None if this is a debit
        "balance"   : float | None, # always None for credit cards
        "confidence": float         # 0.95 normal transaction
                                    # 0.92 fee / surcharge / tax
                                    # 0.70 uncertain
    }}
    \"\"\"

════════════════════════════════════════════
RULES
════════════════════════════════════════════

- Raw Python only. No markdown fences. No imports except re (pre-injected).
- Exactly one of debit or credit per transaction. Never both. Never neither.
- Deduplicate on (date, details, debit, credit) — keep first occurrence.
- Return [] if text is clearly not from {institution}.

The function must begin with exactly: def extract_transactions(text: str) -> list:
"""