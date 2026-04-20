"""
services/post_process.py
────────────────────────
Minimal universal post-processing applied to every transaction
the code parser produces, before comparison with LLM parser.

Two rules only — both are universal across ALL document families:

  1. GHOST ROW FILTER   — skip rows with dates far before the statement period
                          (interest calculation tables, historical schedule rows)

  2. DIRECTION FIX      — correct debit/credit using universal financial keywords
                          that mean the same thing in every Indian financial document

Nothing bank-specific. Nothing family-specific.
Call: cleaned = post_process(transactions)
"""

# import re
# from typing import List, Dict

# # ─────────────────────────────────────────────────────────────
# # RULE 1 — GHOST ROW FILTER
# # ─────────────────────────────────────────────────────────────
# # Rows whose date is more than 2 years before the statement's
# # own date range are never real transactions — they come from
# # interest calculation tables, historical amortisation schedules,
# # or copy-paste artifacts in the PDF.

# _DATE_YEAR = re.compile(r'/(\d{4})')


# def _max_year(transactions: list) -> int:
#     years = []
#     for t in transactions:
#         m = _DATE_YEAR.search(t.get("date", "") or "")
#         if m:
#             years.append(int(m.group(1)))
#     return max(years) if years else 2099


# def _is_ghost(txn: dict, max_yr: int) -> bool:
#     m = _DATE_YEAR.search(txn.get("date", "") or "")
#     if m and max_yr - int(m.group(1)) > 2:
#         return True
#     return False


# # ─────────────────────────────────────────────────────────────
# # RULE 2 — DIRECTION FIX
# # ─────────────────────────────────────────────────────────────
# # These keywords unambiguously mean CREDIT in every Indian
# # financial document — credit card, bank account, loan, wallet.
# # If the code parser assigned debit, we correct it.
# #
# # Similarly, these keywords unambiguously mean DEBIT.
# # If the code parser assigned credit, we correct it.

# _FORCE_CREDIT = re.compile(
#     r'\b(TRANSFERRED TO EMI|FUEL SURCHARGE WAIVER|'
#     r'PETROL SURCHARGE REV|PETROL SURCHARGE REVERSAL|'
#     r'PAYMENT RECEIVED|PAYMENT UPI|CASHBACK|'
#     r'REFUND|REVERSAL|WAIVER|REWARD|CREDIT NOTE|'
#     r'LOAN DISBURSEMENT|DISBURSED)\b',
#     re.IGNORECASE,
# )

# _FORCE_DEBIT = re.compile(
#     r'\b(UPI FUEL SURCHARGE|PROCESSING FEE|ANNUAL FEE|'
#     r'LATE PAYMENT FEE|FOREIGN CURRENCY MARKUP|'
#     r'GST|GOODS AND SERVICE TAX|GOODS & SERVICE TAX|'
#     r'INTEREST CHARGED|FINANCE CHARGE|OVERLIMIT FEE|'
#     r'EMI INSTALMENT|PREPAYMENT CHARGE)\b',
#     re.IGNORECASE,
# )


# def _fix_direction(txn: dict) -> dict:
#     details = txn.get("details", "") or ""

#     # Keyword says CREDIT but parser assigned debit → flip
#     if txn.get("debit") is not None and _FORCE_CREDIT.search(details):
#         txn["credit"] = txn.pop("debit")
#         txn["debit"] = None

#     # Keyword says DEBIT but parser assigned credit → flip
#     elif txn.get("credit") is not None and _FORCE_DEBIT.search(details):
#         txn["debit"] = txn.pop("credit")
#         txn["credit"] = None

#     return txn


# # ─────────────────────────────────────────────────────────────
# # MAIN ENTRY POINT
# # ─────────────────────────────────────────────────────────────

# def post_process(transactions: List[Dict]) -> List[Dict]:
#     """
#     Apply two universal rules to code parser output.
#     Works for ALL document families — no bank-specific logic.

#     Args:
#         transactions: raw list from execute_extraction_code()

#     Returns:
#         cleaned list ready for comparison with LLM parser
#     """
#     if not transactions:
#         return transactions

#     max_yr = _max_year(transactions)
#     cleaned = []

#     for txn in transactions:
#         try:
#             # Rule 1 — skip ghost rows
#             if _is_ghost(txn, max_yr):
#                 continue

#             # Rule 2 — fix direction
#             txn = _fix_direction(txn)

#             # Safety — skip if neither debit nor credit is set
#             if txn.get("debit") is None and txn.get("credit") is None:
#                 continue

#             cleaned.append(txn)

#         except Exception:
#             continue

#     return cleaned