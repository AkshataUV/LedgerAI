
import re

def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Optimized bank statement code generation prompt.
    Reduced from ~3500 to ~800 tokens while maintaining quality.
    """

    institution = identifier_json.get("institution_name", "Unknown")
    doc_family = identifier_json.get("document_family", "BANK_ACCOUNT_STATEMENT")

    # Get parsing hints to provide context
    hints = identifier_json.get("parsing_hints", {})
    layout = hints.get("layout_type", "SINGLE_COLUMN")
    skip_labels = hints.get("summary_section_labels", [])
    boundary_signals = hints.get("transaction_boundary_signals", ["DATE"])
    ref_pattern = hints.get("ref_no_pattern")

    skip_instruction = ""
    if skip_labels:
        skip_instruction = f"\n- Skip lines starting with: {', '.join(skip_labels[:10])}"

    ref_instruction = ""
    if ref_pattern:
        ref_instruction = f"\n- Strip reference numbers matching: {ref_pattern}"

    return f"""
Write extract_transactions(text: str) -> list for this bank statement.

DOCUMENT SAMPLE:
{text_sample}

PARSING CONTEXT:
- Layout: {layout}
- Institution: {institution}{skip_instruction}{ref_instruction}
- Transaction boundaries: {', '.join(boundary_signals)}

APPROACH:
1. Find transaction lines (have date + amounts, not headers/footers/summaries)
2. Handle multi-line transactions (merge lines without dates that follow a transaction)
3. Classify debit vs credit:
   - Use column headers if present (Debit/Credit, Withdrawal/Deposit)
   - Use balance changes (balance decreased = debit, increased = credit)
   - Use keywords (PAYMENT/REFUND = credit, purchases = debit)
4. Extract fields:
   - Date: normalize to YYYY-MM-DD (handle 2-digit years: 00-30→2000s, 31-99→1900s)
   - Details: description only, no dates/amounts/noise
   - Amounts: handle Indian format (1,00,000.00), strip currency symbols
   - Balance: rightmost amount or calculate from previous

OUTPUT FORMAT:
[{{"date": "YYYY-MM-DD", "details": str, "debit": float|None, "credit": float|None, "balance": float|None, "confidence": float}}]

RULES:
- Exactly one of debit/credit per transaction (never both, never neither)
- Deduplicate on (date, details, debit, credit)
- Skip: headers, footers, summaries (Opening/Closing Balance, Total Debit/Credit)
- Confidence: 0.95 normal, 0.92 fees/charges, 0.70 uncertain
- Raw Python only, no markdown
- Only use built-in types (dict, list, str, float, int, bool, None)
- Do NOT import typing, Optional, List, Dict - use lowercase dict, list instead
- Only import re if needed

Write the function now.
"""
