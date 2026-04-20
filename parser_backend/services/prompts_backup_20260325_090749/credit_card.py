def build_prompt(identifier_json: dict, text_sample: str) -> str:
    institution = identifier_json.get("institution_name", "Unknown")

    # Get parsing hints to provide context
    hints = identifier_json.get("parsing_hints", {})
    skip_labels = hints.get("summary_section_labels", [])
    layout = hints.get("layout_type", "SINGLE_COLUMN")

    skip_instruction = ""
    if skip_labels:
        skip_instruction = f"\n- Skip lines starting with: {', '.join(skip_labels[:10])}"

    return f"""
Write extract_transactions(text: str) -> list for this credit card statement.

SAMPLE:
{text_sample}

PARSING CONTEXT:
- Layout: {layout}
- Institution: {institution}{skip_instruction}

TASK:
Extract all transactions (date + description + amount).
Skip: headers, footers, summaries, offers, T&C text.

Classify debit vs credit:
- Credit: PAYMENT, REFUND, CREDIT, REVERSAL, WAIVER
- Debit: purchases, fees, charges

OUTPUT:
[{{"date": "DD/MM/YYYY", "details": str, "debit": float|None, "credit": float|None, "balance": None, "confidence": float}}]

RULES:
- Exactly one of debit/credit per transaction
- Deduplicate on (date, details, debit, credit)
- Confidence: 0.95 normal, 0.92 fees, 0.70 uncertain
- Raw Python only, no markdown
- Only use built-in types (dict, list, str, float, int, bool, None)
- Do NOT import typing, Optional, List, Dict - use lowercase dict, list instead
- Only import re if needed

Write the function now.
"""