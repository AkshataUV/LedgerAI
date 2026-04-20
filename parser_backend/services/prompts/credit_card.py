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
Write extract_transactions(text: str) -> list for {institution} credit card statements.

SAMPLE FROM ONE STATEMENT (your code must work for ALL similar statements, not just this one):
{text_sample}

PARSING CONTEXT:
- Layout: {layout}
- Institution: {institution}{skip_instruction}

CRITICAL: Your code must be ROBUST and handle variations:
- Different transaction descriptions and merchants
- Varying amounts of whitespace and alignment
- Extra text, noise, or formatting differences between statements
- Different page breaks or promotional content
- Use the sample to understand the PATTERN, not to hardcode specific text

TASK:
Extract all transactions (date + description + amount).
Skip: headers, footers, summaries, offers, T&C text, page numbers, noise.

STEPS:
1. **Identify Boundary**: Use 'exclusion_markers' if provided in context to find where the transaction table ends.
2. **Find Rows**: Identify lines with a date pattern.
3. **The Anchor Strategy (CRITICAL)**:
   - In many Credit Card PDFs, the last number on a line is the 'Closing Balance'.
   - The second-to-last number is usually the 'Transaction Amount'.
   - Use this positional 'Anchor' logic to correctly extract the amount and AVOID sidebar noise like 'Total Outstanding' or 'Statement Date'.
4. **Handle multi-line**: merge continuation lines without dates CAREFULLY.
5. **Extract details AS-IS**: Keep reference IDs, merchant names, and prefixes. Do NOT strip anything.
6. **Classify debit vs credit**:
   - Credit (money in): PAYMENT, REFUND, CREDIT, REVERSAL, WAIVER, CASHBACK, or 'CR' suffix.
   - Debit (money out): all purchases, fees, charges, interest, or 'DR' suffix.
   - Most credit card transactions are charges (debits); payments to the card are credits.

OUTPUT:
[{{"date": "YYYY-MM-DD", "details": str, "debit": float|None, "credit": float|None, "balance": float|None, "confidence": float}}]

RULES:
- **PURSUE SIMPLICITY**: Use simple Python loops and string methods over complex, single-group regex.
- **NO HARDCODING**: Write generic code that handles repeating patterns.
- **EXACTLY ONE SIDE**: Every transaction must have exactly one of debit or credit.
- **SKIP NOISE**: Use your logic to ignore headers, footers, and sidebar boxes.
- **CONFIDENCE**: 0.95 for perfect pattern match.

Available imports: re, datetime, json.
Return ONLY the Python function.
"""