
import re

def build_prompt(identifier_json: dict, text_sample: str) -> str:
    """
    Universal intelligent code parser prompt.
    
    This prompt generates code that has LLM-like intelligence:
    - Learns structure from the document itself
    - No bank-specific assumptions
    - Adapts to any format dynamically
    - Uses semantic understanding encoded into logic
    """
    
    institution = identifier_json.get("institution_name", "Unknown")
    doc_family = identifier_json.get("document_family", "BANK_ACCOUNT_STATEMENT")

    return f"""
You are an expert Python code generator specializing in intelligent document parsing.

Generate a universal transaction extraction function that works on ANY bank statement format
by learning the structure from the document itself - like a human would.

════════════════════════════════════════════
DOCUMENT CONTEXT
════════════════════════════════════════════

Institution: {institution}
Document Type: {doc_family}

SAMPLE FROM ACTUAL DOCUMENT:

{text_sample}

════════════════════════════════════════════
INTELLIGENCE FRAMEWORK
════════════════════════════════════════════

Your generated code must have INTELLIGENCE - the ability to:

1. LEARN structure from the document (don't assume)
2. ADAPT to different formats dynamically
3. UNDERSTAND context like a human reader
4. RECOVER from ambiguity gracefully

Think of this as: "How would a human extract transactions if they'd never seen 
this bank's format before?" They would:
- Look for patterns (dates, amounts, balances)
- Understand meaning (this is a header, that's a transaction)
- Learn from examples (first few rows teach me the structure)
- Adapt when the pattern changes

Your code must do the same.

════════════════════════════════════════════
PHASE 1: PATTERN DISCOVERY
════════════════════════════════════════════

The code must FIRST analyze the document to discover its structure.

STEP 1A: Find all date patterns
- Scan entire text for date-like strings
- Detect which format is used: DD/MM/YY, DD/MM/YYYY, DD-MM-YYYY, DD MMM YYYY
- Build a date regex that matches THIS document's format

STEP 1B: Find all amount patterns  
- Scan for number patterns: 1,234.56 or 1234.56 or ₹1,234.56
- Detect if Indian format (1,00,000) or Western (10,000)
- Identify currency symbols used

STEP 1C: Identify transaction lines
- Transaction = line with BOTH a date AND numeric amounts
- NOT a transaction = headers, footers, summaries, account info
- Learn: What do transaction lines look like in THIS document?

STEP 1D: Discover column structure
- Collect first 5-10 transaction lines
- Analyze: where does date appear? Where are amounts? Where is balance?
- Learn column positions or patterns

STEP 1E: Determine debit/credit logic
- Method 1: Look for column headers (Debit, Credit, Withdrawal, Deposit)
- Method 2: Look for Dr/Cr suffixes after amounts
- Method 3: Infer from balance changes:
  * If balance increases → amount is credit
  * If balance decreases → amount is debit
- Method 4: Single amount column (credit cards):
  * "PAYMENT" keyword → credit
  * Purchase descriptions → debit

════════════════════════════════════════════
PHASE 2: INTELLIGENT EXTRACTION
════════════════════════════════════════════

Once structure is learned, extract using that knowledge.

STEP 2A: Line-by-line classification
For each line, determine its TYPE:

TYPE 1: HEADER
- Contains keywords: "Date", "Particulars", "Debit", "Credit", "Balance", "Narration"
- Usually appears at start of pages
- May have decorative lines (====, ----)
- Action: SKIP

TYPE 2: TRANSACTION
- Starts with a date (matches discovered date pattern)
- Contains amount(s) (matches discovered amount pattern)
- Has meaningful description text
- Action: EXTRACT

TYPE 3: CONTINUATION
- Does NOT start with a date
- Follows a transaction line
- Contains text or amounts
- Action: MERGE with previous transaction

TYPE 4: SUMMARY/FOOTER
- Keywords: "Opening Balance", "Closing Balance", "Grand Total", "Total Debit", 
  "Total Credit", "Page", "Generated on", "This is computer generated"
- Action: SKIP

TYPE 5: ACCOUNT INFO
- Keywords: "Account Number", "IFSC", "Branch", "Customer ID", "MICR"
- Action: SKIP

TYPE 6: EMPTY/JUNK
- Whitespace only or decorative characters
- Action: SKIP

STEP 2B: Field extraction with intelligence

For each TRANSACTION line:

DATE:
- Extract using discovered date pattern
- Parse to components (day, month, year)
- Handle 2-digit years intelligently:
  * 00-30 → 2000-2030
  * 31-99 → 1931-1999
- Normalize to YYYY-MM-DD

DESCRIPTION:
- Everything between date and first amount
- OR everything that's not date/amount if single-column description
- Merge continuation lines (lines without dates that follow)
- Clean: strip extra whitespace, remove multiple spaces

AMOUNTS (INTELLIGENT DETECTION):
- Extract all numbers that look like money amounts
- Ignore: reference numbers, phone numbers, account numbers
  * Amount patterns: end with .00 or .XX, have commas in correct positions
  * Not amounts: 8-16 digits with no decimals (likely IDs)

DEBIT/CREDIT (INTELLIGENT CLASSIFICATION):
- If two amount columns detected → left is debit, right is credit (or vice versa)
  * Identify by analyzing first few transactions:
  * Column that increases balance → credit
  * Column that decreases balance → debit
- If Dr/Cr suffix detected → use that
- If one amount column (credit cards) → classify by keywords:
  * Credit keywords: "PAYMENT", "REFUND", "CREDIT", "CR", "REVERSAL"
  * Debit keywords: everything else (purchases)
- If balance direction detected:
  * balance_current < balance_previous → debit
  * balance_current > balance_previous → credit

BALANCE:
- Usually rightmost amount in the row
- May have Cr/Dr suffix indicating type
- If absent, calculate from previous balance + credit - debit

════════════════════════════════════════════
PHASE 3: CONTEXT UNDERSTANDING
════════════════════════════════════════════

The code must understand CONTEXT like a human:

CONTEXT 1: Multi-line transactions
If current line has no date BUT:
- Previous line was a transaction
- Current line has text or amounts
→ This is a continuation, merge it

CONTEXT 2: Transaction type indicators
Some statements have type labels:
- "DEP TFR" or "WDL TFR" on separate line before transaction
- "TFR", "CASH", "CHQ" codes in transaction
→ Use these as hints for classification

CONTEXT 3: Amount zero handling  
If one amount is "0" or "0.00" and other is non-zero:
→ The zero is an empty cell, treat as None (not 0.0)

CONTEXT 4: Duplicate detection
Same date + description + amounts appearing multiple times:
→ Likely duplicate, keep first occurrence only

CONTEXT 5: Summary row detection
Row with description exactly matching:
- "Opening Balance", "Closing Balance", "Grand Total", "GRAND TOTAL"
- "Total Withdrawals", "Total Deposits", "Balance B/F", "Balance C/F"
→ NOT a transaction, skip it

════════════════════════════════════════════
PHASE 4: ERROR RECOVERY
════════════════════════════════════════════

The code must handle ambiguity gracefully:

RECOVERY 1: Unclear date
- Try multiple date formats
- If still fails, mark confidence = 0.70

RECOVERY 2: Amount ambiguity
- If can't determine debit vs credit with certainty
- Use balance direction as fallback
- If still unclear, mark confidence = 0.70

RECOVERY 3: Missing fields
- If balance is missing → leave as None (don't guess)
- If description is missing → use "Unknown transaction"
- Continue processing, don't crash

RECOVERY 4: Format changes mid-document
- Re-run pattern discovery every N lines
- Adapt if column positions change between pages

════════════════════════════════════════════
CODE STRUCTURE
════════════════════════════════════════════

Your generated code should follow this structure:

def extract_transactions(text: str) -> list:
    \"\"\"
    Universal intelligent transaction extractor.
    Works on any bank statement by learning its structure.
    \"\"\"
    
    # ============================================
    # PHASE 1: DISCOVER STRUCTURE
    # ============================================
    
    def discover_date_pattern(text):
        \"\"\"Find what date format this document uses.\"\"\"
        # Try common patterns, return the one that matches most
        pass
    
    def discover_amount_pattern(text):
        \"\"\"Find how amounts are formatted in this document.\"\"\"
        pass
    
    def discover_transaction_pattern(text):
        \"\"\"Learn what transaction lines look like.\"\"\"
        # Find lines with both dates and amounts
        # Return pattern characteristics
        pass
    
    def discover_column_structure(sample_transactions):
        \"\"\"Learn column positions from first few transactions.\"\"\"
        # Analyze where dates, amounts, balance appear
        # Return column mapping
        pass
    
    def discover_debit_credit_logic(sample_transactions):
        \"\"\"Figure out how to classify debit vs credit.\"\"\"
        # Check for headers, Dr/Cr suffixes, or infer from balance
        pass
    
    # Run discovery
    date_pattern = discover_date_pattern(text)
    amount_pattern = discover_amount_pattern(text)
    transaction_pattern = discover_transaction_pattern(text)
    
    # Get sample transactions for structure learning
    sample_lines = [line for line in text.split('\\n') 
                   if matches_transaction_pattern(line)][:10]
    
    column_structure = discover_column_structure(sample_lines)
    debit_credit_logic = discover_debit_credit_logic(sample_lines)
    
    # ============================================
    # PHASE 2: EXTRACT TRANSACTIONS
    # ============================================
    
    def classify_line(line, prev_line):
        \"\"\"Determine line type: TRANSACTION, CONTINUATION, HEADER, etc.\"\"\"
        pass
    
    def extract_date(line, pattern):
        \"\"\"Extract and normalize date.\"\"\"
        pass
    
    def extract_description(line, column_structure):
        \"\"\"Extract transaction description.\"\"\"
        pass
    
    def extract_amounts(line, column_structure):
        \"\"\"Extract debit, credit, balance.\"\"\"
        pass
    
    def classify_amount(amount, description, balance_change, logic):
        \"\"\"Determine if amount is debit or credit.\"\"\"
        pass
    
    # Process lines
    lines = text.split('\\n')
    transactions = []
    current_txn = None
    
    for i, line in enumerate(lines):
        prev_line = lines[i-1] if i > 0 else ""
        line_type = classify_line(line, prev_line)
        
        if line_type == "TRANSACTION":
            # Extract fields
            date = extract_date(line, date_pattern)
            description = extract_description(line, column_structure)
            debit, credit, balance = extract_amounts(line, column_structure)
            
            current_txn = {{
                "date": date,
                "details": description,
                "debit": debit,
                "credit": credit,
                "balance": balance,
                "confidence": 0.95
            }}
            transactions.append(current_txn)
            
        elif line_type == "CONTINUATION" and current_txn:
            # Merge with previous transaction
            current_txn["details"] += " " + line.strip()
            
        # Skip other line types
    
    # ============================================
    # PHASE 3: POST-PROCESSING
    # ============================================
    
    def deduplicate(transactions):
        \"\"\"Remove exact duplicates.\"\"\"
        pass
    
    def validate_transactions(transactions):
        \"\"\"Check consistency, adjust confidence.\"\"\"
        pass
    
    transactions = deduplicate(transactions)
    transactions = validate_transactions(transactions)
    
    return transactions

════════════════════════════════════════════
OUTPUT FORMAT
════════════════════════════════════════════

Each transaction must be a dict:
{{
    "date": "YYYY-MM-DD",           # Normalized date
    "details": str,                 # Transaction description
    "debit": float | None,          # Money out (None if credit)
    "credit": float | None,         # Money in (None if debit)
    "balance": float | None,        # Running balance (None if unavailable)
    "confidence": float             # 0.95 = high, 0.92 = medium, 0.70 = low
}}

RULES:
- Exactly ONE of debit/credit is filled (never both, never neither)
- Handle Indian number format: 1,00,000.00
- Strip currency symbols: ₹, Rs., INR
- Dates normalized to YYYY-MM-DD with 4-digit year
- Deduplicate on (date, details, debit, credit)
- Return [] if no transactions found

════════════════════════════════════════════
CRITICAL REQUIREMENTS
════════════════════════════════════════════

1. NO BANK-SPECIFIC CODE
   - Do NOT write "if bank == HDFC" or "if institution == SBI"
   - ALL logic must be pattern-based and adaptive
   - Code must work on banks you've never seen before

2. LEARN, DON'T ASSUME
   - Do NOT assume column positions
   - Do NOT assume date formats
   - DISCOVER everything from the document

3. INTELLIGENT CLASSIFICATION
   - Use semantic keywords: "PAYMENT" vs "PURCHASE"
   - Use balance changes: increased vs decreased
   - Use context: continuation lines, type indicators

4. GRACEFUL DEGRADATION
   - If unsure → mark low confidence, don't crash
   - If field missing → None, don't skip transaction
   - If format changes → re-learn, adapt

5. RAW PYTHON ONLY
   - No markdown fences (```python)
   - Only import re (pre-injected)
   - No external libraries
   - Start with: def extract_transactions(text: str) -> list:

════════════════════════════════════════════
INTELLIGENCE CHECKLIST
════════════════════════════════════════════

Your code must demonstrate intelligence by:

✓ Discovering date format from document (not hardcoded)
✓ Discovering amount format from document
✓ Learning column structure from first few transactions
✓ Classifying lines by meaning (header vs transaction vs footer)
✓ Understanding continuations (merge multi-line transactions)
✓ Inferring debit/credit when not explicit
✓ Handling format variations within same document
✓ Recovering from ambiguous or missing data
✓ Adapting to credit cards, bank statements, wallets equally well

This is not template-matching. This is intelligent parsing.

════════════════════════════════════════════
FINAL INSTRUCTION
════════════════════════════════════════════

Generate the extract_transactions function now.

Study the SAMPLE text above carefully - that's your training data.
Write code that could handle this document AND any other bank's format.

Remember: A human could extract transactions from ANY statement after reading 
a few lines. Your code must have that same capability.

Output ONLY the Python function. No explanation. No markdown fences.
"""