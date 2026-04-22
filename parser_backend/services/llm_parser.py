"""
services/llm_parser.py
──────────────────────
STEP 4 METHOD 2 — Direct LLM transaction extraction.

Uses call_llm() which tries Gemini direct → Gemini via OpenRouter →
fallback model, so a single provider being overloaded never stalls the pipeline.
"""

import re
import json
import logging

from google.genai import types
from config import LLM_PARSER_MODEL
from services.llm_provider import call_llm   

logger = logging.getLogger("ledgerai.llm_parser")

_CHUNK_SIZE = 10


def parse_with_llm(full_text: str, identifier_json: dict) -> str:
    """
    Split full_text into page chunks, extract transactions per chunk,
    merge all results and return as a JSON array string.
    """
    doc_family  = identifier_json.get("document_family", "BANK_ACCOUNT_STATEMENT")
    doc_subtype = identifier_json.get("document_subtype", "")
    institution = identifier_json.get("institution_name", "Unknown")

    pages = [
        block.strip()
        for block in re.split(r'={80}', full_text)
        if block.strip() and not re.fullmatch(r'\s*PAGE\s+\d+\s*', block.strip(), re.IGNORECASE)
    ]
    if not pages:
        pages = [full_text]

    total_pages = len(pages)
    logger.info(
        "LLM parse start: family=%s institution=%s pages=%d chunk_size=%d",
        doc_family, institution, total_pages, _CHUNK_SIZE,
    )

    all_txns = []

    for chunk_start in range(0, total_pages, _CHUNK_SIZE):
        chunk_pages = pages[chunk_start: chunk_start + _CHUNK_SIZE]
        chunk_end   = chunk_start + len(chunk_pages)
        chunk_text  = "\n\n".join(chunk_pages)

        logger.info(
            "LLM chunk pages %d–%d of %d (text_len=%d)",
            chunk_start + 1, chunk_end, total_pages, len(chunk_text),
        )

        try:
            raw_response = _parse_chunk(
                chunk_text=chunk_text,
                doc_family=doc_family,
                doc_subtype=doc_subtype,
                institution=institution,
            )
            chunk_txns = _safe_parse_json(raw_response, chunk_start + 1, chunk_end)
            all_txns.extend(chunk_txns)
            logger.info(
                "LLM chunk pages %d–%d: extracted %d transactions (running total=%d)",
                chunk_start + 1, chunk_end, len(chunk_txns), len(all_txns),
            )
        except Exception as e:
            logger.error(
                "LLM chunk pages %d–%d FAILED: %s — skipping chunk",
                chunk_start + 1, chunk_end, e,
            )

    logger.info(
        "LLM parse complete: %d total transactions across %d pages",
        len(all_txns), total_pages,
    )

    return json.dumps(all_txns)


def parse_with_vision(pdf_bytes: bytes, identifier_json: dict, note: str = None) -> str:
    """
    Extract transactions using multimodal vision by sending PDF bytes.
    Falls back to text-only OpenRouter models if Gemini is unavailable.
    """
    doc_family  = identifier_json.get("document_family", "BANK_ACCOUNT_STATEMENT")
    doc_subtype = identifier_json.get("document_subtype", "")
    institution = identifier_json.get("institution_name", "Unknown")

    prompt = f"""
You are a financial data extraction engine with COMPUTER VISION capabilities.

Extract ALL transaction entries from the provided PDF document. 
The document might be a scan or have complex layout — use your vision capabilities to accurately read every row.

════════════════════════════════════════════
DOCUMENT INFO
════════════════════════════════════════════
Document Family: {doc_family}
Document Subtype: {doc_subtype}
Institution: {institution}
{f"USER NOTE: {note}" if note else ""}

════════════════════════════════════════════
RULES
════════════════════════════════════════════

1. Extract EVERY transaction row. Look for dates and amounts.
2. DETAILS field must contain ONLY the transaction description. 
   Clean up any noise like page numbers or header fragments.
3. Handle Indian number formats (1,00,000.00).
4. Normalize dates to YYYY-MM-DD.
5. DEBIT/CREDIT: Every transaction MUST have either debit or credit filled.

════════════════════════════════════════════
OUTPUT FORMAT (JSON ARRAY)
════════════════════════════════════════════

[
  {{
    "date": "YYYY-MM-DD",
    "details": "<transaction description only>",
    "debit": <float or null>,
    "credit": <float or null>,
    "balance": <float or null>,
    "confidence": <0.0 to 1.0>
  }}
]

Return ONLY the JSON array. No markdown. No explanation.
"""

    logger.info("Vision extraction starting for %s (note=%s)", institution, "YES" if note else "NO")

    parts = [
        types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        prompt,
    ]

    # call_llm handles fallback — if Gemini is down, OpenRouter text models
    # won't have the PDF bytes but the exception from call_llm will be raised
    # clearly so the caller can decide whether to retry or surface the error.
    return call_llm(parts=parts, temperature=0)


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_chunk(
    chunk_text: str,
    doc_family: str,
    doc_subtype: str,
    institution: str,
) -> str:
    prompt = f"""
You are a financial data extraction engine.

Extract ALL transaction entries from the provided document text.

════════════════════════════════════════════
DOCUMENT INFO
════════════════════════════════════════════
Document Family: {doc_family}
Document Subtype: {doc_subtype}
Institution: {institution}

════════════════════════════════════════════
RULES
════════════════════════════════════════════

1. Extract EVERY transaction row. A transaction starts with a date.
2. SKIP these entirely — they are NOT transactions:
   - Headers (Date, Particulars, Debit, Credit, Balance)
   - Footers (Page numbers, disclaimers, generated on...)
   - Summary rows (Opening Balance, Closing Balance, Total Debit/Credit)
   - Account info (Branch, IFSC, MICR, Account Number)
3. MULTI-LINE NARRATION — CRITICAL:
   - Many bank statements split a single transaction's description across 2 or 3 lines.
   - A continuation line does NOT start with a date and does NOT contain an amount or balance.
   - You MUST append continuation lines to the PREVIOUS transaction's "details" field using a single space.
   - NEVER create a separate transaction entry for a continuation line.
   - Example:
       Line 1: "01/04/2026  UPI/CR/123456/ACME    5000.00  95000.00"
       Line 2: "CORP PAYMENT APRIL"
     → details = "UPI/CR/123456/ACME CORP PAYMENT APRIL"  (one transaction, merged)
4. DETAILS field must contain ONLY the transaction narration/description:
   - Do NOT include dates, amounts, page numbers, or header text in details.
   - Do NOT include footer text, branch info, or account numbers in details.
   - Example GOOD: "NEFT CR ACME CORP SALARY"
   - Example BAD: "01/01/2025 NEFT CR ACME CORP 50000.00 Page 1 of 3"
5. Handle Indian number formats (1,00,000.00).
6. Normalize dates to YYYY-MM-DD.
7. DEBIT/CREDIT: Every transaction MUST have either debit or credit filled (not both None).
   - If running balance increases, the amount is credit.
   - If running balance decreases, the amount is debit.
   - If column headers say Withdrawal/Debit use those.
   - If column headers say Deposit/Credit use those.

════════════════════════════════════════════
OUTPUT FORMAT (JSON ARRAY)
════════════════════════════════════════════

[
  {{
    "date": "YYYY-MM-DD",
    "details": "<transaction description only, no dates/amounts/noise>",
    "debit": <float or null>,
    "credit": <float or null>,
    "balance": <float or null>,
    "confidence": <0.0 to 1.0>
  }}
]

════════════════════════════════════════════
DOCUMENT TEXT
════════════════════════════════════════════

{chunk_text}

════════════════════════════════════════════
Return ONLY the JSON array. No markdown. No explanation.
"""

    return call_llm(prompt=prompt, temperature=0)


def _safe_parse_json(response: str, page_from: int, page_to: int) -> list:
    cleaned = response.replace("```json", "").replace("```", "").strip()
    match = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(
                "LLM chunk pages %d–%d: JSON parse failed (%s). Preview: %s",
                page_from, page_to, e, cleaned[:300],
            )
            return []
    logger.warning(
        "LLM chunk pages %d–%d: no JSON array found in response. Preview: %s",
        page_from, page_to, cleaned[:300],
    )
    return []