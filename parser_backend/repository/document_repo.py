"""
repository/document_repo.py
────────────────────────────
All database operations for the documents table using the Supabase client.

Status values:
  UPLOADED | PASSWORD_REQUIRED | EXTRACTING_TEXT | IDENTIFYING_FORMAT
  PARSING_TRANSACTIONS | AWAITING_REVIEW | CATEGORIZING | POSTED | APPROVE | FAILED

user_id is a UUID string (Supabase auth.users.id).
"""

import json
import logging
from db.connection import get_client

logger = logging.getLogger("ledgerai.document_repo")


# ── READ ─────────────────────────────────────────────────

def get_document(document_id: int) -> dict | None:
    sb = get_client()
    result = sb.table("documents").select("*").eq("document_id", document_id).maybe_single().execute()
    return result.data


def get_document_password(document_id: int) -> str | None:
    sb = get_client()
    result = (
        sb.table("document_password")
        .select("encrypted_password")
        .eq("document_id", document_id)
        .maybe_single()
        .execute()
    )
    if result is None or not result.data:
        return None
    return result.data.get("encrypted_password")


# ── STATUS UPDATES ───────────────────────────────────────

def update_document_status(document_id: int, status: str):
    sb = get_client()
    sb.table("documents").update({"status": status}).eq("document_id", document_id).execute()


def update_processing_start(document_id: int):
    import datetime
    sb = get_client()
    sb.table("documents").update({
        "status": "EXTRACTING_TEXT",
        "processing_started_at": datetime.datetime.utcnow().isoformat(),
    }).eq("document_id", document_id).execute()


def update_processing_complete(document_id: int, parser_type: str):
    import datetime
    sb = get_client()
    sb.table("documents").update({
        "transaction_parsed_type": parser_type,
        "processing_completed_at": datetime.datetime.utcnow().isoformat(),
    }).eq("document_id", document_id).execute()


# ── LINKING ──────────────────────────────────────────────

def update_document_statement(document_id: int, statement_id: int):
    sb = get_client()
    sb.table("documents").update({"statement_id": statement_id}).eq("document_id", document_id).execute()


# ── AUDIT ────────────────────────────────────────────────

def insert_audit(document_id: int, status: str, error_message: str = None):
    if error_message and len(error_message) > 490:
        error_message = error_message[:490] + "..."
    sb = get_client()
    sb.table("document_upload_audit").insert({
        "document_id": document_id,
        "status": status,
        "error_message": error_message,
    }).execute()


# ── TEXT EXTRACTION ──────────────────────────────────────

def insert_text_extraction(document_id: int, extracted_text: str):
    sb = get_client()
    sb.table("document_text_extractions").insert({
        "document_id": document_id,
        "extraction_method": "PDF_TEXT",
        "extracted_text": extracted_text,
        "extraction_status": "SUCCESS",
    }).execute()


def get_text_extraction(document_id: int) -> str | None:
    sb = get_client()
    result = sb.table("document_text_extractions").select("extracted_text").eq("document_id", document_id).limit(1).execute()
    return result.data[0]["extracted_text"] if result.data else None


# ── STAGING TRANSACTIONS ────────────────────────────────

def insert_staging_transactions(
    document_id: int,
    user_id: str,
    code_txns: list,
    llm_txns: list,
    code_confidence: float,
    llm_confidence: float,
):
    sb = get_client()
    sb.table("ai_transactions_staging").insert([
        {
            "document_id": document_id,
            "user_id": user_id,
            "transaction_json": code_txns,
            "parser_type": "CODE",
            "overall_confidence": code_confidence,
        },
        {
            "document_id": document_id,
            "user_id": user_id,
            "transaction_json": llm_txns,
            "parser_type": "LLM",
            "overall_confidence": llm_confidence,
        },
    ]).execute()


def insert_staging_code_only(
    document_id: int,
    user_id: str,
    code_txns: list,
    confidence: float,
):
    """For ACTIVE formats — only CODE transactions, no LLM."""
    sb = get_client()
    sb.table("ai_transactions_staging").insert({
        "document_id": document_id,
        "user_id": user_id,
        "transaction_json": code_txns,
        "parser_type": "CODE",
        "overall_confidence": confidence,
    }).execute()


# ── REVIEW PAGE ─────────────────────────────────────────

def get_review_transactions(document_id: int) -> dict | None:
    """Get the winning parser's staging row for review screen."""
    sb = get_client()
    # First get the document to know which parser_type won
    doc_result = sb.table("documents").select("transaction_parsed_type").eq("document_id", document_id).maybe_single().execute()
    if not doc_result.data:
        return None
    parser_type = doc_result.data.get("transaction_parsed_type")
    if not parser_type:
        return None
    result = (
        sb.table("ai_transactions_staging")
        .select("*")
        .eq("document_id", document_id)
        .eq("parser_type", parser_type)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


# ── FINAL APPROVAL ──────────────────────────────────────

def insert_uncategorized_transactions(
    document_id: int,
    user_id: str,
    statement_id: int,
    staging_transaction_id: int,
    transactions: list,
    account_id: int = None,
):
    sb = get_client()
    rows = [
        {
            "user_id": user_id,
            "account_id": account_id,   # populated from documents.account_id if user linked
            "document_id": document_id,
            "staging_transaction_id": staging_transaction_id,
            "txn_date": txn.get("date"),
            "debit": txn.get("debit"),
            "credit": txn.get("credit"),
            "balance": txn.get("balance"),
            "details": (txn.get("details") or "")[:500],
        }
        for txn in transactions
    ]
    if rows:
        sb.table("uncategorized_transactions").insert(rows).execute()

    sb.table("documents").update({"status": "APPROVE"}).eq("document_id", document_id).execute()