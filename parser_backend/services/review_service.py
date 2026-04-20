"""
services/review_service.py
──────────────────────────
Handles the human review screen data layer using the Supabase client.
user_id is a UUID string (Supabase auth.users.id).
"""

import logging
from db.connection import get_client
from repository.document_repo import (
    get_review_transactions,
    insert_uncategorized_transactions,
    update_document_status,
    insert_audit,
)
from repository.statement_category_repo import activate_statement_category

logger = logging.getLogger("ledgerai.review_service")


def get_document_for_review(document_id: int) -> dict | None:
    """
    Fetch all data needed for the review screen.
    Returns dict with document info + transactions.
    """
    sb = get_client()

    # Get document with joined institution name
    doc_result = (
        sb.table("documents")
        .select("*, statement_categories(institution_name, statement_identifier)")
        .eq("document_id", document_id)
        .maybe_single()
        .execute()
    )
    if not doc_result.data:
        return None

    doc = doc_result.data

    # Get both CODE and LLM staging rows
    staging_result = (
        sb.table("ai_transactions_staging")
        .select("staging_transaction_id, parser_type, transaction_json, overall_confidence")
        .eq("document_id", document_id)
        .order("parser_type")
        .execute()
    )
    staging_rows = staging_result.data or []

    code_txns = []
    llm_txns = []
    code_staging_id = None
    llm_staging_id = None

    for row in staging_rows:
        txn_data = row["transaction_json"]
        if isinstance(txn_data, str):
            import json
            txn_data = json.loads(txn_data)

        if row["parser_type"] == "CODE":
            code_txns = txn_data
            code_staging_id = row["staging_transaction_id"]
        elif row["parser_type"] == "LLM":
            llm_txns = txn_data
            llm_staging_id = row["staging_transaction_id"]

    return {
        "document": doc,
        "code_transactions": code_txns,
        "llm_transactions": llm_txns,
        "code_staging_id": code_staging_id,
        "llm_staging_id": llm_staging_id,
        "final_parser": doc.get("transaction_parsed_type"),
    }


def approve_transactions(
    document_id: int,
    user_id: str,
    statement_id: int,
    staging_transaction_id: int,
    transactions: list,
):
    """Move approved transactions to uncategorized_transactions and activate the statement format."""
    insert_uncategorized_transactions(
        document_id=document_id,
        user_id=user_id,
        statement_id=statement_id,
        staging_transaction_id=staging_transaction_id,
        transactions=transactions,
    )

    # Activate the statement format so future documents use fast path (CODE only)
    if statement_id:
        activate_statement_category(statement_id)
        logger.info("Activated statement_id=%s → ACTIVE (fast path enabled)", statement_id)

    insert_audit(document_id, "APPROVE")
    logger.info("Approved %d transactions for document_id=%s.", len(transactions), document_id)