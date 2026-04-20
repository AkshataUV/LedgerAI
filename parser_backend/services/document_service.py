"""
services/document_service.py
────────────────────────────
Thin service layer used by the Streamlit app for document UI operations.
Processing pipeline is in processing_engine.py.

user_id is a UUID string (Supabase auth.users.id).
"""

import logging
import datetime
from db.connection import get_client

logger = logging.getLogger("ledgerai.document_service")


def create_document(
    user_id: str,
    file_name: str,
    file_path: str,
    is_password_protected: bool,
    password: str = None,
) -> int:
    """Insert document + optional password + audit — returns document_id."""
    sb = get_client()

    result = sb.table("documents").insert({
        "user_id": user_id,
        "file_name": file_name,
        "file_path": file_path,
        "is_password_protected": is_password_protected,
        "status": "UPLOADED",
    }).execute()

    document_id = result.data[0]["document_id"]

    if password:
        sb.table("document_password").insert({
            "document_id": document_id,
            "encrypted_password": password,
        }).execute()

    sb.table("document_upload_audit").insert({
        "document_id": document_id,
        "status": "UPLOADED",
    }).execute()

    logger.info("Created document_id=%s file=%s user_id=%s", document_id, file_name, user_id)
    return document_id


def get_user_documents(user_id: str) -> list:
    """Get all active documents for a user, most recent first."""
    sb = get_client()
    result = (
        sb.table("documents")
        .select(
            "document_id, file_name, status, created_at, "
            "transaction_parsed_type, processing_started_at, processing_completed_at"
        )
        .eq("user_id", user_id)
        .eq("is_active", True)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data or []
