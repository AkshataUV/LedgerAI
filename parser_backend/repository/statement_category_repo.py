"""
repository/statement_category_repo.py
──────────────────────────────────────
CRUD for statement_categories table using the Supabase client.
Status values: ACTIVE | UNDER_REVIEW | DISABLED | EXPERIMENTAL
"""

import json
import logging
from db.connection import get_client

logger = logging.getLogger("ledgerai.statement_category_repo")


# ── FETCH ────────────────────────────────────────────────

def get_all_matchable_formats() -> list:
    """Fetch ACTIVE + UNDER_REVIEW + EXPERIMENTAL formats for matching."""
    sb = get_client()
    result = (
        sb.table("statement_categories")
        .select("*")
        .in_("status", ["ACTIVE", "UNDER_REVIEW", "EXPERIMENTAL"])
        .execute()
    )
    rows = result.data or []
    # statement_identifier comes back as a dict from jsonb — no need to json.loads
    # but guard in case it arrives as a string
    for row in rows:
        if isinstance(row.get("statement_identifier"), str):
            row["statement_identifier"] = json.loads(row["statement_identifier"])
    return rows


def get_active_formats() -> list:
    sb = get_client()
    result = sb.table("statement_categories").select("*").eq("status", "ACTIVE").execute()
    rows = result.data or []
    for row in rows:
        if isinstance(row.get("statement_identifier"), str):
            row["statement_identifier"] = json.loads(row["statement_identifier"])
    return rows


def get_statement_by_id(statement_id: int) -> dict | None:
    sb = get_client()
    result = (
        sb.table("statement_categories")
        .select("*")
        .eq("statement_id", statement_id)
        .maybe_single()
        .execute()
    )
    row = result.data
    if row and isinstance(row.get("statement_identifier"), str):
        row["statement_identifier"] = json.loads(row["statement_identifier"])
    return row


# ── INSERT ───────────────────────────────────────────────

def insert_statement_category(
    statement_type: str,
    format_name: str,
    institution_name: str,
    identifier_json: dict,
    extraction_logic: str,
    ifsc_code: str = None,
    threshold: float = 65.0,
) -> int:
    sb = get_client()
    result = sb.table("statement_categories").insert({
        "statement_type": statement_type,
        "format_name": format_name,
        "institution_name": institution_name,
        "ifsc_code": ifsc_code,
        "statement_identifier": identifier_json,  # supabase-py serialises dict → jsonb
        "extraction_logic": extraction_logic,
        "match_threshold": threshold,
        "logic_version": 1,
        "status": "UNDER_REVIEW",
    }).execute()
    return result.data[0]["statement_id"]


# ── STATUS ───────────────────────────────────────────────

def activate_statement_category(statement_id: int):
    import datetime
    sb = get_client()
    sb.table("statement_categories").update({
        "status": "ACTIVE",
        "last_verified_at": datetime.datetime.utcnow().isoformat(),
    }).eq("statement_id", statement_id).execute()
    logger.info("Statement %s → ACTIVE.", statement_id)


def update_statement_status(statement_id: int, status: str):
    sb = get_client()
    sb.table("statement_categories").update({"status": status}).eq("statement_id", statement_id).execute()


def update_extraction_logic(statement_id: int, new_logic: str):
    sb = get_client()
    # Increment logic_version — fetch current first
    current = sb.table("statement_categories").select("logic_version").eq("statement_id", statement_id).maybe_single().execute()
    current_version = (current.data or {}).get("logic_version", 1)
    sb.table("statement_categories").update({
        "extraction_logic": new_logic,
        "logic_version": current_version + 1,
    }).eq("statement_id", statement_id).execute()


def update_success_rate(statement_id: int, rate: float):
    import datetime
    sb = get_client()
    sb.table("statement_categories").update({
        "success_rate": rate,
        "last_verified_at": datetime.datetime.utcnow().isoformat(),
    }).eq("statement_id", statement_id).execute()


def get_formats_by_institution(
    normalised_institution_name: str,
    document_family: str,
) -> list:
    """
    Fetch all statement_categories rows that share the same normalised
    institution name AND document family.
 
    Used by _find_duplicate_format() to detect duplicate formats before
    inserting a new row into statement_categories.
 
    Args:
        normalised_institution_name: UPPERCASE normalised name, e.g. "HDFC BANK"
        document_family: e.g. "BANK_ACCOUNT_STATEMENT"
 
    Returns:
        List of row dicts (may be empty). Each dict contains at minimum:
        statement_id, institution_name, format_name, statement_identifier,
        status, match_threshold.
    """
    from db.connection import get_client
    sb = get_client()
 
    result = (
        sb.table("statement_categories")
        .select(
            "statement_id, institution_name, format_name, "
            "statement_identifier, status, match_threshold"
        )
        # institution_name is stored UPPERCASE — ilike handles any residual
        # case inconsistency in existing rows gracefully.
        .ilike("institution_name", normalised_institution_name)
        .eq("document_family", document_family)
        .execute()
    )
    return result.data or []