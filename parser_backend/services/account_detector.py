"""
services/account_detector.py
─────────────────────────────
Account linking for uploaded documents.

Uses ONLY account_identifiers table — no accounts table join.
Filters strictly by user_id so each user sees only their own accounts.

Public functions:
  get_user_accounts(user_id)
      → queries account_identifiers WHERE user_id = ? AND is_active = true
      → returns list for the Review screen dropdown

  link_document_to_account(document_id, account_id)
      → sets documents.account_id = account_id
"""

import logging
from db.connection import get_client

logger = logging.getLogger("ledgerai.account_detector")


def get_user_accounts(user_id: str) -> list:
    """
    Fetch all active account_identifiers for this user.

    Returns list of dicts:
    [
        {
            "account_id":            42,
            "institution_name":      "HDFC BANK",
            "account_number_last4":  "8323",
            "account_number_masked": "XXXXXX8323",
            "card_last4":            None,
        },
        ...
    ]
    """
    try:
        sb = get_client()

        result = (
            sb.table("account_identifiers")
            .select(
                "account_id, institution_name, "
                "account_number_last4, account_number_masked, card_last4"
            )
            .eq("user_id", user_id)
            .eq("is_active", True)
            .execute()
        )
        rows = result.data or []

        if not rows:
            logger.info("get_user_accounts: no accounts for user=%s", user_id)
            return []

        # Deduplicate by account_id — keep first row per account
        seen = set()
        accounts = []
        for row in rows:
            aid = row.get("account_id")
            if aid in seen:
                continue
            seen.add(aid)
            accounts.append({
                "account_id":            aid,
                "institution_name":      row.get("institution_name") or "",
                "account_number_last4":  row.get("account_number_last4"),
                "account_number_masked": row.get("account_number_masked"),
                "card_last4":            row.get("card_last4"),
            })

        # Sort by institution name for consistent order
        accounts.sort(key=lambda x: x["institution_name"])

        logger.info(
            "get_user_accounts: user=%s  found=%d", user_id, len(accounts)
        )
        return accounts

    except Exception as exc:
        logger.warning("get_user_accounts: failed — %s", exc)
        return []


def link_document_to_account(document_id: int, account_id: int) -> None:
    """Set documents.account_id for this document."""
    try:
        sb = get_client()
        sb.table("documents").update(
            {"account_id": account_id}
        ).eq("document_id", document_id).execute()
        logger.info(
            "link_document_to_account: doc=%s → account_id=%s",
            document_id, account_id,
        )
    except Exception as exc:
        logger.warning(
            "link_document_to_account: failed for doc=%s: %s", document_id, exc
        )
        raise

def create_user_account(user_id: str, account_data: dict) -> dict:
    """
    Creates a new account and its identifier.
    account_data: {
        "institution_name": str,
        "account_name": str, (optional)
        "type": "BANK" | "CREDIT_CARD",
        "last4": str,
        "ifsc_code": str, (optional)
        "card_network": str (optional)
    }
    """
    try:
        sb = get_client()
        is_bank = account_data.get("type") == "BANK"
        is_credit = account_data.get("type") == "CREDIT_CARD"
        
        acc_name = account_data.get("account_name") or account_data.get("institution_name") or ("Bank Account" if is_bank else "Credit Card")
        
        # 1. Resolve Parent Account ID
        parent_name = "Bank Accounts" if is_bank else "Credit Cards"
        parent_account_id = None
        
        # Find template ID first
        tmpl_res = sb.table("coa_templates").select("template_id").eq("account_name", parent_name).maybe_single().execute()
        if tmpl_res.data:
            tmpl_id = tmpl_res.data["template_id"]
            # Find user's parent account for this template
            parent_acc_res = sb.table("accounts").select("account_id").eq("user_id", user_id).eq("template_id", tmpl_id).maybe_single().execute()
            if parent_acc_res.data:
                parent_account_id = parent_acc_res.data["account_id"]

        # 2. Create entry in accounts table
        new_acc = {
            "user_id": user_id,
            "account_name": acc_name,
            "account_type": "LIABILITY" if is_credit else "ASSET",
            "balance_nature": "DEBIT",
            "is_system_generated": False,
            "parent_account_id": parent_account_id,
            "is_active": True
        }
        
        acc_res = sb.table("accounts").insert(new_acc).execute()
        if not acc_res.data:
            raise Exception("Failed to create account record")
            
        account_id = acc_res.data[0]["account_id"]
        
        # 3. Create entry in account_identifiers table
        ident_data = {
            "account_id": account_id,
            "user_id": user_id,
            "institution_name": account_data.get("institution_name"),
            "is_primary": False,
            "is_active": True
        }
        
        if is_bank:
            ident_data["account_number_last4"] = account_data.get("last4")
            ident_data["ifsc_code"] = account_data.get("ifsc_code")
        elif is_credit:
            ident_data["card_last4"] = account_data.get("last4")
            ident_data["card_network"] = account_data.get("card_network")
            
        sb.table("account_identifiers").insert(ident_data).execute()
        
        return {
            "account_id": account_id,
            "institution_name": account_data.get("institution_name"),
            "account_number_last4": account_data.get("last4") if is_bank else None,
            "card_last4": account_data.get("last4") if is_credit else None,
        }

    except Exception as exc:
        logger.error("create_user_account failed: %s", exc)
        raise