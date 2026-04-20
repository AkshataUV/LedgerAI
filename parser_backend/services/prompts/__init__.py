"""
services/prompts/__init__.py
────────────────────────────
Prompt Registry — maps document family → prompt builder.

Usage:
    from services.prompts import get_prompt
    prompt = get_prompt(family, identifier_json, text_sample)
"""

from services.prompts.bank_statement import build_prompt as _bank
from services.prompts.credit_card import build_prompt as _credit_card
from services.prompts.wallet import build_prompt as _wallet
from services.prompts.loan import build_prompt as _loan
from services.prompts.investment import build_prompt as _investment
from services.prompts.demat import build_prompt as _demat

_REGISTRY = {
    "BANK_ACCOUNT_STATEMENT": _bank,
    "CREDIT_CARD_STATEMENT": _credit_card,
    "WALLET_STATEMENT": _wallet,
    "LOAN_STATEMENT": _loan,
    "INVESTMENT_STATEMENT": _investment,
    "DEMAT_STATEMENT": _demat,
}

# Families that fall back to the bank statement prompt
_BANK_FALLBACK_FAMILIES = {
    "OVERDRAFT_CASH_CREDIT_STATEMENT",
    "PAYMENT_GATEWAY_SETTLEMENT",
    "TAX_LEDGER_STATEMENT",
    "FOREX_STATEMENT",
    "ESCROW_STATEMENT",
    "GENERIC_STATEMENT_OF_ACCOUNT",
}


def get_prompt(document_family: str, identifier_json: dict, text_sample: str) -> str:
    """
    Return the fully-rendered prompt string for the given document family.
    Falls back to bank_statement prompt for unrecognised families.
    """
    builder = _REGISTRY.get(document_family)
    if builder is None and document_family in _BANK_FALLBACK_FAMILIES:
        builder = _bank
    if builder is None:
        builder = _bank  # ultimate fallback
    return builder(identifier_json, text_sample)
