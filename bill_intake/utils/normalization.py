"""
Normalization helpers shared across the bill intake pipeline.

Centralizing these prevents circular imports between DB/storage and extractors.
"""

from __future__ import annotations

import re


def normalize_account_number(raw):
    """Strip spaces, punctuation; return digits only (or original falsy value)."""
    if not raw:
        return raw
    return re.sub(r"[^0-9]", "", str(raw))


def normalize_meter_number(raw):
    """Strip spaces, punctuation; return digits only (or original falsy value)."""
    if not raw:
        return raw
    return re.sub(r"[^0-9]", "", str(raw))


def normalize_utility_name(raw: str | None) -> str:
    """
    Normalize utility company names to a canonical form.

    This prevents duplicate accounts when the LLM returns different variations.
    """
    if not raw:
        return "Unknown"
    name = raw.strip().lower()

    # SCE aliases
    if "southern california edison" in name or name == "sce":
        return "Southern California Edison"

    # SDG&E aliases
    if "san diego gas" in name or name == "sdge" or name == "sdg&e":
        return "San Diego Gas & Electric"

    # LADWP aliases
    if "los angeles department of water" in name or name == "ladwp":
        return "LADWP"

    # PG&E aliases
    if "pacific gas" in name or name == "pge" or name == "pg&e":
        return "Pacific Gas & Electric"

    # Return original with proper casing if no match
    return raw.strip()


