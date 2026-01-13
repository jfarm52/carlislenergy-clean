"""
Normalization helpers shared across the bill intake pipeline.

Centralizing these prevents circular imports between DB/storage and extractors.
"""

from __future__ import annotations

import re


def normalize_account_number(raw):
    """Strip spaces, punctuation; return digits only. Handle special cases."""
    if not raw:
        return "Unknown"
    raw_str = str(raw).strip()
    # Keep placeholder values as-is
    if raw_str.upper() in ("UNKNOWN", "N/A", "NA", "NONE", ""):
        return "Unknown"
    # Extract digits only
    digits = re.sub(r"[^0-9]", "", raw_str)
    # If no digits found, return "Unknown"
    if not digits:
        return "Unknown"
    return digits


def normalize_meter_number(raw):
    """Strip spaces, punctuation; return digits only. Handle special cases."""
    if not raw:
        return "Unknown"
    raw_str = str(raw).strip()
    # Keep placeholder values as-is
    if raw_str.upper() in ("UNKNOWN", "N/A", "NA", "NONE", "PRIMARY", ""):
        return "Unknown"
    # Extract digits only
    digits = re.sub(r"[^0-9]", "", raw_str)
    # If no digits found, return "Unknown"
    if not digits:
        return "Unknown"
    return digits


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


