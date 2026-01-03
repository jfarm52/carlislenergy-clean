"""Database connection utilities for bill intake (psycopg2)."""

from __future__ import annotations

import os

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_connection():
    """Get a database connection."""
    # IMPORTANT: read from the environment at call-time (not import-time).
    # On some setups, dotenv loads after module imports; caching at import-time can go stale.
    db_url = os.environ.get("DATABASE_URL") or DATABASE_URL
    if not db_url:
        raise RuntimeError("DATABASE_URL not configured")
    return psycopg2.connect(db_url)


