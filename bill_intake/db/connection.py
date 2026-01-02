"""Database connection utilities for bill intake (psycopg2)."""

from __future__ import annotations

import os

import psycopg2

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_connection():
    """Get a database connection."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)


