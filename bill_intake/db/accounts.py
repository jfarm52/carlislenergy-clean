"""DB operations for `utility_accounts`."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.utils.normalization import normalize_account_number, normalize_utility_name


def _account_similarity(a: str, b: str) -> float:
    """
    Calculate similarity ratio between two account numbers.
    Returns 1.0 for identical, 0.0 for completely different.
    Handles OCR errors like '8001053647' vs '3001053647' (1 char diff).
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    
    # Different lengths = probably different accounts
    if abs(len(a) - len(b)) > 1:
        return 0.0
    
    # Same length: count matching characters
    if len(a) == len(b):
        matches = sum(1 for i in range(len(a)) if a[i] == b[i])
        return matches / len(a)
    
    # Length differs by 1: use simple ratio
    shorter, longer = (a, b) if len(a) < len(b) else (b, a)
    matches = sum(1 for c in shorter if c in longer)
    return matches / len(longer)


def get_utility_accounts_for_project(project_id, service_filter=None):
    """Get all utility accounts for a project.

    Args:
        project_id: The project ID
        service_filter: Optional filter ('electric' filters to accounts with electric/combined bills)
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if service_filter == "electric":
                cur.execute(
                    """
                    SELECT DISTINCT a.id, a.project_id, a.utility_name, a.account_number, a.created_at
                    FROM utility_accounts a
                    JOIN bills b ON b.account_id = a.id
                    JOIN utility_bill_files ubf ON b.bill_file_id = ubf.id
                    WHERE a.project_id = %s
                      AND (ubf.service_type IN ('electric', 'combined') OR ubf.service_type IS NULL)
                      AND b.total_kwh > 0
                    ORDER BY a.utility_name
                    """,
                    (project_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, project_id, utility_name, account_number, created_at
                    FROM utility_accounts
                    WHERE project_id = %s
                    ORDER BY utility_name
                    """,
                    (project_id,),
                )
            return cur.fetchall()
    finally:
        conn.close()


def upsert_utility_account(project_id, utility_name, account_number):
    """Find or create a utility account. Returns account ID.
    
    Uses fuzzy matching to handle OCR errors - if an account number is 90%+
    similar to an existing one (same utility), returns the existing account
    instead of creating a duplicate.
    """
    utility_name = normalize_utility_name(utility_name)
    account_number = normalize_account_number(account_number)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Check for exact match first
            cur.execute(
                """
                SELECT id FROM utility_accounts
                WHERE project_id = %s AND utility_name = %s AND account_number = %s
                """,
                (project_id, utility_name, account_number),
            )
            row = cur.fetchone()
            if row:
                return row["id"]

            # 2. Check for fuzzy match (handles OCR errors like 8->3)
            # Only check accounts with the same utility
            cur.execute(
                """
                SELECT id, account_number FROM utility_accounts
                WHERE project_id = %s AND utility_name = %s
                """,
                (project_id, utility_name),
            )
            existing_accounts = cur.fetchall()
            
            for existing in existing_accounts:
                similarity = _account_similarity(account_number, existing["account_number"])
                if similarity >= 0.9:  # 90%+ match = probably same account with OCR error
                    print(f"[accounts] Fuzzy match: '{account_number}' ~ '{existing['account_number']}' ({similarity:.0%}) - using existing account {existing['id']}")
                    return existing["id"]

            # 3. No match found - create new account
            cur.execute(
                """
                INSERT INTO utility_accounts (project_id, utility_name, account_number)
                VALUES (%s, %s, %s)
                RETURNING id
                """,
                (project_id, utility_name, account_number),
            )
            result = cur.fetchone()
            conn.commit()
            return result["id"]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


