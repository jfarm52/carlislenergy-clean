"""DB operations for `utility_accounts`."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.utils.normalization import normalize_account_number, normalize_utility_name


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
    """Find or create a utility account. Returns account ID."""
    utility_name = normalize_utility_name(utility_name)
    account_number = normalize_account_number(account_number)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
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


