"""Maintenance utilities for bill intake DB (cleanup of empty accounts, etc)."""

from __future__ import annotations

from bill_intake.db.connection import get_connection


def delete_account_if_empty(account_id):
    """
    Delete an account if it has no bills.
    Returns True if account was deleted, False otherwise.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bills WHERE account_id = %s", (account_id,))
            bill_count = cur.fetchone()[0]

            if bill_count == 0:
                cur.execute("DELETE FROM utility_accounts WHERE id = %s", (account_id,))
                conn.commit()
                print(f"[bills_db] Deleted empty account {account_id} with no bills")
                return True
            return False
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def delete_all_empty_accounts(project_id):
    """
    Delete all accounts in a project that have no bills.
    Returns count of accounts deleted.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ua.id, ua.account_number
                FROM utility_accounts ua
                LEFT JOIN bills b ON ua.id = b.account_id
                WHERE ua.project_id = %s
                GROUP BY ua.id, ua.account_number
                HAVING COUNT(b.id) = 0
                """,
                (project_id,),
            )
            empty_accounts = cur.fetchall()

            deleted_count = 0
            for account_row in empty_accounts:
                account_id = account_row[0]
                account_number = account_row[1]
                cur.execute("DELETE FROM utility_accounts WHERE id = %s", (account_id,))
                deleted_count += 1
                print(
                    f"[bills_db] Deleted empty account {account_id} (account_number={account_number}) with no bills"
                )

            conn.commit()
            if deleted_count > 0:
                print(f"[bills_db] Total empty accounts deleted: {deleted_count}")
            return deleted_count
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


