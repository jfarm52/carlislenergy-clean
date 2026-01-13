"""Maintenance utilities for bill intake DB (cleanup of empty accounts, etc)."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.utils.normalization import normalize_account_number, normalize_utility_name


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


def merge_duplicate_accounts(project_id=None):
    """
    Find and merge duplicate accounts based on normalized utility name and account number.
    If project_id is provided, only merge duplicates for that project.
    Returns dict with 'merged' count and 'details' list.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all accounts (optionally filtered by project)
            if project_id:
                cur.execute("""
                    SELECT id, project_id, utility_name, account_number, created_at
                    FROM utility_accounts
                    WHERE project_id = %s
                    ORDER BY project_id, utility_name, account_number
                """, (project_id,))
            else:
                cur.execute("""
                    SELECT id, project_id, utility_name, account_number, created_at
                    FROM utility_accounts
                    ORDER BY project_id, utility_name, account_number
                """)
            accounts = cur.fetchall()
            
            # Group by (project_id, normalized_utility, normalized_account)
            groups = {}
            for acc in accounts:
                norm_utility = normalize_utility_name(acc['utility_name'])
                norm_account = normalize_account_number(acc['account_number'])
                key = (acc['project_id'], norm_utility, norm_account)
                if key not in groups:
                    groups[key] = []
                groups[key].append(acc)
            
            # Find groups with duplicates
            duplicates = {k: v for k, v in groups.items() if len(v) > 1}
            
            merged_count = 0
            details = []
            
            for key, accs in duplicates.items():
                proj_id, norm_utility, norm_account = key
                # Sort by created_at to keep the oldest
                accs.sort(key=lambda x: x['created_at'] or '9999')
                keeper = accs[0]
                to_delete = accs[1:]
                
                detail = {
                    'project_id': proj_id,
                    'utility': norm_utility,
                    'account': norm_account,
                    'kept_id': keeper['id'],
                    'merged_ids': []
                }
                
                for dup in to_delete:
                    # Update bills to point to keeper account
                    cur.execute("""
                        UPDATE bills SET account_id = %s WHERE account_id = %s
                    """, (keeper['id'], dup['id']))
                    bills_moved = cur.rowcount
                    
                    # Update meters to point to keeper account
                    cur.execute("""
                        UPDATE meters SET account_id = %s WHERE account_id = %s
                    """, (keeper['id'], dup['id']))
                    meters_moved = cur.rowcount
                    
                    # Delete the duplicate account
                    cur.execute("""
                        DELETE FROM utility_accounts WHERE id = %s
                    """, (dup['id'],))
                    
                    detail['merged_ids'].append({
                        'id': dup['id'],
                        'bills_moved': bills_moved,
                        'meters_moved': meters_moved
                    })
                    merged_count += 1
                    print(f"[maintenance] Merged account {dup['id']} into {keeper['id']} ({bills_moved} bills, {meters_moved} meters)")
                
                # Update the keeper's utility_name and account_number to normalized versions
                cur.execute("""
                    UPDATE utility_accounts 
                    SET utility_name = %s, account_number = %s 
                    WHERE id = %s
                """, (norm_utility, norm_account, keeper['id']))
                
                details.append(detail)
            
            conn.commit()
            
            if merged_count > 0:
                print(f"[maintenance] Total duplicate accounts merged: {merged_count}")
            else:
                print("[maintenance] No duplicate accounts found")
            
            return {'merged': merged_count, 'details': details}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
