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
                        UPDATE utility_meters SET utility_account_id = %s WHERE utility_account_id = %s
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


def merge_accounts_by_shared_meter(project_id=None):
    """
    Find and merge accounts that share the same meter number.
    
    This handles race conditions where parallel bill processing creates
    multiple accounts for the same meter (with different OCR'd account numbers).
    
    Rule: A meter can only belong to ONE account. If multiple accounts have
    the same meter, merge them into the oldest one.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Find meters that appear in multiple accounts within the same project
            query = """
                SELECT m.meter_number, a.project_id, 
                       array_agg(DISTINCT a.id ORDER BY a.created_at) as account_ids,
                       array_agg(DISTINCT a.account_number ORDER BY a.created_at) as account_numbers
                FROM utility_meters m
                JOIN utility_accounts a ON m.utility_account_id = a.id
                WHERE m.meter_number != 'Unknown' AND m.meter_number != 'Primary'
            """
            if project_id:
                query += " AND a.project_id = %s"
            query += """
                GROUP BY m.meter_number, a.project_id
                HAVING COUNT(DISTINCT a.id) > 1
            """
            
            if project_id:
                cur.execute(query, (project_id,))
            else:
                cur.execute(query)
            
            shared_meters = cur.fetchall()
            
            merged_count = 0
            details = []
            
            for row in shared_meters:
                meter_number = row['meter_number']
                proj_id = row['project_id']
                account_ids = row['account_ids']
                account_numbers = row['account_numbers']
                
                if len(account_ids) < 2:
                    continue
                
                # Keep the first (oldest) account, merge others into it
                keeper_id = account_ids[0]
                to_merge_ids = account_ids[1:]
                
                print(f"[maintenance] Meter '{meter_number}' found in {len(account_ids)} accounts: {account_numbers}")
                print(f"[maintenance] Keeping account {keeper_id}, merging {to_merge_ids}")
                
                detail = {
                    'project_id': proj_id,
                    'meter_number': meter_number,
                    'kept_account_id': keeper_id,
                    'merged_account_ids': []
                }
                
                for dup_id in to_merge_ids:
                    # Move all bills from duplicate account to keeper
                    cur.execute("""
                        UPDATE bills SET account_id = %s WHERE account_id = %s
                    """, (keeper_id, dup_id))
                    bills_moved = cur.rowcount
                    
                    # Move all meters from duplicate account to keeper
                    # (Note: some meters might already exist in keeper, handle gracefully)
                    cur.execute("""
                        UPDATE utility_meters SET utility_account_id = %s 
                        WHERE utility_account_id = %s 
                        AND meter_number NOT IN (
                            SELECT meter_number FROM utility_meters WHERE utility_account_id = %s
                        )
                    """, (keeper_id, dup_id, keeper_id))
                    meters_moved = cur.rowcount
                    
                    # Delete duplicate meters that already exist in keeper
                    cur.execute("""
                        DELETE FROM utility_meters 
                        WHERE utility_account_id = %s
                    """, (dup_id,))
                    
                    # Delete the duplicate account (cascade will clean up remaining refs)
                    cur.execute("""
                        DELETE FROM utility_accounts WHERE id = %s
                    """, (dup_id,))
                    
                    detail['merged_account_ids'].append({
                        'id': dup_id,
                        'bills_moved': bills_moved,
                        'meters_moved': meters_moved
                    })
                    merged_count += 1
                    print(f"[maintenance] Merged account {dup_id} into {keeper_id} ({bills_moved} bills, {meters_moved} meters)")
                
                details.append(detail)
            
            conn.commit()
            
            if merged_count > 0:
                print(f"[maintenance] Total accounts merged by shared meter: {merged_count}")
            else:
                print("[maintenance] No accounts with shared meters found")
            
            return {'merged': merged_count, 'details': details}
    except Exception as e:
        conn.rollback()
        print(f"[maintenance] Error merging accounts by meter: {e}")
        raise e
    finally:
        conn.close()
