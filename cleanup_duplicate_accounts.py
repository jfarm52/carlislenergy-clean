#!/usr/bin/env python3
"""
Script to find and merge duplicate utility accounts in the database.
"""

import os
import sys

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv("local.env")
    load_dotenv(".env.local")
    load_dotenv(".env")
except ImportError:
    # Manual loading if python-dotenv not available
    for env_file in ["local.env", ".env.local", ".env"]:
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, _, value = line.partition("=")
                        os.environ.setdefault(key.strip(), value.strip())

import psycopg2
from psycopg2.extras import RealDictCursor

from bill_intake.utils.normalization import normalize_account_number, normalize_utility_name

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    return psycopg2.connect(DATABASE_URL)


def find_duplicate_accounts():
    """Find accounts that should be merged based on normalized values."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all accounts
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
            return duplicates
    finally:
        conn.close()


def merge_duplicate_accounts(dry_run=True):
    """Merge duplicate accounts - keep the oldest, transfer bills/meters to it."""
    duplicates = find_duplicate_accounts()
    
    if not duplicates:
        print("✅ No duplicate accounts found!")
        return
    
    print(f"\n🔍 Found {len(duplicates)} sets of duplicate accounts:\n")
    
    for key, accounts in duplicates.items():
        project_id, norm_utility, norm_account = key
        print(f"  Project {project_id}: {norm_utility} / {norm_account}")
        for acc in accounts:
            print(f"    - ID {acc['id']}: '{acc['utility_name']}' / '{acc['account_number']}' (created {acc['created_at']})")
    
    if dry_run:
        print("\n⚠️  DRY RUN - No changes made. Run with --apply to fix.")
        return
    
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for key, accounts in duplicates.items():
                # Sort by created_at to keep the oldest
                accounts.sort(key=lambda x: x['created_at'] or '9999')
                keeper = accounts[0]
                to_delete = accounts[1:]
                
                print(f"\n📦 Merging into account ID {keeper['id']} ({keeper['utility_name']} / {keeper['account_number']})...")
                
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
                    
                    print(f"  ✅ Merged account ID {dup['id']} -> moved {bills_moved} bills, {meters_moved} meters, deleted duplicate")
                
                # Update the keeper's utility_name and account_number to normalized versions
                norm_utility = normalize_utility_name(keeper['utility_name'])
                norm_account = normalize_account_number(keeper['account_number'])
                cur.execute("""
                    UPDATE utility_accounts 
                    SET utility_name = %s, account_number = %s 
                    WHERE id = %s
                """, (norm_utility, norm_account, keeper['id']))
            
            conn.commit()
            print("\n✅ All duplicates merged successfully!")
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        raise
    finally:
        conn.close()


def show_all_accounts():
    """Show all accounts in the database."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT ua.id, ua.project_id, ua.utility_name, ua.account_number, ua.created_at,
                       COUNT(DISTINCT b.id) as bill_count,
                       COUNT(DISTINCT m.id) as meter_count
                FROM utility_accounts ua
                LEFT JOIN bills b ON b.account_id = ua.id
                LEFT JOIN meters m ON m.account_id = ua.id
                GROUP BY ua.id, ua.project_id, ua.utility_name, ua.account_number, ua.created_at
                ORDER BY ua.project_id, ua.utility_name
            """)
            accounts = cur.fetchall()
            
            print(f"\n📊 All accounts in database ({len(accounts)} total):\n")
            current_project = None
            for acc in accounts:
                if acc['project_id'] != current_project:
                    current_project = acc['project_id']
                    print(f"\n  Project: {current_project}")
                print(f"    ID {acc['id']}: {acc['utility_name']} / {acc['account_number']} - {acc['bill_count']} bills, {acc['meter_count']} meters")
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--apply":
        merge_duplicate_accounts(dry_run=False)
    elif len(sys.argv) > 1 and sys.argv[1] == "--list":
        show_all_accounts()
    else:
        print("🔍 Checking for duplicate accounts (dry run)...\n")
        merge_duplicate_accounts(dry_run=True)
        print("\n📋 To see all accounts: python cleanup_duplicate_accounts.py --list")
        print("🔧 To apply fixes: python cleanup_duplicate_accounts.py --apply")
