"""Idempotent schema migrations for bill intake tables."""

from __future__ import annotations


def migrate_all(conn) -> None:
    """Run all non-destructive migrations (add columns/indexes if missing)."""
    _migrate_add_review_columns(conn)
    _migrate_add_bills_tables(conn)
    _migrate_add_tou_columns(conn)
    _migrate_add_due_date_column(conn)
    _migrate_add_normalization_columns(conn)
    _migrate_add_sha256_column(conn)
    _migrate_add_service_type_column(conn)
    _migrate_backfill_service_type_from_payload(conn)


def _migrate_add_review_columns(conn):
    """Add review_status, extraction_payload, reviewed_at, reviewed_by columns if they don't exist."""
    try:
        with conn.cursor() as cur:
            # review_status
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'review_status'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding review_status column...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN review_status VARCHAR(50) DEFAULT 'pending'
                    """
                )

            # extraction_payload
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'extraction_payload'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding extraction_payload column...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN extraction_payload JSONB
                    """
                )

            # reviewed_at
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'reviewed_at'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding reviewed_at column...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN reviewed_at TIMESTAMP
                    """
                )

            # reviewed_by
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'reviewed_by'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding reviewed_by column...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN reviewed_by VARCHAR(255)
                    """
                )

            # bill_screenshots.mime_type
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'bill_screenshots' AND column_name = 'mime_type'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding mime_type column to bill_screenshots...")
                cur.execute(
                    """
                    ALTER TABLE bill_screenshots
                    ADD COLUMN mime_type VARCHAR(100)
                    """
                )

            conn.commit()
            print("[bills_db] Migration complete")
    except Exception as e:
        print(f"[bills_db] Migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_bills_tables(conn):
    """Add service_address column to utility_meters (older schema)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_meters' AND column_name = 'service_address'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding service_address column to utility_meters...")
                cur.execute(
                    """
                    ALTER TABLE utility_meters
                    ADD COLUMN service_address TEXT
                    """
                )

            conn.commit()
            print("[bills_db] Bills tables migration complete")
    except Exception as e:
        print(f"[bills_db] Bills tables migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_tou_columns(conn):
    """Add TOU columns to bills table and missing_fields to utility_bill_files."""
    try:
        with conn.cursor() as cur:
            tou_columns = [
                ("tou_on_kwh", "NUMERIC(12,2)"),
                ("tou_mid_kwh", "NUMERIC(12,2)"),
                ("tou_off_kwh", "NUMERIC(12,2)"),
                ("tou_super_off_kwh", "NUMERIC(12,2)"),
                ("tou_on_rate_dollars", "NUMERIC(10,6)"),
                ("tou_mid_rate_dollars", "NUMERIC(10,6)"),
                ("tou_off_rate_dollars", "NUMERIC(10,6)"),
                ("tou_super_off_rate_dollars", "NUMERIC(10,6)"),
                ("tou_on_cost", "NUMERIC(12,2)"),
                ("tou_mid_cost", "NUMERIC(12,2)"),
                ("tou_off_cost", "NUMERIC(12,2)"),
                ("tou_super_off_cost", "NUMERIC(12,2)"),
                ("blended_rate_dollars", "NUMERIC(10,6)"),
                ("avg_cost_per_day", "NUMERIC(12,2)"),
            ]

            for col_name, col_type in tou_columns:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'bills' AND column_name = %s
                    """,
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"[bills_db] Adding {col_name} column to bills...")
                    cur.execute(f"ALTER TABLE bills ADD COLUMN {col_name} {col_type}")

            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'missing_fields'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding missing_fields column to utility_bill_files...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN missing_fields JSONB DEFAULT '[]'::jsonb
                    """
                )

            conn.commit()
            print("[bills_db] TOU columns migration complete")
    except Exception as e:
        print(f"[bills_db] TOU columns migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_due_date_column(conn):
    """Add due_date column to bills table (older schemas)."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'bills' AND column_name = 'due_date'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding due_date column to bills...")
                cur.execute("ALTER TABLE bills ADD COLUMN due_date DATE")
            conn.commit()
            print("[bills_db] Due date column migration complete")
    except Exception as e:
        print(f"[bills_db] Due date column migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_normalization_columns(conn):
    """Add normalization columns to utility_bill_files for text-based processing."""
    try:
        with conn.cursor() as cur:
            normalization_columns = [
                ("normalized_text", "TEXT"),
                ("normalized_hash", "VARCHAR(64)"),
                ("processing_metrics", "JSONB"),
            ]

            for col_name, col_type in normalization_columns:
                cur.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'utility_bill_files' AND column_name = %s
                    """,
                    (col_name,),
                )
                if not cur.fetchone():
                    print(f"[bills_db] Adding {col_name} column to utility_bill_files...")
                    cur.execute(f"ALTER TABLE utility_bill_files ADD COLUMN {col_name} {col_type}")

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_utility_bill_files_hash
                ON utility_bill_files(normalized_hash)
                WHERE normalized_hash IS NOT NULL
                """
            )

            conn.commit()
            print("[bills_db] Normalization columns migration complete")
    except Exception as e:
        print(f"[bills_db] Normalization columns migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_sha256_column(conn):
    """Add sha256 column to utility_bill_files for upload deduplication."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'sha256'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding sha256 column to utility_bill_files...")
                cur.execute("ALTER TABLE utility_bill_files ADD COLUMN sha256 VARCHAR(64)")

            cur.execute(
                """
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'utility_bill_files'
                  AND constraint_name = 'uq_project_sha256'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding unique constraint on (project_id, sha256)...")
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_project_sha256
                    ON utility_bill_files(project_id, sha256)
                    WHERE sha256 IS NOT NULL
                    """
                )

            conn.commit()
            print("[bills_db] SHA256 column migration complete")
    except Exception as e:
        print(f"[bills_db] SHA256 column migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_add_service_type_column(conn):
    """Add service_type column to utility_bill_files and bills tables."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'utility_bill_files' AND column_name = 'service_type'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding service_type column to utility_bill_files...")
                cur.execute(
                    """
                    ALTER TABLE utility_bill_files
                    ADD COLUMN service_type VARCHAR(50) DEFAULT 'electric'
                    """
                )

            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'bills' AND column_name = 'service_type'
                """
            )
            if not cur.fetchone():
                print("[bills_db] Adding service_type column to bills...")
                cur.execute(
                    """
                    ALTER TABLE bills
                    ADD COLUMN service_type VARCHAR(50) DEFAULT 'electric'
                    """
                )

            conn.commit()
            print("[bills_db] Service type column migration complete")
    except Exception as e:
        print(f"[bills_db] Service type column migration error (non-fatal): {e}")
        conn.rollback()


def _migrate_backfill_service_type_from_payload(conn):
    """
    Backfill service_type on utility_bill_files from extraction_payload.
    
    This fixes files that were processed before service_type was properly saved.
    The extraction_payload JSON contains the detected service_type from the bill content.
    """
    try:
        with conn.cursor() as cur:
            # Count how many files need backfilling
            cur.execute(
                """
                SELECT COUNT(*) FROM utility_bill_files
                WHERE extraction_payload IS NOT NULL
                  AND extraction_payload->>'service_type' IS NOT NULL
                  AND (service_type IS NULL OR service_type = 'electric')
                  AND extraction_payload->>'service_type' != 'electric'
                """
            )
            count = cur.fetchone()[0]
            
            if count > 0:
                print(f"[bills_db] Backfilling service_type for {count} files from extraction_payload...")
                
                # Update service_type from extraction_payload where it differs
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET service_type = extraction_payload->>'service_type'
                    WHERE extraction_payload IS NOT NULL
                      AND extraction_payload->>'service_type' IS NOT NULL
                      AND extraction_payload->>'service_type' IN ('electric', 'water', 'gas', 'combined')
                      AND (service_type IS NULL 
                           OR service_type != extraction_payload->>'service_type')
                    """
                )
                updated = cur.rowcount
                print(f"[bills_db] Updated service_type on {updated} files")
            else:
                print("[bills_db] No files need service_type backfill")
            
            conn.commit()
            print("[bills_db] Service type backfill migration complete")
    except Exception as e:
        print(f"[bills_db] Service type backfill migration error (non-fatal): {e}")
        conn.rollback()

