"""Schema creation for bill intake tables."""

from __future__ import annotations

from bill_intake.db.connection import get_connection
from bill_intake.db.migrations import migrate_all


def init_bills_tables() -> bool:
    """Create all bill intake tables if they don't exist and run safe migrations."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS utility_accounts (
                    id SERIAL PRIMARY KEY,
                    project_id VARCHAR(255) NOT NULL,
                    utility_name VARCHAR(255) NOT NULL,
                    account_number VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_utility_accounts_project
                ON utility_accounts(project_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS utility_meters (
                    id SERIAL PRIMARY KEY,
                    utility_account_id INTEGER REFERENCES utility_accounts(id) ON DELETE CASCADE,
                    meter_number VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_utility_meters_account
                ON utility_meters(utility_account_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS utility_meter_reads (
                    id SERIAL PRIMARY KEY,
                    utility_meter_id INTEGER REFERENCES utility_meters(id) ON DELETE CASCADE,
                    billing_start_date DATE,
                    billing_end_date DATE,
                    statement_date DATE,
                    kwh NUMERIC(12,2),
                    total_charges_usd NUMERIC(12,2),
                    source_file VARCHAR(512),
                    source_page INTEGER,
                    from_summary_table BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_utility_meter_reads_meter
                ON utility_meter_reads(utility_meter_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS utility_bill_files (
                    id SERIAL PRIMARY KEY,
                    project_id VARCHAR(255) NOT NULL,
                    filename VARCHAR(512) NOT NULL,
                    original_filename VARCHAR(512) NOT NULL,
                    file_path VARCHAR(1024) NOT NULL,
                    file_size INTEGER,
                    mime_type VARCHAR(255),
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed BOOLEAN DEFAULT FALSE,
                    processing_status VARCHAR(50) DEFAULT 'pending',
                    review_status VARCHAR(50) DEFAULT 'pending',
                    extraction_payload JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_utility_bill_files_project
                ON utility_bill_files(project_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bill_training_data (
                    id SERIAL PRIMARY KEY,
                    utility_name VARCHAR(200),
                    pdf_hash VARCHAR(64),
                    field_type VARCHAR(100),
                    meter_number VARCHAR(100),
                    period_start_date DATE,
                    period_end_date DATE,
                    corrected_value TEXT,
                    annotated_image_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_bill_training_utility
                ON bill_training_data(utility_name);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bill_screenshots (
                    id SERIAL PRIMARY KEY,
                    bill_id INTEGER REFERENCES utility_bill_files(id) ON DELETE CASCADE,
                    file_path VARCHAR(1024) NOT NULL,
                    original_filename VARCHAR(512),
                    mime_type VARCHAR(100),
                    page_hint VARCHAR(100),
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_bill_screenshots_bill
                ON bill_screenshots(bill_id);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bills (
                    id SERIAL PRIMARY KEY,
                    bill_file_id INTEGER REFERENCES utility_bill_files(id) ON DELETE SET NULL,
                    account_id INTEGER REFERENCES utility_accounts(id) ON DELETE CASCADE,
                    meter_id INTEGER REFERENCES utility_meters(id) ON DELETE CASCADE,
                    utility_name VARCHAR(255),
                    service_address TEXT,
                    rate_schedule VARCHAR(100),
                    period_start DATE,
                    period_end DATE,
                    days_in_period INTEGER,
                    total_kwh NUMERIC(12,2),
                    total_amount_due NUMERIC(12,2),
                    energy_charges NUMERIC(12,2),
                    demand_charges NUMERIC(12,2),
                    other_charges NUMERIC(12,2),
                    taxes NUMERIC(12,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_bills_account
                ON bills(account_id);

                CREATE INDEX IF NOT EXISTS idx_bills_meter
                ON bills(meter_id);

                CREATE INDEX IF NOT EXISTS idx_bills_period
                ON bills(period_start, period_end);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bill_tou_periods (
                    id SERIAL PRIMARY KEY,
                    bill_id INTEGER REFERENCES bills(id) ON DELETE CASCADE,
                    period VARCHAR(50),
                    kwh NUMERIC(12,2),
                    rate_dollars_per_kwh NUMERIC(10,6),
                    est_cost_dollars NUMERIC(12,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_bill_tou_periods_bill
                ON bill_tou_periods(bill_id);
                """
            )

            conn.commit()

            # Run safe migrations to add new columns/indexes if missing
            migrate_all(conn)

            print("[bills_db] Tables initialized successfully")
            return True
    except Exception as e:
        print(f"[bills_db] Error initializing tables: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


