"""DB operations for `utility_meter_reads`."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection


def get_meter_reads_for_project(project_id):
    """Get all meter reads for a project (via accounts and meters)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    r.id, r.billing_start_date, r.billing_end_date, r.statement_date,
                    r.kwh, r.total_charges_usd, r.source_file, r.source_page,
                    r.from_summary_table,
                    m.meter_number,
                    a.utility_name, a.account_number
                FROM utility_meter_reads r
                JOIN utility_meters m ON r.utility_meter_id = m.id
                JOIN utility_accounts a ON m.utility_account_id = a.id
                WHERE a.project_id = %s
                ORDER BY r.billing_end_date DESC
                """,
                (project_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def upsert_meter_read(meter_id, period_start, period_end, kwh, total_charge, source_file=None):
    """
    Upsert a meter reading. Key is (meter_id, period_start, period_end).
    If exists, updates with new values. Otherwise creates new.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM utility_meter_reads
                WHERE utility_meter_id = %s
                  AND billing_start_date = %s
                  AND billing_end_date = %s
                """,
                (meter_id, period_start, period_end),
            )
            row = cur.fetchone()

            if row:
                cur.execute(
                    """
                    UPDATE utility_meter_reads
                    SET kwh = %s, total_charges_usd = %s, source_file = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    RETURNING id
                    """,
                    (kwh, total_charge, source_file, row["id"]),
                )
                result = cur.fetchone()
                conn.commit()
                return result["id"]

            cur.execute(
                """
                INSERT INTO utility_meter_reads
                (utility_meter_id, billing_start_date, billing_end_date, kwh, total_charges_usd, source_file)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (meter_id, period_start, period_end, kwh, total_charge, source_file),
            )
            result = cur.fetchone()
            conn.commit()
            return result["id"]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


