"""DB operations for bill training/corrections (`bill_training_data`)."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection


def save_correction(
    utility_name,
    pdf_hash,
    field_type,
    meter_number,
    period_start,
    period_end,
    corrected_value,
    annotated_image_url=None,
):
    """Save a user correction to training data table."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO bill_training_data
                (utility_name, pdf_hash, field_type, meter_number, period_start_date, period_end_date, corrected_value, annotated_image_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, utility_name, pdf_hash, field_type, meter_number, period_start_date, period_end_date, corrected_value, annotated_image_url, created_at
                """,
                (
                    utility_name,
                    pdf_hash,
                    field_type,
                    meter_number,
                    period_start,
                    period_end,
                    corrected_value,
                    annotated_image_url,
                ),
            )
            result = cur.fetchone()
            conn.commit()
            return dict(result)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def get_corrections_for_utility(utility_name):
    """Get all past corrections for a utility."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, utility_name, pdf_hash, field_type, meter_number,
                       period_start_date, period_end_date, corrected_value,
                       annotated_image_url, created_at
                FROM bill_training_data
                WHERE utility_name = %s
                ORDER BY created_at DESC
                """,
                (utility_name,),
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


