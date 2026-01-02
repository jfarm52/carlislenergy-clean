"""DB operations for `utility_meters`."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.utils.normalization import normalize_meter_number


def upsert_utility_meter(account_id, meter_number, service_address=None):
    """Find or create a utility meter. Returns meter ID."""
    _ = service_address  # column exists in schema but insert path is legacy-compatible
    meter_number = normalize_meter_number(meter_number)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM utility_meters
                WHERE utility_account_id = %s AND meter_number = %s
                """,
                (account_id, meter_number),
            )
            row = cur.fetchone()
            if row:
                return row["id"]

            cur.execute(
                """
                INSERT INTO utility_meters (utility_account_id, meter_number)
                VALUES (%s, %s)
                RETURNING id
                """,
                (account_id, meter_number),
            )
            result = cur.fetchone()
            conn.commit()
            return result["id"]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


