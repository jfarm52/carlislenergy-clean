"""DB operations for `utility_meters`."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.utils.normalization import normalize_meter_number


def _meter_similarity(a: str, b: str) -> float:
    """
    Calculate similarity ratio between two meter numbers.
    Returns 1.0 for identical, 0.0 for completely different.
    """
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a == "Unknown" or b == "Unknown":
        return 0.0  # Don't fuzzy-match "Unknown"
    
    # Different lengths = probably different meters
    if abs(len(a) - len(b)) > 1:
        return 0.0
    
    # Same length: count matching characters
    if len(a) == len(b):
        matches = sum(1 for i in range(len(a)) if a[i] == b[i])
        return matches / len(a)
    
    return 0.0


def upsert_utility_meter(account_id, meter_number, service_address=None):
    """Find or create a utility meter. Returns meter ID.
    
    Uses fuzzy matching to handle OCR errors - if a meter number is 90%+
    similar to an existing one (same account), returns the existing meter
    instead of creating a duplicate.
    """
    _ = service_address  # column exists in schema but insert path is legacy-compatible
    meter_number = normalize_meter_number(meter_number)

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Check for exact match first
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

            # 2. Check for fuzzy match (handles OCR errors)
            cur.execute(
                """
                SELECT id, meter_number FROM utility_meters
                WHERE utility_account_id = %s
                """,
                (account_id,),
            )
            existing_meters = cur.fetchall()
            
            for existing in existing_meters:
                similarity = _meter_similarity(meter_number, existing["meter_number"])
                if similarity >= 0.9:  # 90%+ match = probably same meter with OCR error
                    print(f"[meters] Fuzzy match: '{meter_number}' ~ '{existing['meter_number']}' ({similarity:.0%}) - using existing meter {existing['id']}")
                    return existing["id"]

            # 3. No match found - create new meter
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


