"""DB operations for bill screenshots/annotations (`bill_screenshots`)."""

from __future__ import annotations

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection


def add_bill_screenshot(bill_id, file_path, original_filename=None, mime_type=None, page_hint=None):
    """Add a screenshot/annotation file for a bill."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO bill_screenshots (bill_id, file_path, original_filename, mime_type, page_hint)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, bill_id, file_path, original_filename, mime_type, page_hint, uploaded_at
                """,
                (bill_id, file_path, original_filename, mime_type, page_hint),
            )
            result = cur.fetchone()
            conn.commit()
            return dict(result)
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def get_bill_screenshots(bill_id):
    """Get all screenshots/annotation files for a bill."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, bill_id, file_path, original_filename, mime_type, page_hint, uploaded_at
                FROM bill_screenshots
                WHERE bill_id = %s
                ORDER BY uploaded_at ASC
                """,
                (bill_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def delete_bill_screenshot(screenshot_id):
    """Delete a screenshot by ID. Returns the file_path for cleanup."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT file_path FROM bill_screenshots WHERE id = %s", (screenshot_id,))
            result = cur.fetchone()
            if not result:
                return None
            file_path = result["file_path"]

            cur.execute("DELETE FROM bill_screenshots WHERE id = %s", (screenshot_id,))
            conn.commit()
            return file_path
    finally:
        conn.close()


def get_screenshot_count(bill_id):
    """Get the number of screenshots for a bill."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bill_screenshots WHERE bill_id = %s", (bill_id,))
            return cur.fetchone()[0]
    finally:
        conn.close()


