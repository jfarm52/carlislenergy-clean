"""DB operations for `utility_bill_files` and related file-level utilities."""

from __future__ import annotations

from psycopg2.extras import Json, RealDictCursor

from bill_intake.db.connection import get_connection


def find_bill_file_by_sha256(project_id, sha256):
    """Find an existing bill file by project_id and SHA256 hash."""
    if not sha256:
        return None
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, project_id, filename, original_filename, file_path,
                       file_size, mime_type, upload_date, processed, processing_status,
                       review_status, extraction_payload, sha256, service_type
                FROM utility_bill_files
                WHERE project_id = %s AND sha256 = %s
                """,
                (project_id, sha256),
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_cached_result_by_hash(normalized_hash):
    """
    Look up cached extraction result by normalized text hash.

    Args:
        normalized_hash: SHA256 hash of normalized_text + version

    Returns:
        Dict with extraction_payload if found, None otherwise
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, extraction_payload, processing_metrics, normalized_text
                FROM utility_bill_files
                WHERE normalized_hash = %s
                  AND extraction_payload IS NOT NULL
                  AND processing_status = 'complete'
                ORDER BY upload_date DESC
                LIMIT 1
                """,
                (normalized_hash,),
            )
            result = cur.fetchone()
            if result:
                return {
                    "file_id": result["id"],
                    "parse_result": result["extraction_payload"],
                    "metrics": result["processing_metrics"],
                }
            return None
    finally:
        conn.close()


def save_cache_entry(file_id, normalized_hash, normalized_text, parse_result, metrics):
    """
    Save extraction result to enable future cache hits.

    Args:
        file_id: Database ID of the bill file
        normalized_hash: SHA256 hash of normalized_text + version
        normalized_text: The normalized text content
        parse_result: Extracted bill data (dict)
        metrics: Processing metrics (timing, tokens, etc)
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE utility_bill_files
                SET normalized_hash = %s,
                    normalized_text = %s,
                    extraction_payload = %s,
                    processing_metrics = %s,
                    processing_status = 'complete',
                    processed = TRUE
                WHERE id = %s
                """,
                (
                    normalized_hash,
                    normalized_text[:50000] if normalized_text else None,
                    Json(parse_result),
                    Json(metrics),
                    file_id,
                ),
            )
            conn.commit()
            print(f"[bills_db] Saved cache entry for file {file_id}, hash {normalized_hash[:12]}...")
    except Exception as e:
        conn.rollback()
        print(f"[bills_db] Error saving cache entry: {e}")
        raise
    finally:
        conn.close()


def invalidate_cache_for_file(file_id):
    """Invalidate cache entry for a file (clear hash so it won't match)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE utility_bill_files
                SET normalized_hash = NULL,
                    processing_status = 'pending'
                WHERE id = %s
                """,
                (file_id,),
            )
            conn.commit()
    finally:
        conn.close()


def update_file_processing_status(file_id, status, metrics=None):
    """Update processing status for a bill file."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if metrics:
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET processing_status = %s,
                        processing_metrics = %s
                    WHERE id = %s
                    """,
                    (status, Json(metrics), file_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET processing_status = %s
                    WHERE id = %s
                    """,
                    (status, file_id),
                )
            conn.commit()
    finally:
        conn.close()


def get_bill_files_for_project(project_id):
    """Get all uploaded bill files for a project."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, project_id, filename, original_filename, file_path,
                       file_size, mime_type, upload_date, processed, processing_status,
                       review_status, extraction_payload
                FROM utility_bill_files
                WHERE project_id = %s
                ORDER BY upload_date DESC
                """,
                (project_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_bill_file_by_id(file_id):
    """Get a single bill file by ID."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, project_id, filename, original_filename, file_path,
                       file_size, mime_type, upload_date, processed, processing_status,
                       review_status, extraction_payload
                FROM utility_bill_files
                WHERE id = %s
                """,
                (file_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def add_bill_file(
    project_id,
    filename,
    original_filename,
    file_path,
    file_size,
    mime_type,
    sha256=None,
    service_type="electric",
):
    """Add a bill file record to the database with status='pending'."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO utility_bill_files
                (project_id, filename, original_filename, file_path, file_size, mime_type,
                 review_status, processing_status, sha256, service_type)
                VALUES (%s, %s, %s, %s, %s, %s, 'pending', 'pending', %s, %s)
                RETURNING id, project_id, filename, original_filename, file_path,
                          file_size, mime_type, upload_date, processed, processing_status,
                          review_status, extraction_payload, sha256, service_type
                """,
                (
                    project_id,
                    filename,
                    original_filename,
                    file_path,
                    file_size,
                    mime_type,
                    sha256,
                    service_type,
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


def delete_bill_file(file_id):
    """Delete a bill file record."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM utility_bill_files WHERE id = %s", (file_id,))
            conn.commit()
            return cur.rowcount > 0
    finally:
        conn.close()


def update_bill_file_status(file_id, status, processed=True, missing_fields=None):
    """
    Update the processing status of a bill file.

    If missing_fields is provided, also updates review_status.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if missing_fields is not None:
                review_status = "needs_review" if len(missing_fields) > 0 else "ok"
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET processing_status = %s, processed = %s,
                        missing_fields = %s, review_status = %s
                    WHERE id = %s
                    """,
                    (status, processed, Json(missing_fields), review_status, file_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET processing_status = %s, processed = %s
                    WHERE id = %s
                    """,
                    (status, processed, file_id),
                )
            conn.commit()
            return cur.rowcount > 0
    finally:
        conn.close()


def update_bill_file_review_status(file_id, review_status, extraction_payload=None):
    """Update the review status and extraction payload of a bill file."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if extraction_payload is not None:
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET review_status = %s, extraction_payload = %s
                    WHERE id = %s
                    """,
                    (review_status, Json(extraction_payload), file_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE utility_bill_files
                    SET review_status = %s
                    WHERE id = %s
                    """,
                    (review_status, file_id),
                )
            conn.commit()
            return cur.rowcount > 0
    finally:
        conn.close()


def update_bill_file_extraction_payload(file_id, extraction_payload):
    """Update only the extraction payload of a bill file."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE utility_bill_files
                SET extraction_payload = %s
                WHERE id = %s
                """,
                (Json(extraction_payload), file_id),
            )
            conn.commit()
            return cur.rowcount > 0
    finally:
        conn.close()


def get_files_status_for_project(project_id):
    """Get status summary for all files in a project (for polling)."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, original_filename, review_status, processing_status,
                       processed, upload_date
                FROM utility_bill_files
                WHERE project_id = %s
                ORDER BY upload_date DESC
                """,
                (project_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


def mark_bill_ok(bill_id, reviewed_by=None, note=None):
    """Mark a bill as OK (reviewed). Returns updated record."""
    _ = note  # reserved for future use
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                UPDATE utility_bill_files
                SET review_status = 'ok',
                    processing_status = 'ok',
                    reviewed_at = CURRENT_TIMESTAMP,
                    reviewed_by = %s
                WHERE id = %s
                RETURNING id, project_id, filename, original_filename, review_status,
                          processing_status, reviewed_at, reviewed_by
                """,
                (reviewed_by, bill_id),
            )
            result = cur.fetchone()
            conn.commit()
            return dict(result) if result else None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


