"""
Bills API - Part 3

Screenshots/annotations + analytics/export endpoints extracted from `routes/bills_api.py`.
Registered from the main `routes/bills_api.py` module.
"""

from __future__ import annotations

import base64
import os

from flask import jsonify, request, send_file

from bills_db import (
    add_bill_screenshot,
    delete_bill_screenshot,
    export_bills_csv,
    get_account_summary,
    get_bill_detail,
    get_bill_file_by_id,
    get_bill_screenshots,
    get_connection,
    get_meter_bills,
    get_meter_months,
    get_screenshot_count,
    get_utility_accounts_for_project,
    mark_bill_ok,
    update_bill_file_review_status,
)


def register(*, bills_bp, is_enabled, extraction_progress, populate_normalized_tables):
    """Register the routes contained in this module on the provided blueprint."""

    BILL_SCREENSHOTS_DIR = "bill_screenshots"
    os.makedirs(BILL_SCREENSHOTS_DIR, exist_ok=True)

    @bills_bp.route("/api/bills/<int:bill_id>/screenshots", methods=["GET"])
    def get_screenshots(bill_id):
        """Get all screenshots for a bill."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            screenshots = get_bill_screenshots(bill_id)
            result = []
            for s in screenshots:
                result.append(
                    {
                        "id": s["id"],
                        "bill_id": s["bill_id"],
                        "url": f"/api/bills/screenshots/{s['id']}/image",
                        "original_filename": s["original_filename"],
                        "mime_type": s.get("mime_type"),
                        "page_hint": s["page_hint"],
                        "uploaded_at": s["uploaded_at"].isoformat() if s["uploaded_at"] else None,
                    }
                )
            return jsonify({"success": True, "screenshots": result})
        except Exception as e:
            print(f"[bills] Error getting screenshots: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/screenshots", methods=["POST"])
    def upload_screenshots(bill_id):
        """Upload one or more screenshots for a bill."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(bill_id)
            if not file_record:
                return jsonify({"success": False, "error": "Bill not found"}), 404

            if "files" not in request.files and "file" not in request.files:
                return jsonify({"success": False, "error": "No files provided"}), 400

            files = request.files.getlist("files") or [request.files.get("file")]
            files = [f for f in files if f]
            if not files:
                return jsonify({"success": False, "error": "No files provided"}), 400

            added = []
            for file in files:
                if not file.filename:
                    continue

                mime_type = file.content_type or "application/octet-stream"
                import uuid

                ext = os.path.splitext(file.filename)[1] or ".png"
                unique_name = f"{bill_id}_{uuid.uuid4().hex[:8]}{ext}"
                file_path = os.path.join(BILL_SCREENSHOTS_DIR, unique_name)
                file.save(file_path)

                page_hint = request.form.get("page_hint")
                record = add_bill_screenshot(
                    bill_id=bill_id,
                    file_path=file_path,
                    original_filename=file.filename,
                    mime_type=mime_type,
                    page_hint=page_hint,
                )
                added.append(
                    {
                        "id": record["id"],
                        "bill_id": record["bill_id"],
                        "url": f"/api/bills/screenshots/{record['id']}/image",
                        "original_filename": record["original_filename"],
                        "mime_type": record.get("mime_type"),
                        "page_hint": record["page_hint"],
                        "uploaded_at": record["uploaded_at"].isoformat() if record["uploaded_at"] else None,
                    }
                )

            return jsonify({"success": True, "added": added, "count": len(added)})
        except Exception as e:
            print(f"[bills] Error uploading screenshots: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/screenshots/<int:screenshot_id>/image")
    def serve_screenshot_image(screenshot_id):
        """Serve a screenshot image."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            from psycopg2.extras import RealDictCursor

            conn = get_connection()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT file_path, original_filename, mime_type FROM bill_screenshots WHERE id = %s",
                        (screenshot_id,),
                    )
                    result = cur.fetchone()
            finally:
                conn.close()

            if not result:
                return jsonify({"error": "Screenshot not found"}), 404

            file_path = result["file_path"]
            if not os.path.exists(file_path):
                return jsonify({"error": "Screenshot file not found"}), 404

            mime_type = result.get("mime_type") or "application/octet-stream"
            if mime_type == "application/octet-stream":
                ext = os.path.splitext(file_path)[1].lower()
                mime_map = {
                    ".png": "image/png",
                    ".jpg": "image/jpeg",
                    ".jpeg": "image/jpeg",
                    ".gif": "image/gif",
                    ".webp": "image/webp",
                    ".pdf": "application/pdf",
                }
                mime_type = mime_map.get(ext, "application/octet-stream")

            return send_file(file_path, mimetype=mime_type)
        except Exception as e:
            print(f"[bills] Error serving screenshot: {e}")
            return jsonify({"error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/screenshots/<int:screenshot_id>", methods=["DELETE"])
    def remove_screenshot(bill_id, screenshot_id):
        """Delete a specific screenshot."""
        _ = bill_id
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_path = delete_bill_screenshot(screenshot_id)
            if file_path:
                if os.path.exists(file_path):
                    os.remove(file_path)
                return jsonify({"success": True, "deleted": screenshot_id})
            return jsonify({"success": False, "error": "Screenshot not found"}), 404
        except Exception as e:
            print(f"[bills] Error deleting screenshot: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/mark_ok", methods=["POST"])
    def mark_bill_as_ok(bill_id):
        """Mark a bill as OK (reviewed). Re-runs extraction with annotations if they exist."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        import fitz  # PyMuPDF
        import time

        try:
            file_record = get_bill_file_by_id(bill_id)
            if not file_record:
                return jsonify({"success": False, "error": "Bill not found"}), 404

            current_status = file_record.get("review_status")
            if current_status == "ok":
                print(f"[bills] Bill {bill_id} is already OK, returning success (idempotent)")
                return jsonify(
                    {
                        "success": True,
                        "bill_id": bill_id,
                        "review_status": "ok",
                        "processing_status": file_record.get("processing_status", "ok"),
                        "reviewed_at": file_record.get("reviewed_at").isoformat()
                        if file_record.get("reviewed_at")
                        else None,
                        "reviewed_by": file_record.get("reviewed_by"),
                        "re_extraction_triggered": False,
                        "already_ok": True,
                    }
                )

            guard_key = f"mark_ok_{bill_id}"
            if guard_key in extraction_progress:
                in_flight = extraction_progress[guard_key]
                if time.time() - in_flight.get("updated_at", 0) < 60:
                    print(f"[bills] Bill {bill_id} mark_ok already in progress, returning early")
                    return jsonify({"success": True, "bill_id": bill_id, "in_progress": True, "message": "Request already in progress"})

            extraction_progress[guard_key] = {"status": "processing", "updated_at": time.time()}

            status = file_record.get("processing_status") or file_record.get("review_status")
            if status in ["error", "needs_review"]:
                screenshot_count = get_screenshot_count(bill_id)
                if screenshot_count == 0:
                    del extraction_progress[guard_key]
                    return jsonify(
                        {
                            "success": False,
                            "error": "Please upload at least one annotated file before marking this bill as OK.",
                        }
                    ), 400

            data = request.get_json() or {}
            reviewed_by = data.get("reviewed_by", "User")
            note = data.get("note")

            screenshots = get_bill_screenshots(bill_id)
            annotated_images = []
            re_extraction_triggered = False

            if screenshots:
                print(f"[bills] Found {len(screenshots)} annotation(s) for bill {bill_id}, triggering re-extraction")
                for ss in screenshots:
                    file_path = ss.get("file_path")
                    mime_type = ss.get("mime_type", "")
                    if not file_path or not os.path.exists(file_path):
                        continue

                    try:
                        if mime_type == "application/pdf" or file_path.lower().endswith(".pdf"):
                            doc = fitz.open(file_path)
                            for page_num in range(min(len(doc), 5)):
                                page = doc[page_num]
                                mat = fitz.Matrix(150 / 72, 150 / 72)
                                pix = page.get_pixmap(matrix=mat)
                                img_bytes = pix.tobytes("png")
                                annotated_images.append(base64.b64encode(img_bytes).decode("utf-8"))
                            doc.close()
                        else:
                            with open(file_path, "rb") as f:
                                annotated_images.append(base64.b64encode(f.read()).decode("utf-8"))
                    except Exception as e:
                        print(f"[bills] Error processing annotation file {file_path}: {e}")

                if annotated_images:
                    try:
                        from bill_extractor import extract_bill_data

                        original_file = file_record.get("file_path")
                        if original_file and os.path.exists(original_file):
                            print(f"[bills] Re-extracting with {len(annotated_images)} annotation image(s)")
                            extraction_result = extract_bill_data(original_file, annotated_images=annotated_images)
                            if extraction_result.get("success"):
                                re_extraction_triggered = True
                                bills_saved = populate_normalized_tables(
                                    file_record["project_id"],
                                    extraction_result,
                                    file_record.get("original_filename", "unknown"),
                                    file_id=bill_id,
                                )
                                print(f"[bills] Re-extraction bills saved: {bills_saved}")
                                update_bill_file_review_status(bill_id, "ok", extraction_payload=extraction_result)
                                print(f"[bills] Re-extraction successful for bill {bill_id}")
                            else:
                                print(f"[bills] Re-extraction failed: {extraction_result.get('error')}")
                    except Exception as e:
                        print(f"[bills] Re-extraction error: {e}")
                        import traceback

                        traceback.print_exc()

            result = mark_bill_ok(bill_id, reviewed_by=reviewed_by, note=note)
            extraction_progress.pop(guard_key, None)

            if result:
                return jsonify(
                    {
                        "success": True,
                        "bill_id": bill_id,
                        "review_status": result["review_status"],
                        "processing_status": result["processing_status"],
                        "reviewed_at": result["reviewed_at"].isoformat() if result["reviewed_at"] else None,
                        "reviewed_by": result["reviewed_by"],
                        "re_extraction_triggered": re_extraction_triggered,
                    }
                )
            return jsonify({"success": False, "error": "Failed to update bill"}), 500
        except Exception as e:
            extraction_progress.pop(f"mark_ok_{bill_id}", None)
            print(f"[bills] Error marking bill as OK: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/accounts/<int:account_id>/summary", methods=["GET"])
    def get_account_summary_endpoint(account_id):
        """Get summary for an account: combined totals + per-meter breakdown. No date restrictions."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            result = get_account_summary(account_id)
            return jsonify({"success": True, **result})
        except Exception as e:
            print(f"[bills] Error getting account summary: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/meters/<int:meter_id>/bills", methods=["GET"])
    def get_meter_bills_endpoint(meter_id):
        """Get list of bills for a meter with summary data. No date restrictions."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            result = get_meter_bills(meter_id)
            return jsonify({"success": True, **result})
        except Exception as e:
            print(f"[bills] Error getting meter bills: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/detail", methods=["GET"])
    def get_bill_detail_endpoint(bill_id):
        """Get full detail for a single bill including TOU fields and source file metadata."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            result = get_bill_detail(bill_id)
            if result:
                return jsonify({"success": True, **result})
            return jsonify({"success": False, "error": "Bill not found"}), 404
        except Exception as e:
            print(f"[bills] Error getting bill detail: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/accounts/<int:account_id>/meters/<int:meter_id>/months", methods=["GET"])
    def get_meter_months_endpoint(account_id, meter_id):
        """Get month-by-month breakdown for a specific meter under an account. No date restrictions."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            result = get_meter_months(account_id, meter_id)
            return jsonify({"success": True, **result})
        except Exception as e:
            print(f"[bills] Error getting meter months: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/summary", methods=["GET"])
    def get_project_bills_summary(project_id):
        """Get bills summary for a project including summaries per account. No date restrictions."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            service_filter = request.args.get("service")

            accounts = get_utility_accounts_for_project(project_id, service_filter=service_filter)
            summaries = []
            for acc in accounts:
                summary = get_account_summary(acc["id"], service_filter=service_filter)
                summary["utilityName"] = acc["utility_name"]
                summary["accountNumber"] = acc["account_number"]
                summaries.append(summary)

            if service_filter == "electric":
                service_condition = "AND (service_type IN ('electric', 'combined') OR service_type IS NULL)"
            else:
                service_condition = ""

            file_counts = {"uploaded": 0, "ok": 0, "needsReview": 0, "processing": 0, "error": 0}
            try:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) AS total,
                            COUNT(*) FILTER (WHERE review_status = 'ok') AS ok_count,
                            COUNT(*) FILTER (WHERE review_status = 'needs_review') AS needs_review_count,
                            COUNT(*) FILTER (WHERE processing_status = 'extracting' OR processing_status = 'pending') AS processing_count,
                            COUNT(*) FILTER (WHERE processing_status = 'error') AS error_count
                        FROM utility_bill_files
                        WHERE project_id = %s {service_condition}
                        """,
                        (project_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        file_counts = {
                            "uploaded": row[0] or 0,
                            "ok": row[1] or 0,
                            "needsReview": row[2] or 0,
                            "processing": row[3] or 0,
                            "error": row[4] or 0,
                        }
                conn.close()
            except Exception as fc_err:
                print(f"[bills] Error getting file counts: {fc_err}")

            return jsonify(
                {
                    "success": True,
                    "projectId": project_id,
                    "accounts": summaries,
                    "fileCounts": file_counts,
                }
            )
        except Exception as e:
            print(f"[bills] Error getting project bills summary: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/export-csv", methods=["GET"])
    def export_bills_csv_endpoint(project_id):
        """Export all bills for a project as CSV for jMaster import."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            csv_content = export_bills_csv(project_id)
            if csv_content is None:
                return jsonify({"success": False, "error": "No bills found for this project", "csv": None})
            return jsonify({"success": True, "csv": csv_content})
        except Exception as e:
            print(f"[bills] Error exporting bills CSV: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/merge-duplicate-accounts", methods=["POST"])
    def merge_duplicate_accounts_endpoint(project_id):
        """Merge duplicate accounts that have the same normalized utility name and account number."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            from bill_intake.db.maintenance import merge_duplicate_accounts
            result = merge_duplicate_accounts(project_id)
            return jsonify({"success": True, **result})
        except Exception as e:
            print(f"[bills] Error merging duplicate accounts: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/maintenance/merge-all-duplicate-accounts", methods=["POST"])
    def merge_all_duplicate_accounts_endpoint():
        """Merge ALL duplicate accounts across all projects."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            from bill_intake.db.maintenance import merge_duplicate_accounts
            result = merge_duplicate_accounts(project_id=None)
            return jsonify({"success": True, **result})
        except Exception as e:
            print(f"[bills] Error merging duplicate accounts: {e}")
            return jsonify({"success": False, "error": str(e)}), 500


