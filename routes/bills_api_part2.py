"""
Bills API - Part 2

This module contains the "non-core" endpoints extracted from `routes/bills_api.py`
to keep file sizes manageable. It is registered from `routes/bills_api.py`.
"""

from __future__ import annotations

import os
from datetime import datetime

from flask import jsonify, request, send_file

from stores.project_store import stored_data

from bills_db import (
    add_bill_screenshot,
    clone_bills_for_project,
    delete_all_empty_accounts,
    delete_bill_file,
    delete_bills_for_file,
    get_bill_by_id,
    get_bill_file_by_id,
    get_bill_files_for_project,
    get_bill_review_data,
    get_grouped_bills_data,
    get_corrections_for_utility,
    get_connection,
    get_bill_screenshots,
    recompute_bill_file_missing_fields,
    save_correction,
    update_bill,
    update_bill_file_extraction_payload,
    update_bill_file_review_status,
    update_bill_file_status,
    upsert_meter_read,
    upsert_utility_account,
    upsert_utility_meter,
)


def register(*, bills_bp, is_enabled, populate_normalized_tables):
    """Register the routes contained in this module on the provided blueprint."""

    @bills_bp.route("/api/projects/<source_project_id>/bills/copy-to/<target_project_id>", methods=["POST"])
    def copy_bills_to_project(source_project_id, target_project_id):
        """Copy all bill files and data from source project to target project."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            counts = clone_bills_for_project(source_project_id, target_project_id)
            print(f"[bills] Copied bills from {source_project_id} to {target_project_id}: {counts}")
            return jsonify(
                {
                    "success": True,
                    "source_project_id": source_project_id,
                    "target_project_id": target_project_id,
                    "files_copied": counts.get("files", 0),
                    "bills_copied": counts.get("bills", 0),
                    "accounts_copied": counts.get("accounts", 0),
                    "counts": counts,
                }
            )
        except Exception as e:
            print(f"[bills] Error copying bills: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/review", methods=["GET"])
    def get_bill_file_review(project_id, file_id):
        """Get file details and extraction_payload for review."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"success": False, "error": "File not found"}), 404
            if file_record["project_id"] != project_id:
                return jsonify({"success": False, "error": "File does not belong to this project"}), 403
            return jsonify(
                {
                    "success": True,
                    "file": {
                        "id": file_record["id"],
                        "filename": file_record["filename"],
                        "original_filename": file_record["original_filename"],
                        "file_size": file_record["file_size"],
                        "upload_date": file_record["upload_date"].isoformat() if file_record["upload_date"] else None,
                        "review_status": file_record["review_status"],
                        "processing_status": file_record["processing_status"],
                    },
                    "extraction_payload": file_record["extraction_payload"],
                }
            )
        except Exception as e:
            print(f"[bills] Error getting review data: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/approve", methods=["POST"])
    def approve_bill_file(project_id, file_id):
        """Approve extracted data and upsert to database tables."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"success": False, "error": "File not found"}), 404
            if file_record["project_id"] != project_id:
                return jsonify({"success": False, "error": "File does not belong to this project"}), 403

            extraction_payload = file_record["extraction_payload"]
            if not extraction_payload:
                return jsonify({"success": False, "error": "No extraction data to approve"}), 400
            if not extraction_payload.get("success"):
                return jsonify({"success": False, "error": "Cannot approve failed extraction"}), 400

            original_filename = file_record["original_filename"]
            utility_name = extraction_payload["utility_name"]
            account_number = extraction_payload["account_number"]
            meters = extraction_payload.get("meters", [])

            account_id = upsert_utility_account(project_id, utility_name, account_number)
            print(f"[bills] Approved: Upserted account {account_number} -> ID {account_id}")

            extracted_meters = 0
            extracted_reads = 0

            for meter_data in meters:
                meter_number = meter_data.get("meter_number")
                if not meter_number:
                    continue
                meter_id = upsert_utility_meter(account_id, meter_number)
                extracted_meters += 1

                for read in meter_data.get("reads", []):
                    period_start = read.get("period_start")
                    period_end = read.get("period_end")
                    kwh = read.get("kwh")
                    total_charge = read.get("total_charge")

                    if period_start and period_end:
                        upsert_meter_read(
                            meter_id=meter_id,
                            period_start=period_start,
                            period_end=period_end,
                            kwh=kwh,
                            total_charge=total_charge,
                            source_file=original_filename,
                        )
                        extracted_reads += 1

            update_bill_file_review_status(file_id, "approved")
            update_bill_file_status(file_id, "ok", processed=True)
            print(f"[bills] File {file_id} approved: {extracted_meters} meters, {extracted_reads} reads")

            return jsonify(
                {
                    "success": True,
                    "file_id": file_id,
                    "review_status": "approved",
                    "meters_upserted": extracted_meters,
                    "reads_upserted": extracted_reads,
                }
            )
        except Exception as e:
            print(f"[bills] Error approving file: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/update", methods=["PUT"])
    def update_bill_extraction(project_id, file_id):
        """Update extraction_payload values (for editing before approval)."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"success": False, "error": "File not found"}), 404
            if file_record["project_id"] != project_id:
                return jsonify({"success": False, "error": "File does not belong to this project"}), 403

            updated_payload = request.get_json()
            if not updated_payload:
                return jsonify({"success": False, "error": "No data provided"}), 400

            update_bill_file_extraction_payload(file_id, updated_payload)
            if file_record["review_status"] == "approved":
                update_bill_file_review_status(file_id, "needs_review")

            print(f"[bills] Updated extraction payload for file {file_id}")
            return jsonify(
                {
                    "success": True,
                    "file_id": file_id,
                    "review_status": file_record["review_status"]
                    if file_record["review_status"] != "approved"
                    else "needs_review",
                }
            )
        except Exception as e:
            print(f"[bills] Error updating extraction: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/review", methods=["GET"])
    def get_bill_review(bill_id):
        """Get bill data formatted for review UI."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            review_data = get_bill_review_data(bill_id)
            if not review_data:
                return jsonify({"success": False, "error": "Bill not found"}), 404
            return jsonify(review_data)
        except Exception as e:
            print(f"[bills] Error getting bill review data for {bill_id}: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>", methods=["PATCH"])
    def patch_bill(bill_id):
        """
        Update a bill record with corrections.
        Accepts JSON body with any subset of fields.
        Recomputes blended_rate and avg_cost_per_day automatically.
        Updates missing_fields and review_status on the bill_file if required fields are now filled.
        """
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            updates = request.get_json()
            if not updates:
                return jsonify({"success": False, "error": "No data provided"}), 400

            bill = get_bill_by_id(bill_id)
            if not bill:
                return jsonify({"success": False, "error": "Bill not found"}), 404

            updated_bill = update_bill(bill_id, updates)
            if not updated_bill:
                return jsonify({"success": False, "error": "Failed to update bill"}), 500

            bill_file_id = updated_bill.get("bill_file_id")
            if bill_file_id:
                missing_fields = recompute_bill_file_missing_fields(bill_file_id)
                print(f"[bills] Bill {bill_id} updated, file {bill_file_id} missing fields: {missing_fields}")

            return jsonify(
                {
                    "success": True,
                    "bill": {
                        "id": updated_bill["id"],
                        "utility_name": updated_bill.get("utility_name"),
                        "service_address": updated_bill.get("service_address"),
                        "rate_schedule": updated_bill.get("rate_schedule"),
                        "period_start": str(updated_bill["period_start"]) if updated_bill.get("period_start") else None,
                        "period_end": str(updated_bill["period_end"]) if updated_bill.get("period_end") else None,
                        "days_in_period": updated_bill.get("days_in_period"),
                        "total_kwh": float(updated_bill["total_kwh"]) if updated_bill.get("total_kwh") else None,
                        "total_amount_due": float(updated_bill["total_amount_due"])
                        if updated_bill.get("total_amount_due")
                        else None,
                        "blended_rate_dollars": float(updated_bill["blended_rate_dollars"])
                        if updated_bill.get("blended_rate_dollars")
                        else None,
                        "avg_cost_per_day": float(updated_bill["avg_cost_per_day"])
                        if updated_bill.get("avg_cost_per_day")
                        else None,
                        "energy_charges": float(updated_bill["energy_charges"]) if updated_bill.get("energy_charges") else None,
                        "demand_charges": float(updated_bill["demand_charges"])
                        if updated_bill.get("demand_charges")
                        else None,
                        "other_charges": float(updated_bill["other_charges"]) if updated_bill.get("other_charges") else None,
                        "taxes": float(updated_bill["taxes"]) if updated_bill.get("taxes") else None,
                        "tou_on_kwh": float(updated_bill["tou_on_kwh"]) if updated_bill.get("tou_on_kwh") else None,
                        "tou_mid_kwh": float(updated_bill["tou_mid_kwh"]) if updated_bill.get("tou_mid_kwh") else None,
                        "tou_off_kwh": float(updated_bill["tou_off_kwh"]) if updated_bill.get("tou_off_kwh") else None,
                        "tou_on_rate_dollars": float(updated_bill["tou_on_rate_dollars"])
                        if updated_bill.get("tou_on_rate_dollars")
                        else None,
                        "tou_mid_rate_dollars": float(updated_bill["tou_mid_rate_dollars"])
                        if updated_bill.get("tou_mid_rate_dollars")
                        else None,
                        "tou_off_rate_dollars": float(updated_bill["tou_off_rate_dollars"])
                        if updated_bill.get("tou_off_rate_dollars")
                        else None,
                        "tou_on_cost": float(updated_bill["tou_on_cost"]) if updated_bill.get("tou_on_cost") else None,
                        "tou_mid_cost": float(updated_bill["tou_mid_cost"]) if updated_bill.get("tou_mid_cost") else None,
                        "tou_off_cost": float(updated_bill["tou_off_cost"]) if updated_bill.get("tou_off_cost") else None,
                    },
                }
            )
        except Exception as e:
            print(f"[bills] Error patching bill {bill_id}: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/<int:bill_id>/manual-fix", methods=["PATCH"])
    def manual_fix_bill(bill_id):
        """Accept manual field overrides and mark the parent bill_file as OK."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            updates = request.get_json()
            if not updates:
                return jsonify({"success": False, "error": "No data provided"}), 400

            bill = get_bill_by_id(bill_id)
            if not bill:
                return jsonify({"success": False, "error": "Bill not found"}), 404

            updated_bill = update_bill(bill_id, updates)
            if not updated_bill:
                return jsonify({"success": False, "error": "Failed to update bill"}), 500

            bill_file_id = updated_bill.get("bill_file_id")
            if bill_file_id:
                from psycopg2.extras import Json

                conn = get_connection()
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE utility_bill_files
                            SET missing_fields = %s, review_status = 'ok'
                            WHERE id = %s
                            """,
                            (Json([]), bill_file_id),
                        )
                        conn.commit()
                    print(f"[bills] Bill {bill_id} manual fix applied, file {bill_file_id} marked as OK")
                finally:
                    conn.close()

            return jsonify(
                {
                    "success": True,
                    "message": "Bill saved and marked as OK",
                    "bill": {
                        "id": updated_bill["id"],
                        "utility_name": updated_bill.get("utility_name"),
                        "service_address": updated_bill.get("service_address"),
                        "rate_schedule": updated_bill.get("rate_schedule"),
                        "period_start": str(updated_bill["period_start"]) if updated_bill.get("period_start") else None,
                        "period_end": str(updated_bill["period_end"]) if updated_bill.get("period_end") else None,
                        "days_in_period": updated_bill.get("days_in_period"),
                        "total_kwh": float(updated_bill["total_kwh"]) if updated_bill.get("total_kwh") else None,
                        "total_amount_due": float(updated_bill["total_amount_due"])
                        if updated_bill.get("total_amount_due")
                        else None,
                        "blended_rate_dollars": float(updated_bill["blended_rate_dollars"])
                        if updated_bill.get("blended_rate_dollars")
                        else None,
                        "avg_cost_per_day": float(updated_bill["avg_cost_per_day"])
                        if updated_bill.get("avg_cost_per_day")
                        else None,
                    },
                }
            )
        except Exception as e:
            print(f"[bills] Error applying manual fix to bill {bill_id}: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/file/<int:file_id>/bills", methods=["GET"])
    def get_bills_for_file(file_id):
        """Get all bills associated with a specific file."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            from psycopg2.extras import RealDictCursor

            conn = get_connection()
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT id, utility_name, service_address, rate_schedule,
                               period_start, period_end, total_kwh, total_amount_due
                        FROM bills
                        WHERE bill_file_id = %s
                        """,
                        (file_id,),
                    )
                    bills = cur.fetchall()

                    bills_list = []
                    for bill in bills:
                        bills_list.append(
                            {
                                "id": bill["id"],
                                "utility_name": bill.get("utility_name"),
                                "service_address": bill.get("service_address"),
                                "rate_schedule": bill.get("rate_schedule"),
                                "period_start": str(bill["period_start"]) if bill.get("period_start") else None,
                                "period_end": str(bill["period_end"]) if bill.get("period_end") else None,
                                "total_kwh": float(bill["total_kwh"]) if bill.get("total_kwh") else None,
                                "total_amount_due": float(bill["total_amount_due"])
                                if bill.get("total_amount_due")
                                else None,
                            }
                        )

                    return jsonify({"success": True, "bills": bills_list})
            finally:
                conn.close()
        except Exception as e:
            print(f"[bills] Error getting bills for file {file_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/cancel", methods=["POST"])
    def cancel_bill_processing(project_id, file_id):
        """Cancel processing for a bill file that is stuck or taking too long."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            from bill_intake.db.bill_files import update_file_processing_status, update_bill_file_extraction_payload
            
            # Mark as cancelled with error payload
            update_file_processing_status(file_id, "cancelled", {"reason": "User cancelled"})
            update_bill_file_extraction_payload(file_id, {
                "success": False,
                "error_code": "USER_CANCELLED",
                "error_reason": "Processing was cancelled by user",
                "error": "Cancelled"
            })
            
            print(f"[bills] User cancelled processing for file {file_id}")
            return jsonify({"success": True, "message": "Processing cancelled"})
        except Exception as e:
            print(f"[bills] Error cancelling file {file_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>", methods=["DELETE"])
    def delete_bill_file_route(project_id, file_id):
        """Delete a bill file and all related extracted data."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            # First delete bills and TOU periods for this file
            delete_bills_for_file(file_id)
            
            # Then delete the file record itself
            deleted = delete_bill_file(file_id)
            
            # Finally cleanup any orphaned empty accounts for this project
            if project_id:
                delete_all_empty_accounts(project_id)
            
            if deleted:
                return jsonify({"success": True})
            return jsonify({"success": False, "error": "File not found"}), 404
        except Exception as e:
            print(f"[bills] Error deleting file: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/grouped", methods=["GET"])
    def get_project_bills_grouped(project_id):
        """Get all bill data grouped by account and meter for UI display."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            service_filter = request.args.get("service")
            files = get_bill_files_for_project(project_id)

            if service_filter == "electric":
                # Include electric, combined, and NULL (legacy files before service_type was saved)
                files = [f for f in files if f.get("service_type") in ("electric", "combined", None)]

            grouped_data = get_grouped_bills_data(project_id, service_filter=service_filter)

            files_list = [
                {
                    "id": f["id"],
                    "filename": f["filename"],
                    "original_filename": f["original_filename"],
                    "file_size": f["file_size"],
                    "upload_date": f["upload_date"].isoformat() if f["upload_date"] else None,
                    "processed": f["processed"],
                    "processing_status": f["processing_status"],
                    "review_status": f.get("review_status", "pending"),
                }
                for f in files
            ]

            return jsonify(
                {
                    "success": True,
                    "project_id": project_id,
                    "files": files_list,
                    "accounts": grouped_data.get("accounts", []),
                    "files_status": grouped_data.get("files_status", []),
                }
            )
        except Exception as e:
            print(f"[bills] Error getting grouped bills for project {project_id}: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/detailed", methods=["GET"])
    def get_project_bills_detailed(project_id):
        """Get bill files with detailed extraction data for display."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            files = get_bill_files_for_project(project_id)

            detailed_bills = []
            for f in files:
                if f.get("extraction_payload"):
                    payload = f["extraction_payload"]
                    detailed_data = payload.get("detailed_data", {})

                    detailed_bills.append(
                        {
                            "file_id": f["id"],
                            "original_filename": f["original_filename"],
                            "upload_date": f["upload_date"].isoformat() if f["upload_date"] else None,
                            "review_status": f.get("review_status", "pending"),
                            "utility_name": payload.get("utility_name"),
                            "account_number": payload.get("account_number"),
                            "detailed_data": detailed_data,
                        }
                    )

            detailed_bills.sort(key=lambda x: x.get("upload_date") or "", reverse=True)

            return jsonify({"success": True, "project_id": project_id, "bills": detailed_bills})
        except Exception as e:
            print(f"[bills] Error getting detailed bills for project {project_id}: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/detailed", methods=["GET"])
    def get_bill_file_detailed(project_id, file_id):
        """Get detailed extraction data for a specific bill file."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"success": False, "error": "File not found"}), 404
            if file_record["project_id"] != project_id:
                return jsonify({"success": False, "error": "File does not belong to this project"}), 403

            payload = file_record.get("extraction_payload") or {}
            detailed_data = payload.get("detailed_data", {})

            return jsonify(
                {
                    "success": True,
                    "file_id": file_id,
                    "original_filename": file_record["original_filename"],
                    "utility_name": payload.get("utility_name"),
                    "account_number": payload.get("account_number"),
                    "detailed_data": detailed_data,
                    "extraction_payload": payload,
                }
            )
        except Exception as e:
            print(f"[bills] Error getting detailed data for file {file_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/file/<int:file_id>/pdf")
    def serve_bill_pdf(file_id):
        """Serve the original PDF file for viewing."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"error": "File not found"}), 404

            file_path = file_record.get("file_path")
            if not file_path or not os.path.exists(file_path):
                return jsonify({"error": "PDF file not found on disk"}), 404

            return send_file(
                file_path,
                mimetype="application/pdf",
                as_attachment=False,
                download_name=file_record.get("original_filename", "bill.pdf"),
            )
        except Exception as e:
            print(f"[bills] Error serving PDF for file {file_id}: {e}")
            return jsonify({"error": str(e)}), 500

    @bills_bp.route("/api/projects/<project_id>/bills/files/<int:file_id>/corrections", methods=["POST"])
    def save_bill_correction(project_id, file_id):
        """Save user corrections - supports both full payload updates and individual field corrections."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            file_record = get_bill_file_by_id(file_id)
            if not file_record:
                return jsonify({"success": False, "error": "File not found"}), 404
            if file_record["project_id"] != project_id:
                return jsonify({"success": False, "error": "File does not belong to this project"}), 403

            data = request.get_json()
            if not data:
                return jsonify({"success": False, "error": "No data provided"}), 400

            corrected_payload = data.get("corrected_payload")
            if corrected_payload:
                utility_name = corrected_payload.get("utility_name")
                if not utility_name:
                    existing_payload = file_record.get("extraction_payload") or {}
                    utility_name = existing_payload.get("utility_name")
                if not utility_name:
                    user_id = request.headers.get("X-User-Id", "default")
                    if user_id in stored_data and project_id in stored_data[user_id]:
                        project_data = stored_data[user_id][project_id]
                        utility_name = project_data.get("siteData", {}).get("utility", "Unknown Utility")
                    else:
                        for uid, projects in stored_data.items():
                            if project_id in projects:
                                project_data = projects[project_id]
                                utility_name = project_data.get("siteData", {}).get("utility", "Unknown Utility")
                                break
                utility_name = utility_name or "Unknown Utility"

                corrected_payload["utility_name"] = utility_name

                update_bill_file_extraction_payload(file_id, corrected_payload)
                # Also sync corrected payload into normalized tables so:
                # - embedded bills summaries populate
                # - missing_fields/review_status recompute is based on up-to-date DB values
                try:
                    original_filename = file_record.get("original_filename") or file_record.get("filename") or "bill.pdf"
                    populate_normalized_tables(project_id, corrected_payload, original_filename, file_id=file_id)
                except Exception as sync_err:
                    print(f"[bills] Warning: could not sync corrected payload into normalized tables: {sync_err}")

                missing = recompute_bill_file_missing_fields(file_id)

                print(f"[bills] Updated extraction_payload for file {file_id}, utility={utility_name}")
                return jsonify(
                    {
                        "success": True,
                        "message": "Corrections saved",
                        "missing_fields": missing,
                        "review_status": "needs_review" if missing else "ok",
                    }
                ), 200

            utility_name = data.get("utility_name")
            if not utility_name:
                payload = file_record.get("extraction_payload") or {}
                utility_name = payload.get("utility_name")
            if not utility_name:
                user_id = request.headers.get("X-User-Id", "default")
                if user_id in stored_data and project_id in stored_data[user_id]:
                    project_data = stored_data[user_id][project_id]
                    utility_name = project_data.get("siteData", {}).get("utility", "Unknown Utility")
            utility_name = utility_name or "Unknown Utility"

            field_type = data.get("field_type")
            if not field_type:
                return jsonify({"success": False, "error": "field_type is required"}), 400

            corrected_value = data.get("corrected_value")
            if corrected_value is None:
                return jsonify({"success": False, "error": "corrected_value is required"}), 400

            result = save_correction(
                utility_name=utility_name,
                pdf_hash=data.get("pdf_hash"),
                field_type=field_type,
                meter_number=data.get("meter_number"),
                period_start=data.get("period_start_date"),
                period_end=data.get("period_end_date"),
                corrected_value=str(corrected_value),
                annotated_image_url=data.get("annotated_image_url"),
            )

            if result.get("period_start_date"):
                result["period_start_date"] = str(result["period_start_date"])
            if result.get("period_end_date"):
                result["period_end_date"] = str(result["period_end_date"])
            if result.get("created_at"):
                result["created_at"] = result["created_at"].isoformat()

            print(f"[bills] Saved correction for {utility_name}: {field_type} = {corrected_value}")
            return jsonify({"success": True, "correction": result}), 201

        except Exception as e:
            print(f"[bills] Error saving correction: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500

    @bills_bp.route("/api/bills/training/<utility_name>", methods=["GET"])
    def get_training_data(utility_name):
        """Get past corrections for a utility."""
        if not is_enabled():
            return jsonify({"error": "Bills feature is disabled"}), 403

        try:
            corrections = get_corrections_for_utility(utility_name)
            for c in corrections:
                if c.get("period_start_date"):
                    c["period_start_date"] = str(c["period_start_date"])
                if c.get("period_end_date"):
                    c["period_end_date"] = str(c["period_end_date"])
                if c.get("created_at"):
                    c["created_at"] = c["created_at"].isoformat()

            return jsonify(
                {"success": True, "utility_name": utility_name, "corrections": corrections, "count": len(corrections)}
            )
        except Exception as e:
            print(f"[bills] Error getting training data: {e}")
            import traceback

            traceback.print_exc()
            return jsonify({"success": False, "error": str(e)}), 500


