"""Cloning utilities for bill intake data between projects."""

from __future__ import annotations

from psycopg2.extras import Json, RealDictCursor

from bill_intake.db.connection import get_connection


def clone_bills_for_project(old_project_id, new_project_id):
    """
    Clone all utility bill data from one project to another.

    This includes:
    - utility_bill_files entries (referencing the same file paths)
    - utility_accounts entries
    - utility_meters entries (linked to new accounts)
    - bills entries (linked to new accounts/meters/files)
    - bill_tou_periods entries (linked to new bills)
    - bill_screenshots entries (linked to new bill files)
    """
    conn = get_connection()
    try:
        counts = {"files": 0, "accounts": 0, "meters": 0, "bills": 0, "tou_periods": 0, "screenshots": 0}

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            file_id_map = {}
            account_id_map = {}
            meter_id_map = {}
            bill_id_map = {}

            cur.execute(
                """
                SELECT id, filename, original_filename, file_path, file_size, mime_type,
                       processed, processing_status, review_status, extraction_payload, missing_fields
                FROM utility_bill_files
                WHERE project_id = %s
                """,
                (old_project_id,),
            )
            old_files = cur.fetchall()

            for f in old_files:
                cur.execute(
                    """
                    INSERT INTO utility_bill_files
                    (project_id, filename, original_filename, file_path, file_size, mime_type,
                     processed, processing_status, review_status, extraction_payload, missing_fields)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        new_project_id,
                        f["filename"],
                        f["original_filename"],
                        f["file_path"],
                        f["file_size"],
                        f["mime_type"],
                        f["processed"],
                        f["processing_status"],
                        f["review_status"],
                        Json(f["extraction_payload"]) if f["extraction_payload"] else None,
                        Json(f["missing_fields"]) if f.get("missing_fields") else None,
                    ),
                )
                new_file = cur.fetchone()
                file_id_map[f["id"]] = new_file["id"]
                counts["files"] += 1

            cur.execute(
                """
                SELECT id, utility_name, account_number
                FROM utility_accounts
                WHERE project_id = %s
                """,
                (old_project_id,),
            )
            old_accounts = cur.fetchall()

            for a in old_accounts:
                cur.execute(
                    """
                    INSERT INTO utility_accounts (project_id, utility_name, account_number)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (new_project_id, a["utility_name"], a["account_number"]),
                )
                new_account = cur.fetchone()
                account_id_map[a["id"]] = new_account["id"]
                counts["accounts"] += 1

            cur.execute(
                """
                SELECT id, utility_account_id, meter_number, service_address
                FROM utility_meters
                WHERE utility_account_id IN (SELECT id FROM utility_accounts WHERE project_id = %s)
                """,
                (old_project_id,),
            )
            old_meters = cur.fetchall()

            for m in old_meters:
                new_account_id = account_id_map.get(m["utility_account_id"])
                if new_account_id:
                    cur.execute(
                        """
                        INSERT INTO utility_meters (utility_account_id, meter_number, service_address)
                        VALUES (%s, %s, %s)
                        RETURNING id
                        """,
                        (new_account_id, m["meter_number"], m["service_address"]),
                    )
                    new_meter = cur.fetchone()
                    meter_id_map[m["id"]] = new_meter["id"]
                    counts["meters"] += 1

            cur.execute(
                """
                SELECT b.id, b.bill_file_id, b.account_id, b.meter_id, b.utility_name,
                       b.service_address, b.rate_schedule, b.period_start, b.period_end,
                       b.days_in_period, b.total_kwh, b.total_amount_due,
                       b.energy_charges, b.demand_charges, b.other_charges, b.taxes,
                       b.tou_on_kwh, b.tou_mid_kwh, b.tou_off_kwh,
                       b.tou_on_rate_dollars, b.tou_mid_rate_dollars, b.tou_off_rate_dollars,
                       b.tou_on_cost, b.tou_mid_cost, b.tou_off_cost,
                       b.blended_rate_dollars, b.avg_cost_per_day
                FROM bills b
                WHERE b.account_id IN (SELECT id FROM utility_accounts WHERE project_id = %s)
                """,
                (old_project_id,),
            )
            old_bills = cur.fetchall()

            for b in old_bills:
                new_account_id = account_id_map.get(b["account_id"])
                new_meter_id = meter_id_map.get(b["meter_id"])
                new_file_id = file_id_map.get(b["bill_file_id"])

                if new_account_id and new_meter_id:
                    cur.execute(
                        """
                        INSERT INTO bills
                        (bill_file_id, account_id, meter_id, utility_name, service_address,
                         rate_schedule, period_start, period_end, days_in_period, total_kwh,
                         total_amount_due, energy_charges, demand_charges, other_charges, taxes,
                         tou_on_kwh, tou_mid_kwh, tou_off_kwh,
                         tou_on_rate_dollars, tou_mid_rate_dollars, tou_off_rate_dollars,
                         tou_on_cost, tou_mid_cost, tou_off_cost,
                         blended_rate_dollars, avg_cost_per_day)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            new_file_id,
                            new_account_id,
                            new_meter_id,
                            b["utility_name"],
                            b["service_address"],
                            b["rate_schedule"],
                            b["period_start"],
                            b["period_end"],
                            b["days_in_period"],
                            b["total_kwh"],
                            b["total_amount_due"],
                            b["energy_charges"],
                            b["demand_charges"],
                            b["other_charges"],
                            b["taxes"],
                            b["tou_on_kwh"],
                            b["tou_mid_kwh"],
                            b["tou_off_kwh"],
                            b["tou_on_rate_dollars"],
                            b["tou_mid_rate_dollars"],
                            b["tou_off_rate_dollars"],
                            b["tou_on_cost"],
                            b["tou_mid_cost"],
                            b["tou_off_cost"],
                            b["blended_rate_dollars"],
                            b["avg_cost_per_day"],
                        ),
                    )
                    new_bill = cur.fetchone()
                    bill_id_map[b["id"]] = new_bill["id"]
                    counts["bills"] += 1

            for old_bill_id, new_bill_id in bill_id_map.items():
                cur.execute(
                    """
                    SELECT period, kwh, rate_dollars_per_kwh, est_cost_dollars
                    FROM bill_tou_periods
                    WHERE bill_id = %s
                    """,
                    (old_bill_id,),
                )
                old_tou_periods = cur.fetchall()

                for tp in old_tou_periods:
                    cur.execute(
                        """
                        INSERT INTO bill_tou_periods (bill_id, period, kwh, rate_dollars_per_kwh, est_cost_dollars)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            new_bill_id,
                            tp["period"],
                            tp["kwh"],
                            tp["rate_dollars_per_kwh"],
                            tp["est_cost_dollars"],
                        ),
                    )
                    counts["tou_periods"] += 1

            for old_file_id, new_file_id in file_id_map.items():
                cur.execute(
                    """
                    SELECT file_path, original_filename, mime_type, page_hint
                    FROM bill_screenshots
                    WHERE bill_id = %s
                    """,
                    (old_file_id,),
                )
                old_screenshots = cur.fetchall()

                for ss in old_screenshots:
                    cur.execute(
                        """
                        INSERT INTO bill_screenshots (bill_id, file_path, original_filename, mime_type, page_hint)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (new_file_id, ss["file_path"], ss["original_filename"], ss["mime_type"], ss["page_hint"]),
                    )
                    counts["screenshots"] += 1

            conn.commit()
            print(f"[bills_db] Cloned bills for project {old_project_id} -> {new_project_id}: {counts}")
            return counts
    except Exception as e:
        conn.rollback()
        print(f"[bills_db] Error cloning bills: {e}")
        raise e
    finally:
        conn.close()


