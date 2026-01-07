"""Read/query operations for normalized `bills` data."""

from __future__ import annotations

import json
from datetime import datetime

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection


def get_bills_summary_for_project(project_id):
    """Get a summary of bills data for a project."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT f.id) as file_count,
                    (SELECT COUNT(*) FROM utility_accounts WHERE project_id = %s) as account_count,
                    (SELECT COUNT(*) FROM utility_meter_reads r
                     JOIN utility_meters m ON r.utility_meter_id = m.id
                     JOIN utility_accounts a ON m.utility_account_id = a.id
                     WHERE a.project_id = %s) as read_count
                FROM utility_bill_files f
                WHERE f.project_id = %s
                """,
                (project_id, project_id, project_id),
            )
            return cur.fetchone()
    finally:
        conn.close()


def get_grouped_bills_data(project_id, service_filter=None):
    """
    Get all bills data for a project, grouped by account and meter.
    Returns structure suitable for UI display.
    Also includes file-level review_status info.

    Args:
        project_id: The project ID
        service_filter: Optional filter ('electric' filters to electric/combined/None service types)
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if service_filter == "electric":
                cur.execute(
                    """
                    SELECT DISTINCT a.id, a.utility_name, a.account_number
                    FROM utility_accounts a
                    JOIN bills b ON b.account_id = a.id
                    JOIN utility_bill_files ubf ON b.bill_file_id = ubf.id
                    WHERE a.project_id = %s
                      AND (ubf.service_type IN ('electric', 'combined') OR ubf.service_type IS NULL)
                      AND b.total_kwh > 0
                    ORDER BY a.utility_name, a.account_number
                    """,
                    (project_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, utility_name, account_number
                    FROM utility_accounts
                    WHERE project_id = %s
                    ORDER BY utility_name, account_number
                    """,
                    (project_id,),
                )
            accounts = cur.fetchall()

            result = []
            for acc in accounts:
                account_data = {
                    "id": acc["id"],
                    "utility_name": acc["utility_name"],
                    "account_number": acc["account_number"],
                    "meters": [],
                }

                if service_filter == "electric":
                    cur.execute(
                        """
                        SELECT DISTINCT m.id, m.meter_number
                        FROM utility_meters m
                        JOIN bills b ON b.meter_id = m.id
                        JOIN utility_bill_files ubf ON b.bill_file_id = ubf.id
                        WHERE m.utility_account_id = %s
                          AND (ubf.service_type IN ('electric', 'combined') OR ubf.service_type IS NULL)
                          AND b.total_kwh > 0
                        ORDER BY m.meter_number
                        """,
                        (acc["id"],),
                    )
                else:
                    cur.execute(
                        """
                        SELECT id, meter_number
                        FROM utility_meters
                        WHERE utility_account_id = %s
                        ORDER BY meter_number
                        """,
                        (acc["id"],),
                    )
                meters = cur.fetchall()

                for meter in meters:
                    meter_data = {"id": meter["id"], "meter_number": meter["meter_number"], "bills": []}

                    if service_filter == "electric":
                        cur.execute(
                            """
                            SELECT DISTINCT b.id, b.period_start, b.period_end,
                                   b.total_kwh, b.total_amount_due,
                                   ubf.original_filename AS source_file
                            FROM bills b
                            JOIN utility_bill_files ubf ON b.bill_file_id = ubf.id
                            WHERE b.meter_id = %s
                              AND (ubf.service_type IN ('electric', 'combined') OR ubf.service_type IS NULL)
                              AND b.total_kwh > 0
                            ORDER BY b.period_end DESC
                            """,
                            (meter["id"],),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT id, billing_start_date, billing_end_date,
                                   kwh, total_charges_usd, source_file
                            FROM utility_meter_reads
                            WHERE utility_meter_id = %s
                            ORDER BY billing_end_date DESC
                            """,
                            (meter["id"],),
                        )
                    reads = cur.fetchall()

                    for read in reads:
                        meter_data["bills"].append(
                            {
                                "id": read["id"],
                                "period_start": str(read.get("period_start")) if read.get("period_start") else None,
                                "period_end": str(read.get("period_end")) if read.get("period_end") else None,
                                "total_kwh": float(read.get("total_kwh")) if read.get("total_kwh") else None,
                                "total_amount_due": float(read.get("total_amount_due"))
                                if read.get("total_amount_due")
                                else None,
                                "source_file": read.get("source_file"),
                            }
                        )

                    account_data["meters"].append(meter_data)

                result.append(account_data)

            service_condition = "AND (service_type IN ('electric', 'combined') OR service_type IS NULL)" if service_filter == "electric" else ""

            cur.execute(
                f"""
                SELECT id, original_filename, review_status, processing_status
                FROM utility_bill_files
                WHERE project_id = %s {service_condition}
                ORDER BY upload_date DESC
                """,
                (project_id,),
            )
            files = cur.fetchall()

            files_status = [
                {
                    "id": f["id"],
                    "original_filename": f["original_filename"],
                    "review_status": f["review_status"],
                    "processing_status": f["processing_status"],
                }
                for f in files
            ]

            return {"accounts": result, "files_status": files_status}
    finally:
        conn.close()


def get_account_summary(account_id, months=12, service_filter=None):
    """
    Get summary for an account: combined totals + per-meter breakdown.
    Returns blended rate in dollars/kWh, avg cost per day, and TOU breakdown totals.
    Deduplicates bills by (meter_id, period_start, period_end, total_kwh, total_amount_due).
    """
    conn = get_connection()
    try:
        if service_filter == "electric":
            service_join = "JOIN utility_bill_files ubf ON b.bill_file_id = ubf.id"
            service_condition = "AND (ubf.service_type IN ('electric', 'combined') OR ubf.service_type IS NULL) AND b.total_kwh > 0"
        else:
            service_join = ""
            service_condition = ""

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                WITH dedupe AS (
                    SELECT DISTINCT ON (b.meter_id, b.period_start, b.period_end, b.total_kwh, b.total_amount_due)
                        b.*
                    FROM bills b
                    {service_join}
                    WHERE b.account_id = %s
                    AND b.period_end >= (CURRENT_DATE - INTERVAL '%s months')
                    {service_condition}
                    ORDER BY b.meter_id, b.period_start, b.period_end, b.total_kwh, b.total_amount_due, b.id
                )
                SELECT
                    SUM(total_kwh) AS total_kwh,
                    SUM(total_amount_due) AS total_cost,
                    SUM(days_in_period) AS total_days,
                    COUNT(*) AS bill_count,
                    SUM(tou_on_kwh) AS tou_on_kwh,
                    SUM(tou_mid_kwh) AS tou_mid_kwh,
                    SUM(tou_off_kwh) AS tou_off_kwh,
                    SUM(tou_super_off_kwh) AS tou_super_off_kwh,
                    SUM(tou_on_cost) AS tou_on_cost,
                    SUM(tou_mid_cost) AS tou_mid_cost,
                    SUM(tou_off_cost) AS tou_off_cost,
                    SUM(tou_super_off_cost) AS tou_super_off_cost
                FROM dedupe
                """,
                (account_id, months),
            )
            combined = cur.fetchone()

            combined_data = {
                "sumKwh": float(combined["total_kwh"]) if combined["total_kwh"] else 0,
                "sumCost": float(combined["total_cost"]) if combined["total_cost"] else 0,
                "totalKwh": float(combined["total_kwh"]) if combined["total_kwh"] else 0,
                "totalCost": float(combined["total_cost"]) if combined["total_cost"] else 0,
                "blendedRateDollars": 0,
                "avgCostPerDay": 0,
                "avgCostPerDayDollars": 0,
                "billCount": combined["bill_count"] or 0,
                "tou": {
                    "onPeakKwh": float(combined["tou_on_kwh"]) if combined["tou_on_kwh"] else None,
                    "midPeakKwh": float(combined["tou_mid_kwh"]) if combined["tou_mid_kwh"] else None,
                    "offPeakKwh": float(combined["tou_off_kwh"]) if combined["tou_off_kwh"] else None,
                    "superOffPeakKwh": float(combined["tou_super_off_kwh"])
                    if combined["tou_super_off_kwh"]
                    else None,
                    "onPeakCost": float(combined["tou_on_cost"]) if combined["tou_on_cost"] else None,
                    "midPeakCost": float(combined["tou_mid_cost"]) if combined["tou_mid_cost"] else None,
                    "offPeakCost": float(combined["tou_off_cost"]) if combined["tou_off_cost"] else None,
                    "superOffPeakCost": float(combined["tou_super_off_cost"])
                    if combined["tou_super_off_cost"]
                    else None,
                },
            }
            if combined_data["sumKwh"] > 0:
                combined_data["blendedRateDollars"] = combined_data["sumCost"] / combined_data["sumKwh"]
            if combined["total_days"] and combined["total_days"] > 0:
                combined_data["avgCostPerDay"] = combined_data["sumCost"] / float(combined["total_days"])
                combined_data["avgCostPerDayDollars"] = combined_data["avgCostPerDay"]

            cur.execute(
                f"""
                WITH dedupe AS (
                    SELECT DISTINCT ON (b.meter_id, b.period_start, b.period_end, b.total_kwh, b.total_amount_due)
                        b.*
                    FROM bills b
                    {service_join}
                    WHERE b.account_id = %s
                    AND b.period_end >= (CURRENT_DATE - INTERVAL '%s months')
                    {service_condition}
                    ORDER BY b.meter_id, b.period_start, b.period_end, b.total_kwh, b.total_amount_due, b.id
                )
                SELECT
                    d.meter_id,
                    m.meter_number,
                    SUM(d.total_kwh) AS total_kwh,
                    SUM(d.total_amount_due) AS total_cost,
                    SUM(d.days_in_period) AS total_days,
                    COUNT(*) AS bill_count,
                    SUM(d.tou_on_kwh) AS tou_on_kwh,
                    SUM(d.tou_mid_kwh) AS tou_mid_kwh,
                    SUM(d.tou_off_kwh) AS tou_off_kwh,
                    SUM(d.tou_super_off_kwh) AS tou_super_off_kwh,
                    SUM(d.tou_on_cost) AS tou_on_cost,
                    SUM(d.tou_mid_cost) AS tou_mid_cost,
                    SUM(d.tou_off_cost) AS tou_off_cost,
                    SUM(d.tou_super_off_cost) AS tou_super_off_cost
                FROM dedupe d
                JOIN utility_meters m ON d.meter_id = m.id
                GROUP BY d.meter_id, m.meter_number
                ORDER BY m.meter_number
                """,
                (account_id, months),
            )
            meters_raw = cur.fetchall()

            meters = []
            for m in meters_raw:
                meter_data = {
                    "meterId": m["meter_id"],
                    "meterNumber": m["meter_number"],
                    "sumKwh": float(m["total_kwh"]) if m["total_kwh"] else 0,
                    "sumCost": float(m["total_cost"]) if m["total_cost"] else 0,
                    "totalKwh": float(m["total_kwh"]) if m["total_kwh"] else 0,
                    "totalCost": float(m["total_cost"]) if m["total_cost"] else 0,
                    "blendedRateDollars": 0,
                    "avgCostPerDay": 0,
                    "avgCostPerDayDollars": 0,
                    "billCount": m["bill_count"] or 0,
                    "tou": {
                        "onPeakKwh": float(m["tou_on_kwh"]) if m["tou_on_kwh"] else None,
                        "midPeakKwh": float(m["tou_mid_kwh"]) if m["tou_mid_kwh"] else None,
                        "offPeakKwh": float(m["tou_off_kwh"]) if m["tou_off_kwh"] else None,
                        "superOffPeakKwh": float(m["tou_super_off_kwh"]) if m["tou_super_off_kwh"] else None,
                        "onPeakCost": float(m["tou_on_cost"]) if m["tou_on_cost"] else None,
                        "midPeakCost": float(m["tou_mid_cost"]) if m["tou_mid_cost"] else None,
                        "offPeakCost": float(m["tou_off_cost"]) if m["tou_off_cost"] else None,
                        "superOffPeakCost": float(m["tou_super_off_cost"]) if m["tou_super_off_cost"] else None,
                    },
                }
                if meter_data["sumKwh"] > 0:
                    meter_data["blendedRateDollars"] = meter_data["sumCost"] / meter_data["sumKwh"]
                if m["total_days"] and m["total_days"] > 0:
                    meter_data["avgCostPerDay"] = meter_data["sumCost"] / float(m["total_days"])
                    meter_data["avgCostPerDayDollars"] = meter_data["avgCostPerDay"]
                meters.append(meter_data)

            for meter in meters:
                meter_id = meter["meterId"]
                cur.execute(
                    f"""
                    SELECT
                        b.id, b.period_start, b.period_end, b.days_in_period,
                        b.total_kwh, b.total_amount_due, b.blended_rate_dollars,
                        b.service_address, b.rate_schedule, b.due_date,
                        b.energy_charges, b.demand_charges, b.other_charges, b.taxes,
                        b.tou_on_kwh, b.tou_mid_kwh, b.tou_off_kwh, b.tou_super_off_kwh,
                        b.tou_on_rate_dollars, b.tou_mid_rate_dollars, b.tou_off_rate_dollars, b.tou_super_off_rate_dollars,
                        b.tou_on_cost, b.tou_mid_cost, b.tou_off_cost, b.tou_super_off_cost
                    FROM bills b
                    {service_join}
                    WHERE b.meter_id = %s
                    AND b.period_end >= (CURRENT_DATE - INTERVAL '%s months')
                    {service_condition}
                    ORDER BY b.period_end DESC
                    """,
                    (meter_id, months),
                )
                bills_raw = cur.fetchall()

                bills = []
                for b in bills_raw:
                    total_kwh = float(b["total_kwh"]) if b["total_kwh"] else 0
                    total_cost = float(b["total_amount_due"]) if b["total_amount_due"] else 0
                    days = b["days_in_period"] or 1

                    period_label = ""
                    if b["period_end"]:
                        pe = b["period_end"]
                        if isinstance(pe, str):
                            pe = datetime.strptime(pe, "%Y-%m-%d").date()
                        period_label = pe.strftime("%b %Y")

                    blended_rate = (
                        float(b["blended_rate_dollars"])
                        if b["blended_rate_dollars"]
                        else (total_cost / total_kwh if total_kwh > 0 else 0)
                    )

                    bills.append(
                        {
                            "billId": b["id"],
                            "periodLabel": period_label,
                            "periodStart": str(b["period_start"]) if b["period_start"] else None,
                            "periodEnd": str(b["period_end"]) if b["period_end"] else None,
                            "daysInPeriod": days,
                            "totalKwh": total_kwh,
                            "totalAmountDue": total_cost,
                            "blendedRateDollars": blended_rate,
                            "serviceAddress": b["service_address"],
                            "rateSchedule": b["rate_schedule"],
                            "dueDate": str(b["due_date"]) if b["due_date"] else None,
                        }
                    )

                meter["bills"] = bills

            return {"accountId": account_id, "months": months, "combined": combined_data, "meters": meters}
    finally:
        conn.close()


def get_meter_bills(meter_id, months=12):
    """Get list of bills for a meter with summary data."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id, bill_file_id, account_id, meter_id, utility_name,
                    service_address, rate_schedule, period_start, period_end,
                    days_in_period, total_kwh, total_amount_due,
                    energy_charges, demand_charges, other_charges, taxes,
                    tou_on_kwh, tou_mid_kwh, tou_off_kwh, tou_super_off_kwh,
                    tou_on_rate_dollars, tou_mid_rate_dollars, tou_off_rate_dollars, tou_super_off_rate_dollars,
                    tou_on_cost, tou_mid_cost, tou_off_cost, tou_super_off_cost,
                    blended_rate_dollars, avg_cost_per_day
                FROM bills
                WHERE meter_id = %s
                AND period_end >= (CURRENT_DATE - INTERVAL '%s months')
                ORDER BY period_end DESC
                """,
                (meter_id, months),
            )
            bills_raw = cur.fetchall()

            bills = []
            for b in bills_raw:
                total_kwh = float(b["total_kwh"]) if b["total_kwh"] else 0
                total_cost = float(b["total_amount_due"]) if b["total_amount_due"] else 0
                days = b["days_in_period"] or 1

                period_label = ""
                if b["period_end"]:
                    pe = b["period_end"]
                    if isinstance(pe, str):
                        pe = datetime.strptime(pe, "%Y-%m-%d").date()
                    period_label = pe.strftime("%b %Y")

                blended_rate = (
                    float(b["blended_rate_dollars"])
                    if b["blended_rate_dollars"]
                    else (total_cost / total_kwh if total_kwh > 0 else 0)
                )
                avg_cost_day = (
                    float(b["avg_cost_per_day"])
                    if b["avg_cost_per_day"]
                    else (round(total_cost / days, 2) if days > 0 else 0)
                )

                bills.append(
                    {
                        "billId": b["id"],
                        "periodLabel": period_label,
                        "periodStart": str(b["period_start"]) if b["period_start"] else None,
                        "periodEnd": str(b["period_end"]) if b["period_end"] else None,
                        "daysInPeriod": days,
                        "totalKwh": total_kwh,
                        "totalAmountDue": total_cost,
                        "avgKwhPerDay": round(total_kwh / days, 1) if days > 0 else 0,
                        "blendedRateDollars": blended_rate,
                        "avgCostPerDay": avg_cost_day,
                        "avgCostPerDayDollars": avg_cost_day,
                        "tou": {
                            "onPeakKwh": float(b["tou_on_kwh"]) if b["tou_on_kwh"] else None,
                            "midPeakKwh": float(b["tou_mid_kwh"]) if b["tou_mid_kwh"] else None,
                            "offPeakKwh": float(b["tou_off_kwh"]) if b["tou_off_kwh"] else None,
                            "superOffPeakKwh": float(b["tou_super_off_kwh"]) if b["tou_super_off_kwh"] else None,
                            "onPeakRateDollars": float(b["tou_on_rate_dollars"]) if b["tou_on_rate_dollars"] else None,
                            "midPeakRateDollars": float(b["tou_mid_rate_dollars"])
                            if b["tou_mid_rate_dollars"]
                            else None,
                            "offPeakRateDollars": float(b["tou_off_rate_dollars"]) if b["tou_off_rate_dollars"] else None,
                            "superOffPeakRateDollars": float(b["tou_super_off_rate_dollars"])
                            if b["tou_super_off_rate_dollars"]
                            else None,
                            "onPeakCost": float(b["tou_on_cost"]) if b["tou_on_cost"] else None,
                            "midPeakCost": float(b["tou_mid_cost"]) if b["tou_mid_cost"] else None,
                            "offPeakCost": float(b["tou_off_cost"]) if b["tou_off_cost"] else None,
                            "superOffPeakCost": float(b["tou_super_off_cost"]) if b["tou_super_off_cost"] else None,
                        },
                    }
                )

            return {"meterId": meter_id, "months": months, "bills": bills}
    finally:
        conn.close()


def get_bill_detail(bill_id):
    """Get full detail for a single bill including TOU fields and source file metadata."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.id, b.bill_file_id, b.account_id, b.meter_id, b.utility_name,
                    b.service_address, b.rate_schedule, b.period_start, b.period_end,
                    b.days_in_period, b.total_kwh, b.total_amount_due, b.due_date,
                    b.energy_charges, b.demand_charges, b.other_charges, b.taxes,
                    b.tou_on_kwh, b.tou_mid_kwh, b.tou_off_kwh, b.tou_super_off_kwh,
                    b.tou_on_rate_dollars, b.tou_mid_rate_dollars, b.tou_off_rate_dollars, b.tou_super_off_rate_dollars,
                    b.tou_on_cost, b.tou_mid_cost, b.tou_off_cost, b.tou_super_off_cost,
                    b.blended_rate_dollars, b.avg_cost_per_day,
                    a.account_number,
                    m.meter_number,
                    f.original_filename, f.upload_date, f.file_path, f.extraction_payload
                FROM bills b
                JOIN utility_accounts a ON b.account_id = a.id
                JOIN utility_meters m ON b.meter_id = m.id
                LEFT JOIN utility_bill_files f ON b.bill_file_id = f.id
                WHERE b.id = %s
                """,
                (bill_id,),
            )
            b = cur.fetchone()
            if not b:
                return None

            total_kwh = float(b["total_kwh"]) if b["total_kwh"] else 0
            total_cost = float(b["total_amount_due"]) if b["total_amount_due"] else 0
            days = b["days_in_period"] or 1

            blended_rate = (
                float(b["blended_rate_dollars"])
                if b["blended_rate_dollars"]
                else (total_cost / total_kwh if total_kwh > 0 else 0)
            )
            avg_cost_day = (
                float(b["avg_cost_per_day"])
                if b["avg_cost_per_day"]
                else (round(total_cost / days, 2) if days > 0 else 0)
            )

            payload = b.get("extraction_payload")
            if payload and isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            payload = payload or {}
            detailed_data = payload.get("detailed_data", {}) if payload else {}

            due_date = b["due_date"] or detailed_data.get("due_date") or payload.get("due_date")

            service_address = b["service_address"]
            if not service_address:
                service_address = detailed_data.get("service_address") or payload.get("service_address")
                meters = payload.get("meters", [])
                if not service_address and meters:
                    service_address = meters[0].get("service_address")

            rate_schedule = b["rate_schedule"] or detailed_data.get("rate_schedule") or payload.get("rate_schedule")

            cur.execute(
                """
                SELECT period, kwh, rate_dollars_per_kwh, est_cost_dollars
                FROM bill_tou_periods
                WHERE bill_id = %s
                ORDER BY
                    CASE period
                        WHEN 'On-Peak' THEN 1
                        WHEN 'Mid-Peak' THEN 2
                        WHEN 'Off-Peak' THEN 3
                        ELSE 4
                    END
                """,
                (bill_id,),
            )
            tou_raw = cur.fetchall()

            tou_periods = []
            if tou_raw:
                tou_periods = [
                    {
                        "period": t["period"],
                        "kwh": float(t["kwh"]) if t["kwh"] else 0,
                        "rateDollarsPerKwh": float(t["rate_dollars_per_kwh"])
                        if t["rate_dollars_per_kwh"]
                        else 0,
                        "estCostDollars": float(t["est_cost_dollars"]) if t["est_cost_dollars"] else 0,
                    }
                    for t in tou_raw
                ]

            return {
                "billId": b["id"],
                "billFileId": b["bill_file_id"],
                "accountId": b["account_id"],
                "accountNumber": b["account_number"],
                "meterId": b["meter_id"],
                "meterNumber": b["meter_number"],
                "utilityName": b["utility_name"],
                "serviceAddress": service_address,
                "rateSchedule": rate_schedule,
                "periodStart": str(b["period_start"]) if b["period_start"] else None,
                "periodEnd": str(b["period_end"]) if b["period_end"] else None,
                "dueDate": str(due_date) if due_date else None,
                "daysInPeriod": days,
                "totalKwh": total_kwh,
                "totalAmountDue": total_cost,
                "avgKwhPerDay": round(total_kwh / days, 1) if days > 0 else 0,
                "blendedRateDollars": blended_rate,
                "avgCostPerDay": avg_cost_day,
                "avgCostPerDayDollars": avg_cost_day,
                "charges": {
                    "energyCharges": float(b["energy_charges"]) if b["energy_charges"] else 0,
                    "demandCharges": float(b["demand_charges"]) if b["demand_charges"] else 0,
                    "otherCharges": float(b["other_charges"]) if b["other_charges"] else 0,
                    "taxes": float(b["taxes"]) if b["taxes"] else 0,
                },
                "tou": {
                    "onPeakKwh": float(b["tou_on_kwh"]) if b["tou_on_kwh"] else None,
                    "midPeakKwh": float(b["tou_mid_kwh"]) if b["tou_mid_kwh"] else None,
                    "offPeakKwh": float(b["tou_off_kwh"]) if b["tou_off_kwh"] else None,
                    "superOffPeakKwh": float(b["tou_super_off_kwh"]) if b["tou_super_off_kwh"] else None,
                    "onPeakRateDollars": float(b["tou_on_rate_dollars"]) if b["tou_on_rate_dollars"] else None,
                    "midPeakRateDollars": float(b["tou_mid_rate_dollars"])
                    if b["tou_mid_rate_dollars"]
                    else None,
                    "offPeakRateDollars": float(b["tou_off_rate_dollars"]) if b["tou_off_rate_dollars"] else None,
                    "superOffPeakRateDollars": float(b["tou_super_off_rate_dollars"])
                    if b["tou_super_off_rate_dollars"]
                    else None,
                    "onPeakCost": float(b["tou_on_cost"]) if b["tou_on_cost"] else None,
                    "midPeakCost": float(b["tou_mid_cost"]) if b["tou_mid_cost"] else None,
                    "offPeakCost": float(b["tou_off_cost"]) if b["tou_off_cost"] else None,
                    "superOffPeakCost": float(b["tou_super_off_cost"]) if b["tou_super_off_cost"] else None,
                },
                "touPeriods": tou_periods,
                "sourceFile": {
                    "originalFilename": b["original_filename"],
                    "uploadDate": b["upload_date"].isoformat() if b["upload_date"] else None,
                }
                if b.get("original_filename")
                else None,
            }
    finally:
        conn.close()


def get_meter_months(account_id, meter_id, months=12):
    """Get month-by-month breakdown for a specific meter under an account."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id, period_start, period_end, days_in_period,
                    total_kwh, total_amount_due,
                    tou_on_kwh, tou_mid_kwh, tou_off_kwh, tou_super_off_kwh,
                    tou_on_rate_dollars, tou_mid_rate_dollars, tou_off_rate_dollars, tou_super_off_rate_dollars,
                    tou_on_cost, tou_mid_cost, tou_off_cost, tou_super_off_cost,
                    blended_rate_dollars, avg_cost_per_day
                FROM bills
                WHERE account_id = %s AND meter_id = %s
                AND period_end >= (CURRENT_DATE - INTERVAL '%s months')
                ORDER BY period_end DESC
                """,
                (account_id, meter_id, months),
            )
            bills_raw = cur.fetchall()

            monthly_data = []
            for b in bills_raw:
                total_kwh = float(b["total_kwh"]) if b["total_kwh"] else 0
                total_cost = float(b["total_amount_due"]) if b["total_amount_due"] else 0
                days = b["days_in_period"] or 1

                period_label = ""
                if b["period_end"]:
                    pe = b["period_end"]
                    if isinstance(pe, str):
                        pe = datetime.strptime(pe, "%Y-%m-%d").date()
                    period_label = pe.strftime("%b %Y")

                blended_rate = (
                    float(b["blended_rate_dollars"])
                    if b["blended_rate_dollars"]
                    else (total_cost / total_kwh if total_kwh > 0 else 0)
                )
                avg_cost_day = (
                    float(b["avg_cost_per_day"])
                    if b["avg_cost_per_day"]
                    else (round(total_cost / days, 2) if days > 0 else 0)
                )

                monthly_data.append(
                    {
                        "billId": b["id"],
                        "period": period_label,
                        "periodStart": str(b["period_start"]) if b["period_start"] else None,
                        "periodEnd": str(b["period_end"]) if b["period_end"] else None,
                        "daysInPeriod": days,
                        "totalKwh": total_kwh,
                        "totalCost": total_cost,
                        "blendedRate": blended_rate,
                        "blendedRateDollars": blended_rate,
                        "avgCostPerDay": avg_cost_day,
                        "avgCostPerDayDollars": avg_cost_day,
                        "tou": {
                            "onPeakKwh": float(b["tou_on_kwh"]) if b["tou_on_kwh"] else None,
                            "midPeakKwh": float(b["tou_mid_kwh"]) if b["tou_mid_kwh"] else None,
                            "offPeakKwh": float(b["tou_off_kwh"]) if b["tou_off_kwh"] else None,
                            "superOffPeakKwh": float(b["tou_super_off_kwh"]) if b["tou_super_off_kwh"] else None,
                            "onPeakRateDollars": float(b["tou_on_rate_dollars"]) if b["tou_on_rate_dollars"] else None,
                            "midPeakRateDollars": float(b["tou_mid_rate_dollars"])
                            if b["tou_mid_rate_dollars"]
                            else None,
                            "offPeakRateDollars": float(b["tou_off_rate_dollars"]) if b["tou_off_rate_dollars"] else None,
                            "superOffPeakRateDollars": float(b["tou_super_off_rate_dollars"])
                            if b["tou_super_off_rate_dollars"]
                            else None,
                            "onPeakCost": float(b["tou_on_cost"]) if b["tou_on_cost"] else None,
                            "midPeakCost": float(b["tou_mid_cost"]) if b["tou_mid_cost"] else None,
                            "offPeakCost": float(b["tou_off_cost"]) if b["tou_off_cost"] else None,
                            "superOffPeakCost": float(b["tou_super_off_cost"]) if b["tou_super_off_cost"] else None,
                        },
                    }
                )

            return {"accountId": account_id, "meterId": meter_id, "months": months, "data": monthly_data}
    finally:
        conn.close()


def get_bill_by_id(bill_id):
    """Get a single bill record by ID with all fields."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.id, b.bill_file_id, b.account_id, b.meter_id,
                    b.utility_name, b.service_address, b.rate_schedule,
                    b.period_start, b.period_end, b.days_in_period,
                    b.total_kwh, b.total_amount_due,
                    b.energy_charges, b.demand_charges, b.other_charges, b.taxes,
                    b.tou_on_kwh, b.tou_mid_kwh, b.tou_off_kwh, b.tou_super_off_kwh,
                    b.tou_on_rate_dollars, b.tou_mid_rate_dollars, b.tou_off_rate_dollars, b.tou_super_off_rate_dollars,
                    b.tou_on_cost, b.tou_mid_cost, b.tou_off_cost, b.tou_super_off_cost,
                    b.blended_rate_dollars, b.avg_cost_per_day,
                    b.created_at,
                    f.original_filename, f.upload_date, f.missing_fields
                FROM bills b
                LEFT JOIN utility_bill_files f ON b.bill_file_id = f.id
                WHERE b.id = %s
                """,
                (bill_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def get_bill_review_data(bill_id):
    """Get bill data formatted for review UI."""
    bill = get_bill_by_id(bill_id)
    if not bill:
        return None

    field_labels = {
        "utility_name": "Utility Name",
        "account_number": "Account Number",
        "total_kwh": "Total kWh",
        "total_amount_due": "Total Amount Due",
        "rate_schedule": "Rate Schedule",
        "service_address": "Service Address",
        "period_start": "Period Start",
        "period_end": "Period End",
        "days_in_period": "Days in Period",
        "energy_charges": "Energy Charges",
        "demand_charges": "Demand Charges",
        "other_charges": "Other Charges",
        "taxes": "Taxes",
        "tou_on_kwh": "TOU On-Peak kWh",
        "tou_mid_kwh": "TOU Mid-Peak kWh",
        "tou_off_kwh": "TOU Off-Peak kWh",
        "tou_on_rate_dollars": "TOU On-Peak Rate",
        "tou_mid_rate_dollars": "TOU Mid-Peak Rate",
        "tou_off_rate_dollars": "TOU Off-Peak Rate",
    }

    missing_fields = bill.get("missing_fields") or []
    if isinstance(missing_fields, str):
        try:
            missing_fields = json.loads(missing_fields)
        except Exception:
            missing_fields = []

    missing_list = []
    for field in missing_fields:
        label = field_labels.get(field, field.replace("_", " ").title())
        missing_list.append({"field": field, "label": label})

    current_values = {
        "total_kwh": float(bill["total_kwh"]) if bill["total_kwh"] else None,
        "total_amount_due": float(bill["total_amount_due"]) if bill["total_amount_due"] else None,
        "rate_schedule": bill["rate_schedule"],
        "service_address": bill["service_address"],
        "utility_name": bill["utility_name"],
        "period_start": str(bill["period_start"]) if bill["period_start"] else None,
        "period_end": str(bill["period_end"]) if bill["period_end"] else None,
        "days_in_period": bill["days_in_period"],
        "energy_charges": float(bill["energy_charges"]) if bill["energy_charges"] else None,
        "demand_charges": float(bill["demand_charges"]) if bill["demand_charges"] else None,
        "other_charges": float(bill["other_charges"]) if bill["other_charges"] else None,
        "taxes": float(bill["taxes"]) if bill["taxes"] else None,
        "tou_on_kwh": float(bill["tou_on_kwh"]) if bill["tou_on_kwh"] else None,
        "tou_mid_kwh": float(bill["tou_mid_kwh"]) if bill["tou_mid_kwh"] else None,
        "tou_off_kwh": float(bill["tou_off_kwh"]) if bill["tou_off_kwh"] else None,
        "tou_on_rate_dollars": float(bill["tou_on_rate_dollars"]) if bill["tou_on_rate_dollars"] else None,
        "tou_mid_rate_dollars": float(bill["tou_mid_rate_dollars"]) if bill["tou_mid_rate_dollars"] else None,
        "tou_off_rate_dollars": float(bill["tou_off_rate_dollars"]) if bill["tou_off_rate_dollars"] else None,
        "tou_on_cost": float(bill["tou_on_cost"]) if bill["tou_on_cost"] else None,
        "tou_mid_cost": float(bill["tou_mid_cost"]) if bill["tou_mid_cost"] else None,
        "tou_off_cost": float(bill["tou_off_cost"]) if bill["tou_off_cost"] else None,
        "blended_rate_dollars": float(bill["blended_rate_dollars"]) if bill["blended_rate_dollars"] else None,
        "avg_cost_per_day": float(bill["avg_cost_per_day"]) if bill["avg_cost_per_day"] else None,
        "bill_file_id": bill["bill_file_id"],
        "account_id": bill["account_id"],
        "meter_id": bill["meter_id"],
    }

    return {"billId": bill_id, "missing": missing_list, "currentValues": current_values}


