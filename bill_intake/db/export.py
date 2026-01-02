"""Exports for bill intake data (e.g., CSV)."""

from __future__ import annotations

from bill_intake.db.connection import get_connection


def export_bills_csv(project_id):
    """
    Export all bills for a project as CSV data.
    Returns CSV string with headers and all bill data.
    Format designed for Excel/jMaster import.
    """
    import csv
    import io

    from psycopg2.extras import RealDictCursor

    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    a.utility_name,
                    a.account_number,
                    m.meter_number,
                    b.service_address,
                    b.rate_schedule,
                    b.period_start,
                    b.period_end,
                    b.due_date,
                    b.days_in_period,
                    b.total_kwh,
                    b.total_amount_due,
                    b.blended_rate_dollars,
                    b.avg_cost_per_day,
                    b.energy_charges,
                    b.demand_charges,
                    b.other_charges,
                    b.taxes,
                    b.tou_on_kwh,
                    b.tou_on_rate_dollars,
                    b.tou_on_cost,
                    b.tou_mid_kwh,
                    b.tou_mid_rate_dollars,
                    b.tou_mid_cost,
                    b.tou_off_kwh,
                    b.tou_off_rate_dollars,
                    b.tou_off_cost,
                    b.tou_super_off_kwh,
                    b.tou_super_off_rate_dollars,
                    b.tou_super_off_cost,
                    f.original_filename AS source_file
                FROM bills b
                JOIN utility_accounts a ON b.account_id = a.id
                JOIN utility_meters m ON b.meter_id = m.id
                LEFT JOIN utility_bill_files f ON b.bill_file_id = f.id
                WHERE a.project_id = %s
                ORDER BY a.account_number, m.meter_number, b.period_end DESC
                """,
                (project_id,),
            )
            bills = cur.fetchall()

            if not bills:
                return None

            output = io.StringIO()
            headers = [
                "Utility",
                "Account Number",
                "Meter Number",
                "Service Address",
                "Rate Schedule",
                "Period Start",
                "Period End",
                "Due Date",
                "Days",
                "Total kWh",
                "Total Amount ($)",
                "Blended Rate ($/kWh)",
                "Avg Cost/Day ($)",
                "Energy Charges ($)",
                "Demand Charges ($)",
                "Other Charges ($)",
                "Taxes ($)",
                "On-Peak kWh",
                "On-Peak Rate ($/kWh)",
                "On-Peak Cost ($)",
                "Mid-Peak kWh",
                "Mid-Peak Rate ($/kWh)",
                "Mid-Peak Cost ($)",
                "Off-Peak kWh",
                "Off-Peak Rate ($/kWh)",
                "Off-Peak Cost ($)",
                "Super Off-Peak kWh",
                "Super Off-Peak Rate ($/kWh)",
                "Super Off-Peak Cost ($)",
                "Source File",
            ]

            writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)

            for b in bills:
                row = [
                    b["utility_name"] or "",
                    b["account_number"] or "",
                    b["meter_number"] or "",
                    b["service_address"] or "",
                    b["rate_schedule"] or "",
                    str(b["period_start"]) if b["period_start"] else "",
                    str(b["period_end"]) if b["period_end"] else "",
                    str(b["due_date"]) if b["due_date"] else "",
                    b["days_in_period"] or "",
                    float(b["total_kwh"]) if b["total_kwh"] else "",
                    float(b["total_amount_due"]) if b["total_amount_due"] else "",
                    round(float(b["blended_rate_dollars"]), 4) if b["blended_rate_dollars"] else "",
                    round(float(b["avg_cost_per_day"]), 2) if b["avg_cost_per_day"] else "",
                    float(b["energy_charges"]) if b["energy_charges"] else "",
                    float(b["demand_charges"]) if b["demand_charges"] else "",
                    float(b["other_charges"]) if b["other_charges"] else "",
                    float(b["taxes"]) if b["taxes"] else "",
                    float(b["tou_on_kwh"]) if b["tou_on_kwh"] else "",
                    round(float(b["tou_on_rate_dollars"]), 4) if b["tou_on_rate_dollars"] else "",
                    float(b["tou_on_cost"]) if b["tou_on_cost"] else "",
                    float(b["tou_mid_kwh"]) if b["tou_mid_kwh"] else "",
                    round(float(b["tou_mid_rate_dollars"]), 4) if b["tou_mid_rate_dollars"] else "",
                    float(b["tou_mid_cost"]) if b["tou_mid_cost"] else "",
                    float(b["tou_off_kwh"]) if b["tou_off_kwh"] else "",
                    round(float(b["tou_off_rate_dollars"]), 4) if b["tou_off_rate_dollars"] else "",
                    float(b["tou_off_cost"]) if b["tou_off_cost"] else "",
                    float(b["tou_super_off_kwh"]) if b["tou_super_off_kwh"] else "",
                    round(float(b["tou_super_off_rate_dollars"]), 4)
                    if b["tou_super_off_rate_dollars"]
                    else "",
                    float(b["tou_super_off_cost"]) if b["tou_super_off_cost"] else "",
                    b["source_file"] or "",
                ]
                writer.writerow(row)

            csv_content = output.getvalue()
            output.close()
            return csv_content
    except Exception as e:
        print(f"[bills_db] Error exporting bills CSV: {e}")
        return None
    finally:
        conn.close()


