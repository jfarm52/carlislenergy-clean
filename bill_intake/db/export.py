"""Import/Export for bill intake data (CSV)."""

from __future__ import annotations

import csv
import io

from bill_intake.db.connection import get_connection
from bill_intake.db.accounts import upsert_utility_account
from bill_intake.db.meters import upsert_utility_meter
from bill_intake.db.bills_write import insert_bill


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


def import_bills_csv(project_id, csv_content):
    """
    Import bills from CSV into a project.
    Creates accounts, meters, and bills from the CSV data.
    Returns dict with import stats.
    
    This is the inverse of export_bills_csv() - allows instant recreation
    of all bill data without re-uploading and re-processing PDFs.
    """
    # CSV header to internal field mapping
    HEADER_MAP = {
        "Utility": "utility_name",
        "Account Number": "account_number",
        "Meter Number": "meter_number",
        "Service Address": "service_address",
        "Rate Schedule": "rate_schedule",
        "Period Start": "period_start",
        "Period End": "period_end",
        "Due Date": "due_date",
        "Days": "days_in_period",
        "Total kWh": "total_kwh",
        "Total Amount ($)": "total_amount_due",
        "Blended Rate ($/kWh)": "blended_rate",
        "Avg Cost/Day ($)": "avg_cost_per_day",
        "Energy Charges ($)": "energy_charges",
        "Demand Charges ($)": "demand_charges",
        "Other Charges ($)": "other_charges",
        "Taxes ($)": "taxes",
        "On-Peak kWh": "tou_on_kwh",
        "On-Peak Rate ($/kWh)": "tou_on_rate_dollars",
        "On-Peak Cost ($)": "tou_on_cost",
        "Mid-Peak kWh": "tou_mid_kwh",
        "Mid-Peak Rate ($/kWh)": "tou_mid_rate_dollars",
        "Mid-Peak Cost ($)": "tou_mid_cost",
        "Off-Peak kWh": "tou_off_kwh",
        "Off-Peak Rate ($/kWh)": "tou_off_rate_dollars",
        "Off-Peak Cost ($)": "tou_off_cost",
        "Super Off-Peak kWh": "tou_super_off_kwh",
        "Super Off-Peak Rate ($/kWh)": "tou_super_off_rate_dollars",
        "Super Off-Peak Cost ($)": "tou_super_off_cost",
        "Source File": "source_file",
    }
    
    def parse_float(val):
        """Parse a float value, returning None for empty/invalid."""
        if val is None or val == "" or val == "None":
            return None
        try:
            return float(str(val).replace(",", "").replace("$", "").strip())
        except (ValueError, TypeError):
            return None
    
    def parse_date(val):
        """Parse a date value, returning None for empty/invalid."""
        if val is None or val == "" or val == "None":
            return None
        val = str(val).strip()
        if not val:
            return None
        # Handle YYYY-MM-DD format from export
        if len(val) == 10 and val[4] == "-":
            return val
        return val
    
    try:
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        
        stats = {
            "accounts_created": 0,
            "meters_created": 0,
            "bills_imported": 0,
            "rows_skipped": 0,
            "errors": []
        }
        
        # Track created entities to count unique creations
        seen_accounts = set()
        seen_meters = set()
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
            try:
                # Map headers to internal fields
                data = {}
                for csv_header, internal_key in HEADER_MAP.items():
                    if csv_header in row:
                        data[internal_key] = row[csv_header]
                
                # Required fields check
                utility = data.get("utility_name", "").strip()
                account_num = data.get("account_number", "").strip()
                
                if not utility or not account_num:
                    stats["rows_skipped"] += 1
                    continue
                
                # Get or create account
                account_key = f"{utility}:{account_num}"
                account_id = upsert_utility_account(project_id, utility, account_num)
                if account_key not in seen_accounts:
                    seen_accounts.add(account_key)
                    stats["accounts_created"] += 1
                
                # Get or create meter
                meter_num = data.get("meter_number", "").strip() or "Primary"
                service_addr = data.get("service_address", "").strip()
                meter_key = f"{account_id}:{meter_num}"
                meter_id = upsert_utility_meter(account_id, meter_num, service_addr)
                if meter_key not in seen_meters:
                    seen_meters.add(meter_key)
                    stats["meters_created"] += 1
                
                # Insert bill (bill_file_id = None since imported from CSV)
                bill_id = insert_bill(
                    bill_file_id=None,
                    account_id=account_id,
                    meter_id=meter_id,
                    utility_name=utility,
                    service_address=service_addr,
                    rate_schedule=data.get("rate_schedule", "").strip() or None,
                    period_start=parse_date(data.get("period_start")),
                    period_end=parse_date(data.get("period_end")),
                    total_kwh=parse_float(data.get("total_kwh")),
                    total_amount_due=parse_float(data.get("total_amount_due")),
                    energy_charges=parse_float(data.get("energy_charges")),
                    demand_charges=parse_float(data.get("demand_charges")),
                    other_charges=parse_float(data.get("other_charges")),
                    taxes=parse_float(data.get("taxes")),
                    tou_on_kwh=parse_float(data.get("tou_on_kwh")),
                    tou_mid_kwh=parse_float(data.get("tou_mid_kwh")),
                    tou_off_kwh=parse_float(data.get("tou_off_kwh")),
                    tou_super_off_kwh=parse_float(data.get("tou_super_off_kwh")),
                    tou_on_rate_dollars=parse_float(data.get("tou_on_rate_dollars")),
                    tou_mid_rate_dollars=parse_float(data.get("tou_mid_rate_dollars")),
                    tou_off_rate_dollars=parse_float(data.get("tou_off_rate_dollars")),
                    tou_super_off_rate_dollars=parse_float(data.get("tou_super_off_rate_dollars")),
                    tou_on_cost=parse_float(data.get("tou_on_cost")),
                    tou_mid_cost=parse_float(data.get("tou_mid_cost")),
                    tou_off_cost=parse_float(data.get("tou_off_cost")),
                    tou_super_off_cost=parse_float(data.get("tou_super_off_cost")),
                    due_date=parse_date(data.get("due_date")),
                    service_type="electric",
                )
                
                stats["bills_imported"] += 1
                print(f"[import] Row {row_num}: Imported bill {bill_id} for {utility} account {account_num}")
                
            except Exception as e:
                error_msg = f"Row {row_num}: {str(e)}"
                stats["errors"].append(error_msg)
                print(f"[import] ERROR {error_msg}")
        
        print(f"[import] Complete: {stats['bills_imported']} bills, {stats['accounts_created']} accounts, {stats['meters_created']} meters")
        return stats
        
    except Exception as e:
        print(f"[import] ERROR parsing CSV: {e}")
        return {"error": str(e), "bills_imported": 0}

