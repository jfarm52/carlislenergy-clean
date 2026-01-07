"""Update paths for bill records and review metadata."""

from __future__ import annotations

from psycopg2.extras import Json, RealDictCursor

from bill_intake.db.connection import get_connection
from bill_intake.db.bills_read import get_bill_by_id


def update_bill(bill_id, updates):
    """
    Update a bill record with the provided fields.
    Automatically recomputes blended_rate_dollars and avg_cost_per_day.
    """
    current_bill = get_bill_by_id(bill_id)
    if not current_bill:
        return None

    allowed_fields = {
        "total_kwh",
        "total_amount_due",
        "rate_schedule",
        "service_address",
        "utility_name",
        "period_start",
        "period_end",
        "days_in_period",
        "energy_charges",
        "demand_charges",
        "other_charges",
        "taxes",
        "tou_on_kwh",
        "tou_mid_kwh",
        "tou_off_kwh",
        "tou_on_rate_dollars",
        "tou_mid_rate_dollars",
        "tou_off_rate_dollars",
        "tou_on_cost",
        "tou_mid_cost",
        "tou_off_cost",
    }

    filtered_updates = {k: v for k, v in (updates or {}).items() if k in allowed_fields}
    if not filtered_updates:
        return current_bill

    merged = dict(current_bill)
    merged.update(filtered_updates)

    total_kwh = merged.get("total_kwh")
    total_amount_due = merged.get("total_amount_due")
    days_in_period = merged.get("days_in_period")

    blended_rate = None
    if total_kwh and total_amount_due and float(total_kwh) > 0:
        blended_rate = float(total_amount_due) / float(total_kwh)

    avg_cost_per_day = None
    if days_in_period and total_amount_due and int(days_in_period) > 0:
        avg_cost_per_day = float(total_amount_due) / float(days_in_period)

    filtered_updates["blended_rate_dollars"] = blended_rate
    filtered_updates["avg_cost_per_day"] = avg_cost_per_day

    if "tou_on_kwh" in filtered_updates or "tou_on_rate_dollars" in filtered_updates:
        on_kwh = filtered_updates.get("tou_on_kwh", merged.get("tou_on_kwh"))
        on_rate = filtered_updates.get("tou_on_rate_dollars", merged.get("tou_on_rate_dollars"))
        if on_kwh is not None and on_rate is not None:
            filtered_updates["tou_on_cost"] = round(float(on_kwh) * float(on_rate), 2)

    if "tou_mid_kwh" in filtered_updates or "tou_mid_rate_dollars" in filtered_updates:
        mid_kwh = filtered_updates.get("tou_mid_kwh", merged.get("tou_mid_kwh"))
        mid_rate = filtered_updates.get("tou_mid_rate_dollars", merged.get("tou_mid_rate_dollars"))
        if mid_kwh is not None and mid_rate is not None:
            filtered_updates["tou_mid_cost"] = round(float(mid_kwh) * float(mid_rate), 2)

    if "tou_off_kwh" in filtered_updates or "tou_off_rate_dollars" in filtered_updates:
        off_kwh = filtered_updates.get("tou_off_kwh", merged.get("tou_off_kwh"))
        off_rate = filtered_updates.get("tou_off_rate_dollars", merged.get("tou_off_rate_dollars"))
        if off_kwh is not None and off_rate is not None:
            filtered_updates["tou_off_cost"] = round(float(off_kwh) * float(off_rate), 2)

    conn = get_connection()
    try:
        set_clauses = []
        values = []
        for field, value in filtered_updates.items():
            set_clauses.append(f"{field} = %s")
            values.append(value)

        values.append(bill_id)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE bills
                SET {', '.join(set_clauses)}
                WHERE id = %s
                RETURNING id
                """,
                values,
            )
            result = cur.fetchone()
            conn.commit()
            return get_bill_by_id(bill_id) if result else None
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def recompute_bill_file_missing_fields(bill_file_id):
    """
    Recompute missing fields for a bill file based on current bill data.
    Updates the utility_bill_files record with new missing_fields and review_status.

    Fields are grouped into:
    - CORE: required for review_status to become 'ok' (utility_name, period_end, total_amount_due).
    - CONTEXT: nice-to-have and still reported as missing, but don't block 'ok' status
      (service_address, rate_schedule, meter_number, period_start, total_kwh).
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    b.utility_name, b.service_address, b.rate_schedule,
                    b.period_start, b.period_end,
                    b.total_kwh, b.total_amount_due,
                    m.meter_number
                FROM bills b
                LEFT JOIN utility_meters m ON b.meter_id = m.id
                WHERE b.bill_file_id = %s
                """,
                (bill_file_id,),
            )
            bills = cur.fetchall()

            if not bills:
                return ["no_bills_for_file"]

            # Group into core (blocks ok) and context (warning only)
            core_missing = set()
            context_missing = set()
            first_bill = bills[0]

            if not first_bill.get("utility_name"):
                core_missing.add("utility_name")

            # Context fields
            if not first_bill.get("rate_schedule"):
                context_missing.add("rate_schedule")

            for bill in bills:
                if bill.get("total_amount_due") is None:
                    core_missing.add("total_amount_due")
                if not bill.get("period_end"):
                    core_missing.add("period_end")

                # Context - nice-to-have
                if bill.get("total_kwh") is None:
                    context_missing.add("total_kwh")
                if not bill.get("period_start"):
                    context_missing.add("period_start")
                if not bill.get("meter_number"):
                    context_missing.add("meter_number")
                if not bill.get("service_address"):
                    context_missing.add("service_address")

            missing = list(core_missing | context_missing)

            # Only core fields block 'ok'
            review_status = "needs_review" if core_missing else "ok"
            cur.execute(
                """
                UPDATE utility_bill_files
                SET missing_fields = %s, review_status = %s
                WHERE id = %s
                """,
                (Json(missing), review_status, bill_file_id),
            )
            conn.commit()
            return missing
    finally:
        conn.close()


