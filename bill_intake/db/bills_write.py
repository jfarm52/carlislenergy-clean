"""Write-path operations for normalized `bills` and `bill_tou_periods` tables."""

from __future__ import annotations

from datetime import datetime

from psycopg2.extras import RealDictCursor

from bill_intake.db.connection import get_connection


def delete_bills_for_file(bill_file_id):
    """Delete all bills and their TOU periods for a given bill file ID."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM bill_tou_periods
                WHERE bill_id IN (SELECT id FROM bills WHERE bill_file_id = %s)
                """,
                (bill_file_id,),
            )
            tou_deleted = cur.rowcount

            cur.execute("DELETE FROM bills WHERE bill_file_id = %s", (bill_file_id,))
            bills_deleted = cur.rowcount

            conn.commit()
            if bills_deleted > 0:
                print(
                    f"[bills_db] Deleted {bills_deleted} bill(s) and {tou_deleted} TOU period(s) for file {bill_file_id}"
                )
            return bills_deleted
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def insert_bill(
    bill_file_id,
    account_id,
    meter_id,
    utility_name,
    service_address,
    rate_schedule,
    period_start,
    period_end,
    total_kwh,
    total_amount_due,
    energy_charges=None,
    demand_charges=None,
    other_charges=None,
    taxes=None,
    tou_on_kwh=None,
    tou_mid_kwh=None,
    tou_off_kwh=None,
    tou_super_off_kwh=None,
    tou_on_rate_dollars=None,
    tou_mid_rate_dollars=None,
    tou_off_rate_dollars=None,
    tou_super_off_rate_dollars=None,
    tou_on_cost=None,
    tou_mid_cost=None,
    tou_off_cost=None,
    tou_super_off_cost=None,
    due_date=None,
    service_type="electric",
):
    """Insert a normalized bill record with TOU data. Returns bill ID."""
    conn = get_connection()
    try:
        # Calculate days in period
        days_in_period = None
        if period_start and period_end:
            ps = datetime.strptime(period_start, "%Y-%m-%d").date() if isinstance(period_start, str) else period_start
            pe = datetime.strptime(period_end, "%Y-%m-%d").date() if isinstance(period_end, str) else period_end
            days_in_period = (pe - ps).days + 1

        blended_rate_dollars = None
        if total_kwh is not None and float(total_kwh) > 0 and total_amount_due is not None:
            blended_rate_dollars = float(total_amount_due) / float(total_kwh)

        avg_cost_per_day = None
        if days_in_period is not None and days_in_period > 0 and total_amount_due is not None:
            avg_cost_per_day = float(total_amount_due) / float(days_in_period)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO bills
                (bill_file_id, account_id, meter_id, utility_name, service_address,
                 rate_schedule, period_start, period_end, days_in_period, total_kwh,
                 total_amount_due, energy_charges, demand_charges, other_charges, taxes,
                 tou_on_kwh, tou_mid_kwh, tou_off_kwh, tou_super_off_kwh,
                 tou_on_rate_dollars, tou_mid_rate_dollars, tou_off_rate_dollars, tou_super_off_rate_dollars,
                 tou_on_cost, tou_mid_cost, tou_off_cost, tou_super_off_cost,
                 blended_rate_dollars, avg_cost_per_day, due_date, service_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    bill_file_id,
                    account_id,
                    meter_id,
                    utility_name,
                    service_address,
                    rate_schedule,
                    period_start,
                    period_end,
                    days_in_period,
                    total_kwh,
                    total_amount_due,
                    energy_charges,
                    demand_charges,
                    other_charges,
                    taxes,
                    tou_on_kwh,
                    tou_mid_kwh,
                    tou_off_kwh,
                    tou_super_off_kwh,
                    tou_on_rate_dollars,
                    tou_mid_rate_dollars,
                    tou_off_rate_dollars,
                    tou_super_off_rate_dollars,
                    tou_on_cost,
                    tou_mid_cost,
                    tou_off_cost,
                    tou_super_off_cost,
                    blended_rate_dollars,
                    avg_cost_per_day,
                    due_date,
                    service_type,
                ),
            )
            result = cur.fetchone()
            conn.commit()
            return result["id"]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def insert_bill_tou_period(bill_id, period, kwh, rate_dollars_per_kwh, est_cost_dollars=None):
    """Insert a TOU period for a bill. Returns period ID."""
    conn = get_connection()
    try:
        if est_cost_dollars is None and rate_dollars_per_kwh is not None and kwh is not None:
            est_cost_dollars = round(float(kwh) * float(rate_dollars_per_kwh), 2)

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO bill_tou_periods (bill_id, period, kwh, rate_dollars_per_kwh, est_cost_dollars)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
                """,
                (bill_id, period, kwh, rate_dollars_per_kwh, est_cost_dollars),
            )
            result = cur.fetchone()
            conn.commit()
            return result["id"]
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


