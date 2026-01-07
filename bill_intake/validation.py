"""Validation helpers for bill extraction payloads."""

from __future__ import annotations


def validate_extraction(extraction_payload, *, mode: str = "strict"):
    """
    Validate that all required fields are present in extraction payload.

    Modes:
    - strict: used for “is this payload complete?” style checks
    - analysis: used for “can we still compute / display useful analysis?” checks
      (intentionally does NOT require service_address/rate_schedule and is tolerant of partial meters/reads)

    Returns:
        {
            'is_valid': bool,
            'missing_fields': list of strings describing what's missing
        }
    """
    missing_fields = []

    if not extraction_payload:
        return {"is_valid": False, "missing_fields": ["No extraction data"]}

    mode = (mode or "strict").strip().lower()

    def _extract_meters(payload: dict) -> list:
        meters_local = payload.get("meters", []) or []
        if meters_local:
            return meters_local
        accounts = payload.get("accounts", []) or []
        acc_meters = []
        for a in accounts:
            for m in (a or {}).get("meters", []) or []:
                acc_meters.append(m)
        return acc_meters

    utility_name = extraction_payload.get("utility_name") or (extraction_payload.get("detailed_data") or {}).get("utility_name")
    account_number = extraction_payload.get("account_number") or (extraction_payload.get("detailed_data") or {}).get("account_number")
    meters = _extract_meters(extraction_payload)

    # Utility/account are still important for normalizing into project analytics.
    if not utility_name:
        missing_fields.append("missing utility_name")
    if not account_number:
        missing_fields.append("missing account_number")

    if mode == "analysis":
        # For analysis/display, we only need *some* dated money/kWh signal.
        # Service address and meter numbers are helpful context but not required.
        detailed = extraction_payload.get("detailed_data") or {}
        period_end = (
            detailed.get("billing_period_end")
            or extraction_payload.get("billing_period_end")
            or extraction_payload.get("period_end")
        )
        amount_due = detailed.get("amount_due") or extraction_payload.get("amount_due") or extraction_payload.get("total_amount_due")

        has_any_read = False
        has_any_period_end = bool(period_end)
        has_any_charge = amount_due is not None

        for meter in meters:
            reads = (meter or {}).get("reads", []) or []
            for read in reads:
                has_any_read = True
                if read.get("period_end"):
                    has_any_period_end = True
                if read.get("total_charge") is not None:
                    has_any_charge = True

        if not meters and not detailed:
            missing_fields.append("no meters found")

        if not has_any_period_end:
            missing_fields.append("missing billing_period_end")
        if not has_any_charge:
            missing_fields.append("missing amount_due")

        return {"is_valid": len(missing_fields) == 0, "missing_fields": missing_fields}

    # strict mode (legacy behavior)
    if not meters:
        missing_fields.append("no meters found")
    else:
        for i, meter in enumerate(meters):
            meter_number = meter.get("meter_number")
            service_address = meter.get("service_address")
            reads = meter.get("reads", [])

            meter_id = meter_number or f"meter_{i+1}"

            if not meter_number:
                missing_fields.append(f"missing meter_number for meter {i+1}")
            if not service_address:
                missing_fields.append(f"missing service_address for meter {meter_id}")

            if not reads:
                missing_fields.append(f"no reads found for meter {meter_id}")
            else:
                for j, read in enumerate(reads):
                    period_start = read.get("period_start")
                    period_end = read.get("period_end")
                    kwh = read.get("kwh")
                    total_charge = read.get("total_charge")

                    period_desc = f"{period_start or '?'} to {period_end or '?'}"

                    if not period_start:
                        missing_fields.append(f"missing period_start for meter {meter_id} read {j+1}")
                    if not period_end:
                        missing_fields.append(f"missing period_end for meter {meter_id} read {j+1}")
                    if kwh is None:
                        missing_fields.append(f"missing kWh for period {period_desc} on meter {meter_id}")
                    if total_charge is None:
                        missing_fields.append(
                            f"missing total_charge for period {period_desc} on meter {meter_id}"
                        )

    return {"is_valid": len(missing_fields) == 0, "missing_fields": missing_fields}


