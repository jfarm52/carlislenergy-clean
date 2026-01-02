"""Validation helpers for bill extraction payloads."""

from __future__ import annotations


def validate_extraction(extraction_payload):
    """
    Validate that all required fields are present in extraction payload.

    Required fields:
    - Bill-level: utility_name, account_number
    - Per meter: meter_number, service_address
    - Per read: period_start, period_end, kwh, total_charge

    Returns:
        {
            'is_valid': bool,
            'missing_fields': list of strings describing what's missing
        }
    """
    missing_fields = []

    if not extraction_payload:
        return {"is_valid": False, "missing_fields": ["No extraction data"]}

    utility_name = extraction_payload.get("utility_name")
    account_number = extraction_payload.get("account_number")
    meters = extraction_payload.get("meters", [])

    if not utility_name:
        missing_fields.append("missing utility_name")
    if not account_number:
        missing_fields.append("missing account_number")

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


