"""
Persistence helpers for extraction results.

These functions are intentionally separated from `bill_extractor.py` to keep
module sizes manageable and avoid circular imports.
"""

from __future__ import annotations


def clean_numeric(val):
    """
    Clean a numeric value by stripping $ and commas.
    Returns float or None if parsing fails.
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def parse_dollar_rate(raw):
    """
    Parse a dollar rate string to float.
    Handles formats like: "$0.77194", "0.33583", "0.20657/kWh", "$0.20657 USD"
    Returns rate in dollars/kWh as a float.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)

    cleaned = (
        str(raw)
        .replace("$", "")
        .replace("USD", "")
        .replace("/kWh", "")
        .replace("kWh", "")
        .strip()
    )
    try:
        return float(cleaned)
    except ValueError:
        return None


def save_bill_to_normalized_tables(file_id, project_id, extracted_data):
    """
    Save extracted bill data to the normalized bills and bill_tou_periods tables.
    This is called after successful extraction.
    Idempotent: deletes existing bills for this file_id before inserting new ones.
    """
    # DEBUG: Log what we received
    print(f"\n{'='*80}")
    print(f"[bill_extractor] save_bill_to_normalized_tables called for file_id={file_id}")
    print(f"[bill_extractor] extracted_data keys: {list(extracted_data.keys())}")
    print(f"[bill_extractor] extracted_data: {extracted_data}")
    print(f"{'='*80}\n")

    try:
        from bill_intake.utils.normalization import normalize_utility_name
        from bills_db import (
            delete_all_empty_accounts,
            delete_bills_for_file,
            insert_bill,
            insert_bill_tou_period,
            update_bill_file_review_status,
            upsert_utility_account,
            upsert_utility_meter,
        )

        delete_bills_for_file(file_id)

        detailed = extracted_data.get("detailed_data", {})

        def get_val(*keys):
            for key in keys:
                val = extracted_data.get(key)
                if val is not None:
                    return val
                val = detailed.get(key)
                if val is not None:
                    return val
            return None

        utility_name = normalize_utility_name(get_val("utility_name"))
        account_number = get_val("customer_account", "account_number")

        if not utility_name or not account_number:
            print("[bill_extractor] Cannot save to normalized tables: missing utility_name or account_number")
            return False

        account_id = upsert_utility_account(project_id, utility_name, account_number)

        meters = extracted_data.get("meters", [])
        service_address = get_val("service_address", "")
        rate_schedule = get_val("rate", "rate_schedule", "")

        if rate_schedule:
            bad_phrases = [
                "contact",
                "commission",
                "safety",
                "disconnected",
                "for more information",
                "please",
                "may",
                "ensure",
                "service is",
                "you may",
                "reasons",
                "public utilities",
            ]
            has_bad_phrase = any(phrase in rate_schedule.lower() for phrase in bad_phrases)
            is_too_long = len(rate_schedule) > 25
            if has_bad_phrase or is_too_long:
                print(f"[bill_extractor] Rejecting bad rate_schedule from AI: '{rate_schedule[:60]}...'")
                rate_schedule = ""

        service_address_original = service_address
        if service_address and len(service_address) < 20:
            print(
                f"[bill_extractor] Service address seems incomplete (< 20 chars): '{service_address}' - will try regex fallback"
            )
            service_address = ""

        raw_text = extracted_data.get("_raw_text", "")

        if raw_text:
            import re

            if not rate_schedule or rate_schedule.strip() == "":
                rate_patterns = [
                    r"Rate\s*Schedule\s*[:\-]?\s*([A-Z0-9\-]+(?:\s[A-Z0-9\-]+)?)",
                    r"RATE\s*SCHEDULE\s*[:\-]?\s*([A-Z0-9\-]+(?:\s[A-Z0-9\-]+)?)",
                    r"Rate\s*Plan\s*[:\-]?\s*([A-Z0-9\-]+(?:\s[A-Z0-9\-]+)?)",
                    r"Tariff\s*[:\-]?\s*([A-Z0-9\-]+(?:\s[A-Z0-9\-]+)?)",
                    r"Service\s*Class\s*[:\-]?\s*([A-Z0-9\-]+)",
                    r"Schedule\s*[:\-]?\s*([A-Z0-9\-]+(?:\s[A-Z0-9\-]+)?)",
                ]
                for pattern in rate_patterns:
                    match = re.search(pattern, raw_text, re.MULTILINE)
                    if match:
                        candidate = match.group(1).strip()
                        if 3 <= len(candidate) <= 25 and not any(
                            word in candidate.lower() for word in ["contact", "please", "may", "service"]
                        ):
                            rate_schedule = candidate
                            print(f"[bill_extractor] Regex fallback extracted rate_schedule: {rate_schedule}")
                            break

            if not service_address or service_address.strip() == "":
                address_patterns = [
                    r"SERVICE\s*ADDRESS[:\-]?\s*(.{10,100})",
                    r"Service\s*Location[:\-]?\s*(.{10,100})",
                    r"Premise\s*Address[:\-]?\s*(.{10,100})",
                    r"Site\s*Address[:\-]?\s*(.{10,100})",
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:Street|ST|Avenue|AVE|Boulevard|BLVD|Road|RD|Drive|DR|Lane|LN|Way|WAY|Court|CT|Place|PL|Circle|CIR|Parkway|PKY)[^\n]{0,50})",
                ]
                for pattern in address_patterns:
                    match = re.search(pattern, raw_text, re.IGNORECASE)
                    if match:
                        addr_text = match.group(1)
                        addr_text = re.split(r"\n|POD-ID|BILLING|ACCOUNT|METER", addr_text, maxsplit=1)[0]
                        service_address = addr_text.strip()
                        print(f"[bill_extractor] Regex fallback extracted service_address: {service_address}")
                        break
                if (not service_address or service_address.strip() == "") and service_address_original:
                    service_address = service_address_original
                    print(f"[bill_extractor] Regex found no address, keeping original: {service_address}")

        period_start = get_val("billing_period_start")
        period_end = get_val("billing_period_end")
        due_date = get_val("due_date")

        if due_date and str(due_date).upper() in ("N/A", "NA", "NONE"):
            print(f"[bill_extractor] Rejecting invalid due_date from AI: '{due_date}'")
            due_date = None

        if not due_date and raw_text:
            import re

            due_patterns = [
                r"Due\s*Date\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
                r"Due\s*Date\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
                r"Payment\s*Due\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
                r"Payment\s*Due\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
                r"AUTO\s*PAYMENT\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
                r"AUTO\s*PAYMENT\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
                r"Pay\s*By\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
                r"Pay\s*By\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
                r"DUE\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{4})",
                r"DUE\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
            ]
            for pattern in due_patterns:
                match = re.search(pattern, raw_text, re.IGNORECASE)
                if match:
                    due_date = match.group(1).strip()
                    print(f"[bill_extractor] Regex fallback extracted due_date: {due_date}")
                    break

        tou_breakdown_from_regex = []
        if raw_text:
            import re

            has_tou_keywords = bool(
                re.search(
                    r"\b(TOU|Time[\s\-]*of[\s\-]*Use|Peak|High[\s\-]*Peak|Low[\s\-]*Peak|On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak|Base[\s\-]*Period)\b",
                    raw_text,
                    re.IGNORECASE,
                )
            )
            if has_tou_keywords:
                print("[bill_extractor] Detected TOU keywords in bill text - attempting regex extraction")
                tou_patterns = [
                    r"(High[\s\-]*Peak|Low[\s\-]*Peak|Base|On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak)[\s:]+([\d,]+\.?\d*)\s*kWh(?:\s+\$?([\d,]+\.?\d*))?",
                    r"(High[\s\-]*Peak|Low[\s\-]*Peak|Base|On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak)[^\d\n]{0,20}([\d,]+\.?\d*)[^\d\n]{0,20}\$?([\d,]+\.?\d*)[^\d\n]{0,20}\$?([\d,]+\.?\d*)",
                    r"(High[\s\-]*Peak|Low[\s\-]*Peak|Base|On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak)[:\s]+([\d,]+\.?\d*)\s*kWh\s*@?\s*\$?([\d\.]+)\s*=?\s*\$?([\d,]+\.?\d*)",
                ]

                matches = []
                for pattern in tou_patterns:
                    for match in re.finditer(pattern, raw_text, re.IGNORECASE):
                        period_name = match.group(1).strip()
                        kwh_str = match.group(2).replace(",", "").strip()
                        cost_str = None
                        rate_str = None
                        if len(match.groups()) >= 3 and match.group(3):
                            if len(match.groups()) >= 4 and match.group(4):
                                rate_str = match.group(3).replace(",", "").strip()
                                cost_str = match.group(4).replace(",", "").strip()
                            else:
                                cost_str = match.group(3).replace(",", "").strip()

                        try:
                            kwh = float(kwh_str)
                            cost = float(cost_str) if cost_str else None
                            rate = float(rate_str) if rate_str else None
                            period_normalized = " ".join(period_name.split()).title()
                            if not next((m for m in matches if m["period"] == period_normalized), None):
                                tou_entry = {"period": period_normalized, "kwh": kwh, "rate": rate, "estimated_cost": cost}
                                matches.append(tou_entry)
                                print(
                                    f"[bill_extractor] Regex TOU extraction: {period_normalized} = {kwh} kWh"
                                    + (f" @ ${rate}/kWh = ${cost}" if rate and cost else "")
                                )
                        except (ValueError, TypeError) as e:
                            print(f"[bill_extractor] Failed to parse TOU values: {e}")

                if matches:
                    tou_breakdown_from_regex = matches
                    print(f"[bill_extractor] Generic TOU regex extraction found {len(matches)} periods")

        total_kwh = clean_numeric(get_val("kwh_total", "total_kwh"))
        total_amount = clean_numeric(get_val("amount_due", "total_amount_due", "total_owed", "new_charges"))

        energy_charges = clean_numeric(get_val("energy_charges_total", "energy_charges"))
        demand_charges = clean_numeric(get_val("demand_charges_total", "total_facilities_demand_charge", "demand_charges"))
        taxes = clean_numeric(get_val("taxes_total", "taxes", "total_taxes"))
        other_charges = clean_numeric(get_val("other_charges_total", "other_charges"))

        tou_on_kwh = clean_numeric(get_val("kwh_on_peak", "on_peak_kwh", "tou_on_kwh", "tou_high_peak_kwh"))
        tou_mid_kwh = clean_numeric(get_val("kwh_mid_peak", "mid_peak_kwh", "tou_mid_kwh"))
        tou_off_kwh = clean_numeric(get_val("kwh_off_peak", "off_peak_kwh", "tou_off_kwh", "tou_low_peak_kwh"))

        tou_on_rate = parse_dollar_rate(get_val("rate_on_peak_per_kwh", "on_peak_rate", "tou_on_rate_dollars", "tou_high_peak_rate"))
        tou_mid_rate = parse_dollar_rate(get_val("rate_mid_peak_per_kwh", "mid_peak_rate", "tou_mid_rate_dollars"))
        tou_off_rate = parse_dollar_rate(get_val("rate_off_peak_per_kwh", "off_peak_rate", "tou_off_rate_dollars", "tou_low_peak_rate"))

        tou_super_off_kwh = clean_numeric(get_val("kwh_super_off_peak", "super_off_peak_kwh", "tou_super_off_kwh", "tou_base_kwh"))
        tou_super_off_rate = parse_dollar_rate(get_val("rate_super_off_peak_per_kwh", "super_off_peak_rate", "tou_super_off_rate_dollars", "tou_base_rate"))

        tou_on_cost = clean_numeric(get_val("tou_high_peak_cost", "tou_on_cost"))
        tou_mid_cost = clean_numeric(get_val("tou_mid_cost"))
        tou_off_cost = clean_numeric(get_val("tou_low_peak_cost", "tou_off_cost"))
        tou_super_off_cost = clean_numeric(get_val("tou_base_cost", "tou_super_off_cost"))

        if tou_on_cost is None and tou_on_kwh is not None and tou_on_rate is not None:
            tou_on_cost = round(tou_on_kwh * tou_on_rate, 2)
        if tou_mid_cost is None and tou_mid_kwh is not None and tou_mid_rate is not None:
            tou_mid_cost = round(tou_mid_kwh * tou_mid_rate, 2)
        if tou_off_cost is None and tou_off_kwh is not None and tou_off_rate is not None:
            tou_off_cost = round(tou_off_kwh * tou_off_rate, 2)
        if tou_super_off_cost is None and tou_super_off_kwh is not None and tou_super_off_rate is not None:
            tou_super_off_cost = round(tou_super_off_kwh * tou_super_off_rate, 2)

        missing_fields = []
        if utility_name == "LADWP":
            if not due_date or str(due_date).strip() == "":
                missing_fields.append("due_date")
            if not rate_schedule or str(rate_schedule).strip() == "":
                missing_fields.append("rate_schedule")

        service_type = get_val("service_type") or "electric"
        if service_type not in ("electric", "water", "gas", "combined"):
            service_type = "electric"
        print(f"[bill_extractor] service_type: {service_type}")

        tou_rates = extracted_data.get("tou_rates", []) or extracted_data.get("tou_breakdown", [])
        if not tou_rates and tou_breakdown_from_regex:
            tou_rates = tou_breakdown_from_regex
            print(f"[bill_extractor] Using regex-extracted TOU data ({len(tou_rates)} periods)")

        if meters:
            for meter_data in meters:
                meter_number = meter_data.get("meter_number") or meter_data.get("meter_id", "Unknown")
                meter_service_address = meter_data.get("service_address") or service_address
                meter_id = upsert_utility_meter(account_id, meter_number, meter_service_address)

                m_kwh = clean_numeric(meter_data.get("kwh_total"))
                m_amount = clean_numeric(meter_data.get("total_charge"))

                reads = meter_data.get("reads", [])
                if reads:
                    first_read = reads[0]
                    if m_kwh is None:
                        m_kwh = clean_numeric(first_read.get("kwh"))
                    if m_amount is None:
                        m_amount = clean_numeric(first_read.get("total_charge"))
                    if not period_start:
                        period_start = first_read.get("period_start")
                    if not period_end:
                        period_end = first_read.get("period_end")

                print(f"[bill_extractor] DEBUG: meter {meter_number} - m_kwh={m_kwh} (type={type(m_kwh)})")
                if m_kwh is None or m_kwh == 0:
                    print(f"[bill_extractor] Skipping non-electric meter {meter_number} - no kWh data")
                    continue

                if m_amount is None:
                    m_amount = total_amount

                bill_id = insert_bill(
                    bill_file_id=file_id,
                    account_id=account_id,
                    meter_id=meter_id,
                    utility_name=utility_name,
                    service_address=service_address,
                    rate_schedule=rate_schedule,
                    period_start=period_start,
                    period_end=period_end,
                    total_kwh=m_kwh,
                    total_amount_due=m_amount,
                    energy_charges=energy_charges,
                    demand_charges=demand_charges,
                    other_charges=other_charges,
                    taxes=taxes,
                    tou_on_kwh=tou_on_kwh,
                    tou_mid_kwh=tou_mid_kwh,
                    tou_off_kwh=tou_off_kwh,
                    tou_super_off_kwh=tou_super_off_kwh,
                    tou_on_rate_dollars=tou_on_rate,
                    tou_mid_rate_dollars=tou_mid_rate,
                    tou_off_rate_dollars=tou_off_rate,
                    tou_super_off_rate_dollars=tou_super_off_rate,
                    tou_on_cost=tou_on_cost,
                    tou_mid_cost=tou_mid_cost,
                    tou_off_cost=tou_off_cost,
                    tou_super_off_cost=tou_super_off_cost,
                    due_date=due_date,
                    service_type=service_type,
                )

                for tou in tou_rates:
                    period = tou.get("period") or tou.get("period_name", "Unknown")
                    kwh = clean_numeric(tou.get("kwh"))
                    rate = parse_dollar_rate(tou.get("rate") or tou.get("rate_per_kwh"))
                    est_cost = clean_numeric(tou.get("estimated_cost") or tou.get("est_cost"))
                    if kwh is not None:
                        insert_bill_tou_period(bill_id, period, kwh, rate, est_cost)

                print(f"[bill_extractor] Saved bill {bill_id} for meter {meter_number} - kwh={m_kwh}, amount=${m_amount}")
        else:
            meter_id = upsert_utility_meter(account_id, "Primary", service_address)
            bill_id = insert_bill(
                bill_file_id=file_id,
                account_id=account_id,
                meter_id=meter_id,
                utility_name=utility_name,
                service_address=service_address,
                rate_schedule=rate_schedule,
                period_start=period_start,
                period_end=period_end,
                total_kwh=total_kwh,
                total_amount_due=total_amount,
                energy_charges=energy_charges,
                demand_charges=demand_charges,
                other_charges=other_charges,
                taxes=taxes,
                tou_on_kwh=tou_on_kwh,
                tou_mid_kwh=tou_mid_kwh,
                tou_off_kwh=tou_off_kwh,
                tou_super_off_kwh=tou_super_off_kwh,
                tou_on_rate_dollars=tou_on_rate,
                tou_mid_rate_dollars=tou_mid_rate,
                tou_off_rate_dollars=tou_off_rate,
                tou_super_off_rate_dollars=tou_super_off_rate,
                tou_on_cost=tou_on_cost,
                tou_mid_cost=tou_mid_cost,
                tou_off_cost=tou_off_cost,
                tou_super_off_cost=tou_super_off_cost,
                due_date=due_date,
                service_type=service_type,
            )

            for tou in tou_rates:
                period = tou.get("period") or tou.get("period_name", "Unknown")
                kwh = clean_numeric(tou.get("kwh"))
                rate = parse_dollar_rate(tou.get("rate") or tou.get("rate_per_kwh"))
                est_cost = clean_numeric(tou.get("estimated_cost") or tou.get("est_cost"))
                if kwh is not None:
                    insert_bill_tou_period(bill_id, period, kwh, rate, est_cost)

            print(f"[bill_extractor] Saved bill {bill_id} (single meter) - kwh={total_kwh}, amount=${total_amount}")

        if missing_fields:
            update_bill_file_review_status(file_id, "needs_review")
            print(f"[bill_extractor] Updated bill file {file_id} review_status to 'needs_review' - missing: {missing_fields}")

        delete_all_empty_accounts(project_id)
        return True
    except Exception as e:
        print(f"[bill_extractor] Error saving to normalized tables: {e}")
        import traceback

        traceback.print_exc()
        return False


