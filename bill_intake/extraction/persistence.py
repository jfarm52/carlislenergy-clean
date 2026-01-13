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
        # Prioritize account_number (what frontend sets) over customer_account
        account_number = get_val("account_number", "customer_account", "service_account")
        # Normalization will convert None/N/A to "Unknown" - don't fail the save!
        if not account_number:
            account_number = "Unknown"
            print(f"[bill_extractor] Using default account 'Unknown' for file")

        account_id = upsert_utility_account(project_id, utility_name, account_number)

        meters = extracted_data.get("meters", [])
        service_address = get_val("service_address", "")
        rate_schedule = get_val("rate", "rate_schedule", "rate_code", "schedule")

        if rate_schedule:
            # Reject obvious AI hallucinations (sentences, not rate codes)
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
            if has_bad_phrase:
                print(f"[bill_extractor] Rejecting bad rate_schedule from AI: '{rate_schedule[:60]}...'")
                rate_schedule = ""
            # Truncate to column size if somehow longer than 100 chars
            elif len(rate_schedule) > 100:
                rate_schedule = rate_schedule[:100]

        service_address_original = service_address
        if service_address and len(service_address) < 20:
            print(
                f"[bill_extractor] Service address seems incomplete (< 20 chars): '{service_address}' - will try regex fallback"
            )
            service_address = ""

        raw_text = extracted_data.get("_raw_text", "")

        if raw_text:
            import re

            # Reject rate_schedule that contains field boundary text (AI grabbed across fields)
            if rate_schedule and "next scheduled" in rate_schedule.lower():
                print(f"[bill_extractor] Rejecting rate_schedule with field boundary text: '{rate_schedule}'")
                rate_schedule = ""

            # LADWP: capture full rate schedule line, stopping at known field boundaries
            if not rate_schedule or rate_schedule.strip() == "":
                ladwp_rate_match = re.search(
                    r"RATE\s*SCHEDULE[:\s]*(.+?)(?=\s*NEXT\s*SCHEDULED|\s*METER\s*NUMBER|\s*BILLING\s*PERIOD|\s*SERVES|\n\n|$)",
                    raw_text, re.IGNORECASE | re.DOTALL
                )
                if ladwp_rate_match:
                    rate_schedule = ladwp_rate_match.group(1).strip()
                    # Clean up any trailing whitespace or partial words
                    rate_schedule = re.sub(r'\s+', ' ', rate_schedule).strip()
                    print(f"[bill_extractor] Rate schedule extracted: {rate_schedule}")

            # Service address extraction
            if not service_address or service_address.strip() == "":
                address_patterns = [
                    r"SERVICE\s*ADDRESS[:\-]?\s*(.{10,100})",
                    r"Service\s*Location[:\-]?\s*(.{10,100})",
                    r"Premise\s*Address[:\-]?\s*(.{10,100})",
                    r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:Street|ST|Avenue|AVE|Boulevard|BLVD|Road|RD|Drive|DR|Lane|LN|Way|WAY|Court|CT|Place|PL|Circle|CIR|Parkway|PKY)[,\s]+[A-Z][A-Za-z\s]+[,\s]+[A-Z]{2}\s*\d{5})",
                ]
                for pattern in address_patterns:
                    match = re.search(pattern, raw_text, re.IGNORECASE)
                    if match:
                        addr_text = match.group(1)
                        addr_text = re.split(r"\n|POD-ID|BILLING|ACCOUNT|METER|RATE", addr_text, maxsplit=1)[0]
                        service_address = addr_text.strip()
                        print(f"[bill_extractor] Regex extracted service_address: {service_address}")
                        break
                if (not service_address or service_address.strip() == "") and service_address_original:
                    service_address = service_address_original
                    print(f"[bill_extractor] Keeping original service_address: {service_address}")

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
            print("[bill_extractor] Attempting TOU regex extraction")
            matches = []

            # LADWP-specific: "High Peak Subtotal (11,680 kWh x $0.25624/kWh) $2,992.92"
            ladwp_tou_pattern = r"(High\s*Peak|Low\s*Peak|Base)\s*Subtotal\s*\(\s*([\d,]+(?:\.\d+)?)\s*kWh\s*x\s*\$?([\d.]+)/kWh\s*\)\s*\$?([\d,]+(?:\.\d+)?)"
            for match in re.finditer(ladwp_tou_pattern, raw_text, re.IGNORECASE):
                period_name = match.group(1).strip()
                kwh_str = match.group(2).replace(",", "")
                rate_str = match.group(3)
                cost_str = match.group(4).replace(",", "")
                try:
                    kwh = float(kwh_str)
                    rate = float(rate_str)
                    cost = float(cost_str)
                    # Map LADWP names to standard names
                    period_map = {"High Peak": "On-Peak", "Low Peak": "Off-Peak", "Base": "Super Off-Peak"}
                    period_normalized = period_map.get(period_name.title(), period_name.title())
                    if not any(m["period"] == period_normalized for m in matches):
                        matches.append({"period": period_normalized, "kwh": kwh, "rate": rate, "estimated_cost": cost})
                        print(f"[bill_extractor] LADWP TOU: {period_normalized} = {kwh} kWh @ ${rate}/kWh = ${cost}")
                except (ValueError, TypeError) as e:
                    print(f"[bill_extractor] Failed to parse LADWP TOU: {e}")

            # SCE/generic TOU patterns (only if LADWP didn't match)
            if not matches:
                generic_patterns = [
                    # SCE: On-Peak 3,064.000 0.17271 529.59
                    r"(On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak)[\s\t]+([\d,]+\.?\d*)\s+([\d\.]+)\s+([\d,]+\.?\d*)",
                    # Generic: On-Peak 3,064 kWh @ $0.17/kWh = $529
                    r"(On[\s\-]*Peak|Mid[\s\-]*Peak|Off[\s\-]*Peak|Super[\s\-]*Off[\s\-]*Peak)[:\s]+([\d,]+\.?\d*)\s*kWh\s*[@x]\s*\$?([\d\.]+)(?:/kWh)?\s*=?\s*\$?([\d,]+\.?\d*)",
                ]
                for pattern in generic_patterns:
                    for match in re.finditer(pattern, raw_text, re.IGNORECASE):
                        period_name = match.group(1).strip()
                        kwh_str = match.group(2).replace(",", "").strip()
                        rate_str = match.group(3).replace(",", "").strip() if match.group(3) else None
                        cost_str = match.group(4).replace(",", "").strip() if len(match.groups()) >= 4 and match.group(4) else None
                        try:
                            kwh = float(kwh_str)
                            rate = float(rate_str) if rate_str else None
                            cost = float(cost_str) if cost_str else None
                            period_normalized = " ".join(period_name.split()).title()
                            if not any(m["period"] == period_normalized for m in matches):
                                matches.append({"period": period_normalized, "kwh": kwh, "rate": rate, "estimated_cost": cost})
                                print(f"[bill_extractor] TOU: {period_normalized} = {kwh} kWh" + (f" @ ${rate}/kWh = ${cost}" if rate and cost else ""))
                        except (ValueError, TypeError) as e:
                            print(f"[bill_extractor] Failed to parse TOU: {e}")

            if matches:
                tou_breakdown_from_regex = matches
                print(f"[bill_extractor] TOU extraction found {len(matches)} periods")

        total_kwh = clean_numeric(get_val("kwh_total", "total_kwh"))
        total_amount = clean_numeric(get_val("amount_due", "total_amount_due", "total_owed", "new_charges", "total_amount"))

        energy_charges = clean_numeric(get_val("energy_charges_total", "energy_charges"))
        demand_charges = clean_numeric(get_val("demand_charges_total", "total_facilities_demand_charge", "demand_charges"))
        taxes = clean_numeric(get_val("taxes_total", "taxes", "total_taxes"))
        other_charges = clean_numeric(get_val("other_charges_total", "other_charges"))

        # Parse tou_breakdown array into flat values if present
        tou_breakdown = get_val("tou_breakdown") or []
        tou_parsed = {"on": {}, "mid": {}, "off": {}, "super_off": {}}
        for entry in tou_breakdown:
            if not isinstance(entry, dict):
                continue
            period = str(entry.get("period", "")).lower().replace("-", " ").replace("_", " ")
            kwh = entry.get("kwh")
            rate = entry.get("rate")
            cost = entry.get("estimated_cost") or entry.get("cost")
            if "super" in period:
                tou_parsed["super_off"] = {"kwh": kwh, "rate": rate, "cost": cost}
            elif "on" in period:
                tou_parsed["on"] = {"kwh": kwh, "rate": rate, "cost": cost}
            elif "mid" in period:
                tou_parsed["mid"] = {"kwh": kwh, "rate": rate, "cost": cost}
            elif "off" in period:
                tou_parsed["off"] = {"kwh": kwh, "rate": rate, "cost": cost}

        # Get TOU values from tou_breakdown first, then fall back to flat keys
        tou_on_kwh = clean_numeric(tou_parsed["on"].get("kwh")) or clean_numeric(get_val("kwh_on_peak", "on_peak_kwh", "tou_on_kwh", "tou_high_peak_kwh"))
        tou_mid_kwh = clean_numeric(tou_parsed["mid"].get("kwh")) or clean_numeric(get_val("kwh_mid_peak", "mid_peak_kwh", "tou_mid_kwh"))
        tou_off_kwh = clean_numeric(tou_parsed["off"].get("kwh")) or clean_numeric(get_val("kwh_off_peak", "off_peak_kwh", "tou_off_kwh", "tou_low_peak_kwh"))

        tou_on_rate = parse_dollar_rate(tou_parsed["on"].get("rate")) or parse_dollar_rate(get_val("rate_on_peak_per_kwh", "on_peak_rate", "tou_on_rate_dollars", "tou_high_peak_rate"))
        tou_mid_rate = parse_dollar_rate(tou_parsed["mid"].get("rate")) or parse_dollar_rate(get_val("rate_mid_peak_per_kwh", "mid_peak_rate", "tou_mid_rate_dollars"))
        tou_off_rate = parse_dollar_rate(tou_parsed["off"].get("rate")) or parse_dollar_rate(get_val("rate_off_peak_per_kwh", "off_peak_rate", "tou_off_rate_dollars", "tou_low_peak_rate"))

        tou_super_off_kwh = clean_numeric(tou_parsed["super_off"].get("kwh")) or clean_numeric(get_val("kwh_super_off_peak", "super_off_peak_kwh", "tou_super_off_kwh", "tou_base_kwh"))
        tou_super_off_rate = parse_dollar_rate(tou_parsed["super_off"].get("rate")) or parse_dollar_rate(get_val("rate_super_off_peak_per_kwh", "super_off_peak_rate", "tou_super_off_rate_dollars", "tou_base_rate"))

        tou_on_cost = clean_numeric(tou_parsed["on"].get("cost")) or clean_numeric(get_val("tou_high_peak_cost", "tou_on_cost"))
        tou_mid_cost = clean_numeric(tou_parsed["mid"].get("cost")) or clean_numeric(get_val("tou_mid_cost"))
        tou_off_cost = clean_numeric(tou_parsed["off"].get("cost")) or clean_numeric(get_val("tou_low_peak_cost", "tou_off_cost"))
        tou_super_off_cost = clean_numeric(tou_parsed["super_off"].get("cost")) or clean_numeric(get_val("tou_base_cost", "tou_super_off_cost"))

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
        
        # Debug: log current values before service_type inference
        print(f"[bill_extractor] Before service_type inference: service_type={service_type}, rate_schedule='{rate_schedule}'")
        
        # Infer service_type from context if not explicitly set or defaulted to electric
        if service_type == "electric":
            rate_lower = (rate_schedule or "").lower()
            raw_lower = (raw_text or "").lower()
            print(f"[bill_extractor] Checking rate_lower='{rate_lower}' for water/gas keywords")

            # Utility name hints
            if utility_name and "water" in utility_name.lower():
                service_type = "water"
                print(f"[bill_extractor] Inferred service_type='water' from utility_name: {utility_name}")

            # Rate/description hints
            if service_type == "electric":
                if "water" in rate_lower or "sewer" in rate_lower:
                    service_type = "water"
                    print(f"[bill_extractor] Inferred service_type='water' from rate_schedule: {rate_schedule}")
                elif "gas" in rate_lower or "therm" in rate_lower:
                    service_type = "gas"
                    print(f"[bill_extractor] Inferred service_type='gas' from rate_schedule: {rate_schedule}")

            # NOTE: We removed raw_text keyword detection here because SCE bills
            # contain boilerplate text mentioning "gas", "therm", etc. when explaining
            # other utility services, which caused false positives.
            # The regex_extract_all_fields function now handles service_type detection
            # more accurately based on actual content markers.

            # NOTE: We intentionally DO NOT override electric→other based on missing kWh
            # Missing kWh means our regex patterns failed, not that it's non-electric
            # The bill will be flagged as "needs_review" due to missing critical fields
            if service_type == "electric" and (total_kwh is None or total_kwh == 0):
                print(f"[bill_extractor] WARNING: Electric bill with no kWh extracted - patterns may need update")
        
        if service_type not in ("electric", "water", "gas", "combined", "other"):
            service_type = "electric"
        print(f"[bill_extractor] service_type: {service_type}")
        
        # Save service_type to the file record for proper filtering
        from bill_intake.db.bill_files import update_bill_file_service_type
        update_bill_file_service_type(file_id, service_type)

        # Update extraction_payload with regex-extracted values so modal can see them
        from bills_db import update_bill_file_extraction_payload
        payload_updates = {}
        if "detailed_data" not in extracted_data:
            extracted_data["detailed_data"] = {}
        dd = extracted_data["detailed_data"]
        # Only update if we extracted new values via regex
        if service_address and not dd.get("service_address"):
            dd["service_address"] = service_address
            payload_updates["service_address"] = service_address
        if rate_schedule and not dd.get("rate_schedule") and not dd.get("rate"):
            dd["rate_schedule"] = rate_schedule
            dd["rate"] = rate_schedule
            payload_updates["rate_schedule"] = rate_schedule
        if due_date and not dd.get("due_date"):
            dd["due_date"] = due_date
            payload_updates["due_date"] = due_date
        if tou_breakdown_from_regex:
            dd["tou_breakdown"] = tou_breakdown_from_regex
            payload_updates["tou_breakdown"] = tou_breakdown_from_regex
        if payload_updates:
            print(f"[bill_extractor] Updating extraction_payload with regex values: {list(payload_updates.keys())}")
            update_bill_file_extraction_payload(file_id, extracted_data)

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

                # NOTE: Don't drop whole bills just because kWh is missing/0.
                # We still want partial cost/charges analysis and the ability to fix kWh later.
                print(f"[bill_extractor] DEBUG: meter {meter_number} - m_kwh={m_kwh} (type={type(m_kwh)})")
                if m_kwh == 0:
                    m_kwh = None

                # If this is effectively a single-meter bill and meter kWh is missing, fall back to bill-level total_kwh.
                if m_kwh is None and total_kwh is not None and len(meters) == 1:
                    m_kwh = total_kwh

                has_any_money = (
                    m_amount is not None
                    or total_amount is not None
                    or energy_charges is not None
                    or demand_charges is not None
                    or other_charges is not None
                    or taxes is not None
                )
                has_any_usage = m_kwh is not None or total_kwh is not None
                has_any_dates = bool(period_start or period_end)
                has_any_tou = bool(tou_rates)

                if not (has_any_money or has_any_usage or has_any_dates or has_any_tou):
                    print(f"[bill_extractor] Skipping empty meter {meter_number} - no usable data")
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
            # Use extracted meter_number if available, otherwise default to "Primary"
            extracted_meter_num = get_val("meter_number", "meter_id") or "Primary"
            meter_id = upsert_utility_meter(account_id, extracted_meter_num, service_address)
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

        # Check if this is a non-electric bill - if so, mark as skipped (not applicable)
        if service_type and service_type.lower() != "electric":
            update_bill_file_review_status(file_id, "skipped")
            print(f"[bill_extractor] Status 'skipped' (gray) - non-electric bill: {service_type}")
        else:
            # Determine review status based on what's present/missing
            # Critical fields: kWh, amount, dates - without these, bill is unusable
            # Non-critical fields: service_address, rate_schedule - nice to have
            critical_missing = []
            non_critical_missing = []
            
            if total_kwh is None or total_kwh == 0:
                critical_missing.append("total_kwh")
            if total_amount is None or total_amount == 0:
                critical_missing.append("total_amount")
            if not period_start and not period_end:
                critical_missing.append("billing_period")
            
            if not service_address or service_address.strip() == "":
                non_critical_missing.append("service_address")
            if not rate_schedule or rate_schedule.strip() == "":
                non_critical_missing.append("rate_schedule")
            
            if critical_missing:
                # Yellow - missing critical info
                update_bill_file_review_status(file_id, "needs_review")
                print(f"[bill_extractor] Status 'needs_review' (yellow) - critical missing: {critical_missing}")
            elif non_critical_missing:
                # Green - critical data present, some non-critical missing
                update_bill_file_review_status(file_id, "ok")
                print(f"[bill_extractor] Status 'ok' (green) - non-critical missing: {non_critical_missing}")
            else:
                # Blue - perfect extraction, all fields present
                update_bill_file_review_status(file_id, "complete")
                print(f"[bill_extractor] Status 'complete' (blue) - all fields present")

        # NOTE: delete_all_empty_accounts removed from here to avoid race conditions
        # during parallel bill processing. Empty accounts are cleaned up during file deletion.
        return True
    except Exception as e:
        print(f"[bill_extractor] Error saving to normalized tables: {e}")
        import traceback

        traceback.print_exc()
        return False


