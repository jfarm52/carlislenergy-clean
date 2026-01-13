"""
Bill Extractor Module - Uses xAI Grok 4 for PDF bill data extraction
Supports SCE, LADWP, and other utility bills with detailed TOU breakdown
"""

import os
import json
import base64
import requests
try:
    import pymupdf as fitz  # PyMuPDF 1.26+
except ImportError:
    import fitz  # PyMuPDF legacy

XAI_API_KEY = os.environ.get("XAI_API_KEY")
XAI_BASE_URL = "https://api.x.ai/v1"

def _xai_chat_completions(payload: dict, timeout_s: int = 180) -> dict:
    """Call xAI OpenAI-compatible REST API without importing the OpenAI SDK."""
    if not XAI_API_KEY:
        raise ValueError("XAI_API_KEY environment variable not set")
    headers = {"Authorization": f"Bearer {XAI_API_KEY}", "Content-Type": "application/json"}
    resp = requests.post(f"{XAI_BASE_URL}/chat/completions", json=payload, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return resp.json()

def pdf_to_images(file_path, max_pages=10):
    """Convert PDF pages to base64-encoded images for vision API"""
    images = []
    try:
        doc = fitz.open(file_path)
        for page_num in range(min(len(doc), max_pages)):
            page = doc[page_num]
            mat = fitz.Matrix(150/72, 150/72)
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("png")
            b64_img = base64.b64encode(img_bytes).decode('utf-8')
            images.append(b64_img)
        doc.close()
    except Exception as e:
        print(f"[bill_extractor] Error converting PDF to images: {e}")
    return images


def file_to_images(file_path, max_pages=10):
    """
    Convert a file (PDF or image) to base64-encoded images for vision API.
    Returns list of tuples: (base64_data, mime_type) for proper data URL construction.
    """
    ext = os.path.splitext(file_path)[1].lower()
    
    mime_map = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.webp': 'image/webp',
        '.heic': 'image/heic',
        '.bmp': 'image/bmp',
        '.tiff': 'image/tiff'
    }
    
    if ext == '.pdf':
        images = pdf_to_images(file_path, max_pages)
        return [(img, 'image/png') for img in images]
    elif ext in mime_map:
        try:
            with open(file_path, 'rb') as f:
                img_bytes = f.read()
                b64_img = base64.b64encode(img_bytes).decode('utf-8')
                return [(b64_img, mime_map[ext])]
        except Exception as e:
            print(f"[bill_extractor] Error reading image file: {e}")
            return []
    else:
        print(f"[bill_extractor] Unsupported file type: {ext}")
        return []


# Compatibility re-exports (moved out to keep this file < 1000 lines)
from bill_intake.extraction.persistence import save_bill_to_normalized_tables  # noqa: E402
from bill_intake.utils.normalization import normalize_utility_name  # noqa: E402


def flatten_grok_response(raw_result):
    """
    Flatten nested Grok response into a flat dictionary.
    Grok sometimes returns nested structure like:
    {"ACCOUNT INFO": {...}, "BILLING PERIOD": {...}, "USAGE (kWh)": {...}}
    This function flattens it to a single level.
    """
    if not isinstance(raw_result, dict):
        return raw_result
    
    section_keys = [
        "ACCOUNT INFO", "BILLING PERIOD", "AMOUNTS", "USAGE (kWh)", "USAGE",
        "CHARGES BREAKDOWN", "DEMAND", "TOU RATES", "USAGE HISTORY", "LINE ITEMS",
        "METERS", "account_info", "billing_period", "amounts", "usage", 
        "charges_breakdown", "demand", "tou_rates", "usage_history", "line_items"
    ]
    
    has_nested_sections = any(key in raw_result for key in section_keys)
    
    if not has_nested_sections:
        return raw_result
    
    flat = {}
    
    for key, value in raw_result.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                flat[inner_key] = inner_value
        elif isinstance(value, list):
            normalized_key = key.lower().replace(" ", "_").replace("(", "").replace(")", "")
            flat[normalized_key] = value
        else:
            flat[key] = value
    
    return flat


def extract_bill_data(file_path, progress_callback=None, training_hints=None, annotated_images=None):
    """
    Extract utility bill data from PDF using xAI Grok 4 vision
    
    Args:
        file_path: Path to the PDF file
        progress_callback: Optional callback function(progress_value, status_message=None)
        training_hints: Optional list of past corrections for this utility
        annotated_images: Optional list of base64-encoded annotated images
    
    Returns a comprehensive JSON structure with detailed bill breakdown
    """
    def notify_progress(value, message=None):
        if progress_callback:
            try:
                progress_callback(value, message)
            except Exception as e:
                print(f"[bill_extractor] Progress callback error: {e}")
    
    print(f"[bill_extractor] Processing: {file_path}")
    
    notify_progress(0.1, "Converting file to images")
    
    image_tuples = file_to_images(file_path)
    if not image_tuples:
        return {
            "success": False,
            "utility_name": None,
            "account_number": None,
            "meters": [],
            "error": "Could not read file"
        }
    
    print(f"[bill_extractor] Converted {len(image_tuples)} page(s)/image(s) for processing")
    
    notify_progress(0.3, "File converted to images")
    
    try:
        training_hints_text = ""
        if training_hints and len(training_hints) > 0:
            hints_list = []
            for hint in training_hints[:20]:
                field = hint.get('field_type', 'unknown')
                value = hint.get('corrected_value', '')
                meter = hint.get('meter_number', '')
                period_start = hint.get('period_start_date', '')
                period_end = hint.get('period_end_date', '')
                
                hint_desc = f"- {field}: correct value is '{value}'"
                if meter:
                    hint_desc += f" for meter {meter}"
                if period_start and period_end:
                    hint_desc += f" (period {period_start} to {period_end})"
                hints_list.append(hint_desc)
            
            training_hints_text = """

CORRECTION HINTS (based on past user corrections for this utility):
""" + "\n".join(hints_list)
        
        extraction_prompt = """You are an expert commercial electric-bill parser for the SiteWalk field app.
You MUST respond with STRICT valid JSON and nothing else. No explanations, no markdown.

Analyze this electric bill and return ONLY JSON with these keys:

ACCOUNT INFO:
- customer_account: string (main customer account number - IMPORTANT: For LADWP bills, use the "ACCOUNT NUMBER" from the bill header, NOT the "SA #" which is a Service Agreement number)
- service_account: string (service/meter account number - For LADWP, this is the SA# or Service Agreement number)
- service_address: string (full address)
- pod_id: string (POD-ID if shown)
- utility_name: string (e.g., "SCE", "Southern California Edison", "LADWP", "PG&E")
- rate: string (rate schedule name, e.g., "TOU-GS-2-E")
- rate_schedule: string (rate schedule code from electric charges section)
- rotating_outage_group: string (if shown)

BILLING PERIOD:
- bill_prepared_date: string (YYYY-MM-DD)
- billing_period_start: string (YYYY-MM-DD)
- billing_period_end: string (YYYY-MM-DD)
- days_in_period: number
- due_date: string (YYYY-MM-DD)

AMOUNTS:
- amount_due: number (total amount due)
- previous_balance: number
- payment_received: number (most recent payment, positive number)
- balance_forward: number
- new_charges: number
- total_owed: number

USAGE (kWh) - ELECTRIC ONLY (exclude water):
- kwh_total: number (total kWh for billing period - ELECTRIC ONLY, do NOT include water usage)
- kwh_on_peak: number (on-peak kWh, also called "High Peak" on LADWP bills)
- kwh_mid_peak: number (mid-peak kWh if applicable)
- kwh_off_peak: number (off-peak kWh, also called "Low Peak" or "Base" on LADWP bills)
- reactive_kvarh: number (reactive usage if shown)
- daily_avg_kwh: number (average daily kWh)

CHARGES BREAKDOWN - ELECTRIC ONLY (exclude water charges):
- energy_charges_total: number (total ELECTRIC energy charges in dollars - EXCLUDE any water charges)
- demand_charges_total: number (total demand charges in dollars)
- other_charges_total: number (customer charges, wildfire fund, etc. - EXCLUDE water-related charges)
- taxes_total: number (all taxes for electric only)
- water_charges_total: number (total water charges if present - report separately, do NOT include in electric totals)

DEMAND (kW):
- max_demand_kw: number (maximum demand reached)
- max_demand_threshold_kw: number (threshold/limit if shown)
- max_demand_on_peak_kw: number
- max_demand_mid_peak_kw: number
- max_demand_off_peak_kw: number

RATES PER kWh (extract from rate details):
- rate_on_peak_per_kwh: number (blended on-peak/high-peak rate in dollars)
- rate_mid_peak_per_kwh: number (blended mid-peak rate in dollars)
- rate_off_peak_per_kwh: number (blended off-peak/low-peak rate in dollars)
- rate_delivery_on_peak: number (delivery portion)
- rate_delivery_mid_peak: number
- rate_delivery_off_peak: number
- rate_generation_on_peak: number (generation portion)
- rate_generation_mid_peak: number
- rate_generation_off_peak: number

TOU BREAKDOWN (Time of Use - required for LADWP and SCE):
- tou_high_peak_kwh: number (High Peak kWh usage - LADWP terminology)
- tou_high_peak_cost: number (High Peak cost in dollars)
- tou_high_peak_rate: number (High Peak rate per kWh)
- tou_low_peak_kwh: number (Low Peak kWh usage - LADWP terminology)
- tou_low_peak_cost: number (Low Peak cost in dollars)
- tou_low_peak_rate: number (Low Peak rate per kWh)
- tou_base_kwh: number (Base kWh if applicable)
- tou_base_cost: number (Base cost in dollars)
- tou_base_rate: number (Base rate per kWh)

SERVICE TYPE DETECTION:
- service_type: string - REQUIRED. Detect what type of utility bill this is:
  - "electric" = electric-only bill (most common for SCE, PG&E)
  - "water" = water-only bill
  - "gas" = gas-only bill  
  - "combined" = combined electric and water bill (common for LADWP)
  For LADWP bills: Check if bill includes BOTH electric charges AND water charges. If both are present, use "combined". If only electric section is present, use "electric". If only water section is present, use "water".

OTHER:
- rate_schedule: string (rate schedule code from electric charges section)
- service_voltage: string (e.g., "240 volts")

LINE ITEMS (array of all itemized charges):
- line_items: array of objects with:
  - category: string ("delivery", "generation", "other", "tax", "water")
  - label: string (charge description)
  - calc: string (calculation shown, e.g., "17,829 kWh x $0.00595")
  - amount: number (dollar amount, negative for credits)
  - is_water_charge: boolean (true if this line item is for water service)

USAGE HISTORY (13 months if available):
- usage_history: array of objects with:
  - month: string (e.g., "Sep '23", "Oct '24")
  - kwh: number
  - days: number (billing days)
  - avg_kwh_per_day: number

METERS (for multi-meter bills):
- meters: array with:
  - meter_number: string
  - service_address: string
  - reads: array with period_start, period_end, kwh, total_charge

LADWP-SPECIFIC INSTRUCTIONS:
1. ACCOUNT NUMBER: Use the "ACCOUNT NUMBER" from the bill header (usually near top). DO NOT use "SA #" (Service Agreement) as the customer_account - that goes in service_account.
2. WATER vs ELECTRIC: LADWP bills often include both water AND electric charges. You MUST separate them:
   - Include ONLY electric kWh in kwh_total
   - Include ONLY electric charges in energy_charges_total
   - Put water charges separately in water_charges_total
   - Mark water line items with is_water_charge: true
3. TOU (Time of Use): Look in the "Electric Charges" section for:
   - "High Peak" = on-peak (use tou_high_peak_kwh, tou_high_peak_cost, tou_high_peak_rate)
   - "Low Peak" = off-peak (use tou_low_peak_kwh, tou_low_peak_cost, tou_low_peak_rate)
   - "Base" = base usage if present
4. RATE SCHEDULE: Extract from electric charges section (e.g., "Rate Schedule: R-1B")
5. DUE DATE: Look for "DUE DATE" or "Payment Due" on the front page

SCE-SPECIFIC INSTRUCTIONS:
1. UTILITY NAME: Southern California Edison bills may show "SCE" or "Southern California Edison" in the header. ALWAYS use "Southern California Edison" or "SCE" for utility_name, NEVER "LADWP" or other utilities.
2. ACCOUNT NUMBER - CRITICAL: 
   - SCE bills have TWO account numbers: "Customer account" and "Service account"
   - ALWAYS prefer "Service account" (the 10-digit number like 8001053647) for customer_account
   - If Service account is not visible, use Customer account as fallback
   - DO NOT use "Rotating outage Group" or POD-ID as account numbers - those are NOT account numbers!
   - The account number should be a pure numeric string (8-12 digits), not text like "Rotating"
3. AMOUNT DUE - CRITICAL:
   - Use "Amount due $XX,XXX.XX" shown prominently on page 1 (usually top right)
   - Or use "Total amount you owe by [date] $XX,XXX.XX"
   - DO NOT use "Your new charges" - that's only new charges, not the total owed!
   - The amount_due should be the TOTAL amount owed, including any previous balance + late fees
4. DUE DATE - CRITICAL:
   - SCE shows "Due by MM/DD/YY" right below or next to "Amount due" on page 1
   - Extract this as YYYY-MM-DD format
5. METER DATA: SCE bills show electric meter information in the usage section. Look for:
   - Meter Number (usually format: E-XXXXXXX or similar)
   - Service Address for each meter
   - kWh usage per meter per billing period
   Each meter should have non-zero kWh values if it's an active electric meter.
6. TOU (Time of Use): SCE uses "On-Peak", "Mid-Peak", "Off-Peak", and sometimes "Super Off-Peak" periods. Map these to:
   - On-Peak → kwh_on_peak, rate_on_peak_per_kwh
   - Mid-Peak → kwh_mid_peak, rate_mid_peak_per_kwh
   - Off-Peak → kwh_off_peak, rate_off_peak_per_kwh
   - Super Off-Peak → use tou fields if present
7. RATE SCHEDULE: SCE rate schedules are SHORT CODES like "TOU-GS-2-E", "TOU-8-B", "TOU-GS-1-E". Look in these locations:
   - Near the account information header
   - In the "Electric Charges" section header
   - Below the service address
   IMPORTANT: The rate schedule is a SHORT code (usually 5-15 characters), NOT a long description.
8. SERVICE ADDRESS: Extract the complete service address including street, city, state, and ZIP code if visible.
9. SERVICE TYPE: SCE bills are typically "electric" only (not combined with water like LADWP).
9. IDENTITY CHECK: Before finalizing, verify the utility_name matches what's actually shown on the bill. If the bill header says "Southern California Edison" or "SCE", utility_name MUST be "Southern California Edison" or "SCE", NOT "LADWP".

Use null for any field you cannot confidently extract. Amounts should be numbers (no $ signs or commas).""" + training_hints_text
        
        content = [
            {
                "type": "text",
                "text": extraction_prompt
            }
        ]
        
        for i, (img_b64, mime_type) in enumerate(image_tuples):
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{img_b64}",
                    "detail": "high"
                }
            })
        
        if annotated_images:
            for i, ann_img_b64 in enumerate(annotated_images):
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{ann_img_b64}",
                        "detail": "high"
                    }
                })
            print(f"[bill_extractor] Added {len(annotated_images)} annotated image(s)")
        
        print(f"[bill_extractor] Sending {len(image_tuples)} page(s) to Grok 4 vision...")
        
        notify_progress(0.6, "Analyzing bill with Grok AI...")
        
        import time
        start_time = time.time()
        
        response = _xai_chat_completions({
            "model": "grok-4",
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert commercial electric-bill parser. You MUST respond with STRICT valid JSON only. No explanations, no markdown, no prose."
                },
                {
                    "role": "user",
                    "content": content
                }
            ],
            "temperature": 0
        })
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        result_text = (response.get("choices", [{}])[0].get("message", {}) or {}).get("content", "")
        print(f"[bill_extractor] Grok 4 API call took {elapsed:.2f} seconds")
        print(f"[bill_extractor] Got response from Grok 4: {result_text[:500]}...")
        
        notify_progress(0.9, "Structuring extracted data")
        
        clean_text = result_text.strip()
        if clean_text.startswith("```json"):
            clean_text = clean_text[7:]
        if clean_text.startswith("```"):
            clean_text = clean_text[3:]
        if clean_text.endswith("```"):
            clean_text = clean_text[:-3]
        clean_text = clean_text.strip()
        
        raw_result = json.loads(clean_text)
        
        result = flatten_grok_response(raw_result)
        
        utility_name = result.get("utility_name")
        account_number = result.get("customer_account") or result.get("account_number")
        meters = result.get("meters", [])
        kwh_total = result.get("kwh_total")
        
        has_valid_data = (
            utility_name and 
            account_number and 
            kwh_total is not None
        )
        
        if has_valid_data:
            print(f"[bill_extractor] Successfully extracted: {utility_name}, account {account_number}, {kwh_total} kWh")
            notify_progress(1.0, "Extraction complete")
            return {
                "success": True,
                "utility_name": utility_name,
                "account_number": account_number,
                "meters": meters,
                "detailed_data": result,
                "error": None
            }
        else:
            missing = []
            if not utility_name:
                missing.append("utility_name")
            if not account_number:
                missing.append("account_number")
            if kwh_total is None:
                missing.append("kwh_total")
            error_msg = f"Missing: {', '.join(missing)}" if missing else "Unknown extraction error"
            print(f"[bill_extractor] Incomplete extraction: {error_msg}")
            print(f"[bill_extractor] Result keys: {list(result.keys())}")
            notify_progress(1.0, "Extraction complete with issues")
            return {
                "success": False,
                "utility_name": utility_name,
                "account_number": account_number,
                "meters": meters,
                "detailed_data": result,
                "error": error_msg
            }
            
    except json.JSONDecodeError as e:
        print(f"[bill_extractor] JSON parse error: {e}")
        print(f"[bill_extractor] Raw response: {result_text[:1000] if 'result_text' in dir() else 'N/A'}")
        return {
            "success": False,
            "utility_name": None,
            "account_number": None,
            "meters": [],
            "error": f"Failed to parse AI response: {e}"
        }
    except Exception as e:
        print(f"[bill_extractor] Error: {e}")
        return {
            "success": False,
            "utility_name": None,
            "account_number": None,
            "meters": [],
            "error": str(e)
        }


def compute_missing_fields(extracted_data, *, include_optional_fields: bool = False):
    """
    Compute which required fields are missing from extracted bill data.
    
    Required fields checked:
    - Bill-level: utility_name, account_number, total_kwh, total_amount_due
    - Per meter: meter_number
    - Per read: period_start, period_end, kwh, total_charge
    
    Optional (non-blocking) fields (when include_optional_fields=True):
    - rate_schedule, service_address
    
    Args:
        extracted_data: dict with extraction results (may include 'detailed_data' key)
    
    Returns:
        List of objects like: [{"field": "total_charge", "label": "Total Charge", "reason": "Value is missing"}]
    """
    missing = []
    
    if not extracted_data:
        return [{"field": "no_extraction_data", "label": "Extraction Data", "reason": "No extraction data available"}]
    
    # Handle both raw extraction format and detailed_data wrapper
    data = extracted_data.get('detailed_data', extracted_data)
    
    # Helper to add missing field
    def add_missing(field, label, reason):
        missing.append({"field": field, "label": label, "reason": reason})
    
    # Bill-level required fields
    utility_name = data.get('utility_name')
    account_number = data.get('customer_account') or data.get('account_number')
    total_kwh = data.get('kwh_total') or data.get('total_kwh')
    total_amount = data.get('amount_due') or data.get('total_amount_due')
    
    if not utility_name:
        add_missing("utility_name", "Utility Name", "Value is missing")
    if not account_number:
        add_missing("account_number", "Account Number", "Value is missing")
    if total_kwh is None:
        add_missing("total_kwh", "Total kWh", "Value is missing or zero")
    elif total_kwh == 0:
        add_missing("total_kwh", "Total kWh", "Value is zero")
    if total_amount is None:
        add_missing("total_amount_due", "Total Charge", "Value is missing")
    elif total_amount == 0:
        add_missing("total_amount_due", "Total Charge", "Value is zero")
    
    # Optional: rate_schedule (useful context but shouldn't block display/analytics)
    if include_optional_fields:
        rate_schedule = data.get('rate') or data.get('rate_schedule')
        if not rate_schedule:
            add_missing("rate_schedule", "Rate Schedule", "Value is missing")
    
    # Check meters
    meters = data.get('meters', [])
    if not meters:
        # No meter array - check if we have billing period at top level
        period_start = data.get('billing_period_start')
        period_end = data.get('billing_period_end')
        
        if not period_start:
            add_missing("period_start", "Period Start", "Value is missing")
        if not period_end:
            add_missing("period_end", "Period End", "Value is missing")
    else:
        # Check each meter
        for i, meter in enumerate(meters):
            meter_number = meter.get('meter_number') or meter.get('meter_id')
            service_address = meter.get('service_address')
            
            meter_label = meter_number or f"Meter {i+1}"
            
            if not meter_number:
                add_missing(f"meter_{i+1}_meter_number", f"Meter {i+1} Number", "Meter number is missing")
            if include_optional_fields:
                service_address = meter.get('service_address')
                if not service_address:
                    add_missing(
                        f"meter_{i+1}_service_address",
                        f"{meter_label} Service Address",
                        "Service address is missing",
                    )
            
            # Check reads for this meter
            reads = meter.get('reads', [])
            if not reads:
                add_missing(f"meter_{i+1}_no_reads", f"{meter_label} Reads", "No billing reads found")
            else:
                for j, read in enumerate(reads):
                    period_start = read.get('period_start')
                    period_end = read.get('period_end')
                    kwh = read.get('kwh')
                    total_charge = read.get('total_charge')
                    
                    if not period_start:
                        add_missing(f"meter_{i+1}_read_{j+1}_period_start", f"{meter_label} Read {j+1} Period Start", "Period start date is missing")
                    if not period_end:
                        add_missing(f"meter_{i+1}_read_{j+1}_period_end", f"{meter_label} Read {j+1} Period End", "Period end date is missing")
                    if kwh is None:
                        add_missing(f"meter_{i+1}_read_{j+1}_kwh", f"{meter_label} Read {j+1} kWh", "kWh value is missing")
                    if total_charge is None:
                        add_missing(f"meter_{i+1}_read_{j+1}_total_charge", f"{meter_label} Read {j+1} Total Charge", "Total charge is missing")
    
    return missing


def _normalize_date_to_iso(date_str):
    """
    Convert various date formats to ISO YYYY-MM-DD format.
    Handles: MM/DD/YY, MM/DD/YYYY, Month DD, YYYY, etc.
    """
    if not date_str:
        return None
    
    import re
    from datetime import datetime
    
    date_str = date_str.strip()
    
    # Try MM/DD/YY or MM/DD/YYYY
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$', date_str)
    if match:
        month, day, year = match.groups()
        year = int(year)
        if year < 100:
            year = 2000 + year if year < 50 else 1900 + year
        try:
            return f"{year:04d}-{int(month):02d}-{int(day):02d}"
        except:
            pass
    
    # Try "Month DD, YYYY" or "Month DD YYYY"
    try:
        for fmt in ["%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except:
                continue
    except:
        pass
    
    # Already in ISO format?
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    
    return date_str  # Return as-is if can't parse


def regex_extract_all_fields(raw_text):
    """
    Extract ALL bill fields using regex patterns BEFORE calling AI.
    This is the first-pass extraction that can skip AI entirely if successful.
    
    Returns dict with extracted fields and service_type detection.
    """
    import re
    
    result = {
        "success": False,
        "utility_name": None,
        "account_number": None,
        "customer_account": None,  # Store both for UI
        "service_account": None,   # Preferred
        "service_address": None,
        "rate_schedule": None,
        "billing_period_start": None,
        "billing_period_end": None,
        "due_date": None,
        "total_kwh": None,
        "total_amount": None,
        "meter_number": None,
        "service_type": "electric",  # default, will be overridden if non-electric detected
        "_extraction_method": "regex",
        "_raw_text": raw_text,
    }
    
    if not raw_text:
        print("[regex_extract] ERROR: Empty raw_text provided!")
        return result
    
    # Debug: Show sample of text being processed
    text_preview = raw_text[:500].replace('\n', '\\n')
    print(f"[regex_extract] Processing {len(raw_text)} chars. Preview: {text_preview}...")
    
    text_lower = raw_text.lower()
    
    # ========== NON-ELECTRIC DETECTION ==========
    # Be specific: look for ACTUAL bill content, not just utility name
    # LADWP serves both water AND electric, so "water and power" in name doesn't tell us bill type
    
    # Look for actual WATER charges/usage section
    water_bill_markers = [
        "water charges",
        "water service charges", 
        "sewer charges",
        "gallons used",
        "hcf used",
        "ccf used",
        "water usage",
        "sewer usage",
        "water schedule",
    ]
    
    # Look for actual ELECTRIC charges/usage section  
    electric_bill_markers = [
        # Generic electric terms
        "electric charges",
        "electric service",
        "electric usage",
        "electricity",
        "kwh used",
        "kwh total",
        "total kwh",
        "energy charges",
        "demand charges",
        "on-peak kwh",
        "off-peak kwh",
        "mid-peak kwh",
        "super off-peak",
        "high peak kwh",
        "low peak kwh",
        "base kwh",
        # SCE specific
        "southern california edison",
        "sce",
        "delivery service",
        "generation service", 
        "transmission",
        "distribution",
        "your electric use",
        "your usage",
        "billed kwh",
        "total energy",
        "electric meter",
        "kilowatt",
        "kilowatt-hour",
        # PG&E / other utilities
        "pacific gas",
        "pg&e",
        "sdg&e",
        "san diego gas",
        # Rate schedule indicators
        "tou-",
        "time-of-use",
        "time of use",
    ]
    
    # Gas bill markers
    gas_bill_markers = [
        "gas charges",
        "gas service charges",
        "therms used",
        "therm usage",
        "natural gas",
    ]
    
    has_water_content = any(marker in text_lower for marker in water_bill_markers)
    has_electric_content = any(marker in text_lower for marker in electric_bill_markers)
    has_gas_content = any(marker in text_lower for marker in gas_bill_markers)
    
    print(f"[regex_extract] Content detection: water={has_water_content}, electric={has_electric_content}, gas={has_gas_content}")
    
    # Determine service type based on ACTUAL content
    if has_water_content and not has_electric_content:
        result["service_type"] = "water"
        print(f"[regex_extract] Detected NON-ELECTRIC bill: water (has water charges, no electric charges)")
    elif has_gas_content and not has_electric_content:
        result["service_type"] = "gas"
        print(f"[regex_extract] Detected NON-ELECTRIC bill: gas (has gas charges, no electric charges)")
    elif has_water_content and has_electric_content:
        # This is a combined bill - treat as electric for extraction purposes
        result["service_type"] = "electric"
        print(f"[regex_extract] Combined water+electric bill - treating as ELECTRIC")
    elif has_electric_content:
        result["service_type"] = "electric"
        print(f"[regex_extract] Electric bill detected")
    else:
        # No clear markers - default to electric and let AI figure it out
        result["service_type"] = "electric"
        print(f"[regex_extract] No clear service type markers - defaulting to electric")
    
    # ========== UTILITY NAME ==========
    # Comprehensive list: California + Major National Utilities
    utility_patterns = [
        # --- CALIFORNIA ---
        (r"(Southern California Edison|SCE)", "Southern California Edison"),
        (r"(Los Angeles Department of Water and Power|LADWP|LA\s*DWP)", "Los Angeles Department of Water and Power"),
        (r"(Pacific Gas (?:and|&) Electric|PG&E|PGE)", "Pacific Gas and Electric"),
        (r"(San Diego Gas (?:and|&) Electric|SDG&E|SDGE)", "San Diego Gas & Electric"),
        (r"(Sacramento Municipal Utility District|SMUD)", "Sacramento Municipal Utility District"),
        (r"(Burbank Water and Power|BWP)", "Burbank Water and Power"),
        (r"(Glendale Water (?:and|&) Power|GWP)", "Glendale Water and Power"),
        (r"(Pasadena Water (?:and|&) Power|PWP)", "Pasadena Water and Power"),
        (r"(Los Angeles Water (?:and|&) Power)", "Los Angeles Department of Water and Power"),
        (r"(Imperial Irrigation District|IID)", "Imperial Irrigation District"),
        (r"(Riverside Public Utilities|RPU)", "Riverside Public Utilities"),
        (r"(Anaheim Public Utilities)", "Anaheim Public Utilities"),
        # --- TEXAS ---
        (r"(TXU Energy|TXU)", "TXU Energy"),
        (r"(Reliant Energy|Reliant)", "Reliant Energy"),
        (r"(Direct Energy)", "Direct Energy"),
        (r"(Oncor)", "Oncor"),
        (r"(CenterPoint Energy)", "CenterPoint Energy"),
        (r"(AEP Texas)", "AEP Texas"),
        # --- NORTHEAST ---
        (r"(Con Edison|ConEd|Consolidated Edison)", "Consolidated Edison"),
        (r"(PSEG|Public Service Electric (?:and|&) Gas)", "Public Service Electric and Gas"),
        (r"(National Grid)", "National Grid"),
        (r"(Eversource)", "Eversource"),
        (r"(PECO Energy|PECO)", "PECO Energy"),
        (r"(PPL Electric)", "PPL Electric"),
        (r"(Jersey Central Power (?:and|&) Light|JCP&L)", "Jersey Central Power & Light"),
        # --- SOUTHEAST ---
        (r"(Duke Energy)", "Duke Energy"),
        (r"(Florida Power (?:and|&) Light|FPL)", "Florida Power & Light"),
        (r"(Georgia Power)", "Georgia Power"),
        (r"(Tampa Electric|TECO)", "Tampa Electric"),
        (r"(Dominion Energy|Dominion Virginia Power)", "Dominion Energy"),
        (r"(Progress Energy)", "Progress Energy"),
        (r"(Entergy)", "Entergy"),
        # --- MIDWEST ---
        (r"(ComEd|Commonwealth Edison)", "Commonwealth Edison"),
        (r"(Ameren)", "Ameren"),
        (r"(DTE Energy|DTE)", "DTE Energy"),
        (r"(Consumers Energy)", "Consumers Energy"),
        (r"(We Energies|Wisconsin Energy)", "We Energies"),
        (r"(Xcel Energy)", "Xcel Energy"),
        (r"(MidAmerican Energy)", "MidAmerican Energy"),
        # --- WEST ---
        (r"(Arizona Public Service|APS)", "Arizona Public Service"),
        (r"(Salt River Project|SRP)", "Salt River Project"),
        (r"(NV Energy|Nevada Energy)", "NV Energy"),
        (r"(Rocky Mountain Power)", "Rocky Mountain Power"),
        (r"(PacifiCorp)", "PacifiCorp"),
        (r"(Puget Sound Energy|PSE)", "Puget Sound Energy"),
        (r"(Portland General Electric|PGE)", "Portland General Electric"),
        (r"(Hawaiian Electric|HECO)", "Hawaiian Electric"),
        # --- GENERIC FALLBACK ---
        (r"Electric\s*(?:Company|Service|Utility)[:\s]*([A-Z][A-Za-z\s&]+(?:Electric|Power|Energy|Utility))", None),
    ]
    for pattern, canonical_name in utility_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            result["utility_name"] = canonical_name if canonical_name else match.group(1).strip()
            print(f"[regex_extract] utility_name: {result['utility_name']}")
            break
    
    # ========== ACCOUNT NUMBER ==========
    # Priority: Service account > Customer account (user preference)
    # SCE bills have BOTH - prefer Service account (e.g., "8001053647")
    # 
    # SCE FORMAT OBSERVED:
    #   Customer account
    #   700203608397
    #   
    #   Service account
    #   8001053647
    #   1151 OLYMPIC DR
    #   CORONA, CA 92881
    #
    # Need patterns that handle:
    # 1. Multi-line format (label on one line, number on next)
    # 2. Same-line format (label: number)
    # 3. Various separators (newlines, colons, spaces)
    
    account_patterns = [
        # ===== SCE SERVICE ACCOUNT (HIGHEST PRIORITY) =====
        # SCE multi-line: "Service account" then number on next line(s)
        # Allow some garbage between label and number (like blank lines)
        r"Service\s*account\s*[\n\r]+\s*(\d{10})\b",
        r"Service\s*account\s*[\n\r\s]+(\d{10})\b",
        r"Service\s*account[:\s]*(\d{10})\b",
        # Service acct with potential "POD-ID" between
        r"Service\s*account\s*(?:POD[\-]?ID[^\n]*[\n\r]+)?\s*(\d{10})\b",
        # Any "Service" + "account" + 10-digit number within 100 chars
        r"Service\s+account[\s\S]{0,50}?(\d{10})\b",
        
        # ===== LADWP SA # =====
        r"SA\s*#\s*[:\s]*(\d{10})",
        
        # ===== SCE CUSTOMER ACCOUNT (FALLBACK) =====
        r"Customer\s*account\s*[\n\r]+\s*(\d{12})\b",
        r"Customer\s*account\s*[\n\r\s]+(\d{12})\b",
        r"Customer\s*account[:\s]*(\d{12})\b",
        r"Customer\s+account[\s\S]{0,50}?(\d{12})\b",
        
        # ===== GENERIC ACCOUNT PATTERNS =====
        # Standard labeled: "Account Number: 123456789"
        r"Account\s*(?:Number|#|No\.?)[:\s]+(\d{6,15})",
        # Just "Account:" followed by number
        r"Account[:\s]+(\d{6,15})",
        # XXX-XXX-XXXX format
        r"Account[:\s]+(\d{3,4}[\-\s]\d{3,4}[\-\s]\d{3,4})",
        # Acct abbreviation
        r"Acct\.?\s*#?[:\s]+(\d{6,15})",
        # Bill account
        r"Bill(?:ing)?\s*Account[:\s]+(\d{6,15})",
        # Service ID
        r"Service\s*ID[:\s]+(\d{6,15})",
        # Electric account
        r"Electric\s*Account[:\s]+(\d{6,15})",
    ]
    
    # First try to extract SERVICE account (preferred for SCE)
    service_acct_patterns = [
        r"Service\s*account\s*[\n\r]+\s*(\d{10})\b",
        r"Service\s*account\s*[\n\r\s]+(\d{10})\b",
        r"Service\s*account[:\s]*(\d{10})\b",
        r"Service\s+account[\s\S]{0,50}?(\d{10})\b",
        r"SA\s*#\s*[:\s]*(\d{10})",
    ]
    for pattern in service_acct_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
        if match:
            acct = match.group(1).strip()
            if acct.isdigit() and len(acct) == 10:
                result["service_account"] = acct
                result["account_number"] = acct  # Use service account as primary
                print(f"[regex_extract] service_account: {acct} (pattern: {pattern[:40]}...)")
                break
    
    # Then try to extract CUSTOMER account (fallback/secondary)
    customer_acct_patterns = [
        r"Customer\s*account\s*[\n\r]+\s*(\d{12})\b",
        r"Customer\s*account\s*[\n\r\s]+(\d{12})\b",
        r"Customer\s*account[:\s]*(\d{12})\b",
        r"Customer\s+account[\s\S]{0,50}?(\d{12})\b",
    ]
    for pattern in customer_acct_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
        if match:
            acct = match.group(1).strip()
            if acct.isdigit() and len(acct) == 12:
                result["customer_account"] = acct
                # Only use customer account as primary if we didn't find service account
                if not result["account_number"]:
                    result["account_number"] = acct
                print(f"[regex_extract] customer_account: {acct} (pattern: {pattern[:40]}...)")
                break
    
    # Generic fallback patterns if neither service nor customer account found
    if not result["account_number"]:
        for pattern in account_patterns:
            match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
            if match:
                acct = match.group(1).strip()
                # Validate: must be purely numeric and reasonable length
                if acct.isdigit() and 6 <= len(acct) <= 15:
                    result["account_number"] = acct
                    print(f"[regex_extract] account_number (fallback): {result['account_number']} (pattern: {pattern[:50]}...)")
                    break
    
    # ========== SERVICE ADDRESS ==========
    # Universal patterns - look for labeled addresses and street patterns
    address_patterns = [
        # Labeled SERVICE ADDRESS (most reliable)
        r"SERVICE\s*ADDRESS[:\-]?\s*(.{10,100})",
        r"Service\s*Location[:\-]?\s*(.{10,100})",
        r"Premise\s*(?:Address)?[:\-]?\s*(.{10,100})",
        r"Property\s*Address[:\-]?\s*(.{10,100})",
        r"Installation\s*Address[:\-]?\s*(.{10,100})",
        r"Billing\s*Address[:\-]?\s*(.{10,100})",
        r"Location[:\-]?\s*(.{10,100})",
        # Generic US street address with state abbreviation + ZIP
        r"(\d{2,5}\s+[A-Z][A-Za-z\s]+(?:ST|STREET|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|WAY|LN|LANE|CT|COURT|PL|PLACE|CIR|CIRCLE|TRL|TRAIL|PKWY|PARKWAY)[,\s]+[A-Z][A-Za-z\s]+[,\s]*(?:CA|NY|TX|FL|IL|PA|OH|GA|NC|MI|NJ|VA|WA|AZ|MA|TN|IN|MO|MD|WI|CO|MN|SC|AL|LA|KY|OR|OK|CT|UT|IA|NV|AR|MS|KS|NM|NE|WV|ID|HI|NH|ME|RI|MT|DE|SD|ND|AK|VT|WY|DC)\s*\d{5}(?:-\d{4})?)",
        # Street number + name + apt/unit (without full city/state)
        r"(\d{2,5}\s+[A-Z][A-Za-z0-9\s]+(?:ST|STREET|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|WAY|LN|LANE|CT|COURT|PL|PLACE)(?:\s*(?:#|APT|UNIT|STE|SUITE)\s*[A-Z0-9]+)?)",
    ]
    for pattern in address_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            addr = match.group(1).strip()
            # Clean up - stop at common field boundaries
            addr = re.split(r"\n\n|\nPOD-ID|\nBILLING|\nACCOUNT|\nMETER|\nRATE|\nNEXT|\nSERVICE\s*ACCOUNT|\nCUSTOMER", addr, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            # Remove trailing punctuation
            addr = addr.rstrip(',;:.')
            # Skip if it looks like a meter number (contains APM, kVARH, etc.)
            if any(skip in addr.upper() for skip in ["APM", "KVARH", "BASE KVARH", "METER NUMBER", "SERVES"]):
                continue
            # Must have at least a number and some letters
            if len(addr) >= 10 and re.search(r'\d', addr) and re.search(r'[A-Za-z]', addr):
                result["service_address"] = addr
                print(f"[regex_extract] service_address: {result['service_address']}")
                break
    
    # ========== RATE SCHEDULE ==========
    # Universal patterns for rate schedule/tariff across utilities
    rate_patterns = [
        # LADWP: "RATE SCHEDULE" followed by full description
        r"RATE\s*SCHEDULE\s*\n([A-Z][\-\d\[\]i]+(?:\s+and\s+[A-Z][\-\d\[\]i]+)?[^\n]*(?:\n[A-Z][^\n]*)?)",
        r"RATE\s*SCHEDULE[:\s]*([A-Z][\-\d\[\]i]+(?:\s+and\s+[A-Z][\-\d\[\]i]+)?[^\n]*)",
        # SCE TOU rates: "TOU-D-4-9PM", "TOU-D-A", "TOU-GS-1", etc.
        r"Rate[:\s]*(TOU[\-\s]?[A-Z0-9\-]+(?:\-[A-Z0-9]+)*)",
        # SCE: "Schedule TOU-D-4-9PM" or "Schedule D"
        r"Schedule[:\s]*(TOU[\-\s]?[A-Z0-9\-]+)",
        r"Schedule[:\s]*([A-Z][\-]?[A-Z0-9\-]*)",
        # Generic rate code: A-1, D, GS-1, etc.
        r"Rate\s*(?:Code|Schedule)?[:\s]*([A-Z][\-]?\d+[A-Z]?(?:\-[A-Z0-9]+)?)",
        # Your Rate:
        r"Your\s*Rate[:\s]+([A-Z0-9\-]+(?:\s*[A-Z0-9\-]+)?)",
        # Tariff
        r"Tariff[:\s]*([A-Z0-9\-]+)",
        # Service Classification
        r"Service\s*Classification[:\s]*([A-Z0-9\-]+)",
        # Rate Class
        r"Rate\s*Class[:\s]*([A-Z0-9\-\s]+)",
        # Electric Service Rate
        r"Electric\s*(?:Service\s*)?Rate[:\s]*([A-Z0-9\-]+)",
        # Residential/Commercial rate names
        r"Rate[:\s]*(Residential|Commercial|Industrial|Small\s*Business|General\s*Service|Time[\-\s]*of[\-\s]*Use)",
        # Water rate schedules
        r"(?:Water\s*)?Schedule[:\s]*(Water\s*Schedule\s*[A-Z][\s\-\w]*)",
    ]
    for pattern in rate_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE | re.MULTILINE)
        if match:
            rate = match.group(1).strip()
            # Normalize whitespace (including newlines)
            rate = re.sub(r'\s+', ' ', rate).strip()
            # Reject garbage - stop at "NEXT SCHEDULED" or other field boundaries
            rate = re.split(r'NEXT\s*SCHEDULED|METER\s*NUMBER|BILLING\s*PERIOD|ZONE|RIN:', rate, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            # Validation
            if len(rate) >= 1 and len(rate) <= 100 and not any(bad in rate.lower() for bad in ["please", "www.", "contact", "questions"]):
                result["rate_schedule"] = rate
                print(f"[regex_extract] rate_schedule: {result['rate_schedule']}")
                break
    
    # ========== BILLING PERIOD ==========
    # SCE FORMAT OBSERVED:
    #   Page 3: "For meter V349N-002081 from 07/21/25 to 08/18/25"
    #   Also: Date bill prepared 08/19/25 (not billing period, but related)
    #
    period_patterns = [
        # ===== SCE SPECIFIC (HIGHEST PRIORITY) =====
        # SCE: "from 07/21/25 to 08/18/25" - exact format from page 3
        r"from\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+to\s+(\d{1,2}/\d{1,2}/\d{2,4})",
        # SCE: "For meter XXXX from MM/DD/YY to MM/DD/YY"
        r"[Ff]or\s+meter\s+[A-Z0-9\-]+\s+from\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+to\s+(\d{1,2}/\d{1,2}/\d{2,4})",
        
        # ===== GENERIC LABELED PATTERNS =====
        # Labeled: "Billing Period: MM/DD/YYYY - MM/DD/YYYY"
        r"(?:Billing|Service|Statement|Usage)\s*Period[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–to]+\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Labeled with word dates: "Billing Period: Nov 1, 2024 to Dec 1, 2024"
        r"(?:Billing|Service|Statement)\s*Period[:\s]*(\w+\s+\d{1,2},?\s+\d{4})\s*[-–to]+\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # From/To format
        r"From[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:To|[-–])\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Service From/To
        r"Service\s*From[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:To|Through)[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Just two dates with dash/to (common)
        r"(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–]\s*(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:\d+\s*days?)?",
        r"(\d{1,2}/\d{1,2}/\d{2,4})\s+to\s+(\d{1,2}/\d{1,2}/\d{2,4})",
        # SCE: "For usage from Nov 05 to Dec 04, 2024"
        r"(?:For\s*)?[Uu]sage\s*(?:from\s*)?(\w+\s+\d{1,2})\s*(?:to|through|-)\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # Read dates: "Current Read: 12/04/24  Previous Read: 11/05/24"
        r"Previous\s*Read[:\s]*(\d{1,2}/\d{1,2}/\d{2,4}).*?Current\s*Read[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Current\s*Read[:\s]*(\d{1,2}/\d{1,2}/\d{2,4}).*?Previous\s*Read[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Meter read dates
        r"Meter\s*Read\s*Dates?[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–to]+\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Days in billing period with dates
        r"(\d{1,2}/\d{1,2}/\d{2,4})\s*(?:thru|through)\s*(\d{1,2}/\d{1,2}/\d{2,4})",
    ]
    for pattern in period_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            start_raw = match.group(1).strip()
            end_raw = match.group(2).strip()
            # Convert to ISO format (YYYY-MM-DD)
            result["billing_period_start"] = _normalize_date_to_iso(start_raw)
            result["billing_period_end"] = _normalize_date_to_iso(end_raw)
            if result["billing_period_start"] and result["billing_period_end"]:
                print(f"[regex_extract] billing_period: {start_raw} -> {result['billing_period_start']} to {end_raw} -> {result['billing_period_end']}")
                break
    
    # ========== DUE DATE ==========
    # Universal patterns for payment due date
    due_patterns = [
        # SCE: "Due by 10/08/25" - HIGHEST PRIORITY (exact match from bill)
        r"Due\s*[Bb]y\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # SCE: "Total amount you owe by 10/08/25"
        r"(?:Total\s*)?[Aa]mount\s*(?:you\s*)?[Oo]we\s*[Bb]y\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Standard due date labels
        r"Due\s*Date\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Due\s*Date\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        r"Payment\s*Due\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Payment\s*Due\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # Pay by
        r"Pay\s*By\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Pay\s*By\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # Please pay by
        r"Please\s*Pay\s*By\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Please\s*Pay\s*By\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # Due on
        r"Due\s*[Oo]n\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        r"Due\s*[Oo]n\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        # Auto payment date
        r"AUTO\s*PAYMENT\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        r"Auto(?:matic)?\s*Payment\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Amount Due by
        r"Amount\s*Due\s*(?:By|On)\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Bill Due
        r"Bill\s*Due\s*[:\-]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        # Just DUE: date
        r"\bDUE[:\s]+(\d{1,2}/\d{1,2}/\d{2,4})",
    ]
    for pattern in due_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            due_raw = match.group(1).strip()
            result["due_date"] = _normalize_date_to_iso(due_raw)
            if result["due_date"]:
                print(f"[regex_extract] due_date: {due_raw} -> {result['due_date']}")
                break
    
    # ========== TOTAL KWH ==========
    # Run kWh extraction regardless of initial service_type detection
    # If we find kWh, that confirms it's electric
    #
    # SCE FORMAT OBSERVED:
    #   Page 3: "Total electricity you used this month in kWh    160,474"
    #   Also on page 3: Table showing On peak/Mid peak/Off peak summing to 160474 kWh
    #
    kwh_patterns = [
        # ===== SCE SPECIFIC (HIGHEST PRIORITY) =====
        # SCE Page 3: "Total electricity you used this month in kWh    160,474"
        r"Total\s+electricity\s+you\s+used\s+this\s+month\s+in\s+kWh\s+([\d,]+)",
        # SCE: "Total electricity you used this month in kWh" with number anywhere after
        r"Total\s+electricity\s+you\s+used[\s\S]{0,30}?([\d,]{4,})",
        # SCE: usage summary total line - "160474 kWh" standalone on page 3
        r"^\s*([\d,]{4,})\s*kWh\s*$",
        # SCE table: "On peak [bar] 25246 kWh"
        r"(?:On|Off|Mid)\s*peak\s*[\s\S]{0,50}?([\d,]+)\s*kWh",
        
        # ===== LADWP SPECIFIC =====
        # LADWP: "Electric Charges 10/24/25 - 11/26/25 81,920 kWh"
        r"Electric\s*Charges\s*\d{1,2}/\d{1,2}/\d{2,4}\s*[-–]\s*\d{1,2}/\d{1,2}/\d{2,4}\s*([\d,]+(?:\.\d+)?)\s*kWh",
        # LADWP: "Total kWh Consumption"
        r"Total\s*kWh\s*Consumption[^\d]*([\d,]+(?:\.\d+)?)",
        
        # ===== GENERIC PATTERNS =====
        # Total Usage/kWh/Energy
        r"Total\s*(?:Usage|kWh|Energy)[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        r"Total\s*(?:Usage|kWh|Energy)[:\s]*([\d,]+(?:\.\d+)?)\s*(?:kilowatt)",
        # X kWh Total
        r"([\d,]+(?:\.\d+)?)\s*kWh\s*Total",
        # Total kWh:
        r"Total\s*kWh[:\s]*([\d,]+(?:\.\d+)?)",
        # Usage: X kWh
        r"Usage[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # kWh Used
        r"kWh\s*Used[:\s]*([\d,]+(?:\.\d+)?)",
        # Energy Charges ... X kWh
        r"Energy\s*Charges.*?([\d,]+(?:\.\d+)?)\s*kWh",
        # Your usage this month
        r"Your\s*[Uu]sage\s*(?:this\s*month)?[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        r"Billed\s*kWh[:\s]*([\d,]+(?:\.\d+)?)",
        r"Total\s*Billed\s*kWh[:\s]*([\d,]+(?:\.\d+)?)",
        r"kWh\s*Billed[:\s]*([\d,]+(?:\.\d+)?)",
        # Table formats
        r"Delivery[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        r"Generation[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # Meter Reading shows kWh
        r"Meter\s*Reading.*?([\d,]+(?:\.\d+)?)\s*kWh",
        # Total Consumption
        r"Total\s*Consumption[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # Electricity Used
        r"Electricity\s*Used[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # kWh Delivered
        r"kWh\s*Delivered[:\s]*([\d,]+(?:\.\d+)?)",
        # Electric Delivery
        r"Electric\s*Delivery[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # X kWh near "usage" or "used"
        r"[Uu](?:sage|sed)[:\s]*([\d,]+(?:\.\d+)?)\s*kWh",
        # Standalone large number + kWh (last resort)
        r"\b([\d,]{4,})\s*kWh\b",
    ]
    for pattern in kwh_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            try:
                kwh_str = match.group(1).replace(",", "")
                kwh_val = float(kwh_str)
                # Sanity check - kWh should be reasonable (not a phone number, etc.)
                if kwh_val > 0 and kwh_val < 10000000:
                    result["total_kwh"] = kwh_val
                    # If we found kWh, this is definitely electric
                    if result["service_type"] not in ("electric", "combined"):
                        result["service_type"] = "electric"
                        print(f"[regex_extract] Found kWh - upgrading service_type to 'electric'")
                    print(f"[regex_extract] total_kwh: {result['total_kwh']}")
                    break
            except (ValueError, TypeError):
                pass
    
    # ========== TOTAL AMOUNT ==========
    # SCE FORMAT OBSERVED:
    #   Page 1: "Amount due $37,225.88" (top right, prominent)
    #   Page 1: "Total amount you owe by 09/08/25   $37,225.88"
    #
    # IMPORTANT: Prioritize "Total amount you owe" and "Amount due" over "New charges"
    amount_patterns = [
        # ===== SCE SPECIFIC (HIGHEST PRIORITY) =====
        # SCE: "Amount due $37,225.88" - exact format from bill page 1
        r"Amount\s+due\s+\$\s*([\d,]+\.\d{2})",
        r"Amount\s+due\s*\$?([\d,]+\.\d{2})",
        # SCE: "Total amount you owe by 09/08/25 $37,225.88"
        r"Total\s+amount\s+you\s+owe\s+by\s+\d{1,2}/\d{1,2}/\d{2,4}\s+\$?\s*([\d,]+\.\d{2})",
        r"Total\s+amount\s+you\s+owe[\s\S]{0,30}?\$\s*([\d,]+\.\d{2})",
        # SCE: Big prominent "Amount due" with $ amount 
        r"Amount\s*due[:\s]*\$\s*([\d,]+\.\d{2})",
        
        # ===== LADWP =====
        # LADWP: "Total Amount Due $ 22,462.77"
        r"Total\s*Amount\s*Due\s*\$\s*([\d,]+\.\d{2})",
        
        # ===== GENERIC PATTERNS =====
        # Total Amount You Owe (case insensitive)
        r"Total\s*Amount\s*You\s*Owe[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Total Due/Owed/Charges
        r"Total\s*(?:Due|Owed)[:\s]*\$?\s*([\d,]+\.\d{2})",
        r"Total\s*Amount\s*(?:Owed|Payable)[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Amount Due (various formats) - but NOT "new charges"
        r"(?:Total\s*)?Amount\s*Due[:\s]*\$?\s*([\d,]+\.\d{2})",
        r"Amount\s*(?:Now\s*)?Due[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Please Pay / Pay This Amount
        r"(?:Please\s*)?Pay\s*(?:This\s*)?Amount[:\s]*\$?\s*([\d,]+\.\d{2})",
        r"Amount\s*(?:To\s*)?Pay[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Balance Due
        r"(?:Total\s*)?Balance\s*Due[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Total Electric/Gas Charges
        r"Total\s*Electric\s*Charges[:\s]*\$?\s*([\d,]+\.\d{2})",
        r"Total\s*Gas\s*Charges[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Your Bill / This Bill
        r"(?:Your|This)\s*Bill[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Pay Online amount
        r"Pay\s*Online[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Total Bill
        r"Total\s*Bill[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Statement Balance
        r"Statement\s*Balance[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Current Charges (lower priority)
        r"(?:Total\s*)?Current\s*Charges[:\s]*\$?\s*([\d,]+\.\d{2})",
        # New Charges (LOWEST PRIORITY - not the total owed!)
        r"Your\s+new\s+charges[:\s]*\$?\s*([\d,]+\.\d{2})",
        r"(?:Total\s*)?New\s*Charges[:\s]*\$?\s*([\d,]+\.\d{2})",
        # Generic $ amount after "total" or "due"
        r"(?:total|due)[:\s]*\$\s*([\d,]+\.\d{2})",
    ]
    for pattern in amount_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            try:
                amt_str = match.group(1).replace(",", "")
                amt_val = float(amt_str)
                # Sanity check - amount should be reasonable
                if amt_val > 0 and amt_val < 10000000:
                    result["total_amount"] = amt_val
                    print(f"[regex_extract] total_amount: {result['total_amount']}")
                    break
            except (ValueError, TypeError):
                pass
    
    # ========== METER NUMBER ==========
    # SCE FORMAT OBSERVED:
    #   "For meter V349N-002081 from 07/21/25 to 08/18/25"
    #   Pattern: V + 3 digits + letter + hyphen + 6 digits
    #
    # LADWP FORMAT:
    #   "APMYV00222-00027735"
    #   Pattern: APM + alphanumeric + hyphen + digits
    #
    # Also seen: E-1234567, 12345678, etc.
    
    meter_patterns = [
        # ===== SCE METER FORMAT (HIGHEST PRIORITY) =====
        # SCE: "For meter V349N-002081" - letter + digits + letter + hyphen + digits
        r"[Ff]or\s*meter\s+([A-Z]\d{3}[A-Z][\-]\d{6})",
        # SCE: "meter V349N-002081" anywhere
        r"\bmeter\s+([A-Z]\d{3}[A-Z][\-]\d{6})",
        # SCE standalone meter ID: V###X-###### format
        r"\b([A-Z]\d{3}[A-Z][\-]\d{6})\b",
        # SCE: E-####### or similar with E prefix
        r"\b(E[\-]\d{7,10})\b",
        
        # ===== LADWP METER FORMAT =====
        r"(APM[A-Z0-9]{2,5}[\-][0-9\-]{8,15})",
        
        # ===== GENERIC LABELED PATTERNS =====
        # "METER NUMBER: V349N-002081" or "Meter #: 12345678"
        r"METER\s*(?:NUMBER|#|NO\.?|ID)[\s:]+([A-Z0-9][\-A-Z0-9]{4,20})",
        r"Meter\s*(?:#|No\.?|Number|ID)[:\s]+([A-Z0-9][\-A-Z0-9]{4,20})",
        # Electric Meter
        r"Electric\s*Meter[:\s]+([A-Z0-9][\-A-Z0-9]{4,20})",
        # Meter Serial Number
        r"Meter\s*Serial[:\s]+([A-Z0-9][\-A-Z0-9]{4,20})",
        # Service Point ID
        r"Service\s*Point\s*(?:ID|#)?[:\s]+([A-Z0-9][\-A-Z0-9]{4,20})",
        
        # ===== NUMERIC ONLY (LOWER PRIORITY) =====
        # "Meter: 12345678" - pure numeric
        r"\bMeter[:\s]+(\d{6,15})\b",
        # METER NUMBER followed by numeric
        r"METER\s*(?:NUMBER|#|NO\.?|ID)[:\s]+(\d{6,15})",
    ]
    
    for pattern in meter_patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            meter_val = match.group(1).strip()
            # Skip common false positives
            skip_values = ["SERVES", "NUMBER", "READING", "READ", "TYPE", "LOCATION", "STATUS", "UNKNOWN"]
            if meter_val.upper() in skip_values:
                continue
            # Must have reasonable length and contain digits
            if len(meter_val) >= 5 and re.search(r'\d', meter_val):
                result["meter_number"] = meter_val
                print(f"[regex_extract] meter_number: {result['meter_number']} (pattern: {pattern[:40]}...)")
                break
    
    # ========== TOU BREAKDOWN ==========
    # Run TOU extraction always - if we find TOU data, that confirms it's electric
    tou_data = []
    # LADWP format: "High Peak Subtotal (11,680 kWh x $0.25624/kWh) $2,992.92"
    ladwp_tou = r"(High\s*Peak|Low\s*Peak|Base)\s*Subtotal\s*\(\s*([\d,]+(?:\.\d+)?)\s*kWh\s*x\s*\$?([\d.]+)/kWh\s*\)\s*\$?([\d,]+(?:\.\d+)?)"
    for match in re.finditer(ladwp_tou, raw_text, re.IGNORECASE):
        period = match.group(1).strip()
        kwh = float(match.group(2).replace(",", ""))
        rate = float(match.group(3))
        cost = float(match.group(4).replace(",", ""))
        period_map = {"High Peak": "On-Peak", "Low Peak": "Off-Peak", "Base": "Super Off-Peak"}
        tou_data.append({
            "period": period_map.get(period.title(), period.title()),
            "kwh": kwh, "rate": rate, "estimated_cost": cost
        })
    
    # SCE bills: TOU periods may have explicit labels like:
    # "On peak 32249 kwh x $0.12384 = $3,993.72"
    # "Mid peak 67241 kwh x $0.12272 = $8,251.82"  
    # "Off peak 52319 kwh x $0.08918 = $4,665.81"
    # "Super off peak 45000 kwh x $0.05918 = $2,665.81"
    # OCR may produce artifacts like "kwh =x" or "kwh_ x" so we need flexible matching
    # First try to extract WITH explicit labels
    if not tou_data:
        # Pattern with explicit period label - flexible to handle OCR artifacts
        # Matches: "Off peak 65375 kwh =x $0.12227 = $7,993.41" (OCR typo =x)
        # Matches: "Super off peak 51345 kwh_ x $0.08875 = $4,556.87" (OCR underscore)
        # Matches: "Super off peak 21571 kWh x = $0.08875 = $1,914.42" (extra = after x)
        # Matches: "On peak 28006 kWh x $0.14823 2 $4,151.33" (2 instead of =)
        sce_labeled_pattern = r"(On[\s\-]*Peak|Mid[\s\-]*Peak|Super\s*Off[\s\-]*Peak|Off[\s\-]*Peak)\s+([\d,]+)\s*kWh[_\s]*[=]?\s*x\s*[=]?\s*\$?([\d.]+)\s*[=2]\s*\$?([\d,]+\.\d{2})"
        for match in re.finditer(sce_labeled_pattern, raw_text, re.IGNORECASE):
            period_raw = match.group(1).strip()
            kwh = float(match.group(2).replace(",", ""))
            rate = float(match.group(3))
            cost = float(match.group(4).replace(",", ""))
            # Skip small entries
            if kwh < 100:
                continue
            # Normalize period name
            period_lower = period_raw.lower().replace("-", " ").replace("  ", " ")
            if "super" in period_lower:
                period = "Super Off-Peak"
            elif "on" in period_lower:
                period = "On-Peak"
            elif "mid" in period_lower:
                period = "Mid-Peak"
            elif "off" in period_lower:
                period = "Off-Peak"
            else:
                period = period_raw.title()
            tou_data.append({"period": period, "kwh": kwh, "rate": rate, "estimated_cost": cost})
            print(f"[regex_extract] SCE TOU explicit: {period} - {kwh} kWh @ ${rate} = ${cost}")
    
    # If no labeled data found, try inferring from rate (fallback)
    # "13401 kWh   x   $0.14823   =   $1,986.43"
    if not tou_data:
        # Pattern: "13401 kWh x $0.14823 = $1,986.43" or "13,401 kWh x $0.14823 = $1,986.43"
        sce_usage_pattern = r"([\d,]+)\s*kWh\s+x\s+\$?([\d.]+)\s*=\s*\$?([\d,]+\.\d{2})"
        sce_matches = []
        for match in re.finditer(sce_usage_pattern, raw_text, re.IGNORECASE):
            kwh_str = match.group(1).replace(",", "")
            rate_str = match.group(2)
            cost_str = match.group(3).replace(",", "")
            try:
                kwh = float(kwh_str)
                rate = float(rate_str)
                cost = float(cost_str)
                # Skip very small entries (likely line items, not TOU periods)
                if kwh < 100:
                    continue
                sce_matches.append({"kwh": kwh, "rate": rate, "estimated_cost": cost})
            except ValueError:
                continue
        
        # If we found 2-4 rows, these are likely TOU periods
        # Sort by rate descending: On-Peak (highest), Mid-Peak, Off-Peak (lowest)
        if 2 <= len(sce_matches) <= 4:
            sce_matches.sort(key=lambda x: x["rate"], reverse=True)
            # Assign period names based on position
            if len(sce_matches) == 2:
                period_names = ["On-Peak", "Off-Peak"]
            elif len(sce_matches) == 3:
                period_names = ["On-Peak", "Mid-Peak", "Off-Peak"]
            else:  # 4 periods
                period_names = ["On-Peak", "Mid-Peak", "Off-Peak", "Super Off-Peak"]
            
            for i, entry in enumerate(sce_matches):
                entry["period"] = period_names[i]
                tou_data.append(entry)
                print(f"[regex_extract] SCE TOU inferred: {entry['period']} - {entry['kwh']} kWh @ ${entry['rate']} = ${entry['estimated_cost']}")
    
    if tou_data:
        result["tou_breakdown"] = tou_data
        # If we found TOU data, this is definitely electric
        if result["service_type"] not in ("electric", "combined"):
            result["service_type"] = "electric"
            print(f"[regex_extract] Found TOU data - upgrading service_type to 'electric'")
        print(f"[regex_extract] tou_breakdown: {len(tou_data)} periods")
    
    # ========== DETERMINE SUCCESS ==========
    # For non-electric: success if we got utility + account + amount
    if result["service_type"] in ("water", "gas", "other"):
        result["success"] = bool(
            result["utility_name"] and 
            result["account_number"] and 
            result["total_amount"]
        )
    else:
        # For electric: success if we got most critical fields
        # Don't require kWh - it might just not be extractable via regex
        # The AI fallback will handle it
        has_utility = bool(result["utility_name"])
        has_account = bool(result["account_number"])
        has_amount = bool(result["total_amount"])
        has_dates = bool(result["billing_period_start"] and result["billing_period_end"])
        has_kwh = bool(result["total_kwh"])
        
        # If we have 4/5 critical fields, call it successful enough to skip AI
        # AI fallback will be called only if truly missing critical data
        critical_count = sum([has_utility, has_account, has_amount, has_dates, has_kwh])
        result["success"] = critical_count >= 4
        
        print(f"[regex_extract] Critical fields: utility={has_utility}, account={has_account}, "
              f"amount={has_amount}, dates={has_dates}, kwh={has_kwh} ({critical_count}/5)")
    
    print(f"[regex_extract] Extraction {'SUCCESSFUL' if result['success'] else 'INCOMPLETE'} - service_type={result['service_type']}")
    return result


def transform_to_ui_payload(result):
    """
    Transform extraction result to the nested structure the UI expects.
    The UI reads from payload.detailed_data for modal display.
    """
    # If already has detailed_data from AI parser, just ensure consistency
    if "detailed_data" in result and result["detailed_data"]:
        dd = result["detailed_data"]
        # Ensure flat fields are also in detailed_data for UI consistency
        if not dd.get("service_address") and result.get("service_address"):
            dd["service_address"] = result["service_address"]
        if not dd.get("rate_schedule") and result.get("rate_schedule"):
            dd["rate_schedule"] = result["rate_schedule"]
        if not dd.get("due_date") and result.get("due_date"):
            dd["due_date"] = result["due_date"]
        if not dd.get("amount_due") and result.get("total_amount"):
            dd["amount_due"] = result["total_amount"]
            dd["total_amount_due"] = result["total_amount"]
        if not dd.get("kwh_total") and result.get("total_kwh"):
            dd["kwh_total"] = result["total_kwh"]
        if not dd.get("billing_period_start") and result.get("billing_period_start"):
            dd["billing_period_start"] = result["billing_period_start"]
        if not dd.get("billing_period_end") and result.get("billing_period_end"):
            dd["billing_period_end"] = result["billing_period_end"]
        if not dd.get("tou_breakdown") and result.get("tou_breakdown"):
            dd["tou_breakdown"] = result["tou_breakdown"]
        return result
    
    # Create detailed_data from flat regex result
    detailed_data = {
        "service_address": result.get("service_address", ""),
        "rate_schedule": result.get("rate_schedule", ""),
        "rate": result.get("rate_schedule", ""),  # alias
        "billing_period_start": result.get("billing_period_start"),
        "billing_period_end": result.get("billing_period_end"),
        "period_start": result.get("billing_period_start"),  # alias
        "period_end": result.get("billing_period_end"),  # alias
        "due_date": result.get("due_date"),
        "kwh_total": result.get("total_kwh"),
        "amount_due": result.get("total_amount"),
        "total_amount_due": result.get("total_amount"),  # alias
        "total_cost": result.get("total_amount"),  # alias
        "tou_breakdown": result.get("tou_breakdown", []),
        "meter_number": result.get("meter_number"),
        "service_account": result.get("service_account"),
        "customer_account": result.get("customer_account"),
    }
    
    # Build the transformed payload
    transformed = {
        "success": result.get("success", False),
        "utility_name": result.get("utility_name"),
        "account_number": result.get("account_number"),
        "service_type": result.get("service_type", "electric"),
        "rate_schedule": result.get("rate_schedule"),
        "due_date": result.get("due_date"),
        "service_address": result.get("service_address"),
        "meter_number": result.get("meter_number"),
        "_extraction_method": result.get("_extraction_method", "regex"),
        "detailed_data": detailed_data,
    }
    
    return transformed


def extract_bill_data_text_based(file_id, job_queue, file_path, project_id):
    """
    Text-based bill extraction using smart fallback pipeline.
    
    Strategy (optimized for speed and cost):
    1. Extract NATIVE text only (no OCR) - fast, free
    2. Run regex extraction on native text
    3. If regex gets all critical fields → DONE (skip OCR and AI entirely!)
    4. If missing fields → try OCR (slower but still cheaper than AI)
    5. Run regex on OCR text
    6. If regex succeeds → DONE
    7. If still missing fields → call AI as last resort
    
    This approach handles scanned PDFs with clear printed text efficiently,
    ignoring handwritten annotations that don't contain critical data.
    """
    from bills import NormalizationService, TextCleaner, CacheService
    from bills.parser import TwoPassParser
    from bills.job_queue import JobState
    from bills.cache import build_metrics
    from bills_db import update_bill_file_extraction_payload, update_bill_file_status, update_file_processing_status
    import time
    
    start_time = time.time()
    print(f"[bill_extractor] Starting SMART text-based extraction for file {file_id}")

    try:
        normalizer = NormalizationService()
        cleaner = TextCleaner()
        
        # ========== STEP 1: TRY NATIVE TEXT FIRST (NO OCR) ==========
        job_queue.update_state(file_id, JobState.EXTRACTING_TEXT, "Extracting native text (fast)")
        native_result = normalizer.normalize_native_only(file_path)
        
        if not native_result.success:
            print(f"[bill_extractor] Native text extraction failed: {native_result.error}")
            # Fall through to full normalization (which may OCR)
            native_text = ""
        else:
            native_text = native_result.text
            print(f"[bill_extractor] Native text: {len(native_text)} chars via {native_result.metadata.get('method')}")
        
        # ========== STEP 2: RUN REGEX ON NATIVE TEXT ==========
        regex_result = None
        if native_text and len(native_text.strip()) > 50:
            job_queue.update_state(file_id, JobState.CLEANING, "Analyzing native text with patterns")
            clean_native = cleaner.clean(native_text)
            regex_result = regex_extract_all_fields(clean_native.cleaned_text)
            
            # Check if regex got all critical fields from native text
            if regex_result.get("success"):
                duration_ms = (time.time() - start_time) * 1000
                print(f"[bill_extractor] ✅ NATIVE TEXT SUCCESS - all critical fields extracted!")
                print(f"[bill_extractor] Skipping OCR and AI - extraction complete in {duration_ms:.0f}ms")
                
                save_bill_to_normalized_tables(file_id, project_id, regex_result)
                
                metrics = build_metrics(
                    method=native_result.metadata.get("method", "native"),
                    duration_ms=duration_ms,
                    tokens_in=0,  # No AI!
                    tokens_out=0,
                    pages=native_result.metadata.get("pages", 1),
                    char_count=len(clean_native.cleaned_text),
                    cache_hit=False,
                    pass_used="native_regex_only",
                )
                
                update_bill_file_extraction_payload(file_id, transform_to_ui_payload(regex_result))
                missing_fields = compute_missing_fields(regex_result)
                update_bill_file_status(file_id, "complete", processed=True, missing_fields=missing_fields)
                update_file_processing_status(file_id, "complete", metrics)
                return regex_result
            else:
                # Log what's missing so we know why we're falling back
                has_utility = bool(regex_result.get("utility_name"))
                has_account = bool(regex_result.get("account_number"))
                has_amount = bool(regex_result.get("total_amount"))
                has_dates = bool(regex_result.get("billing_period_start") and regex_result.get("billing_period_end"))
                has_kwh = bool(regex_result.get("total_kwh"))
                print(f"[bill_extractor] Native regex incomplete: utility={has_utility}, account={has_account}, "
                      f"amount={has_amount}, dates={has_dates}, kwh={has_kwh}")
        
        # ========== STEP 3: TRY OCR IF NATIVE TEXT INSUFFICIENT ==========
        # Only OCR if native text didn't give us what we need
        norm_result = native_result  # Start with native result
        clean_result = cleaner.clean(native_text) if native_text else None
        
        needs_ocr = (
            not regex_result or 
            not regex_result.get("success") or 
            native_result.metadata.get("method") in ("pdf_native_sparse", "image_no_native")
        )
        
        if needs_ocr:
            print(f"[bill_extractor] Native text insufficient - trying OCR...")
            job_queue.update_state(file_id, JobState.EXTRACTING_TEXT, "Running OCR (slower)")
            
            # Full normalization (includes OCR for scanned PDFs)
            norm_result = normalizer.normalize(file_path)
            
            if not norm_result.success:
                print(f"[bill_extractor] Normalization failed: {norm_result.error}")
                err = norm_result.error or "Normalization failed"
                payload = {
                    "success": False,
                    "error_code": "NORMALIZATION_FAILED",
                    "error_reason": err,
                    "error": err,
                }
                update_bill_file_extraction_payload(file_id, payload)
                update_bill_file_status(file_id, "failed", processed=True)
                update_file_processing_status(file_id, "failed", {"error": norm_result.error})
                return payload
            
            print(f"[bill_extractor] OCR extracted {len(norm_result.text)} chars via {norm_result.metadata.get('method')}")
            
            job_queue.update_state(file_id, JobState.CLEANING, "Cleaning OCR text")
            clean_result = cleaner.clean(norm_result.text)
            print(f"[bill_extractor] Cleaned text: {clean_result.stats}")
            
            # ========== STEP 4: RUN REGEX ON OCR TEXT ==========
            job_queue.update_state(file_id, JobState.CLEANING, "Extracting with patterns (OCR text)")
            regex_result = regex_extract_all_fields(clean_result.cleaned_text)
        
        # If we don't have clean_result yet (shouldn't happen, but safety)
        if not clean_result:
            clean_result = cleaner.clean(norm_result.text if norm_result else "")
        
        # ========== STEP 5: NON-ELECTRIC EARLY EXIT ==========
        if regex_result and regex_result.get("service_type") in ("water", "gas"):
            print(f"[bill_extractor] NON-ELECTRIC bill detected ({regex_result['service_type']}) - skipping AI")
            duration_ms = (time.time() - start_time) * 1000
            
            regex_result["success"] = True
            save_bill_to_normalized_tables(file_id, project_id, regex_result)
            
            metrics = build_metrics(
                method=norm_result.metadata.get("method", "unknown"),
                duration_ms=duration_ms,
                tokens_in=0,
                tokens_out=0,
                pages=norm_result.metadata.get("pages", 1),
                char_count=len(clean_result.cleaned_text),
                cache_hit=False,
                pass_used="regex_only",
            )
            
            update_bill_file_extraction_payload(file_id, transform_to_ui_payload(regex_result))
            update_bill_file_status(file_id, "complete", processed=True)
            update_file_processing_status(file_id, "complete", metrics)
            print(f"[bill_extractor] Non-electric bill processed in {duration_ms:.0f}ms (NO AI CALL)")
            return regex_result
        
        # ========== STEP 6: CHECK IF OCR REGEX GOT ALL CRITICAL FIELDS ==========
        if regex_result and regex_result.get("success"):
            print(f"[bill_extractor] ✅ OCR REGEX SUCCESS - all critical fields extracted!")
            duration_ms = (time.time() - start_time) * 1000
            
            save_bill_to_normalized_tables(file_id, project_id, regex_result)
            
            metrics = build_metrics(
                method=norm_result.metadata.get("method", "unknown"),
                duration_ms=duration_ms,
                tokens_in=0,
                tokens_out=0,
                pages=norm_result.metadata.get("pages", 1),
                char_count=len(clean_result.cleaned_text),
                cache_hit=False,
                pass_used="ocr_regex_only",
            )
            
            update_bill_file_extraction_payload(file_id, transform_to_ui_payload(regex_result))
            missing_fields = compute_missing_fields(regex_result)
            update_bill_file_status(file_id, "complete", processed=True, missing_fields=missing_fields)
            update_file_processing_status(file_id, "complete", metrics)
            print(f"[bill_extractor] Extraction complete via OCR+REGEX in {duration_ms:.0f}ms (NO AI CALL)")
            return regex_result
        
        # ========== STEP 7: CHECK CACHE (for AI results) ==========
        cache = CacheService()
        text_hash, cached = cache.check_and_get(clean_result.cleaned_text)

        if cached:
            job_queue.update_state(file_id, JobState.CACHED_HIT, "Using cached result")
            print(f"[bill_extractor] Cache hit for hash {text_hash[:12]}")
            result = cached["parse_result"]
            # Merge with regex result (regex fills any gaps)
            merged = {**regex_result, **result} if regex_result else result
            merged["_raw_text"] = clean_result.cleaned_text
            save_bill_to_normalized_tables(file_id, project_id, merged)

            update_bill_file_extraction_payload(file_id, transform_to_ui_payload(merged))
            missing_fields = compute_missing_fields(merged)
            update_bill_file_status(file_id, "complete", processed=True, missing_fields=missing_fields)
            update_file_processing_status(file_id, "complete", cached.get("metrics", {}))
            return merged

        # ========== STEP 8: CALL AI (only if native + OCR regex both failed) ==========
        print(f"[bill_extractor] ⚠️ Native + OCR regex both incomplete - calling AI as last resort")
        job_queue.update_state(file_id, JobState.PARSING_PASS_A, "Parsing with AI (Pass A)")
        parser = TwoPassParser()
        parse_result = parser.parse(clean_result.cleaned_text, clean_result.evidence_lines)

        if parse_result.pass_used == "A+B":
            job_queue.update_state(file_id, JobState.PARSING_PASS_B, "Extended parsing (Pass B)")

        duration_ms = (time.time() - start_time) * 1000
        print(f"[bill_extractor] AI parsing complete: pass={parse_result.pass_used}, success={parse_result.success}")

        metrics = build_metrics(
            method=norm_result.metadata.get("method", "unknown"),
            duration_ms=duration_ms,
            tokens_in=parse_result.tokens_in,
            tokens_out=parse_result.tokens_out,
            pages=norm_result.metadata.get("pages", 1),
            char_count=len(clean_result.cleaned_text),
            cache_hit=False,
            pass_used=parse_result.pass_used,
        )

        if parse_result.success:
            cache.save_result(file_id, text_hash, clean_result.cleaned_text, parse_result.data, metrics)
            # Merge AI result with regex result (AI takes priority, regex fills gaps)
            merged = {**(regex_result or {}), **parse_result.data}
            merged["_raw_text"] = clean_result.cleaned_text
            save_bill_to_normalized_tables(file_id, project_id, merged)

            update_bill_file_extraction_payload(file_id, transform_to_ui_payload(merged))
            missing_fields = compute_missing_fields(merged)
            update_bill_file_status(file_id, "complete", processed=True, missing_fields=missing_fields)
            update_file_processing_status(file_id, "complete", metrics)

            print(f"[bill_extractor] Extraction complete (AI+regex) for file {file_id}")
            return merged

        # "Soft failure" where parser returns a structured error
        err = parse_result.error or "Parsing failed"
        payload = {
            "success": False,
            "error_code": "PARSING_FAILED",
            "error_reason": err,
            "error": err,  # back-compat
        }
        update_bill_file_extraction_payload(file_id, payload)
        update_bill_file_status(file_id, "failed", processed=True)
        update_file_processing_status(file_id, "failed", metrics)
        print(f"[bill_extractor] Extraction failed: {parse_result.error}")
        return payload

    except Exception as e:
        # Ensure we never leave a file stuck in 'processing' due to an unhandled exception
        err = str(e) or "Unknown error"
        print(f"[bill_extractor] Unhandled exception in text extraction: {err}")
        payload = {
            "success": False,
            "error_code": "EXTRACTION_EXCEPTION",
            "error_reason": err,
            "error": err,  # back-compat
        }
        try:
            update_bill_file_extraction_payload(file_id, payload)
            update_bill_file_status(file_id, "failed", processed=True)
            update_file_processing_status(file_id, "failed", {"error": err})
        except Exception:
            # Avoid masking the original exception if DB writes fail
            pass
        raise
