from __future__ import annotations

from typing import Any, Dict, List, Optional

import os
import requests


class GooglePlacesError(RuntimeError):
    pass


def get_api_key() -> Optional[str]:
    # Prefer environment variables (which can be sourced from .env in dev).
    return os.getenv("GOOGLE_PLACES_API_KEY") or None


def place_details(place_id: str, *, timeout_s: float = 5.0) -> Dict[str, Any]:
    api_key = get_api_key()
    if not api_key:
        raise GooglePlacesError(
            "Google Places API key not configured. Set GOOGLE_PLACES_API_KEY (env or .env)."
        )

    url = "https://maps.googleapis.com/maps/api/place/details/json"
    resp = requests.get(url, params={"place_id": place_id, "key": api_key}, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK":
        raise GooglePlacesError(f"Place not found: {data.get('status')}")

    result = data.get("result", {}) or {}
    address_components = result.get("address_components", []) or []

    parsed = {"name": result.get("name", ""), "street": "", "city": "", "state": "", "zip": ""}
    for comp in address_components:
        types = comp.get("types", []) or []
        value = comp.get("long_name", "") or ""
        if "street_number" in types or "route" in types:
            parsed["street"] += value + " "
        elif "locality" in types:
            parsed["city"] = value
        elif "administrative_area_level_1" in types:
            parsed["state"] = comp.get("short_name", "") or ""
        elif "postal_code" in types:
            parsed["zip"] = value

    parsed["street"] = parsed["street"].strip()
    return parsed


def autocomplete(input_str: str, lat: Optional[str] = None, lng: Optional[str] = None, *, timeout_s: float = 5.0) -> List[Dict[str, str]]:
    api_key = get_api_key()
    if not api_key:
        raise GooglePlacesError(
            "Google Places API key not configured. Set GOOGLE_PLACES_API_KEY (env or .env)."
        )

    url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    params: Dict[str, str] = {"input": input_str, "key": api_key, "components": "country:us"}

    if lat and lng:
        params["location"] = f"{lat},{lng}"
        params["radius"] = "10000"

    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") not in ["OK", "ZERO_RESULTS"]:
        return []

    results: List[Dict[str, str]] = []
    for pred in (data.get("predictions", []) or [])[:8]:
        structured = pred.get("structured_formatting", {}) or {}
        main_text = structured.get("main_text", "") or ""
        secondary_text = structured.get("secondary_text", "") or ""

        if not main_text:
            description = pred.get("description", "") or ""
            parts = description.split(",", 1)
            main_text = parts[0].strip() if parts else description
            secondary_text = parts[1].strip() if len(parts) > 1 else ""

        results.append(
            {
                "main": main_text,
                "secondary": secondary_text,
                "value": pred.get("description", "") or "",
                "place_id": pred.get("place_id", "") or "",
            }
        )

    return results


def nearby_businesses(lat: str, lng: str, *, timeout_s: float = 5.0) -> List[Dict[str, str]]:
    api_key = get_api_key()
    if not api_key:
        raise GooglePlacesError(
            "Google Places API key not configured. Set GOOGLE_PLACES_API_KEY (env or .env)."
        )

    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {"location": f"{lat},{lng}", "radius": 250, "key": api_key, "type": "establishment"}
    resp = requests.get(url, params=params, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()

    api_status = data.get("status")
    if api_status == "ZERO_RESULTS":
        return []
    if api_status != "OK":
        error_message = data.get("error_message", "") or ""
        user_error = f"Google Places API error: {api_status}"
        if api_status == "REQUEST_DENIED":
            user_error = "Google Places API access denied. Please check API key configuration."
        elif api_status == "INVALID_REQUEST":
            user_error = "Invalid request to Google Places API. Please try a different location."
        elif api_status == "OVER_QUERY_LIMIT":
            user_error = "Google Places API quota exceeded. Please try again later."
        raise GooglePlacesError(user_error + (f" ({error_message})" if error_message else ""))

    businesses: List[Dict[str, str]] = []
    for place in (data.get("results", []) or [])[:10]:
        place_id = place.get("place_id")
        if not place_id:
            continue

        try:
            details = place_details(place_id, timeout_s=timeout_s)
            # For nearby, we want name + parsed address components; keep same output shape as before.
            businesses.append(
                {
                    "name": place.get("name", "Unknown") or "Unknown",
                    "address": details.get("street", "") or "",
                    "city": details.get("city", "") or "",
                    "state": details.get("state", "") or "CA",
                    "zip": details.get("zip", "") or "",
                }
            )
        except Exception:
            # Skip items we fail to parse; better fewer results than partial junk.
            continue

    # Filter out entries without a street address (matches previous behavior).
    businesses = [b for b in businesses if b.get("address")]
    return businesses


def geocode(address: str, *, timeout_s: float = 5.0) -> Dict[str, Any]:
    api_key = get_api_key()
    if not api_key:
        raise GooglePlacesError(
            "Google Places API key not configured. Set GOOGLE_PLACES_API_KEY (env or .env)."
        )

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    resp = requests.get(url, params={"address": address, "key": api_key}, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()

    status = data.get("status")
    if status == "ZERO_RESULTS":
        raise GooglePlacesError("Address not found")
    if status != "OK":
        raise GooglePlacesError(f"Geocoding failed: {status}")

    results = data.get("results", []) or []
    if not results:
        raise GooglePlacesError("No results found")

    location = (results[0].get("geometry", {}) or {}).get("location", {}) or {}
    lat = location.get("lat")
    lng = location.get("lng")
    if lat is None or lng is None:
        raise GooglePlacesError("Could not extract coordinates")

    return {"lat": lat, "lng": lng, "formatted_address": results[0].get("formatted_address", "") or ""}


