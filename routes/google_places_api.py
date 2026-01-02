from __future__ import annotations

import logging
import requests
from flask import Blueprint, jsonify, request

from services.google_places import GooglePlacesError
from services import google_places as gp

google_places_bp = Blueprint("google_places", __name__)
logger = logging.getLogger(__name__)


@google_places_bp.get("/api/place-details")
def place_details():
    place_id = request.args.get("place_id", "").strip()
    if not place_id:
        return jsonify({"error": "place_id required"}), 400
    try:
        return jsonify(gp.place_details(place_id)), 200
    except requests.Timeout:
        return jsonify({"error": "Request timeout"}), 504
    except GooglePlacesError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        logger.exception("Place details error")
        return jsonify({"error": f"Error: {str(e)}"}), 500


@google_places_bp.get("/api/place-autocomplete")
def place_autocomplete():
    input_str = request.args.get("input", "").strip()
    if len(input_str) < 2:
        return jsonify([]), 200
    try:
        lat = request.args.get("lat")
        lng = request.args.get("lng")
        return jsonify(gp.autocomplete(input_str, lat=lat, lng=lng)), 200
    except requests.Timeout:
        return jsonify([]), 200
    except GooglePlacesError:
        return jsonify({"error": "Google Places API key not configured."}), 503
    except Exception:
        logger.exception("Place autocomplete error")
        return jsonify([]), 200


@google_places_bp.get("/api/nearby-businesses")
def nearby_businesses():
    lat = request.args.get("lat")
    lng = request.args.get("lng")
    if not lat or not lng:
        return jsonify({"error": "Missing required parameters: lat and lng"}), 400

    try:
        return jsonify(gp.nearby_businesses(lat, lng)), 200
    except requests.Timeout:
        return jsonify({"error": "Google Places API request timed out. Please try again."}), 504
    except GooglePlacesError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        logger.exception("Error fetching nearby businesses")
        return jsonify({"error": f"Error fetching nearby businesses: {str(e)}"}), 500


@google_places_bp.get("/api/geocode")
def geocode_address():
    address = request.args.get("address")
    if not address:
        return jsonify({"error": "Missing address parameter"}), 400

    try:
        return jsonify(gp.geocode(address)), 200
    except requests.Timeout:
        return jsonify({"error": "Request timeout"}), 504
    except GooglePlacesError as e:
        msg = str(e)
        if msg == "Address not found":
            return jsonify({"error": msg}), 404
        return jsonify({"error": msg}), 503
    except Exception as e:
        logger.exception("Geocode error")
        return jsonify({"error": str(e)}), 500


