from __future__ import annotations

import os
from flask import Blueprint, current_app, jsonify

config_api_bp = Blueprint("config_api", __name__)


@config_api_bp.get("/api/config")
def get_config():
    """
    Return application configuration including feature flags.
    Used by the frontend to check feature availability.
    """
    bills_enabled = bool(current_app.config.get("BILLS_FEATURE_ENABLED", True))
    # Keep backward-compatible behavior: if key exists, feature is enabled.
    places_enabled = bool(os.getenv("GOOGLE_PLACES_API_KEY"))

    # If YAML/env explicitly disabled places, allow that to win.
    try:
        app_cfg = current_app.config.get("APP_CFG", {}) or {}
        places_cfg = app_cfg.get("features", {}).get("google_places_enabled", None)
        if places_cfg is not None:
            places_enabled = bool(places_cfg) and places_enabled
    except Exception:
        pass

    return jsonify({"billsEnabled": bills_enabled, "googlePlacesEnabled": places_enabled})


