from __future__ import annotations

import logging
from flask import Blueprint, current_app, jsonify, request

from services.dropbox_service import get_root_path, upload_csv

dropbox_api_bp = Blueprint("dropbox_api", __name__)
logger = logging.getLogger(__name__)


@dropbox_api_bp.post("/api/upload_csv_to_dropbox")
def upload_csv_to_dropbox():
    payload = request.get_json() or {}
    filename = (payload.get("filename") or "").strip()
    csv_text = payload.get("csv") or ""

    if not filename or not csv_text:
        return jsonify({"ok": False, "error": "Missing filename or csv in request body"}), 400

    app_cfg = current_app.config.get("APP_CFG", {}) or {}
    root_path = get_root_path(app_cfg)

    try:
        success, result = upload_csv(filename, csv_text, root_path=root_path)
        if success:
            return jsonify({"ok": True, "status": "uploaded", "path": result}), 200
        return jsonify({"ok": False, "error": result}), 500
    except Exception as e:
        logger.exception("Unexpected exception during Dropbox upload")
        return jsonify({"ok": False, "error": str(e)}), 500


