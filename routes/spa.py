from __future__ import annotations
import os

from flask import Blueprint, jsonify, send_file, send_from_directory, current_app

spa_bp = Blueprint("spa", __name__)


@spa_bp.route("/")
def index():
    index_path = os.path.join(current_app.root_path, "index.html")
    response = send_file(index_path)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@spa_bp.route("/health")
def health_check():
    """
    Health check endpoint for deployment monitoring.

    Returns immediately without database dependency to ensure
    workers stay alive during startup.
    """
    return jsonify({"status": "ok", "service": "sitewalk"}), 200


@spa_bp.route("/static/<path:filename>")
def serve_static(filename: str):
    return send_from_directory("static", filename)


@spa_bp.route("/<path:path>")
def catch_all(path: str):
    """Catch-all route for SPA deep-links - serves index.html for non-API and non-static paths."""
    if path.startswith("api/") or path.startswith("static/"):
        return jsonify({"error": "Not found"}), 404

    index_path = os.path.join(current_app.root_path, "index.html")
    response = send_file(index_path)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


