from __future__ import annotations

from datetime import datetime

from flask import Blueprint, jsonify, request

from stores.project_store import (
    load_autosave,
    save_autosave,
    load_project_autosaves,
    save_project_autosaves,
)

autosave_bp = Blueprint("autosave", __name__)


@autosave_bp.route("/api/autosave", methods=["GET", "POST", "DELETE"])
def handle_autosave():
    """Per-user autosave data."""
    user_id = request.headers.get("X-User-Id", "default")
    autosave_data = load_autosave()

    if request.method == "GET":
        user_autosave = autosave_data.get(user_id)
        if user_autosave:
            return (
                jsonify(
                    {
                        "status": "success",
                        "exists": True,
                        "data": user_autosave.get("data"),
                        "saved_at": user_autosave.get("saved_at"),
                    }
                ),
                200,
            )
        return jsonify({"status": "success", "exists": False}), 200

    if request.method == "POST":
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        timestamp = datetime.now().isoformat()
        autosave_data[user_id] = {"data": data, "saved_at": timestamp}
        save_autosave(autosave_data)
        return jsonify({"status": "success", "message": "Autosave saved", "saved_at": timestamp}), 200

    # DELETE
    if user_id in autosave_data:
        del autosave_data[user_id]
        save_autosave(autosave_data)
    return jsonify({"status": "success", "message": "Autosave cleared"}), 200


@autosave_bp.route("/api/projects/<project_id>/autosave", methods=["GET", "POST", "DELETE"])
def handle_project_autosave(project_id: str):
    """Project-specific autosave endpoint."""
    user_id = request.headers.get("X-User-Id", "default")
    autosave_data = load_project_autosaves()

    if request.method == "GET":
        project_autosave = autosave_data.get(user_id, {}).get(project_id)
        if project_autosave:
            return (
                jsonify(
                    {
                        "status": "success",
                        "exists": True,
                        "project_id": project_id,
                        "project_data": project_autosave.get("project_data"),
                        "autosave_timestamp": project_autosave.get("autosave_timestamp"),
                        "server_saved_at": project_autosave.get("server_saved_at"),
                    }
                ),
                200,
            )
        return jsonify({"status": "success", "exists": False, "project_id": project_id}), 200

    if request.method == "POST":
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        project_data = data.get("project_data")
        autosave_timestamp = data.get("autosave_timestamp")
        if not project_data:
            return jsonify({"status": "error", "message": "project_data is required"}), 400

        server_timestamp = datetime.now().isoformat()
        if user_id not in autosave_data:
            autosave_data[user_id] = {}

        autosave_data[user_id][project_id] = {
            "project_data": project_data,
            "autosave_timestamp": autosave_timestamp or server_timestamp,
            "server_saved_at": server_timestamp,
        }
        save_project_autosaves(autosave_data)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Project autosave saved",
                    "project_id": project_id,
                    "autosave_timestamp": autosave_timestamp or server_timestamp,
                    "server_saved_at": server_timestamp,
                }
            ),
            200,
        )

    # DELETE
    if user_id in autosave_data and project_id in autosave_data[user_id]:
        del autosave_data[user_id][project_id]
        save_project_autosaves(autosave_data)

    return jsonify({"status": "success", "message": "Project autosave cleared", "project_id": project_id}), 200


@autosave_bp.get("/api/autosaves")
def list_project_autosaves():
    """List all autosaves for current user."""
    user_id = request.headers.get("X-User-Id", "default")
    autosave_data = load_project_autosaves()
    user_autosaves = autosave_data.get(user_id, {})

    result = []
    for project_id, autosave in user_autosaves.items():
        project_data = autosave.get("project_data", {}) or {}
        site_data = project_data.get("siteData", {}) or {}
        metadata = project_data.get("_metadata", {}) or {}

        result.append(
            {
                "project_id": project_id,
                "autosave_timestamp": autosave.get("autosave_timestamp"),
                "server_saved_at": autosave.get("server_saved_at"),
                "project_name": metadata.get("name") or project_data.get("currentProjectName"),
                "customer": site_data.get("customer") or metadata.get("customer"),
                "evaporator_count": len([e for e in project_data.get("entries", []) if e.get("type") == "evaporator"]),
                "condenser_count": len([e for e in project_data.get("entries", []) if e.get("type") == "condenser"]),
                "photo_count": len(project_data.get("photos", [])),
            }
        )

    result.sort(key=lambda x: x.get("autosave_timestamp", "") or "", reverse=True)
    return jsonify({"status": "success", "autosaves": result, "count": len(result)}), 200


