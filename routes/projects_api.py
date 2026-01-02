from __future__ import annotations

from datetime import datetime
import uuid

from flask import Blueprint, current_app, jsonify, request

from stores.project_store import (
    deleted_projects,
    save_data,
    save_deleted_projects,
    stored_data,
    users_db,
)

projects_bp = Blueprint("projects", __name__)


@projects_bp.route("/api/data", methods=["GET", "POST"])
def handle_data():
    # Get user context from headers (default to admin user for backward compatibility)
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    if request.method == "POST":
        data = request.get_json()
        if data:
            provided_project_id = data.get("project_id")
            customer = data.get("siteData", {}).get("customer", "Unknown")
            timestamp = datetime.now().isoformat()

            # Log client action and version headers for debugging
            client_action = request.headers.get("X-Client-Action", "unknown")
            client_version = request.headers.get("X-Client-Version", "unknown")
            print(
                f"[projects] CREATE request: X-Client-Action={client_action}, X-Client-Version={client_version}, projectId={provided_project_id or 'NEW'}"
            )

            CURRENT_BUILD_ID = "2025-12-17-v1"

            if client_version != "unknown" and client_version != CURRENT_BUILD_ID:
                print(f"[projects] BUILD MISMATCH: client={client_version}, server={CURRENT_BUILD_ID}")
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Client out of date, please refresh the page.",
                            "code": "BUILD_MISMATCH",
                            "client_version": client_version,
                            "server_version": CURRENT_BUILD_ID,
                        }
                    ),
                    409,
                )

            allowed_create_actions = ["create_new", "duplicate", "import"]
            if client_action not in allowed_create_actions:
                print(
                    f"[projects] REJECTED: Invalid X-Client-Action={client_action} for POST. Must be one of {allowed_create_actions}"
                )
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Cannot create project. Please refresh the page.",
                            "code": "CREATE_ACTION_INVALID",
                            "received_action": client_action,
                            "allowed_actions": allowed_create_actions,
                        }
                    ),
                    409,
                )

            idempotency_key = request.headers.get("Idempotency-Key")
            if not idempotency_key:
                print("[projects] REJECTED: Missing Idempotency-Key header for POST")
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": "Missing Idempotency-Key header. Please refresh the page.",
                            "code": "IDEMPOTENCY_KEY_MISSING",
                        }
                    ),
                    409,
                )

            # Simple in-memory idempotency cache (attach to current_app)
            if not hasattr(current_app, "_idempotency_cache"):
                current_app._idempotency_cache = {}  # type: ignore[attr-defined]

            cache = current_app._idempotency_cache  # type: ignore[attr-defined]
            if len(cache) > 1000:
                sorted_keys = sorted(cache.keys(), key=lambda k: cache[k]["timestamp"])
                for key in sorted_keys[:500]:
                    del cache[key]

            if idempotency_key in cache:
                cached = cache[idempotency_key]
                print(f"[projects] IDEMPOTENCY HIT: Key {idempotency_key} already used for project {cached['project_id']}")
                return (
                    jsonify(
                        {
                            "status": "success",
                            "project_id": cached["project_id"],
                            "message": "Project already created (idempotent)",
                            "idempotent": True,
                        }
                    ),
                    200,
                )

            existing_metadata = {}
            is_update = False

            if provided_project_id:
                if user_id in stored_data and provided_project_id in stored_data[user_id]:
                    existing_metadata = stored_data[user_id][provided_project_id].get("_metadata", {})
                    is_update = True
                else:
                    for uid, projects in stored_data.items():
                        if provided_project_id in projects:
                            existing_metadata = projects[provided_project_id].get("_metadata", {})
                            is_update = True
                            break

                if not is_update:
                    print(f"[api/data] REJECTED unknown project_id: {provided_project_id}")
                    return (
                        jsonify(
                            {
                                "status": "error",
                                "message": "Project not found - ID may be stale",
                                "code": "PROJECT_NOT_FOUND",
                                "provided_id": provided_project_id,
                            }
                        ),
                        404,
                    )

            project_id = provided_project_id or str(uuid.uuid4())
            project_name = data.get("name") or existing_metadata.get("name") or customer
            created_at = existing_metadata.get("created_at") if is_update else timestamp

            data["_metadata"] = {
                "project_id": project_id,
                "owner_id": user_id,
                "customer": customer,
                "name": project_name,
                "created_at": created_at,
                "saved_at": timestamp,
                "updated_at": timestamp,
            }

            if user_id not in stored_data:
                stored_data[user_id] = {}

            stored_data[user_id][project_id] = data
            save_data(stored_data)

            cache[idempotency_key] = {"project_id": project_id, "timestamp": timestamp, "user_id": user_id}
            print(f"[projects] IDEMPOTENCY STORED: Key {idempotency_key} -> project {project_id}")

            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Project saved",
                        "project_id": project_id,
                        "owner_id": user_id,
                        "name": project_name,
                    }
                ),
                200,
            )

        return jsonify({"status": "error", "message": "No data provided"}), 400

    # GET request - retrieve specific project
    project_id = request.args.get("project")
    if not project_id:
        return jsonify({"error": "project_id required"}), 400

    if user_role == "admin":
        for uid, projects in stored_data.items():
            if project_id in projects:
                return jsonify(projects[project_id])
    else:
        if user_id in stored_data and project_id in stored_data[user_id]:
            return jsonify(stored_data[user_id][project_id])

    return jsonify({"error": "Project not found"}), 404


@projects_bp.get("/api/projects")
def list_projects():
    """List projects with optional pagination."""
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    try:
        limit = min(int(request.args.get("limit", 25)), 100)
        offset = max(int(request.args.get("offset", 0)), 0)
    except ValueError:
        limit, offset = 25, 0

    projects = []
    if user_role == "admin":
        for uid, user_projects in stored_data.items():
            for pid, data in user_projects.items():
                owner_name = users_db.get(uid, {}).get("display_name", uid)
                metadata = data.get("_metadata", {})
                evap_count = sum(int(e.get("room-count", 0)) for e in data.get("entries", []) if e.get("section") == "evap")
                cond_count = sum(int(e.get("room-count", 0)) for e in data.get("entries", []) if e.get("section") == "cond")
                projects.append(
                    {
                        "id": pid,
                        "owner_id": uid,
                        "owner_name": owner_name,
                        "customer": metadata.get("customer") or data.get("siteData", {}).get("customer", "Unknown"),
                        "name": metadata.get("name")
                        or metadata.get("customer")
                        or data.get("siteData", {}).get("customer", "Unknown"),
                        "created_at": metadata.get("created_at"),
                        "updated_at": metadata.get("updated_at"),
                        "saved_at": metadata.get("saved_at"),
                        "evap_count": evap_count,
                        "cond_count": cond_count,
                        "city": data.get("siteData", {}).get("city"),
                        "state": data.get("siteData", {}).get("state"),
                    }
                )
    else:
        if user_id in stored_data:
            for pid, data in stored_data[user_id].items():
                metadata = data.get("_metadata", {})
                evap_count = sum(int(e.get("room-count", 0)) for e in data.get("entries", []) if e.get("section") == "evap")
                cond_count = sum(int(e.get("room-count", 0)) for e in data.get("entries", []) if e.get("section") == "cond")
                projects.append(
                    {
                        "id": pid,
                        "owner_id": user_id,
                        "customer": metadata.get("customer") or data.get("siteData", {}).get("customer", "Unknown"),
                        "name": metadata.get("name")
                        or metadata.get("customer")
                        or data.get("siteData", {}).get("customer", "Unknown"),
                        "created_at": metadata.get("created_at"),
                        "updated_at": metadata.get("updated_at"),
                        "saved_at": metadata.get("saved_at"),
                        "evap_count": evap_count,
                        "cond_count": cond_count,
                        "city": data.get("siteData", {}).get("city"),
                        "state": data.get("siteData", {}).get("state"),
                    }
                )

    projects.sort(key=lambda x: x.get("saved_at") or "", reverse=True)
    total = len(projects)
    projects = projects[offset : offset + limit]
    return jsonify({"items": projects, "total": total, "limit": limit, "offset": offset, "hasMore": offset + limit < total})


@projects_bp.delete("/api/data/<project_id>")
def delete_project(project_id: str):
    """Move project to deleted archive instead of permanent deletion. Idempotent."""
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    already_deleted = False
    if user_role == "admin":
        for uid, user_projects in deleted_projects.items():
            if project_id in user_projects:
                already_deleted = True
                break
    else:
        if user_id in deleted_projects and project_id in deleted_projects[user_id]:
            already_deleted = True

    if already_deleted:
        return jsonify({"status": "success", "message": "Project already deleted", "project_id": project_id, "idempotent": True}), 200

    archived = False
    owner_id = None
    project_data = None

    if user_role == "admin":
        for uid, user_projects in stored_data.items():
            if project_id in user_projects:
                project_data = user_projects[project_id]
                del user_projects[project_id]
                owner_id = uid
                archived = True
                break
    else:
        if user_id in stored_data and project_id in stored_data[user_id]:
            project_data = stored_data[user_id][project_id]
            del stored_data[user_id][project_id]
            owner_id = user_id
            archived = True

    if archived and project_data:
        project_data["_deleted_at"] = datetime.now().isoformat() + "Z"
        if owner_id not in deleted_projects:
            deleted_projects[owner_id] = {}
        deleted_projects[owner_id][project_id] = project_data
        save_data(stored_data)
        save_deleted_projects(deleted_projects)
        return jsonify({"status": "success", "message": "Project moved to Recently Deleted", "project_id": project_id, "owner_id": owner_id}), 200

    return jsonify({"status": "success", "message": "Project not found (already deleted)", "project_id": project_id, "idempotent": True}), 200


@projects_bp.post("/api/projects/create")
def create_project():
    user_id = request.headers.get("X-User-Id", "default")
    data = request.get_json() or {}
    project_name = (data.get("name") or "").strip()

    if not project_name:
        return jsonify({"status": "error", "message": "Project name is required"}), 400

    project_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()

    new_project = {
        "project_id": project_id,
        "siteData": {
            "customer": "",
            "street": "",
            "city": "",
            "state": "",
            "zip": "",
            "contact": "",
            "phone": "",
            "email": "",
            "utility": "",
            "dateOfWalk": "",
            "technician": "",
        },
        "entries": [],
        "photos": [],
        "_metadata": {
            "project_id": project_id,
            "owner_id": user_id,
            "customer": "",
            "name": project_name,
            "created_at": timestamp,
            "saved_at": timestamp,
            "updated_at": timestamp,
        },
    }

    if user_id not in stored_data:
        stored_data[user_id] = {}
    stored_data[user_id][project_id] = new_project
    save_data(stored_data)

    return (
        jsonify(
            {
                "status": "success",
                "project_id": project_id,
                "id": project_id,
                "name": project_name,
                "customer": "",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        ),
        201,
    )


@projects_bp.post("/api/import-csv")
def import_csv():
    import csv
    from io import StringIO

    user_id = request.headers.get("X-User-Id", "default")

    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"success": False, "error": "No file selected"}), 400

    try:
        content = file.read().decode("utf-8-sig")
        lines = content.strip().split("\n")
        if len(lines) < 2:
            return jsonify({"success": False, "error": "CSV file is empty or invalid"}), 400

        def parse_csv_row(line):
            reader = csv.reader(StringIO(line))
            return next(reader, [])

        site_headers = parse_csv_row(lines[0])
        site_values = parse_csv_row(lines[1]) if len(lines) > 1 else []

        if len(site_headers) < 4 or site_headers[0].strip() != "Customer":
            return (
                jsonify({"success": False, "error": "Unrecognized CSV format. Please use a CSV exported from this app."}),
                400,
            )

        raw_date = site_values[5].strip() if len(site_values) > 5 else ""
        site_data = {
            "customer": site_values[0].strip() if len(site_values) > 0 else "",
            "street": site_values[1].strip() if len(site_values) > 1 else "",
            "city": "",
            "state": "",
            "zip": "",
            "contact": site_values[3].strip() if len(site_values) > 3 else "",
            "phone": site_values[4].strip() if len(site_values) > 4 else "",
            "date": raw_date,
            "visitDate": raw_date,
            "dateOfWalk": raw_date,
            "utility": site_values[6].strip() if len(site_values) > 6 else "",
        }

        city_state_zip = site_values[2].strip() if len(site_values) > 2 else ""
        if city_state_zip:
            parts = [p.strip() for p in city_state_zip.split(",")]
            if len(parts) >= 1:
                site_data["city"] = parts[0]
            if len(parts) >= 2:
                state_zip = parts[1].strip().split()
                if len(state_zip) >= 1:
                    site_data["state"] = state_zip[0]
                if len(state_zip) >= 2:
                    site_data["zip"] = state_zip[1]
            if len(parts) >= 3:
                site_data["zip"] = parts[2]

        header_to_field = {
            "Zone": "room-zone",
            "Sheet": "sheetNumber",
            "Name": "room-name",
            "Evap QTY": "room-count",
            "Cond QTY": "room-count",
            "Motor QTY": "room-fanMotorsPerUnit",
            "Volts": "room-voltage",
            "Amps": "room-amps",
            "Phase": "room-phase",
            "Motor HP": "room-hp",
            "Split": "room-split",
            "Operation Time Factor": "room-runTime",
            "Mfg": "room-mfg",
            "Motor Mounting": "room-motorMounting",
            "Frame": "room-frame",
            "RPM": "room-rpm",
            "Rotation": "room-rotation",
            "Shaft": "room-shaftSize",
            "Shaft Adptr QTY": "room-shaftAdapterQty",
            "Shaft Adptr Type": "room-shaftAdapterType",
            "Blade Specs": "room-bladeSpec",
            "QTY Blades Needed": "room-bladesNeeded",
            "Current Temp": "room-currentTemp",
            "Set Point": "room-setPoint",
        }

        entries = []
        current_section = None
        headers = []
        for i in range(2, len(lines)):
            line = lines[i].strip()
            if not line:
                continue

            row = parse_csv_row(line)
            if not row:
                continue

            first_cell = row[0].strip()
            if first_cell == "Evaporators":
                current_section = "evap"
                headers = [h.strip() for h in row[1:]]
                continue
            if first_cell == "Condensers":
                current_section = "cond"
                headers = [h.strip() for h in row[1:]]
                continue

            if current_section and headers and first_cell == "":
                entry = {"section": current_section, "mode": "detailed"}
                values = row[1:]
                for j, header in enumerate(headers):
                    if j < len(values):
                        val = values[j].strip()
                        if val:
                            field_name = header_to_field.get(header, "room-" + header.lower().replace(" ", ""))
                            entry[field_name] = val

                if entry.get("room-shaftAdapterQty") and int(entry.get("room-shaftAdapterQty", 0)) > 0:
                    entry["room-shaftAdapters"] = "Yes"
                entry["id"] = str(uuid.uuid4())
                entries.append(entry)

        customer_name = site_data["customer"].strip() if site_data.get("customer") else ""
        project_name = customer_name or "Imported Project"
        project_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()

        new_project = {
            "project_id": project_id,
            "siteData": site_data,
            "entries": entries,
            "photos": [],
            "_metadata": {
                "project_id": project_id,
                "owner_id": user_id,
                "customer": customer_name,
                "name": project_name,
                "created_at": timestamp,
                "saved_at": timestamp,
                "updated_at": timestamp,
                "imported": True,
            },
        }

        if user_id not in stored_data:
            stored_data[user_id] = {}
        stored_data[user_id][project_id] = new_project
        save_data(stored_data)

        evap_count = len([e for e in entries if e.get("section") == "evap"])
        cond_count = len([e for e in entries if e.get("section") == "cond"])

        return jsonify({"success": True, "projectId": project_id, "projectName": project_name, "evapCount": evap_count, "condCount": cond_count}), 201

    except Exception as e:
        return jsonify({"success": False, "error": f"Failed to parse CSV: {str(e)}"}), 400


@projects_bp.route("/api/projects/<project_id>", methods=["GET", "PUT", "DELETE"])
def project_by_id(project_id: str):
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    if request.method == "GET":
        project_data = None
        if user_role == "admin":
            for uid, projects in stored_data.items():
                if project_id in projects:
                    project_data = projects[project_id]
                    break
        else:
            if user_id in stored_data and project_id in stored_data[user_id]:
                project_data = stored_data[user_id][project_id]

        if not project_data:
            return jsonify({"status": "error", "message": "Project not found or unauthorized"}), 404
        return jsonify(project_data), 200

    if request.method == "PUT":
        client_action = request.headers.get("X-Client-Action", "unknown")
        client_version = request.headers.get("X-Client-Version", "unknown")
        timestamp = datetime.now().isoformat()
        print(f"[projects] UPDATE via X-Client-Action={client_action}, X-Client-Version={client_version}, projectId={project_id}, timestamp={timestamp}")

        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        existing_data = None
        existing_owner = None

        if user_role == "admin":
            for uid, projects in stored_data.items():
                if project_id in projects:
                    existing_data = projects[project_id]
                    existing_owner = uid
                    break
        else:
            if user_id in stored_data and project_id in stored_data[user_id]:
                existing_data = stored_data[user_id][project_id]
                existing_owner = user_id

        if not existing_data:
            return jsonify({"status": "error", "message": "Project not found or unauthorized"}), 404

        timestamp = datetime.now().isoformat()
        existing_metadata = existing_data.get("_metadata", {})

        merged_data = {}
        if "siteData" in data:
            merged_data["siteData"] = {**existing_data.get("siteData", {}), **data["siteData"]}
        else:
            merged_data["siteData"] = existing_data.get("siteData", {})

        merged_data["entries"] = data.get("entries", existing_data.get("entries", []))
        merged_data["photos"] = data.get("photos", existing_data.get("photos", []))

        for key in data:
            if key not in ("_metadata", "project_id", "siteData", "entries", "photos"):
                merged_data[key] = data[key]
        for key in existing_data:
            if key not in merged_data and key not in ("_metadata", "project_id"):
                merged_data[key] = existing_data[key]

        customer = merged_data.get("siteData", {}).get("customer", existing_metadata.get("customer", ""))
        project_name = data.get("name") or existing_metadata.get("name") or customer or "Untitled"

        merged_data["_metadata"] = {
            "project_id": project_id,
            "owner_id": existing_owner,
            "customer": customer,
            "name": project_name,
            "created_at": existing_metadata.get("created_at") or timestamp,
            "saved_at": timestamp,
            "updated_at": timestamp,
        }
        merged_data["project_id"] = project_id

        stored_data[existing_owner][project_id] = merged_data
        save_data(stored_data)

        return jsonify({"status": "success", "message": "Project updated", "id": project_id, "name": project_name, "customer": customer, "created_at": merged_data["_metadata"]["created_at"], "updated_at": timestamp}), 200

    # DELETE
    archived = False
    owner_id = None
    project_data = None

    if user_role == "admin":
        for uid, projects in stored_data.items():
            if project_id in projects:
                project_data = projects[project_id]
                del projects[project_id]
                owner_id = uid
                archived = True
                break
    else:
        if user_id in stored_data and project_id in stored_data[user_id]:
            project_data = stored_data[user_id][project_id]
            del stored_data[user_id][project_id]
            owner_id = user_id
            archived = True

    if archived and project_data:
        project_data["_deleted_at"] = datetime.now().isoformat() + "Z"
        if owner_id not in deleted_projects:
            deleted_projects[owner_id] = {}
        deleted_projects[owner_id][project_id] = project_data
        save_data(stored_data)
        save_deleted_projects(deleted_projects)
        return jsonify({"status": "success", "message": "Project moved to Recently Deleted", "project_id": project_id, "owner_id": owner_id}), 200

    return jsonify({"status": "error", "message": "Project not found or unauthorized"}), 404


@projects_bp.route("/api/projects/duplicate/<project_id>", methods=["POST"])
@projects_bp.route("/api/projects/<project_id>/duplicate", methods=["POST"])
def duplicate_project(project_id: str):
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    original_data = None
    if user_role == "admin":
        for uid, projects in stored_data.items():
            if project_id in projects:
                original_data = projects[project_id]
                break
    else:
        if user_id in stored_data and project_id in stored_data[user_id]:
            original_data = stored_data[user_id][project_id]

    if not original_data:
        return jsonify({"status": "error", "message": "Project not found or unauthorized"}), 404

    import copy

    new_data = copy.deepcopy(original_data)
    new_project_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()

    original_metadata = original_data.get("_metadata", {})
    original_name = original_metadata.get("name") or original_metadata.get("customer") or "Untitled"
    new_name = f"{original_name} (Copy)"

    new_data["_metadata"] = {
        "project_id": new_project_id,
        "owner_id": user_id,
        "customer": original_metadata.get("customer", "Unknown"),
        "name": new_name,
        "created_at": timestamp,
        "saved_at": timestamp,
        "updated_at": timestamp,
    }
    new_data["project_id"] = new_project_id

    if user_id not in stored_data:
        stored_data[user_id] = {}
    stored_data[user_id][new_project_id] = new_data
    save_data(stored_data)

    bills_cloned = {"files": 0, "accounts": 0, "meters": 0, "bills": 0}
    try:
        from bills_db import clone_bills_for_project

        bills_cloned = clone_bills_for_project(project_id, new_project_id)
    except Exception as e:
        print(f"[duplicate_project] Warning: Failed to clone bills: {e}")

    return jsonify({"status": "success", "message": "Project duplicated", "project_id": new_project_id, "name": new_name, "owner_id": user_id, "bills_cloned": bills_cloned}), 200


