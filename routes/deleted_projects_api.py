from __future__ import annotations

from datetime import datetime

from flask import Blueprint, jsonify, request

from stores.project_store import (
    DELETED_RETENTION_DAYS,
    deleted_projects,
    save_data,
    save_deleted_projects,
    stored_data,
)

deleted_projects_bp = Blueprint("deleted_projects", __name__)


@deleted_projects_bp.get("/api/deleted-projects")
def list_deleted_projects():
    """List deleted projects for current user (admin can see all)."""
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    projects = []
    now = datetime.now()

    if user_role == "admin":
        for uid, user_projects in deleted_projects.items():
            for pid, data in user_projects.items():
                deleted_at_str = data.get("_deleted_at", "")
                try:
                    deleted_at = datetime.fromisoformat(deleted_at_str.replace("Z", ""))
                    days_elapsed = (now - deleted_at).days
                    days_remaining = max(0, DELETED_RETENTION_DAYS - days_elapsed)
                except Exception:
                    days_remaining = DELETED_RETENTION_DAYS

                metadata = data.get("_metadata", {})
                projects.append(
                    {
                        "id": pid,
                        "owner_id": uid,
                        "name": metadata.get("name") or metadata.get("customer") or "Untitled",
                        "customer": metadata.get("customer") or data.get("siteData", {}).get("customer", ""),
                        "deleted_at": deleted_at_str,
                        "days_remaining": days_remaining,
                        "city": data.get("siteData", {}).get("city"),
                        "state": data.get("siteData", {}).get("state"),
                    }
                )
    else:
        if user_id in deleted_projects:
            for pid, data in deleted_projects[user_id].items():
                deleted_at_str = data.get("_deleted_at", "")
                try:
                    deleted_at = datetime.fromisoformat(deleted_at_str.replace("Z", ""))
                    days_elapsed = (now - deleted_at).days
                    days_remaining = max(0, DELETED_RETENTION_DAYS - days_elapsed)
                except Exception:
                    days_remaining = DELETED_RETENTION_DAYS

                metadata = data.get("_metadata", {})
                projects.append(
                    {
                        "id": pid,
                        "owner_id": user_id,
                        "name": metadata.get("name") or metadata.get("customer") or "Untitled",
                        "customer": metadata.get("customer") or data.get("siteData", {}).get("customer", ""),
                        "deleted_at": deleted_at_str,
                        "days_remaining": days_remaining,
                        "city": data.get("siteData", {}).get("city"),
                        "state": data.get("siteData", {}).get("state"),
                    }
                )

    projects.sort(key=lambda x: x.get("deleted_at") or "", reverse=True)
    return jsonify(projects)


@deleted_projects_bp.post("/api/deleted-projects/<project_id>/restore")
def restore_deleted_project(project_id: str):
    """Restore a deleted project back to active projects. Idempotent."""
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    # Already active? (idempotent)
    if user_role == "admin":
        for uid, user_projects in stored_data.items():
            if project_id in user_projects:
                return (
                    jsonify(
                        {
                            "status": "success",
                            "message": "Project already active",
                            "project_id": project_id,
                            "idempotent": True,
                        }
                    ),
                    200,
                )
    else:
        if user_id in stored_data and project_id in stored_data[user_id]:
            return (
                jsonify(
                    {
                        "status": "success",
                        "message": "Project already active",
                        "project_id": project_id,
                        "idempotent": True,
                    }
                ),
                200,
            )

    restored = False
    owner_id = None
    project_data = None

    if user_role == "admin":
        for uid, user_projects in deleted_projects.items():
            if project_id in user_projects:
                project_data = user_projects[project_id]
                del user_projects[project_id]
                owner_id = uid
                restored = True
                break
    else:
        if user_id in deleted_projects and project_id in deleted_projects[user_id]:
            project_data = deleted_projects[user_id][project_id]
            del deleted_projects[user_id][project_id]
            owner_id = user_id
            restored = True

    if restored and project_data:
        project_data.pop("_deleted_at", None)
        if project_data.get("_metadata"):
            project_data["_metadata"]["updated_at"] = datetime.now().isoformat()

        if owner_id not in stored_data:
            stored_data[owner_id] = {}
        stored_data[owner_id][project_id] = project_data

        save_data(stored_data)
        save_deleted_projects(deleted_projects)

        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Project restored successfully",
                    "project_id": project_id,
                    "owner_id": owner_id,
                }
            ),
            200,
        )

    return (
        jsonify(
            {
                "status": "success",
                "message": "Project not found in deleted archive",
                "project_id": project_id,
                "idempotent": True,
            }
        ),
        200,
    )


@deleted_projects_bp.delete("/api/deleted-projects/<project_id>")
def permanently_delete_project(project_id: str):
    """Permanently delete a project from archive. Idempotent."""
    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    deleted = False
    owner_id = None

    if user_role == "admin":
        for uid, user_projects in deleted_projects.items():
            if project_id in user_projects:
                del user_projects[project_id]
                owner_id = uid
                deleted = True
                break
    else:
        if user_id in deleted_projects and project_id in deleted_projects[user_id]:
            del deleted_projects[user_id][project_id]
            owner_id = user_id
            deleted = True

    if deleted:
        for uid in list(deleted_projects.keys()):
            if not deleted_projects[uid]:
                del deleted_projects[uid]
        save_deleted_projects(deleted_projects)
        return (
            jsonify(
                {
                    "status": "success",
                    "message": "Project permanently deleted",
                    "project_id": project_id,
                    "owner_id": owner_id,
                }
            ),
            200,
        )

    return (
        jsonify(
            {
                "status": "success",
                "message": "Project not found (already deleted)",
                "project_id": project_id,
                "idempotent": True,
            }
        ),
        200,
    )


@deleted_projects_bp.post("/api/deleted-projects/bulk-restore")
def bulk_restore_deleted_projects():
    data = request.get_json() or {}
    project_ids = data.get("project_ids", [])
    if not project_ids:
        return jsonify({"status": "error", "message": "No project IDs provided"}), 400

    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    restored = []
    failed = []

    for project_id in project_ids:
        found = False
        owner_id = None
        project_data = None

        if user_role == "admin":
            for uid, user_projects in deleted_projects.items():
                if project_id in user_projects:
                    project_data = user_projects[project_id]
                    del user_projects[project_id]
                    owner_id = uid
                    found = True
                    break
        else:
            if user_id in deleted_projects and project_id in deleted_projects[user_id]:
                project_data = deleted_projects[user_id][project_id]
                del deleted_projects[user_id][project_id]
                owner_id = user_id
                found = True

        if found and project_data:
            project_data.pop("_deleted_at", None)
            if project_data.get("_metadata"):
                project_data["_metadata"]["updated_at"] = datetime.now().isoformat()
            if owner_id not in stored_data:
                stored_data[owner_id] = {}
            stored_data[owner_id][project_id] = project_data
            restored.append(project_id)
        else:
            # Idempotent: treat as restored
            restored.append(project_id)

    for uid in list(deleted_projects.keys()):
        if not deleted_projects[uid]:
            del deleted_projects[uid]

    save_data(stored_data)
    save_deleted_projects(deleted_projects)

    return (
        jsonify(
            {
                "status": "success",
                "message": f"{len(restored)} project(s) restored",
                "restored": restored,
                "failed": failed,
            }
        ),
        200,
    )


@deleted_projects_bp.post("/api/deleted-projects/bulk-delete")
def bulk_permanently_delete_projects():
    data = request.get_json() or {}
    project_ids = data.get("project_ids", [])
    if not project_ids:
        return jsonify({"status": "error", "message": "No project IDs provided"}), 400

    user_id = request.headers.get("X-User-Id", "default")
    user_role = request.headers.get("X-User-Role", "admin")

    deleted_list = []
    for project_id in project_ids:
        if user_role == "admin":
            for uid, user_projects in deleted_projects.items():
                if project_id in user_projects:
                    del user_projects[project_id]
                    break
        else:
            if user_id in deleted_projects and project_id in deleted_projects[user_id]:
                del deleted_projects[user_id][project_id]
        deleted_list.append(project_id)  # idempotent

    for uid in list(deleted_projects.keys()):
        if not deleted_projects[uid]:
            del deleted_projects[uid]

    save_deleted_projects(deleted_projects)

    return (
        jsonify(
            {
                "status": "success",
                "message": f"{len(deleted_list)} project(s) permanently deleted",
                "deleted": deleted_list,
            }
        ),
        200,
    )


