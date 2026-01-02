"""
File-based storage for SiteWalk projects, users, deleted-projects, and autosaves.

This is extracted from the original monolithic `app.py` to:
- keep `app.py` smaller
- isolate file IO and migrations
- make future blueprint extraction easier
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict

# Lock for thread-safe access to in-memory state and for deep-copying during writes.
data_lock = threading.Lock()


DATA_FILE = "projects_data.json"
USERS_FILE = "users.json"
AUTOSAVE_FILE = "autosave_data.json"
DELETED_FILE = "deleted_projects.json"
PROJECT_AUTOSAVE_FILE = "project_autosaves.json"

# Retention period for deleted projects (30 days)
DELETED_RETENTION_DAYS = 30


def load_data() -> Dict[str, Any]:
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                # Migrate old structure to new user-based structure if needed
                if data and not any(isinstance(v, dict) and "role" not in v for v in data.values()):
                    # Already in new format or empty
                    return data
                # Old format: {project_id: project_data}
                # New format: {user_id: {project_id: project_data}}
                if data and all(isinstance(v, dict) and "_metadata" in v for v in data.values()):
                    migrated = {"default": data}
                    save_data(migrated)
                    return migrated
                return data
        except Exception:
            return {}
    return {}


def save_data(data: Dict[str, Any]) -> None:
    import copy

    with data_lock:
        data_copy = copy.deepcopy(data)
    with open(DATA_FILE, "w") as f:
        json.dump(data_copy, f, indent=2)


def load_users() -> Dict[str, Any]:
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    # Default: create admin user
    return {
        "default": {
            "user_id": "default",
            "display_name": "Carlisle Energy",
            "role": "admin",
            "created_at": datetime.now().isoformat(),
        }
    }


def save_users(users: Dict[str, Any]) -> None:
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)


def load_deleted_projects() -> Dict[str, Any]:
    """Structure: {user_id: {project_id: {...project_data..., '_deleted_at': timestamp}}}"""
    if os.path.exists(DELETED_FILE):
        try:
            with open(DELETED_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_deleted_projects(data: Dict[str, Any]) -> None:
    import copy

    with data_lock:
        data_copy = copy.deepcopy(data)
    with open(DELETED_FILE, "w") as f:
        json.dump(data_copy, f, indent=2)


def cleanup_expired_deleted_projects(deleted_projects: Dict[str, Any]) -> int:
    """Remove projects deleted more than DELETED_RETENTION_DAYS ago."""
    from datetime import timedelta

    cutoff_date = datetime.now() - timedelta(days=DELETED_RETENTION_DAYS)
    removed_count = 0

    for user_id in list(deleted_projects.keys()):
        projects = deleted_projects[user_id]
        for project_id in list(projects.keys()):
            project = projects[project_id]
            deleted_at_str = project.get("_deleted_at")
            if deleted_at_str:
                try:
                    deleted_at = datetime.fromisoformat(deleted_at_str.replace("Z", "+00:00").replace("+00:00", ""))
                    if deleted_at < cutoff_date:
                        del projects[project_id]
                        removed_count += 1
                except Exception:
                    pass
        if not projects:
            del deleted_projects[user_id]

    if removed_count > 0:
        save_deleted_projects(deleted_projects)
    return removed_count


def load_autosave() -> Dict[str, Any]:
    if os.path.exists(AUTOSAVE_FILE):
        try:
            with open(AUTOSAVE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_autosave(data: Dict[str, Any]) -> None:
    with open(AUTOSAVE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def load_project_autosaves() -> Dict[str, Any]:
    if os.path.exists(PROJECT_AUTOSAVE_FILE):
        try:
            with open(PROJECT_AUTOSAVE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_project_autosaves(data: Dict[str, Any]) -> None:
    with open(PROJECT_AUTOSAVE_FILE, "w") as f:
        json.dump(data, f, indent=2)


# Module-level state (kept for backwards compatibility with existing route code)
stored_data = load_data()
users_db = load_users()
deleted_projects = load_deleted_projects()

# Ensure users file exists
if not os.path.exists(USERS_FILE):
    save_users(users_db)

# Cleanup expired deleted projects on import/startup
cleanup_expired_deleted_projects(deleted_projects)


