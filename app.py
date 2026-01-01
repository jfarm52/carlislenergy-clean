"""
Refrigeration Data Collection - Flask Backend
==============================================

OVERVIEW:
This Flask backend provides persistent storage and cross-device access for refrigeration
site walk data. It uses a simple JSON file-based storage system with user buckets.

CROSS-DEVICE BEHAVIOR:
- All devices currently use X-User-Id: "default" (no real authentication yet)
- All saved projects are stored in a shared pool under the "default" user
- Creating a project on Device A makes it immediately available on Device B
- This is intentional for single-user/single-company deployment

ACCURATE UNIT COUNTS:
- Backend correctly sums the 'room-count' field from entries (NOT card count)
- A single card with room-count=5 represents 5 units, not 1 unit
- This matches the frontend's calculation logic

DATA STRUCTURE:
- projects_data.json: {user_id: {project_id: project_data}}
- Each project includes: siteData, entries, photos, _metadata
- _metadata: {project_id, owner_id, customer, name, created_at, saved_at, updated_at}

API ENDPOINTS FOR PROJECT MANAGEMENT:
- POST /api/projects/create - Create new empty project with name
- GET /api/projects - List all projects (includes name field)
- GET /api/projects/<project_id> - Fetch single project by id
- PUT /api/projects/<project_id> - Update existing project
- POST /api/projects/duplicate/<project_id> - Duplicate a project
- DELETE /api/data/<project_id> - Delete a project

KNOWN LIMITATIONS:
- No real authentication - all devices share "default" user pool
- No per-user isolation - admin role sees all projects
- No real-time sync - clients must poll /api/projects
- File-based storage - not suitable for high concurrency

FUTURE ENHANCEMENTS:
- Add real authentication (OAuth, JWT)
- Per-user project isolation
- WebSocket for real-time sync
- Database backend (PostgreSQL)
"""

# ==============================================================================
# CRITICAL: Safe print wrapper - MUST be FIRST before any imports
# Prevents >50KB objects from crashing Replit chat service
# ==============================================================================
import sys
import builtins

_MAX_PRINT_BYTES = 50 * 1024  # 50KB limit

_original_print = builtins.print

def _safe_print(*args, **kwargs):
    """Safe print wrapper that truncates large objects"""
    safe_args = []
    for arg in args:
        try:
            if isinstance(arg, (dict, list)):
                import json
                s = json.dumps(arg, default=str)
                if len(s) > _MAX_PRINT_BYTES:
                    if isinstance(arg, dict):
                        keys = list(arg.keys())[:10]
                        safe_args.append(f"[OMITTED] dict size={len(s)} keys={keys}...")
                    else:
                        safe_args.append(f"[OMITTED] list size={len(s)} len={len(arg)}")
                    continue
            elif isinstance(arg, str) and len(arg) > _MAX_PRINT_BYTES:
                safe_args.append(f"[OMITTED] str size={len(arg)}")
                continue
            elif isinstance(arg, bytes) and len(arg) > _MAX_PRINT_BYTES:
                safe_args.append(f"[OMITTED] bytes size={len(arg)}")
                continue
        except Exception:
            pass
        safe_args.append(arg)
    return _original_print(*safe_args, **kwargs)

builtins.print = _safe_print
print("[PRINT] Safe print wrapper active - max per-arg: 50KB")

import traceback

def log_exception(exc_type, exc_value, exc_tb):
    print(f"[FATAL] Uncaught exception: {exc_type.__name__}: {exc_value}")
    traceback.print_exception(exc_type, exc_value, exc_tb)
    
sys.excepthook = log_exception

# ==============================================================================
# Central config + logging (PR1)
# ==============================================================================
import logging

# Load environment variables from .env file (if it exists)
# NOTE: load_dotenv() returns False when no .env is found; log accurately to reduce confusion.
try:
    from dotenv import load_dotenv, find_dotenv
    _dotenv_path = find_dotenv(usecwd=True)
    if _dotenv_path:
        _loaded = load_dotenv(dotenv_path=_dotenv_path, override=False)
        if _loaded:
            print(f"[ENV] Loaded environment variables from .env ({_dotenv_path})")
        else:
            print(f"[ENV] Found .env at {_dotenv_path}, but no variables were loaded/changed")
    else:
        print("[ENV] No .env file found (set env vars via shell or create .env from env.example)")
except ImportError:
    print("[ENV] python-dotenv not installed - skipping .env file loading")
except Exception as e:
    print(f"[ENV] Warning: Could not load .env file: {e}")

_APP_CFG = {}
try:
    # Always set up logging with safe defaults first.
    from logging_setup import setup_logging, init_request_logging
    setup_logging({})

    # Then, optionally load YAML config (requires PyYAML to be installed).
    try:
        from config_loader import get_config
        _APP_CFG = get_config()
        setup_logging(_APP_CFG)
    except Exception as e:
        print(f"[BOOT] Warning: YAML config load failed (continuing with defaults): {e}")
except Exception as e:
    # Never break app import due to logging setup.
    print(f"[BOOT] Warning: logging init failed: {e}")

from flask import Flask, send_file, jsonify, request, redirect
from flask_cors import CORS
import json
import os
from datetime import datetime
import uuid

# File-based storage (extracted from monolithic app.py)
from stores.project_store import (
    data_lock,
    stored_data,
    users_db,
    deleted_projects,
    load_data,
    save_data,
    load_users,
    save_users,
    load_deleted_projects,
    save_deleted_projects,
    cleanup_expired_deleted_projects,
    load_autosave,
    save_autosave,
    load_project_autosaves,
    save_project_autosaves,
)

# data_lock is provided by stores.project_store


def create_app() -> Flask:
    """
    Flask application factory.

    Note: for now, the module still defines routes using `@app.route`.
    This factory is a first refactor step to make later blueprint extraction
    possible without changing import paths (`from app import app`).
    """
    app = Flask(__name__, static_folder=None)
    # Make resolved config available to blueprints/services.
    app.config["APP_CFG"] = _APP_CFG or {}

    # ------------------------------------------------------------------------------
    # Bills config (feature flag + uploads dir + upload size limit)
    # ------------------------------------------------------------------------------
    try:
        app_cfg = app.config.get("APP_CFG", {}) or {}

        # Feature flag (YAML/env)
        app.config["BILLS_FEATURE_ENABLED"] = bool(
            (app_cfg.get("features", {}) or {}).get("utility_bills_enabled", True)
        )
        if os.environ.get("UTILITY_BILLS_ENABLED") is not None:
            app.config["BILLS_FEATURE_ENABLED"] = os.environ["UTILITY_BILLS_ENABLED"].strip().lower() in (
                "1",
                "true",
                "yes",
                "y",
                "on",
            )

        # Uploads directory (YAML/env)
        app.config["BILL_UPLOADS_DIR"] = str(
            os.environ.get("BILL_UPLOADS_DIR")
            or (app_cfg.get("bills", {}) or {}).get("uploads_dir", "bill_uploads")
        )
        os.makedirs(app.config["BILL_UPLOADS_DIR"], exist_ok=True)

        # Request size limit for uploads (Flask MAX_CONTENT_LENGTH)
        max_upload_mb = os.environ.get("MAX_UPLOAD_MB") or (app_cfg.get("bills", {}) or {}).get("max_upload_mb", 50)
        try:
            max_upload_mb = int(max_upload_mb)
        except Exception:
            max_upload_mb = 50
        app.config["MAX_CONTENT_LENGTH"] = max_upload_mb * 1024 * 1024

        # Executor sizing (used by bills blueprint)
        try:
            app.config["BILL_MAX_WORKERS"] = int((app_cfg.get("bills", {}) or {}).get("max_workers", 3))
        except Exception:
            app.config["BILL_MAX_WORKERS"] = 3
    except Exception as e:
        print(f"[BOOT] Warning: could not apply bills config: {e}")

    # Optional: use config.yml/env to restrict CORS instead of allowing all.
    try:
        cors_cfg = (_APP_CFG or {}).get("app", {}).get("cors", {})
        cors_enabled = bool(cors_cfg.get("enabled", True))
        if cors_enabled:
            origins = cors_cfg.get("origins", ["*"])
            CORS(app, origins=origins)
    except Exception:
        # Fallback to legacy behavior
        CORS(app)

    try:
        init_request_logging(app)
    except Exception as e:
        print(f"[BOOT] Warning: request logging init failed: {e}")

    return app


app = create_app()

# Register small, dependency-light route groups first.
try:
    from routes.spa import spa_bp
    app.register_blueprint(spa_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register SPA blueprint: {e}")

try:
    from routes.config_api import config_api_bp
    app.register_blueprint(config_api_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register config API blueprint: {e}")

try:
    from routes.google_places_api import google_places_bp
    app.register_blueprint(google_places_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register Google Places blueprint: {e}")

try:
    from routes.dropbox_api import dropbox_api_bp
    app.register_blueprint(dropbox_api_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register Dropbox API blueprint: {e}")

try:
    from routes.print_pdf import print_pdf_bp
    app.register_blueprint(print_pdf_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register print PDF blueprint: {e}")

try:
    from routes.deleted_projects_api import deleted_projects_bp
    app.register_blueprint(deleted_projects_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register deleted-projects blueprint: {e}")

try:
    from routes.autosave_api import autosave_bp
    app.register_blueprint(autosave_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register autosave blueprint: {e}")

try:
    from routes.projects_api import projects_bp
    app.register_blueprint(projects_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register projects blueprint: {e}")

try:
    from routes.bills_api import bills_bp
    app.register_blueprint(bills_bp)
except Exception as e:
    print(f"[BOOT] Warning: could not register bills blueprint: {e}")

# ==================================================================================
# RESPONSE SIZE LIMITER - BLOCK payloads > 1MB (dev) or > 2MB (prod)
# ==================================================================================
import os as _os_for_env
_IS_PRODUCTION = _os_for_env.environ.get('REPLIT_DEPLOYMENT', '') != ''
MAX_RESPONSE_SIZE_MB = 2 if _IS_PRODUCTION else 1
MAX_RESPONSE_SIZE_BYTES = MAX_RESPONSE_SIZE_MB * 1024 * 1024

@app.after_request
def check_response_size(response):
    """Block JSON responses that exceed size limit - prevents Replit crash"""
    if response.content_type and 'application/json' in response.content_type:
        content_length = response.content_length or len(response.get_data())
        endpoint = request.endpoint or request.path
        size_kb = content_length / 1024
        
        # Log size for every JSON response (route + bytes)
        logging.getLogger("api.size").debug("%s %s bytes=%s", request.method, request.path, content_length)
        
        if content_length > MAX_RESPONSE_SIZE_BYTES:
            size_mb = content_length / (1024 * 1024)
            # DO NOT include payload in error - only size + route
            logging.getLogger("api.size").warning(
                "Response too large: %s = %.2fMB (limit=%sMB)", endpoint, size_mb, MAX_RESPONSE_SIZE_MB
            )
            # Return error response instead of giant payload
            error_response = jsonify({
                'error': 'Response too large',
                'route': endpoint,
                'size_mb': round(size_mb, 2),
                'limit_mb': MAX_RESPONSE_SIZE_MB
            })
            error_response.status_code = 413
            return error_response
    return response

@app.before_request
def redirect_to_custom_domain():
    """Redirect Replit URLs to custom domain in production only."""
    if os.environ.get('REPLIT_DEPLOYMENT'):
        host = request.host.lower()
        if 'replit.app' in host or 'replit.dev' in host:
            new_url = 'https://sitewalk.carlislenergy.com' + request.full_path
            if new_url.endswith('?'):
                new_url = new_url[:-1]
            return redirect(new_url, code=301)

from backend_upload_to_dropbox import upload_bp
app.register_blueprint(upload_bp)

# (Bills routes moved to routes/bills_api.py)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
