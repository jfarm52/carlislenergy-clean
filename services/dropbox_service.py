from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import requests
import dropbox
from dropbox.exceptions import AuthError

logger = logging.getLogger(__name__)


_token_cache: Dict[str, Any] = {"access_token": None, "expires_at": None}


def get_root_path(app_cfg: Optional[Dict[str, Any]] = None) -> str:
    # Precedence: env override -> config.yml -> legacy default
    if os.getenv("DROPBOX_ROOT_PATH"):
        return os.getenv("DROPBOX_ROOT_PATH")  # type: ignore[return-value]
    try:
        return str((app_cfg or {}).get("dropbox", {}).get("root_path"))
    except Exception:
        return "/1. CES/1. FRIGITEK/1. FRIGITEK ANALYSIS/SiteWalk Exports"


def get_access_token() -> Optional[str]:
    """
    Get a fresh Dropbox access token using OAuth refresh token flow.

    Uses DROPBOX_REFRESH_TOKEN + DROPBOX_APP_KEY + DROPBOX_APP_SECRET.
    Falls back to DROPBOX_ACCESS_TOKEN.
    """
    global _token_cache

    if _token_cache["access_token"] and _token_cache["expires_at"]:
        if datetime.now() < _token_cache["expires_at"] - timedelta(seconds=60):
            return _token_cache["access_token"]

    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")

    if refresh_token and app_key and app_secret:
        try:
            resp = requests.post(
                "https://api.dropboxapi.com/oauth2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": app_key,
                    "client_secret": app_secret,
                },
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                access_token = data.get("access_token")
                expires_in = data.get("expires_in", 14400)
                if access_token:
                    _token_cache["access_token"] = access_token
                    _token_cache["expires_at"] = datetime.now() + timedelta(seconds=expires_in)
                    return access_token
            else:
                logger.warning("Dropbox token refresh failed: %s %s", resp.status_code, resp.text[:200])
        except Exception:
            logger.exception("Dropbox token refresh error")

    return os.getenv("DROPBOX_ACCESS_TOKEN") or os.getenv("DROPBOX_ACCESS_TC")


def upload_csv(filename: str, csv_text: str, *, root_path: str) -> Tuple[bool, str]:
    token = get_access_token()
    if not token:
        return (False, "Missing Dropbox access token")

    try:
        dbx = dropbox.Dropbox(token)

        # Optional auth check
        try:
            dbx.users_get_current_account()
        except AuthError as e:
            return (False, f"AuthError (invalid or expired token): {e}")

        dropbox_path = f"{root_path}/{filename}"

        # Ensure folder chain exists
        parts = root_path.strip("/").split("/")
        current = ""
        for part in parts:
            current = current + "/" + part
            try:
                dbx.files_create_folder_v2(current)
            except dropbox.exceptions.ApiError as e:
                if "conflict" in str(e).lower():
                    pass
                else:
                    logger.warning("Dropbox folder create error %s: %s", current, e)

        dbx.files_upload(
            csv_text.encode("utf-8"),
            dropbox_path,
            mode=dropbox.files.WriteMode("overwrite"),
        )

        return (True, dropbox_path)
    except Exception as e:
        logger.exception("Dropbox upload exception")
        return (False, str(e))


