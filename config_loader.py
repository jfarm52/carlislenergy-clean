"""
Simple YAML-backed configuration loader with environment-variable overrides.

Design goals:
- Minimal dependencies and minimal magic
- Safe defaults from config.yml
- Environment variables can override deployment-specific values (secrets, feature flags)
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Dict, Optional

import yaml


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge override into base (override wins)."""
    out: Dict[str, Any] = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _parse_bool(value: Optional[str], default: Optional[bool] = None) -> Optional[bool]:
    if value is None:
        return default
    v = value.strip().lower()
    if v in ("1", "true", "yes", "y", "on"):
        return True
    if v in ("0", "false", "no", "n", "off"):
        return False
    return default


def _env_override_dict() -> Dict[str, Any]:
    """
    Map env vars to config keys.
    Keep this small and explicit.
    """
    overrides: Dict[str, Any] = {}

    # Feature flags
    bills_enabled = _parse_bool(os.getenv("UTILITY_BILLS_ENABLED"))
    if bills_enabled is not None:
        overrides = _deep_merge(overrides, {"features": {"utility_bills_enabled": bills_enabled}})

    # If an API key exists, feature can be considered enabled even if YAML says false
    places_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if places_key:
        overrides = _deep_merge(overrides, {"features": {"google_places_enabled": True}})

    # CORS origins (comma-separated)
    cors_origins = os.getenv("CORS_ORIGINS")
    if cors_origins:
        origins = [o.strip() for o in cors_origins.split(",") if o.strip()]
        overrides = _deep_merge(overrides, {"app": {"cors": {"origins": origins}}})

    # Logging
    log_level = os.getenv("LOG_LEVEL")
    if log_level:
        overrides = _deep_merge(overrides, {"logging": {"level": log_level}})

    # Dropbox paths
    dropbox_root = os.getenv("DROPBOX_ROOT_PATH")
    if dropbox_root:
        overrides = _deep_merge(overrides, {"dropbox": {"root_path": dropbox_root}})

    # Upload dir
    bills_upload_dir = os.getenv("BILL_UPLOADS_DIR")
    if bills_upload_dir:
        overrides = _deep_merge(overrides, {"bills": {"uploads_dir": bills_upload_dir}})

    return overrides


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load config.yml and apply environment overrides.
    """
    config_path = path or os.getenv("APP_CONFIG_PATH", "config.yml")
    if not os.path.exists(config_path):
        # Safe fallback: empty config; caller must handle defaults
        cfg: Dict[str, Any] = {}
    else:
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    return _deep_merge(cfg, _env_override_dict())


_CONFIG_CACHE: Optional[Dict[str, Any]] = None


def get_config(path: Optional[str] = None, *, force_reload: bool = False) -> Dict[str, Any]:
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None or force_reload:
        _CONFIG_CACHE = load_config(path)
    return _CONFIG_CACHE


