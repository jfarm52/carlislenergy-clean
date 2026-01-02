"""
Central logging setup.

- Uses stdlib logging (no external deps)
"""

from __future__ import annotations

import logging
import logging.config
import time
import uuid
from typing import Any, Dict, Optional

from flask import Flask, g, request


def setup_logging(cfg: Optional[Dict[str, Any]] = None) -> None:
    cfg = cfg or {}
    log_cfg = cfg.get("logging", {}) if isinstance(cfg, dict) else {}
    level = str(log_cfg.get("level", "INFO")).upper()
    fmt = str(log_cfg.get("format", "%(asctime)s %(levelname)s %(name)s %(message)s"))

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"default": {"format": fmt}},
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "default",
                }
            },
            "root": {"handlers": ["console"], "level": level},
        }
    )


def init_request_logging(app: Flask) -> None:
    logger = logging.getLogger("http")

    @app.before_request
    def _start_request_timer() -> None:
        g._request_start = time.time()
        g.request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())

    @app.after_request
    def _log_request(response):  # type: ignore[no-untyped-def]
        try:
            start = getattr(g, "_request_start", None)
            dur_ms = None if start is None else round((time.time() - start) * 1000, 2)
            logger.info(
                "%s %s status=%s dur_ms=%s request_id=%s",
                request.method,
                request.path,
                response.status_code,
                dur_ms,
                getattr(g, "request_id", None),
            )
        except Exception:
            # never break responses due to logging
            pass

        response.headers["X-Request-Id"] = getattr(g, "request_id", "")
        return response


