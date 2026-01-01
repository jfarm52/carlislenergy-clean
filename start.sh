#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/.run"
PID_FILE="$RUN_DIR/app.pid"
LOG_FILE="$RUN_DIR/app.log"

mkdir -p "$RUN_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -d "$ROOT_DIR/.venv" ]]; then
  echo "[start] .venv not found; creating it..."
  "$PYTHON_BIN" -m venv "$ROOT_DIR/.venv"
fi

if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${PID}" ]] && kill -0 "$PID" 2>/dev/null; then
    echo "[start] App already running (pid=$PID)."
    echo "[start] Log: $LOG_FILE"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

# shellcheck disable=SC1091
source "$ROOT_DIR/.venv/bin/activate"

if [[ ! -f "$ROOT_DIR/.env" ]] && [[ -f "$ROOT_DIR/.env.example" ]]; then
  echo "[start] .env not found; creating from .env.example..."
  cp "$ROOT_DIR/.env.example" "$ROOT_DIR/.env"
fi

if [[ -f "$ROOT_DIR/requirements.txt" ]]; then
  echo "[start] Ensuring dependencies are installed..."
  python -m pip install --upgrade pip >/dev/null 2>&1 || true
  python -m pip install -r "$ROOT_DIR/requirements.txt"
fi

echo "[start] Starting app..."
echo "[start] Logs: $LOG_FILE"

# Defaults: app is expected on localhost:5001
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-5001}"

# IMPORTANT: when running in background, disable Flask debug/reloader so the PID we capture is the real server process.
export FLASK_DEBUG="${FLASK_DEBUG:-false}"

# Start in background and write PID
python -u main.py >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

sleep 0.5
PID="$(cat "$PID_FILE")"
echo "[start] Started (pid=$PID)."


