#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/.run"
PID_FILE="$RUN_DIR/app.pid"
LOG_FILE="$RUN_DIR/app.log"

mkdir -p "$RUN_DIR"

if [[ ! -d "$ROOT_DIR/.venv" ]]; then
  echo "[start] .venv not found. Create it first:"
  echo "  python -m venv .venv"
  echo "  source .venv/bin/activate"
  echo "  pip install -r requirements.txt"
  exit 1
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

echo "[start] Starting app..."
echo "[start] Logs: $LOG_FILE"

# Start in background and write PID
python -u main.py >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"

sleep 0.5
PID="$(cat "$PID_FILE")"
echo "[start] Started (pid=$PID)."


