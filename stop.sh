#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/.run"
PID_FILE="$RUN_DIR/app.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "[stop] No PID file found ($PID_FILE). App may not be running."
  exit 0
fi

PID="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -z "${PID}" ]]; then
  echo "[stop] PID file was empty; removing it."
  rm -f "$PID_FILE"
  exit 0
fi

if kill -0 "$PID" 2>/dev/null; then
  echo "[stop] Stopping app (pid=$PID)..."
  kill "$PID"

  # Wait briefly for shutdown
  for _ in {1..20}; do
    if kill -0 "$PID" 2>/dev/null; then
      sleep 0.2
    else
      break
    fi
  done

  if kill -0 "$PID" 2>/dev/null; then
    echo "[stop] App did not exit gracefully; forcing kill -9."
    kill -9 "$PID" || true
  fi
else
  echo "[stop] Process not running (pid=$PID); cleaning PID file."
fi

rm -f "$PID_FILE"
echo "[stop] Done."


