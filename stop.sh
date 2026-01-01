#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

RUN_DIR="$ROOT_DIR/.run"
PID_FILE="$RUN_DIR/app.pid"

# Function to kill a process and wait for it to exit
kill_process() {
  local pid=$1
  local desc=$2
  
  if kill -0 "$pid" 2>/dev/null; then
    echo "[stop] Stopping $desc (pid=$pid)..."
    kill "$pid"

    # Wait briefly for shutdown
    for _ in {1..20}; do
      if kill -0 "$pid" 2>/dev/null; then
        sleep 0.2
      else
        break
      fi
    done

    if kill -0 "$pid" 2>/dev/null; then
      echo "[stop] Process did not exit gracefully; forcing kill -9."
      kill -9 "$pid" || true
      sleep 0.5
    fi
    return 0
  else
    return 1
  fi
}

# Try PID file first
FOUND=false
if [[ -f "$PID_FILE" ]]; then
  PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${PID}" ]]; then
    if kill_process "$PID" "app"; then
      FOUND=true
    else
      echo "[stop] Process from PID file not running (pid=$PID); cleaning PID file."
    fi
  else
    echo "[stop] PID file was empty; removing it."
  fi
  rm -f "$PID_FILE"
fi

# If PID file method didn't work, try finding process by port
if [[ "$FOUND" == false ]]; then
  # Check common ports (5001 first since that's where the app is running, then 5000)
  for port in 5001 5000; do
    PIDS=$(lsof -ti:$port 2>/dev/null || true)
    if [[ -n "$PIDS" ]]; then
      echo "[stop] Found process(es) on port $port: $PIDS"
      for pid in $PIDS; do
        # Kill the process if it exists
        if kill_process "$pid" "app on port $port"; then
          FOUND=true
        fi
      done
      break
    fi
  done
fi

if [[ "$FOUND" == false ]]; then
  echo "[stop] No running app process found."
else
  echo "[stop] Done."
fi


