#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Stop the app
"$ROOT_DIR/stop.sh"

# Start the app
"$ROOT_DIR/start.sh"
