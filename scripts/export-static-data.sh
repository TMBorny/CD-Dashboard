#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$PROJECT_ROOT/client-health-backend"
OUTPUT_DIR="${1:-$PROJECT_ROOT/public/static-data}"

if command -v poetry >/dev/null 2>&1; then
  (cd "$BACKEND_DIR" && poetry run python scripts/export_static_data.py --output-dir "$OUTPUT_DIR")
else
  PYTHONPATH="$BACKEND_DIR${PYTHONPATH:+:$PYTHONPATH}" \
    python3 "$BACKEND_DIR/scripts/export_static_data.py" --output-dir "$OUTPUT_DIR"
fi
