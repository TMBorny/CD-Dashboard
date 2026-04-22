#!/usr/bin/env bash
# setup-dev.sh — one-shot local dev setup for Client Health Dashboard
#
# Usage: bash scripts/setup-dev.sh
#
# All configuration lives in a single root .env shared by the frontend and backend.
# This script creates it from .env.example and generates the internal API key.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
ENV_EXAMPLE="$REPO_ROOT/.env.example"

echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   Client Health Dashboard — Dev Setup        ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# ── 0. Check prerequisites ─────────────────────────────────────────────────────

MISSING_PREREQS=0

check_cmd() {
  local cmd="$1" label="$2" hint="$3"
  if ! command -v "$cmd" &>/dev/null; then
    echo "✗  $label not found"
    echo "   → $hint"
    MISSING_PREREQS=1
  else
    echo "✓  $label found ($(\"$cmd\" --version 2>&1 | head -1))"
  fi
}

check_cmd node "Node.js" "Install from https://nodejs.org (v18+)"
check_cmd npm  "npm"     "Comes with Node.js"

# Python — require 3.9+
if command -v python3 &>/dev/null; then
  PY_VER=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
  PY_MAJOR=$(echo "$PY_VER" | cut -d. -f1)
  PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
  if [[ "$PY_MAJOR" -ge 3 && "$PY_MINOR" -ge 9 ]]; then
    echo "✓  Python $PY_VER found"
  else
    echo "✗  Python $PY_VER found — need 3.9+"
    echo "   → Install from https://python.org or use pyenv"
    MISSING_PREREQS=1
  fi
else
  echo "✗  python3 not found"
  echo "   → Install from https://python.org (3.9+)"
  MISSING_PREREQS=1
fi

# Poetry
if ! command -v poetry &>/dev/null; then
  echo "✗  Poetry not found"
  echo "   → Recommended: pipx install poetry  (https://python-poetry.org/docs/)"
  echo "     Or via pip:   pip3 install poetry"
  echo "     NOTE: the backend uses fastapi[standard] — do NOT install packages"
  echo "           manually with pip; let 'poetry install' handle everything."
  MISSING_PREREQS=1
else
  echo "✓  Poetry found ($(poetry --version))"
fi

if [[ "$MISSING_PREREQS" -ne 0 ]]; then
  echo ""
  echo "✗  One or more prerequisites are missing. Install them and re-run this script."
  exit 1
fi

echo ""

# ── 1. Create .env from example ───────────────────────────────────────────────

if [[ -f "$ENV_FILE" ]]; then
  echo "✓  .env already exists — skipping creation"
else
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  echo "✓  Created .env from .env.example"
fi

# ── 2. Generate VITE_INTERNAL_API_KEY if not set ─────────────────────────────

EXISTING_KEY=$(grep -E '^VITE_INTERNAL_API_KEY=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '[:space:]' || true)

if [[ -n "$EXISTING_KEY" && "$EXISTING_KEY" != "replace-with-a-long-random-string" ]]; then
  echo "✓  VITE_INTERNAL_API_KEY already set"
else
  API_KEY=$(openssl rand -hex 24)
  if grep -qE '^VITE_INTERNAL_API_KEY=' "$ENV_FILE"; then
    sed -i.bak "s|^VITE_INTERNAL_API_KEY=.*|VITE_INTERNAL_API_KEY=$API_KEY|" "$ENV_FILE" && rm -f "$ENV_FILE.bak"
  else
    echo "VITE_INTERNAL_API_KEY=$API_KEY" >> "$ENV_FILE"
  fi
  echo "✓  Generated VITE_INTERNAL_API_KEY"
fi

# ── 3. Remind about Coursedog credentials ─────────────────────────────────────

COURSEDOG_EMAIL=$(grep -E '^VITE_COURSEDOG_EMAIL=' "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '[:space:]' || true)
if [[ -z "$COURSEDOG_EMAIL" || "$COURSEDOG_EMAIL" == "your-email@example.com" ]]; then
  echo ""
  echo "⚠  Fill in your Coursedog credentials in .env:"
  echo "     VITE_COURSEDOG_EMAIL=your-email@coursedog.com"
  echo "     VITE_COURSEDOG_PASSWORD=your-password"
  echo "   Without these, the backend will start but syncs will fail."
fi

# ── 4. Next steps ─────────────────────────────────────────────────────────────

echo ""
echo "══ Next steps ══════════════════════════════════"
echo ""
echo "  1. Install frontend deps:"
echo "       npm install"
echo ""
echo "  2. Install backend deps:"
echo "       cd client-health-backend && poetry install && cd .."
echo ""
echo "  3. Start the backend (in one terminal):"
echo "       cd client-health-backend && poetry run fastapi dev app/main.py"
echo ""
echo "  4. Start the frontend (in another terminal):"
echo "       npm run dev"
echo ""
echo "  Then open http://localhost:5173"
echo ""
