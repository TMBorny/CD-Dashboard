#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_DIR="$PROJECT_ROOT/client-health-backend"
STATIC_DATA_DIR="$PROJECT_ROOT/public/static-data"
PAGES_BRANCH="${STATIC_PAGES_BRANCH:-gh-pages}"
PAGES_REMOTE="${STATIC_PAGES_REMOTE:-origin}"
SITE_BASE="${VITE_SITE_BASE:-}"
WORKTREE_DIR="${STATIC_PAGES_WORKTREE:-$(mktemp -d "${TMPDIR:-/tmp}/cd-dashboard-pages.XXXXXX")}"

cleanup() {
  # Remove the worktree directly; bypass 'worktree list' check to avoid macOS /private/tmp symlink path mismatch
  git -C "$PROJECT_ROOT" worktree remove --force "$WORKTREE_DIR" >/dev/null 2>&1 || true
  # Always prune as a fallback to ensure clean git state
  git -C "$PROJECT_ROOT" worktree prune >/dev/null 2>&1 || true
}

trap cleanup EXIT

if [[ -z "$SITE_BASE" || "$SITE_BASE" != "./" ]]; then
  if [[ -n "$SITE_BASE" && "$SITE_BASE" != "./" ]]; then
    echo "Notice: Overriding VITE_SITE_BASE='$SITE_BASE' with './' to perfectly support GitHub Pages subfolder routing."
  fi
  SITE_BASE="./"
fi

echo "Exporting static data into $STATIC_DATA_DIR"
bash "$PROJECT_ROOT/scripts/export-static-data.sh" "$STATIC_DATA_DIR"

echo "Building static frontend"
(cd "$PROJECT_ROOT" && VITE_DATA_MODE=static VITE_SITE_BASE="$SITE_BASE" npm run build)

echo "Preparing $PAGES_BRANCH worktree"
if git -C "$PROJECT_ROOT" show-ref --verify --quiet "refs/heads/$PAGES_BRANCH"; then
  git -C "$PROJECT_ROOT" worktree add --force "$WORKTREE_DIR" "$PAGES_BRANCH" >/dev/null
else
  if git -C "$PROJECT_ROOT" ls-remote --exit-code --heads "$PAGES_REMOTE" "$PAGES_BRANCH" >/dev/null 2>&1; then
    git -C "$PROJECT_ROOT" fetch "$PAGES_REMOTE" "$PAGES_BRANCH" >/dev/null
    git -C "$PROJECT_ROOT" worktree add --force -B "$PAGES_BRANCH" "$WORKTREE_DIR" "refs/remotes/$PAGES_REMOTE/$PAGES_BRANCH" >/dev/null
  else
    git -C "$PROJECT_ROOT" worktree add --force -B "$PAGES_BRANCH" "$WORKTREE_DIR" HEAD >/dev/null
    find "$WORKTREE_DIR" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
  fi
fi

find "$WORKTREE_DIR" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
cp -R "$PROJECT_ROOT/dist/." "$WORKTREE_DIR/"
touch "$WORKTREE_DIR/.nojekyll"

git -C "$WORKTREE_DIR" add -A
if git -C "$WORKTREE_DIR" diff --cached --quiet; then
  echo "No changes to publish."
  exit 0
fi

git -C "$WORKTREE_DIR" commit -m "Publish static site $(date -u +"%Y-%m-%dT%H:%M:%SZ")" >/dev/null
git -C "$WORKTREE_DIR" push "$PAGES_REMOTE" "$PAGES_BRANCH"

echo "Published static site to $PAGES_REMOTE/$PAGES_BRANCH"
