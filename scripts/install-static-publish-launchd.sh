#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_PATH="$PLIST_DIR/com.coursedog.dashboard.static-publish.plist"
SITE_BASE="${VITE_SITE_BASE:-}"
HOUR="${STATIC_PUBLISH_HOUR:-8}"
MINUTE="${STATIC_PUBLISH_MINUTE:-0}"

if [[ -z "$SITE_BASE" ]]; then
  echo "VITE_SITE_BASE is required before installing the launchd job."
  exit 1
fi

mkdir -p "$PLIST_DIR"

cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.coursedog.dashboard.static-publish</string>
    <key>WorkingDirectory</key>
    <string>$PROJECT_ROOT</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>$PROJECT_ROOT/scripts/publish-static-site.sh</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
      <key>VITE_SITE_BASE</key>
      <string>$SITE_BASE</string>
    </dict>
    <key>StartCalendarInterval</key>
    <dict>
      <key>Hour</key>
      <integer>$HOUR</integer>
      <key>Minute</key>
      <integer>$MINUTE</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>$PROJECT_ROOT/.static-publish.log</string>
    <key>StandardErrorPath</key>
    <string>$PROJECT_ROOT/.static-publish.log</string>
    <key>RunAtLoad</key>
    <false/>
  </dict>
</plist>
EOF

launchctl unload "$PLIST_PATH" >/dev/null 2>&1 || true
launchctl load "$PLIST_PATH"

echo "Installed launchd job at $PLIST_PATH"
