#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/abcp-b24-garage-sync"

sudo install -d -m 755 -o "${USER}" -g "${USER}" "$APP_ROOT/data" "$APP_ROOT/logs"
sudo ln -sf "$APP_ROOT/.env" "$APP_ROOT/current/.env" 2>/dev/null || true

sudo cp "$APP_ROOT/current/deploy/systemd/abcp-b24-garage-sync.service" /etc/systemd/system/abcp-b24-garage-sync.service
sudo cp "$APP_ROOT/current/deploy/systemd/abcp-b24-garage-sync.timer"    /etc/systemd/system/abcp-b24-garage-sync.timer

sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.timer
echo "systemd timer enabled:"
systemctl status abcp-b24-garage-sync.timer --no-pager
