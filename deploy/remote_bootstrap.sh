#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/abcp-b24-garage-sync"

sudo install -d -m 755 -o "${USER}" -g "${USER}" "$APP_ROOT/data" "$APP_ROOT/logs"

# Preserve an already configured current/.env when bootstrapping the server.
# If the shared .env is missing but there is a real file inside current/, move it
# to the shared location instead of replacing it with a dangling symlink.
if [ ! -f "$APP_ROOT/.env" ] && [ -f "$APP_ROOT/current/.env" ] && [ ! -L "$APP_ROOT/current/.env" ]; then
    sudo install -m 600 -o "${USER}" -g "${USER}" "$APP_ROOT/current/.env" "$APP_ROOT/.env"
fi

if [ -f "$APP_ROOT/.env" ]; then
    sudo ln -sf "$APP_ROOT/.env" "$APP_ROOT/current/.env"
else
    echo "WARNING: $APP_ROOT/.env not found — create it manually before enabling systemd units" >&2
fi

sudo cp "$APP_ROOT/current/deploy/systemd/abcp-b24-garage-sync.service" /etc/systemd/system/abcp-b24-garage-sync.service
sudo cp "$APP_ROOT/current/deploy/systemd/abcp-b24-garage-sync.timer"    /etc/systemd/system/abcp-b24-garage-sync.timer

sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.timer
echo "systemd timer enabled:"
systemctl status abcp-b24-garage-sync.timer --no-pager
