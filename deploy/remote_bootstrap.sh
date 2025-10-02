#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/opt/abcp-b24-garage-sync"

sudo install -d -m 755 -o "${USER}" -g "${USER}" "$APP_ROOT/data" "$APP_ROOT/logs"

PROJECT_ENV="$APP_ROOT/current/.env"
SHARED_ENV="$APP_ROOT/.env"

# Если раньше использовалась общая .env, переносим её обратно в каталог проекта.
if [ -L "$PROJECT_ENV" ]; then
    sudo rm -f "$PROJECT_ENV"
fi

if [ -f "$SHARED_ENV" ] && [ ! -e "$PROJECT_ENV" ]; then
    sudo install -m 600 -o "${USER}" -g "${USER}" "$SHARED_ENV" "$PROJECT_ENV"
    echo "Migrated existing $SHARED_ENV to $PROJECT_ENV"
fi

if [ ! -f "$PROJECT_ENV" ]; then
    echo "WARNING: $PROJECT_ENV not found — create it manually before enabling systemd units" >&2
fi

sudo cp "$APP_ROOT/current/deploy/systemd/abcp-b24-garage-sync.service" /etc/systemd/system/abcp-b24-garage-sync.service

# Старый таймер больше не используется.
if systemctl list-unit-files "abcp-b24-garage-sync.timer" >/dev/null 2>&1; then
    sudo systemctl disable --now abcp-b24-garage-sync.timer >/dev/null 2>&1 || true
    sudo rm -f /etc/systemd/system/abcp-b24-garage-sync.timer
fi

sudo systemctl daemon-reload
sudo systemctl enable --now abcp-b24-garage-sync.service
echo "systemd service enabled:"
systemctl status abcp-b24-garage-sync.service --no-pager
