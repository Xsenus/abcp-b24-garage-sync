#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR/.."
python -m abcp_b24_garage_sync --from 2020-01-01 --to 2025-12-31
