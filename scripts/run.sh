#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR/.."
python -m abcp_b24_garage_sync "$@"
