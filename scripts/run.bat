@echo off
cd /d %~dp0\..
python -m abcp_b24_garage_sync --from 2020-01-01 --to 2025-12-31
