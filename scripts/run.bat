@echo off
cd /d %~dp0\..
python -m abcp_b24_garage_sync %*
