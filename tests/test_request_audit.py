from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

from abcp_b24_garage_sync import request_audit


class RequestAuditTests(unittest.TestCase):
    def test_audit_writes_daily_jsonl_and_masks_secrets(self) -> None:
        started_at = datetime(2026, 3, 27, 14, 15, 16, tzinfo=timezone(timedelta(hours=7)))

        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(request_audit, "REQUEST_AUDIT_ENABLED", True), \
             mock.patch("abcp_b24_garage_sync.request_audit._resolve_log_dir", return_value=Path(tmp)):
            request_audit.audit_http_transaction(
                service="abcp",
                method="GET",
                url="https://example.test/cp/garage/?userlogin=masked",
                request_payload={
                    "userlogin": "admin",
                    "userpsw": "secret",
                    "dateUpdatedStart": "2026-03-27 00:00:00",
                },
                request_headers={"Authorization": "Bearer token"},
                started_at=started_at,
                elapsed_ms=12.3456,
                status_code=200,
                ok=True,
                outcome="success",
                response_body={"result": "ok"},
                response_content_length=17,
                meta={"token": "secret-token"},
            )

            log_path = Path(tmp) / "http-requests-2026-03-27.jsonl"
            self.assertTrue(log_path.exists())
            lines = log_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        payload = json.loads(lines[0])
        self.assertEqual(payload["request"]["payload"]["userlogin"], "********")
        self.assertEqual(payload["request"]["payload"]["userpsw"], "********")
        self.assertEqual(payload["request"]["headers"]["Authorization"], "********")
        self.assertEqual(payload["meta"]["token"], "********")
        self.assertEqual(payload["response"]["outcome"], "success")
        self.assertEqual(payload["response"]["duration_ms"], 12.346)

    def test_audit_can_be_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(request_audit, "REQUEST_AUDIT_ENABLED", False), \
             mock.patch("abcp_b24_garage_sync.request_audit._resolve_log_dir", return_value=Path(tmp)):
            request_audit.audit_http_transaction(
                service="bitrix24",
                method="POST",
                url="https://example.test/rest/1/token/crm.deal.list",
                ok=False,
                outcome="transport_error",
            )

            self.assertEqual(list(Path(tmp).glob("*")), [])
