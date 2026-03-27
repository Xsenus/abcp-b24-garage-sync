from __future__ import annotations

import unittest
from datetime import datetime
from unittest import mock

from abcp_b24_garage_sync import abcp_client


class AbcpAuditTests(unittest.TestCase):
    def test_fetch_garage_audits_successful_response(self) -> None:
        response = mock.Mock()
        response.status_code = 200
        response.content = b'{"100":[{"id":1}]}'
        response.text = '{"100":[{"id":1}]}'
        response.url = "https://example.test/cp/garage/?userlogin=user&userpsw=secret"
        response.json.return_value = {"100": [{"id": 1}]}

        start = datetime(2026, 3, 26, 0, 0, 0)
        end = datetime(2026, 3, 27, 0, 0, 0)

        with mock.patch.object(abcp_client, "ABCP_USERLOGIN", "user"), \
             mock.patch.object(abcp_client, "ABCP_USERPSW", "secret"), \
             mock.patch.object(abcp_client, "ABCP_BASE_URL", "https://example.test/cp/garage/"), \
             mock.patch.object(abcp_client, "RATE_LIMIT_SLEEP", 0), \
             mock.patch.object(abcp_client.SESSION, "get", return_value=response), \
             mock.patch("abcp_b24_garage_sync.abcp_client.audit_http_transaction") as audit_mock:
            result = abcp_client.fetch_garage(start, end)

        self.assertEqual(result, {"100": [{"id": 1}]})
        audit_mock.assert_called_once()
        self.assertEqual(audit_mock.call_args.kwargs["outcome"], "success")
        self.assertTrue(audit_mock.call_args.kwargs["ok"])
        self.assertEqual(audit_mock.call_args.kwargs["meta"]["items"], 1)
