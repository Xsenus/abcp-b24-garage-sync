from __future__ import annotations

import unittest
from unittest import mock

from abcp_b24_garage_sync import b24_client


class BatchHelpersTests(unittest.TestCase):
    def test_build_batch_command_flattens_nested_params(self) -> None:
        command = b24_client._build_batch_command(
            "crm.deal.update",
            {
                "id": 7,
                "fields": {"UF_TEST": "abc"},
                "params": {"REGISTER_SONET_EVENT": "N"},
            },
        )

        self.assertIn("crm.deal.update?", command)
        self.assertIn("id=7", command)
        self.assertIn("fields%5BUF_TEST%5D=abc", command)
        self.assertIn("params%5BREGISTER_SONET_EVENT%5D=N", command)

    def test_extract_batch_payload_reads_nested_result_shape(self) -> None:
        payload = {
            "result": {
                "result": {"cmd_1": {"ID": "1"}},
                "result_error": {"cmd_2": {"error": "failed"}},
            }
        }

        results, errors = b24_client._extract_batch_payload(payload)
        self.assertEqual(results["cmd_1"]["ID"], "1")
        self.assertIn("cmd_2", errors)


class CallAuditTests(unittest.TestCase):
    def test_call_audits_successful_response(self) -> None:
        response = mock.Mock()
        response.status_code = 200
        response.ok = True
        response.content = b'{"result":[{"ID":"1"}]}'
        response.json.return_value = {"result": [{"ID": "1"}]}

        with mock.patch.object(b24_client.SESSION, "post", return_value=response), \
             mock.patch("abcp_b24_garage_sync.b24_client.audit_http_transaction") as audit_mock, \
             mock.patch.object(b24_client, "RATE_LIMIT_SLEEP", 0):
            result = b24_client._call("crm.deal.list", {"filter": {"ID": "1"}})

        self.assertEqual(result, [{"ID": "1"}])
        audit_mock.assert_called_once()
        self.assertEqual(audit_mock.call_args.kwargs["outcome"], "success")
        self.assertTrue(audit_mock.call_args.kwargs["ok"])
        self.assertEqual(audit_mock.call_args.kwargs["meta"]["bitrix_method"], "crm.deal.list")

    def test_call_audits_api_error(self) -> None:
        response = mock.Mock()
        response.status_code = 200
        response.ok = True
        response.content = b'{"error":"INVALID","error_description":"Bad request"}'
        response.json.return_value = {
            "error": "INVALID",
            "error_description": "Bad request",
        }

        with mock.patch.object(b24_client.SESSION, "post", return_value=response), \
             mock.patch("abcp_b24_garage_sync.b24_client.audit_http_transaction") as audit_mock, \
             mock.patch.object(b24_client, "RATE_LIMIT_SLEEP", 0):
            with self.assertRaises(RuntimeError):
                b24_client._call("crm.deal.list", {"filter": {"ID": "1"}})

        audit_mock.assert_called_once()
        self.assertEqual(audit_mock.call_args.kwargs["outcome"], "api_error")
        self.assertFalse(audit_mock.call_args.kwargs["ok"])


if __name__ == "__main__":
    unittest.main()
