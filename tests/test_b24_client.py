from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
