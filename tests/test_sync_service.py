from __future__ import annotations

import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from abcp_b24_garage_sync import sync_service


@contextmanager
def _patched_env(values: dict[str, str]):
    with patch.dict(os.environ, values, clear=True):
        yield


class BuildUpdateFieldsTests(unittest.TestCase):
    def test_all_non_empty_env_codes_are_used(self) -> None:
        row = {
            "id": 10,
            "userId": 555,
            "name": "Test car",
            "comment": "note",
            "year": 2024,
            "vin": "VIN",
            "frame": "F",
            "mileage": 1000,
            "manufacturerId": 1,
            "manufacturer": "Brand",
            "modelId": 2,
            "model": "Model",
            "modificationId": 3,
            "modification": "Mod",
            "dateUpdated": "2024-01-01",
            "vehicleRegPlate": "123",
        }

        env = {
            "UF_B24_DEAL_GARAGE_ID": "UF_CODE_ID",
            "UF_B24_DEAL_GARAGE_USER_ID": "UF_CODE_USER_1",
            "UF_B24_DEAL_ABCP_USER_ID": "UF_CODE_USER_2",
            "UF_B24_DEAL_GARAGE_NAME": "UF_CODE_NAME",
            "UF_B24_DEAL_GARAGE_COMMENT": "UF_CODE_COMMENT",
            "UF_B24_DEAL_GARAGE_YEAR": "UF_CODE_YEAR",
            "UF_B24_DEAL_GARAGE_VIN": "UF_CODE_VIN",
            "UF_B24_DEAL_GARAGE_FRAME": "UF_CODE_FRAME",
            "UF_B24_DEAL_GARAGE_MILEAGE": "UF_CODE_MILEAGE",
            "UF_B24_DEAL_GARAGE_MANUFACTURER_ID": "UF_CODE_MANU_ID",
            "UF_B24_DEAL_GARAGE_MANUFACTURER": "UF_CODE_MANU",
            "UF_B24_DEAL_GARAGE_MODEL_ID": "UF_CODE_MODEL_ID",
            "UF_B24_DEAL_GARAGE_MODEL": "UF_CODE_MODEL",
            "UF_B24_DEAL_GARAGE_MODIFICATION_ID": "UF_CODE_MOD_ID",
            "UF_B24_DEAL_GARAGE_MODIFICATION": "UF_CODE_MOD",
            "UF_B24_DEAL_GARAGE_DATE_UPDATED": "UF_CODE_DATE",
            "UF_B24_DEAL_GARAGE_VEHICLE_REG_PLATE": "UF_CODE_PLATE",
        }

        with _patched_env(env):
            fields = sync_service._build_update_fields(row)

        self.assertEqual(fields["UF_CODE_ID"], "10")
        self.assertEqual(fields["UF_CODE_USER_1"], "555")
        self.assertEqual(fields["UF_CODE_USER_2"], "555")
        self.assertEqual(fields["UF_CODE_COMMENT"], "note")
        self.assertEqual(fields["UF_CODE_YEAR"], "2024")


class LocalSkipTests(unittest.TestCase):
    def test_skips_only_when_cached_deal_and_source_match(self) -> None:
        row = {
            "id": 123,
            "dateUpdated": "2026-03-26 12:00:00",
            "cachedDealId": 456,
            "cachedSourceGarageId": 123,
            "cachedSourceDateUpdated": "2026-03-26 12:00:00",
            "cachedSourcePayloadHash": None,
            "cachedLastResult": "updated",
        }
        self.assertTrue(sync_service._can_skip_remote_sync(row))

    def test_does_not_skip_without_cached_deal(self) -> None:
        row = {
            "id": 123,
            "dateUpdated": "2026-03-26 12:00:00",
            "cachedDealId": None,
            "cachedSourceGarageId": 123,
            "cachedSourceDateUpdated": "2026-03-26 12:00:00",
            "cachedSourcePayloadHash": None,
            "cachedLastResult": "updated",
        }
        self.assertFalse(sync_service._can_skip_remote_sync(row))

    def test_does_not_skip_after_error(self) -> None:
        row = {
            "id": 123,
            "dateUpdated": "2026-03-26 12:00:00",
            "cachedDealId": 456,
            "cachedSourceGarageId": 123,
            "cachedSourceDateUpdated": "2026-03-26 12:00:00",
            "cachedSourcePayloadHash": None,
            "cachedLastResult": "error",
        }
        self.assertFalse(sync_service._can_skip_remote_sync(row))

    def test_skips_by_payload_hash_even_when_source_row_changed(self) -> None:
        payload_hash = "abc123"
        row = {
            "id": 999,
            "dateUpdated": "2026-03-26 12:30:00",
            "cachedDealId": 456,
            "cachedSourceGarageId": 123,
            "cachedSourceDateUpdated": "2026-03-26 12:00:00",
            "cachedSourcePayloadHash": payload_hash,
            "cachedLastResult": "updated",
        }
        self.assertTrue(sync_service._can_skip_remote_sync(row, payload_hash))

    def test_payload_hash_is_stable_for_same_field_set(self) -> None:
        fields_a = {"UF_A": "1", "UF_B": "2"}
        fields_b = {"UF_B": "2", "UF_A": "1"}
        self.assertEqual(
            sync_service._stable_payload_hash(fields_a),
            sync_service._stable_payload_hash(fields_b),
        )


if __name__ == "__main__":
    unittest.main()
