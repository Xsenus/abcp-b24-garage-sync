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
        # второй код для userId также должен присутствовать
        self.assertEqual(fields["UF_CODE_USER_2"], "555")
        self.assertEqual(fields["UF_CODE_COMMENT"], "note")


if __name__ == "__main__":
    unittest.main()
