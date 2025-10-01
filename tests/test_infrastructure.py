from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from abcp_b24_garage_sync import db
from abcp_b24_garage_sync import main as cli_main


class DbPathTests(unittest.TestCase):
    def test_absolute_path_is_used_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            absolute = Path(tmp) / "db.sqlite"
            resolved = db._resolve_db_path(str(absolute))
            self.assertEqual(resolved, absolute)
            self.assertTrue(resolved.parent.exists())

    def test_relative_path_uses_data_dir_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            with mock.patch.dict(os.environ, {"ABCP_B24_DATA_DIR": str(data_dir)}, clear=True):
                resolved = db._resolve_db_path("storage/garage.sqlite")
                self.assertTrue(resolved.is_absolute())
                self.assertTrue(str(resolved).startswith(str(data_dir)))
                self.assertEqual(resolved.name, "garage.sqlite")
                self.assertTrue(resolved.parent.exists())

    def test_relative_path_falls_back_to_project_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project_root = Path(tmp)
            env = {
                "ABCP_B24_PROJECT_ROOT": str(project_root),
            }
            with mock.patch.dict(os.environ, env, clear=True):
                resolved = db._resolve_db_path("garage.sqlite")
                self.assertEqual(resolved, project_root / "garage.sqlite")


class DiscoverEnvFileTests(unittest.TestCase):
    def test_prefers_explicit_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "current"
            project.mkdir()
            override = Path(tmp) / "custom.env"
            override.write_text("TEST=1")
            env = {"ABCP_B24_ENV_FILE": str(override)}
            with mock.patch.dict(os.environ, env, clear=True):
                discovered = cli_main._discover_env_file(project)
            self.assertEqual(discovered, override)

    def test_project_env_before_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "current"
            project.mkdir()
            project_env = project / ".env"
            parent_env = Path(tmp) / ".env"
            parent_env.write_text("PARENT=1")
            project_env.write_text("PROJECT=1")

            with mock.patch.dict(os.environ, {}, clear=True):
                discovered = cli_main._discover_env_file(project)

            self.assertEqual(discovered, project_env)

    def test_none_when_no_env_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            project = Path(tmp) / "current"
            project.mkdir()

            with mock.patch.dict(os.environ, {}, clear=True):
                discovered = cli_main._discover_env_file(project)

            self.assertIsNone(discovered)


if __name__ == "__main__":
    unittest.main()
