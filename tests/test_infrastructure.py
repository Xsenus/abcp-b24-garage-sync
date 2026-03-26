from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

from abcp_b24_garage_sync import db
from abcp_b24_garage_sync import main as cli_main


class FrozenDateTime(datetime):
    current = datetime(2026, 3, 26, 12, 0, 0)

    @classmethod
    def now(cls, tz=None):
        if tz is not None:
            return tz.fromutc(cls.current.replace(tzinfo=tz))
        return cls.current


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
            env = {"ABCP_B24_PROJECT_ROOT": str(project_root)}
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


class FetchStateTests(unittest.TestCase):
    def test_error_does_not_reset_last_success_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = {
                "ABCP_B24_DATA_DIR": tmp,
                "ABCP_B24_PROJECT_ROOT": tmp,
            }
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(db, "SQLITE_PATH", "garage.sqlite"):
                db.init_db()
                db.save_fetch_state(
                    source="abcp_garage",
                    requested_from="2026-03-26 10:00:00",
                    requested_to="2026-03-26 10:30:00",
                    success_from="2026-03-26 10:00:00",
                    success_to="2026-03-26 10:30:00",
                    status="success",
                )
                db.save_fetch_state(
                    source="abcp_garage",
                    requested_from="2026-03-26 10:30:00",
                    requested_to="2026-03-26 11:00:00",
                    success_from=None,
                    success_to=None,
                    status="error",
                    error="network",
                )

                state = db.get_fetch_state("abcp_garage")

        self.assertIsNotNone(state)
        self.assertEqual(state["lastSuccessTo"], "2026-03-26 10:30:00")
        self.assertEqual(state["lastStatus"], "error")

    def test_init_db_adds_payload_hash_columns(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env = {
                "ABCP_B24_DATA_DIR": tmp,
                "ABCP_B24_PROJECT_ROOT": tmp,
            }
            with mock.patch.dict(os.environ, env, clear=True), \
                 mock.patch.object(db, "SQLITE_PATH", "garage.sqlite"):
                db.init_db()
                with db.connect() as c:
                    sync_status_cols = {row["name"] for row in c.execute("PRAGMA table_info(sync_status)").fetchall()}
                    sync_audit_cols = {row["name"] for row in c.execute("PRAGMA table_info(sync_audit)").fetchall()}

        self.assertIn("sourcePayloadHash", sync_status_cols)
        self.assertIn("sourcePayloadHash", sync_audit_cols)


class AutoPeriodTests(unittest.TestCase):
    def test_uses_fetch_state_cursor_with_overlap(self) -> None:
        fake_log = mock.Mock()
        with mock.patch("abcp_b24_garage_sync.main.datetime", FrozenDateTime), \
             mock.patch("abcp_b24_garage_sync.db.get_fetch_state", return_value={"lastSuccessTo": "2026-03-26 11:50:00"}), \
             mock.patch("abcp_b24_garage_sync.config.ABCP_INCREMENTAL_OVERLAP_MINUTES", 5):
            dt_from, dt_to = cli_main._resolve_auto_period(fake_log)

        self.assertEqual(dt_from, datetime(2026, 3, 26, 11, 45, 0))
        self.assertEqual(dt_to, datetime(2026, 3, 26, 12, 0, 0))

    def test_bootstrap_uses_configured_lookback_years(self) -> None:
        fake_log = mock.Mock()
        with mock.patch("abcp_b24_garage_sync.main.datetime", FrozenDateTime), \
             mock.patch("abcp_b24_garage_sync.db.get_fetch_state", return_value=None), \
             mock.patch("abcp_b24_garage_sync.config.ABCP_INITIAL_LOOKBACK_YEARS", 2):
            dt_from, dt_to = cli_main._resolve_auto_period(fake_log)

        self.assertEqual(dt_from, datetime(2024, 1, 1, 0, 0, 0))
        self.assertEqual(dt_to, datetime(2026, 3, 26, 12, 0, 0))


class LoopModeTests(unittest.TestCase):
    def test_loop_respects_limit_env(self) -> None:
        env = {"ABCP_B24_LOOP_LIMIT": "2"}
        with mock.patch.dict(os.environ, env, clear=False):
            with mock.patch("abcp_b24_garage_sync.main._execute_sync") as execute_mock, \
                 mock.patch("abcp_b24_garage_sync.main.time.sleep") as sleep_mock:
                cli_main.main(["--from", "2024-01-01", "--to", "2024-01-02", "--loop-every", "1"])

        self.assertEqual(execute_mock.call_count, 2)
        self.assertEqual(sleep_mock.call_count, 1)

    def test_loop_uses_auto_period_when_dates_missing(self) -> None:
        env = {"ABCP_B24_LOOP_LIMIT": "1"}
        with mock.patch.dict(os.environ, env, clear=False):
            with mock.patch("abcp_b24_garage_sync.main._execute_sync") as execute_mock:
                cli_main.main(["--loop-every", "30"])

        execute_mock.assert_called_once()
        args, _ = execute_mock.call_args
        parsed_args = args[0]
        self.assertIsNone(parsed_args.date_from)
        self.assertIsNone(parsed_args.date_to)
        self.assertTrue(getattr(parsed_args, "auto_period", False))


if __name__ == "__main__":
    unittest.main()
