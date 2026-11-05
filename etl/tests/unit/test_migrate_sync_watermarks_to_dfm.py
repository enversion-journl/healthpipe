# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SyncWatermarks → DFMState migrator CLI helpers."""

from __future__ import annotations

import os
import unittest
from unittest.mock import MagicMock, patch

import pytest
from DSO.load.migrate_sync_watermarks_to_dfm import (
    MigrationStats,
    _target_type_for_schema,
    main,
    migrate_sync_watermarks_to_dfm,
    mssql_from_env,
)


class TestTargetTypeForSchema:
    def test_known_schemas(self):
        assert _target_type_for_schema("dim") == "dimension"
        assert _target_type_for_schema("fhir") == "fhir_table"
        assert _target_type_for_schema("sec") == "security"

    def test_unknown_schema_passthrough(self):
        assert _target_type_for_schema("custom") == "custom"


class TestMssqlFromEnv:
    def test_builds_resource_from_prefix(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MSSQL_TARGET_DSO_HOST", "dso-host")
        monkeypatch.setenv("MSSQL_TARGET_DSO_PORT", "1434")
        monkeypatch.setenv("MSSQL_TARGET_DSO_DB", "DSO_DB")
        monkeypatch.setenv("MSSQL_TARGET_DSO_USER", "dso_user")
        monkeypatch.setenv("MSSQL_TARGET_DSO_PASSWORD", "secret")

        res = mssql_from_env("MSSQL_TARGET_DSO")
        assert res.host == "dso-host"
        assert res.port == 1434
        assert res.database == "DSO_DB"
        assert res.user == "dso_user"
        assert res.password == "secret"

    def test_missing_password_exits(self, monkeypatch: pytest.MonkeyPatch):
        for key in list(os.environ):
            if key.startswith("MSSQL_TARGET_DSO_"):
                monkeypatch.delenv(key, raising=False)
        with pytest.raises(SystemExit, match="MSSQL_TARGET_DSO_PASSWORD"):
            mssql_from_env("MSSQL_TARGET_DSO")


class TestMigrateSyncWatermarksToDfm:
    def test_no_rows_short_circuits(self):
        dso = MagicMock()
        staging = MagicMock()
        dso_conn = MagicMock()
        dso_cursor = MagicMock()
        dso_cursor.fetchall.return_value = []
        dso_conn.cursor.return_value = dso_cursor
        dso.get_connection.return_value.__enter__.return_value = dso_conn

        stats = migrate_sync_watermarks_to_dfm(dso, staging)
        assert stats == MigrationStats(inserted=0, updated=0, skipped=0)
        staging.get_connection.assert_not_called()
        select_sql = dso_cursor.execute.call_args[0][0]
        assert "DATEADD(DAY, -1, [last_sync_timestamp])" in select_sql

    def test_merges_non_sfi_rows(self):
        dso = MagicMock()
        staging = MagicMock()

        dso_conn = MagicMock()
        dso_cursor = MagicMock()
        dso_cursor.fetchall.return_value = [
            ("dso", "dim", "Kreditor", "2025-01-01 00:00:00.000"),
            ("dso", "fhir", "Patient", "2025-02-01 00:00:00.000"),
        ]
        dso_conn.cursor.return_value = dso_cursor
        dso.get_connection.return_value.__enter__.return_value = dso_conn

        stg_conn = MagicMock()
        stg_cursor = MagicMock()
        stg_cursor.fetchall.side_effect = [[("INSERT",)], [("INSERT",)]]
        stg_conn.cursor.return_value = stg_cursor
        staging.get_connection.return_value.__enter__.return_value = stg_conn

        stats = migrate_sync_watermarks_to_dfm(dso, staging)
        assert stats == MigrationStats(inserted=2, updated=0, skipped=0)
        assert stg_cursor.execute.call_count == 2
        stg_conn.commit.assert_called_once()
        merge_sql = stg_cursor.execute.call_args_list[0][0][0]
        assert "WHEN MATCHED AND tgt.[checkpoint_value] IS NULL THEN" in merge_sql
        assert "OUTPUT $action" in merge_sql
        select_sql = dso_cursor.execute.call_args[0][0]
        assert "DATEADD(DAY, -1, [last_sync_timestamp])" in select_sql
        first_args = stg_cursor.execute.call_args_list[0][0][1]
        assert first_args[0] == "dso"
        assert first_args[1] == "dimension"
        assert first_args[2] == "dim"
        assert first_args[3] == "Kreditor"

    def test_updates_null_seeds_and_skips_existing(self):
        dso = MagicMock()
        staging = MagicMock()

        dso_conn = MagicMock()
        dso_cursor = MagicMock()
        dso_cursor.fetchall.return_value = [
            ("dso", "dim", "Kreditor", "2025-01-01 00:00:00.000"),
            ("dso", "fhir", "Patient", "2025-02-01 00:00:00.000"),
            ("dso", "fct", "Faktura", "2025-03-01 00:00:00.000"),
        ]
        dso_conn.cursor.return_value = dso_cursor
        dso.get_connection.return_value.__enter__.return_value = dso_conn

        stg_conn = MagicMock()
        stg_cursor = MagicMock()
        # INSERT new / UPDATE NULL seed / skip (no OUTPUT row)
        stg_cursor.fetchall.side_effect = [[("INSERT",)], [("UPDATE",)], []]
        stg_conn.cursor.return_value = stg_cursor
        staging.get_connection.return_value.__enter__.return_value = stg_conn

        stats = migrate_sync_watermarks_to_dfm(dso, staging)
        assert stats == MigrationStats(inserted=1, updated=1, skipped=1)

    def test_sfi_store_watermark_becomes_the_store_checkpoint_three_hours_back(self):
        """Without it the store's first refresh after deploy rebuilds the whole store."""
        dso = MagicMock()
        staging = MagicMock()
        dso_conn = MagicMock()
        dso_cursor = MagicMock()
        dso_cursor.fetchall.return_value = [
            ("sfi_store", "fhir", "SFI_datapoint_store", "2026-10-23 03:48:57.000"),
        ]
        dso_conn.cursor.return_value = dso_cursor
        dso.get_connection.return_value.__enter__.return_value = dso_conn
        stg_conn = MagicMock()
        stg_cursor = MagicMock()
        stg_cursor.fetchall.side_effect = [[("INSERT",)]]
        stg_conn.cursor.return_value = stg_cursor
        staging.get_connection.return_value.__enter__.return_value = stg_conn

        stats = migrate_sync_watermarks_to_dfm(dso, staging)

        assert stats == MigrationStats(inserted=1, updated=0, skipped=0)
        select_sql, select_params = dso_cursor.execute.call_args[0]
        store_leg = select_sql.split("UNION ALL")[1]
        assert "N'sfi_store'" in store_leg
        assert "DATEADD(HOUR, -3, [last_sync_timestamp])" in store_leg
        assert "[schema_name] = ? AND [table_name] = ?" in store_leg
        assert select_params == ("sfi_datapoint", "sfi_datapoint", "refresh_daily")
        # The store's own run reads this row: scope, target type, schema and table must match.
        assert stg_cursor.execute.call_args[0][1] == (
            "sfi_store",
            "fhir_table",
            "fhir",
            "SFI_datapoint_store",
            "2026-10-23 03:48:57.000",
        )


class TestMainEntry:
    def test_main_wires_env_resources_and_migrates(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("MSSQL_TARGET_DSO_PASSWORD", "dso-pw")
        monkeypatch.delenv("MSSQL_TARGET_STAGING_PASSWORD", raising=False)
        monkeypatch.setenv("MSSQL_SOURCE_STAGING_PASSWORD", "stg-pw")

        with (
            patch(
                "DSO.load.migrate_sync_watermarks_to_dfm.mssql_from_env",
                side_effect=lambda prefix: MagicMock(
                    name=prefix, user="u", host="h", database="db"
                ),
            ) as mock_from_env,
            patch(
                "DSO.load.migrate_sync_watermarks_to_dfm.migrate_sync_watermarks_to_dfm",
                return_value=MigrationStats(inserted=3, updated=0, skipped=0),
            ) as mock_migrate,
        ):
            assert main([]) == 0

        assert mock_from_env.call_args_list[0].args == ("MSSQL_TARGET_DSO",)
        assert mock_from_env.call_args_list[1].args == ("MSSQL_SOURCE_STAGING",)
        mock_migrate.assert_called_once()


class TestMigrateAssetRegistration:
    def test_asset_deps_and_name(self):
        from adhoc.migrate_dso_sync_watermarks import (
            migrate_dso_sync_watermarks_to_dfm as asset_fn,
        )

        assert next(iter(asset_fn.keys)).to_user_string() == "migrate_dso_sync_watermarks_to_dfm"
        dep_keys = {k.to_user_string() for k in asset_fn.dependency_keys}
        assert "staging_infra_dfm" in dep_keys
        assert "dso_infra_meta" in dep_keys

    def test_job_selects_asset(self):
        from jobs.infrastructure_jobs import migrate_dso_sync_watermarks_to_dfm_job

        assert migrate_dso_sync_watermarks_to_dfm_job.name == (
            "migrate_dso_sync_watermarks_to_dfm_job"
        )


if __name__ == "__main__":
    unittest.main()
