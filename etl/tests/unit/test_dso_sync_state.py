# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for DSO Sync State Manager

Tests the SyncStateManager class that manages watermark state for delta sync
operations. All tests use mocked MSSQLResource and Dagster context — no
database connection is required.
"""

from contextlib import contextmanager
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from DSO.load.sync_state import SyncStateManager

pytestmark = pytest.mark.unit


def _create_mock_dso_resource():
    """Create a mock MSSQLResource whose get_connection() is a proper context manager.

    Returns (mock_resource, mock_conn, mock_cursor) so tests can set up
    cursor return values and assert calls on conn/cursor.
    """
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_resource = MagicMock()

    @contextmanager
    def _get_connection():
        yield mock_conn

    mock_resource.get_connection = _get_connection
    return mock_resource, mock_conn, mock_cursor


def _create_mock_context():
    """Create a mock Dagster AssetExecutionContext with a mock logger."""
    ctx = MagicMock()
    ctx.log = MagicMock()
    return ctx


# =============================================================================
# get_last_sync_timestamp
# =============================================================================


class TestGetLastSyncTimestamp:
    """Tests for SyncStateManager.get_last_sync_timestamp()."""

    def test_returns_none_for_new_table(self):
        """No watermark row exists — should return None (first run)."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = None

        mgr = SyncStateManager(resource, ctx)
        result = mgr.get_last_sync_timestamp("dim", "Kreditor")

        assert result is None
        cursor.execute.assert_called_once()

    def test_returns_datetime_for_existing_table(self):
        """Watermark row exists — should return the stored timestamp."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()
        expected_ts = datetime(2026, 2, 10, 14, 30, 0)
        cursor.fetchone.return_value = (expected_ts,)

        mgr = SyncStateManager(resource, ctx)
        result = mgr.get_last_sync_timestamp("dim", "Kreditor")

        assert result == expected_ts

    def test_returns_none_on_db_error(self):
        """Database error during read — should return None and log a warning."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()
        cursor.execute.side_effect = Exception("connection timeout")

        mgr = SyncStateManager(resource, ctx)
        result = mgr.get_last_sync_timestamp("fct", "Fakturalinje")

        assert result is None
        ctx.log.warning.assert_called_once()
        warning_msg = ctx.log.warning.call_args[0][0]
        assert "fct.Fakturalinje" in warning_msg
        assert "connection timeout" in warning_msg

    def test_sql_contains_correct_schema_and_table(self):
        """Verify the SELECT query references the right schema and table."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = None

        mgr = SyncStateManager(resource, ctx)
        mgr.get_last_sync_timestamp("fct", "Postering")

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[SyncWatermarks]" in sql
        assert "schema_name = 'fct'" in sql
        assert "table_name = 'Postering'" in sql
        assert "last_sync_timestamp" in sql


# =============================================================================
# update_sync_state
# =============================================================================


class TestUpdateSyncState:
    """Tests for SyncStateManager.update_sync_state()."""

    def test_generates_merge_sql(self):
        """Verify the MERGE SQL targets [meta].[SyncWatermarks] with SYSDATETIME()."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", rows_affected=100, duration_ms=500)

        sql = cursor.execute.call_args[0][0]
        assert "MERGE [meta].[SyncWatermarks] AS tgt" in sql
        assert "'dim' AS schema_name" in sql
        assert "'Kreditor' AS table_name" in sql
        assert "SYSDATETIME()" in sql
        assert "WHEN MATCHED THEN" in sql
        assert "WHEN NOT MATCHED THEN" in sql

    def test_duration_ms_null_when_none(self):
        """duration_ms=None should produce NULL in the SQL."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", duration_ms=None)

        sql = cursor.execute.call_args[0][0]
        assert "last_sync_duration_ms = NULL" in sql

    def test_duration_ms_set_when_provided(self):
        """duration_ms=1234 should appear as integer in the SQL."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", duration_ms=1234)

        sql = cursor.execute.call_args[0][0]
        assert "1234" in sql
        # Should NOT contain NULL for the duration field
        assert "last_sync_duration_ms = 1234" in sql

    def test_rows_affected_in_sql(self):
        """rows_affected value should appear in both UPDATE SET and INSERT VALUES."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("fct", "Fakturalinje", rows_affected=42000)

        sql = cursor.execute.call_args[0][0]
        assert "last_sync_rows = 42000" in sql
        assert "42000" in sql

    def test_calls_commit(self):
        """conn.commit() must be called exactly once after execute."""
        resource, mock_conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor")

        mock_conn.commit.assert_called_once()

    def test_logs_info_after_update(self):
        """context.log.info should be called with schema.table reference."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", rows_affected=75, status="success")

        ctx.log.info.assert_called_once()
        log_msg = ctx.log.info.call_args[0][0]
        assert "dim.Kreditor" in log_msg
        assert "rows=75" in log_msg

    def test_status_in_sql(self):
        """Custom status string should appear in the MERGE SQL."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", status="partial")

        sql = cursor.execute.call_args[0][0]
        assert "'partial'" in sql

    def test_default_status_is_success(self):
        """Default status should be 'success'."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor")

        sql = cursor.execute.call_args[0][0]
        assert "'success'" in sql

    def test_insert_values_match_update_set(self):
        """WHEN NOT MATCHED INSERT should contain the same schema/table as the source."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("fct", "Postering", rows_affected=300, duration_ms=999)

        sql = cursor.execute.call_args[0][0]
        insert_part = sql.split("WHEN NOT MATCHED")[1]
        assert "'fct'" in insert_part
        assert "'Postering'" in insert_part
        assert "300" in insert_part
        assert "999" in insert_part

    def test_last_sync_timestamp_uses_dso_sysdatetime(self):
        """last_sync_timestamp must be set from DSO SYSDATETIME() to avoid clock skew (not client time)."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("dim", "Kreditor", rows_affected=10)

        sql = cursor.execute.call_args[0][0]
        assert "SYSDATETIME()" in sql
        assert "last_sync_timestamp" in sql
        assert "updated_at" in sql
        # Both timestamp columns must use SYSDATETIME (server time), not a passed-in value
        assert sql.count("SYSDATETIME()") >= 2

    def test_passed_watermark_replaces_the_server_clock(self):
        """A source high-water mark is stored as-is; only updated_at keeps the server clock."""
        resource, _conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state(
            "sfi_datapoint", "refresh_daily", watermark=datetime(2026, 10, 16, 8, 41, 48)
        )

        sql = cursor.execute.call_args[0][0]
        assert "last_sync_timestamp = CAST('2026-10-16 08:41:48' AS datetime2(0))" in sql
        assert (
            "VALUES ('sfi_datapoint', 'refresh_daily', CAST('2026-10-16 08:41:48' AS datetime2(0))"
            in sql
        )
        assert sql.count("SYSDATETIME()") == 1


# =============================================================================
# Watermark skip contract
# =============================================================================


class TestWatermarkSkipContract:
    """Verify that SyncStateManager always writes when called (it's a dumb writer).

    The guard against advancing the watermark on zero rows lives at the CALL
    SITES in sync_utils.py, NOT in SyncStateManager itself. These tests
    document that contract: callers MUST check rows_affected > 0 before
    calling update_sync_state().
    """

    def test_update_sync_state_writes_even_with_zero_rows(self):
        """SyncStateManager.update_sync_state() does NOT skip zero-row writes —
        callers are responsible for guarding."""
        resource, mock_conn, cursor = _create_mock_dso_resource()
        ctx = _create_mock_context()

        mgr = SyncStateManager(resource, ctx)
        mgr.update_sync_state("fct", "Fakturalinje", rows_affected=0, duration_ms=50)

        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "MERGE" in sql
        assert "last_sync_rows = 0" in sql
        mock_conn.commit.assert_called_once()
