# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for EtlWatermarkManager

Tests the EtlWatermarkManager class that tracks delta state for ETL watermarks.
All tests use mocked MSSQLResource and Dagster context — no database connection
is required.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

from core.etl_watermark_manager import EtlWatermarkManager


def _create_mock_resource():
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
    ctx.run_id = "test-run-id-abc123"
    return ctx


# =============================================================================
# get_watermark
# =============================================================================


class TestGetWatermark:
    """Tests for EtlWatermarkManager.get_watermark()."""

    def test_returns_none_when_no_row(self):
        """No watermark row exists — should return None (first run)."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = None

        mgr = EtlWatermarkManager(resource, ctx)
        result = mgr.get_watermark("stg_fct", "stg", "Lagerbeholdning")

        assert result is None
        cursor.execute.assert_called_once()

    def test_returns_string_when_row_exists(self):
        """Watermark row exists — should return the stored string."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        expected = "2026-03-15 10:00:00"
        cursor.fetchone.return_value = (expected,)

        mgr = EtlWatermarkManager(resource, ctx)
        result = mgr.get_watermark("stg_fct", "stg", "Lagerbeholdning")

        assert result == expected

    def test_queries_correct_table(self):
        """Verify the SELECT query targets [meta].[etl_watermark_state]."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = None

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.get_watermark("stg_fct", "stg", "Vareudlevering")

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[etl_watermark_state]" in sql
        assert "watermark_value" in sql
        assert "entity_type = 'stg_fct'" in sql
        assert "schema_name = 'stg'" in sql
        assert "table_name = 'Vareudlevering'" in sql

    def test_returns_none_and_logs_warning_on_error(self):
        """Database error — should return None and log a warning."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.execute.side_effect = Exception("timeout reading watermark")

        mgr = EtlWatermarkManager(resource, ctx)
        result = mgr.get_watermark("stg_fct", "stg", "Lagerbeholdning")

        assert result is None
        ctx.log.warning.assert_called_once()
        warning_msg = ctx.log.warning.call_args[0][0]
        assert "stg.Lagerbeholdning" in warning_msg
        assert "timeout reading watermark" in warning_msg


# =============================================================================
# advance_watermark
# =============================================================================


class TestAdvanceWatermark:
    """Tests for EtlWatermarkManager.advance_watermark()."""

    def test_uses_merge_with_sysdatetime(self):
        """Verify the MERGE SQL uses SYSDATETIME() for the watermark value."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "MERGE" in sql
        assert "SYSDATETIME()" in sql

    def test_has_matched_clause(self):
        """MERGE must have a WHEN MATCHED THEN UPDATE clause."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "WHEN MATCHED THEN" in sql

    def test_has_not_matched_clause(self):
        """MERGE must have a WHEN NOT MATCHED THEN INSERT clause."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "WHEN NOT MATCHED THEN" in sql

    def test_targets_state_table(self):
        """MERGE must target [meta].[etl_watermark_state]."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("stg_dim", "stg", "Vare")

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[etl_watermark_state]" in sql
        assert "'stg_dim'" in sql
        assert "'stg'" in sql
        assert "'Vare'" in sql

    def test_calls_commit(self):
        """conn.commit() must be called exactly once after execute."""
        resource, mock_conn, _cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("stg_fct", "stg", "Lagerbeholdning")

        mock_conn.commit.assert_called_once()


# =============================================================================
# log_start_run
# =============================================================================


class TestLogStartRun:
    """Tests for EtlWatermarkManager.log_start_run()."""

    def test_inserts_and_returns_id(self):
        """INSERT with OUTPUT INSERTED.id — must return the log entry id."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (42,)

        mgr = EtlWatermarkManager(resource, ctx)
        log_id = mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning")

        assert log_id == 42
        cursor.execute.assert_called_once()

    def test_sql_targets_log_table(self):
        """INSERT must target [meta].[etl_watermark_log]."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[etl_watermark_log]" in sql
        assert "INSERT" in sql
        assert "OUTPUT INSERTED" in sql

    def test_status_is_running(self):
        """The inserted row must have status='running'."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "'running'" in sql

    def test_run_started_at_uses_sysdatetime(self):
        """run_started_at must be set from SYSDATETIME() server-side."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        assert "SYSDATETIME()" in sql

    def test_passes_optional_watermark_before(self):
        """watermark_before string should appear in the SQL when provided."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (5,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run(
            "stg_fct", "stg", "Lagerbeholdning", watermark_before="2026-03-01 08:00:00"
        )

        sql = cursor.execute.call_args[0][0]
        assert "2026-03-01" in sql

    def test_uses_null_for_unset_watermark_before(self):
        """watermark_before=None should produce NULL in the SQL."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning", watermark_before=None)

        sql = cursor.execute.call_args[0][0]
        assert "NULL" in sql

    def test_passes_optional_dagster_run_id(self):
        """dagster_run_id should appear in the SQL when provided."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (3,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning", dagster_run_id="run-xyz-999")

        sql = cursor.execute.call_args[0][0]
        assert "run-xyz-999" in sql

    def test_uses_null_for_unset_dagster_run_id(self):
        """dagster_run_id=None should produce NULL in the SQL."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("stg_fct", "stg", "Lagerbeholdning", dagster_run_id=None)

        sql = cursor.execute.call_args[0][0]
        assert "NULL" in sql


# =============================================================================
# log_complete_run
# =============================================================================


class TestLogCompleteRun:
    """Tests for EtlWatermarkManager.log_complete_run()."""

    def test_updates_status_to_success(self):
        """UPDATE must set status='success'."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=42)

        sql = cursor.execute.call_args[0][0]
        assert "'success'" in sql
        assert "UPDATE" in sql

    def test_uses_sysdatetime_for_completed_at(self):
        """run_completed_at must be set from SYSDATETIME() server-side."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=42)

        sql = cursor.execute.call_args[0][0]
        assert "SYSDATETIME()" in sql

    def test_targets_log_table(self):
        """UPDATE must target [meta].[etl_watermark_log]."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=7)

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[etl_watermark_log]" in sql

    def test_filters_by_log_id(self):
        """WHERE clause must filter by the provided log_id."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=99)

        sql = cursor.execute.call_args[0][0]
        assert "99" in sql

    def test_watermark_after_in_sql_when_provided(self):
        """watermark_after string should appear in the SQL when provided."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=10, watermark_after="2026-04-01 12:00:00")

        sql = cursor.execute.call_args[0][0]
        assert "2026-04-01" in sql

    def test_rows_affected_in_sql_when_provided(self):
        """rows_affected should appear in the SQL when provided."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=10, rows_affected=500)

        sql = cursor.execute.call_args[0][0]
        assert "500" in sql

    def test_calls_commit(self):
        """conn.commit() must be called exactly once after execute."""
        resource, mock_conn, _cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=10)

        mock_conn.commit.assert_called_once()


# =============================================================================
# log_fail_run
# =============================================================================


class TestLogFailRun:
    """Tests for EtlWatermarkManager.log_fail_run()."""

    def test_updates_status_to_failed(self):
        """UPDATE must set status='failed'."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_fail_run(log_id=55)

        sql = cursor.execute.call_args[0][0]
        assert "'failed'" in sql
        assert "UPDATE" in sql

    def test_uses_sysdatetime_for_completed_at(self):
        """run_completed_at must be set from SYSDATETIME() server-side."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_fail_run(log_id=55)

        sql = cursor.execute.call_args[0][0]
        assert "SYSDATETIME()" in sql

    def test_targets_log_table(self):
        """UPDATE must target [meta].[etl_watermark_log]."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_fail_run(log_id=55)

        sql = cursor.execute.call_args[0][0]
        assert "[meta].[etl_watermark_log]" in sql

    def test_filters_by_log_id(self):
        """WHERE clause must filter by the provided log_id."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_fail_run(log_id=77)

        sql = cursor.execute.call_args[0][0]
        assert "77" in sql

    def test_calls_commit(self):
        """conn.commit() must be called exactly once after execute."""
        resource, mock_conn, _cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_fail_run(log_id=55)

        mock_conn.commit.assert_called_once()
