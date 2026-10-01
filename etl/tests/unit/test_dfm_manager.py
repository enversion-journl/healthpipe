# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for DfmManager.

Tests with a mocked DB connection covering:
  - get_checkpoint: returns value, returns None for missing row, raises on DB error
  - start_dfm_run: checkpoint resolution (own, checkpoint_from, non-owner)
  - set_checkpoint: parameterized MERGE with explicit value and SYSDATETIME fallback
  - complete_dfm_run / fail_dfm_run: status updates
  - fail_dfm_run resilience: never raises, retries on a fresh connection
  - sweep_stale_runs: stale 'running' rows → 'abandoned'; runs at start_dfm_run
  - _ensure_state_row_and_get_checkpoint: first-run auto-creation, DB error propagation

Contract tests:
  - SCHEMA_TO_TARGET_TYPE matches the stored procedure CASE mapping
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from core.dfm_manager import (
    CHECKPOINT_OWNER_SCHEMAS,
    SCHEMA_TO_TARGET_TYPE,
    DfmManager,
    DfmRunContext,
)

pytestmark = pytest.mark.unit

# ===========================================================================
# Helpers
# ===========================================================================


class FakeCursor:
    """Minimal cursor mock that tracks executed SQL and returns configured results."""

    def __init__(self, fetchone_results=None):
        self.executed = []
        self.executed_params = []
        self._fetchone_results = list(fetchone_results or [])
        self._call_idx = 0
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.executed.append(sql)
        self.executed_params.append(params)

    def fetchone(self):
        if self._call_idx < len(self._fetchone_results):
            result = self._fetchone_results[self._call_idx]
            self._call_idx += 1
            return result
        return None

    def close(self):
        pass


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass


def _make_dfm(fetchone_results=None):
    """Create a DfmManager with a mocked MSSQLResource and context."""
    fake_cursor = FakeCursor(fetchone_results)
    fake_conn = FakeConnection(fake_cursor)

    mssql = MagicMock()

    @contextmanager
    def _get_conn():
        yield fake_conn

    mssql.get_connection = _get_conn

    context = MagicMock()
    context.run_id = "test-run-123"
    context.log = MagicMock()

    dfm = DfmManager(mssql, context)
    return dfm, fake_cursor, context


# ===========================================================================
# get_checkpoint
# ===========================================================================


class TestGetCheckpoint:
    def test_returns_checkpoint_value_when_row_exists(self):
        dfm, cursor, _ = _make_dfm(fetchone_results=[("2025-06-01 12:00:00",)])
        result = dfm.get_checkpoint("arc", "ifdb_encounter")

        assert result == "2025-06-01 12:00:00"
        assert len(cursor.executed) == 1
        assert "checkpoint_value" in cursor.executed[0]
        assert cursor.executed_params[0] == ("staging", "archive", "arc", "ifdb_encounter")

    def test_returns_none_when_row_not_found(self):
        dfm, _, _ = _make_dfm(fetchone_results=[None])
        result = dfm.get_checkpoint("arc", "ifdb_encounter")
        assert result is None

    def test_raises_on_unknown_schema(self):
        dfm, _, _ = _make_dfm()
        with pytest.raises(ValueError, match="Unknown schema"):
            dfm.get_checkpoint("nonexistent_schema", "some_table")

    def test_raises_on_db_error(self):
        """DB errors propagate instead of silently falling back to full-load."""
        mssql = MagicMock()

        @contextmanager
        def _boom():
            conn = MagicMock()
            cursor = MagicMock()
            cursor.execute.side_effect = RuntimeError("connection lost")
            conn.cursor.return_value = cursor
            yield conn

        mssql.get_connection = _boom
        context = MagicMock()
        dfm = DfmManager(mssql, context)

        with pytest.raises(RuntimeError, match="connection lost"):
            dfm.get_checkpoint("arc", "ifdb_encounter")


# ===========================================================================
# start_dfm_run
# ===========================================================================


class TestStartDfmRun:
    def test_checkpoint_owner_reads_own_state(self):
        """Checkpoint owner (arc) without checkpoint_from reads its own DFMState."""
        dfm, cursor, _ = _make_dfm(
            fetchone_results=[
                ("2025-01-01 00:00:00",),  # _ensure_state_row SELECT
                (42,),  # _insert_run_log OUTPUT
            ]
        )
        result = dfm.start_dfm_run("arc", "ifdb_encounter", dagster_run_id="run-1", mode="delta")

        assert isinstance(result, DfmRunContext)
        assert result.checkpoint == "2025-01-01 00:00:00"
        assert result.log_id == 42

    def test_checkpoint_from_reads_other_state(self):
        """checkpoint_from delegates to get_checkpoint on another entry."""
        dfm, cursor, _ = _make_dfm(
            fetchone_results=[
                ("2025-03-15 08:00:00",),  # get_checkpoint SELECT
                (99,),  # _insert_run_log OUTPUT
            ]
        )
        result = dfm.start_dfm_run(
            "ext",
            "ifdb_encounter",
            checkpoint_from=("arc", "ifdb_encounter"),
            dagster_run_id="run-2",
            mode="delta",
        )

        assert result.checkpoint == "2025-03-15 08:00:00"
        assert result.log_id == 99

    def test_non_owner_schema_gets_none_checkpoint(self):
        """Non-checkpoint-owner (map) gets checkpoint=None, no DFMState read."""
        dfm, cursor, _ = _make_dfm(
            fetchone_results=[
                (7,),  # _insert_run_log OUTPUT
            ]
        )
        result = dfm.start_dfm_run("map", "Lagerbeholdning", dagster_run_id="run-3", mode="delta")

        assert result.checkpoint is None
        assert result.log_id == 7

    def test_first_run_creates_state_row(self):
        """First run for a checkpoint owner creates a DFMState row and returns None."""
        dfm, cursor, ctx = _make_dfm(
            fetchone_results=[
                None,  # _ensure_state_row SELECT → row not found
                (1,),  # _insert_run_log OUTPUT
            ]
        )
        result = dfm.start_dfm_run("arc", "new_table", dagster_run_id="run-4", mode="delta")

        assert result.checkpoint is None
        assert result.log_id == 1
        insert_calls = [s for s in cursor.executed if "INSERT INTO" in s and "DFMState" in s]
        assert len(insert_calls) == 1
        ctx.log.info.assert_called()


# ===========================================================================
# set_checkpoint
# ===========================================================================


class TestSetCheckpoint:
    def test_explicit_value_uses_coalesce_with_param(self):
        dfm, cursor, _ = _make_dfm()
        dfm.set_checkpoint("arc", "ifdb_encounter", value="2025-06-01 12:00:00")

        assert len(cursor.executed) == 1
        sql = cursor.executed[0]
        assert "COALESCE(?" in sql
        assert "SYSDATETIME()" in sql
        params = cursor.executed_params[0]
        assert "2025-06-01 12:00:00" in params
        assert "delta" in params

    def test_none_value_passes_null_for_sysdatetime_fallback(self):
        dfm, cursor, _ = _make_dfm()
        dfm.set_checkpoint("arc", "ifdb_encounter")

        params = cursor.executed_params[0]
        value_params = [p for p in params if p is None]
        assert len(value_params) >= 1

    def test_custom_mode_bound_as_parameter(self):
        dfm, cursor, _ = _make_dfm()
        dfm.set_checkpoint("arc", "ifdb_encounter", value="2025-01-01", mode="period")

        params = cursor.executed_params[0]
        assert "period" in params

    def test_no_string_interpolation_of_value(self):
        """Value is never interpolated into SQL — it's always a bound parameter."""
        dfm, cursor, _ = _make_dfm()
        evil_value = "'; DROP TABLE meta.DFMState; --"
        dfm.set_checkpoint("arc", "ifdb_encounter", value=evil_value)

        sql = cursor.executed[0]
        assert evil_value not in sql
        params = cursor.executed_params[0]
        assert evil_value in params


# ===========================================================================
# complete_dfm_run / fail_dfm_run
# ===========================================================================


class TestRunLifecycle:
    def test_complete_sets_success(self):
        dfm, cursor, _ = _make_dfm()
        dfm.complete_dfm_run(42, checkpoint_after="2025-06-01", rows_affected=100)

        sql = cursor.executed[0]
        assert "'success'" in sql
        assert cursor.executed_params[0] == ("2025-06-01", 100, 42)

    def test_fail_sets_failed(self):
        dfm, cursor, _ = _make_dfm()
        dfm.fail_dfm_run(42, error_message="something broke")

        sql = cursor.executed[0]
        assert "'failed'" in sql
        assert cursor.executed_params[0] == ("something broke", 42)


# ===========================================================================
# _ensure_state_row_and_get_checkpoint — DB error propagation
# ===========================================================================


class TestEnsureStateRowDbError:
    def test_db_error_propagates_instead_of_fallback(self):
        """A DB error in _ensure_state_row should raise, not return None."""
        mssql = MagicMock()

        @contextmanager
        def _boom():
            conn = MagicMock()
            cursor = MagicMock()
            cursor.execute.side_effect = RuntimeError("lock timeout")
            conn.cursor.return_value = cursor
            yield conn

        mssql.get_connection = _boom
        context = MagicMock()
        dfm = DfmManager(mssql, context)

        with pytest.raises(RuntimeError, match="lock timeout"):
            dfm._ensure_state_row_and_get_checkpoint(
                "staging", "archive", "arc", "ifdb_encounter", "delta"
            )


# ===========================================================================
# SCHEMA_TO_TARGET_TYPE contract — must match stored procedure CASE mapping
# ===========================================================================


class TestConnectionContextManager:
    """Verify that dfm.connection() shares a single DB connection across calls."""

    def test_shared_connection_reuses_same_conn(self):
        """Within connection(), all methods use the single shared connection."""
        fake_cursor = FakeCursor(
            fetchone_results=[
                ("2025-01-01 00:00:00",),  # get_checkpoint
                (42,),  # _insert_run_log
            ]
        )
        fake_conn = FakeConnection(fake_cursor)
        mssql = MagicMock()

        call_count = 0

        @contextmanager
        def _get_conn():
            nonlocal call_count
            call_count += 1
            yield fake_conn

        mssql.get_connection = _get_conn
        context = MagicMock()
        context.run_id = "test-run-shared"
        context.log = MagicMock()

        dfm = DfmManager(mssql, context)

        with dfm.connection():
            dfm.get_checkpoint("arc", "ifdb_encounter")
            dfm.complete_dfm_run(1, rows_affected=10)

        assert call_count == 1, f"Expected 1 get_connection call (shared), got {call_count}"

    def test_without_context_manager_opens_per_call(self):
        """Without connection(), each method opens its own connection."""
        fake_cursor = FakeCursor(
            fetchone_results=[
                ("2025-01-01 00:00:00",),
            ]
        )
        fake_conn = FakeConnection(fake_cursor)
        mssql = MagicMock()

        call_count = 0

        @contextmanager
        def _get_conn():
            nonlocal call_count
            call_count += 1
            yield fake_conn

        mssql.get_connection = _get_conn
        context = MagicMock()
        context.log = MagicMock()

        dfm = DfmManager(mssql, context)
        dfm.get_checkpoint("arc", "ifdb_encounter")
        dfm.complete_dfm_run(1, rows_affected=5)

        assert call_count == 2, f"Expected 2 get_connection calls (per-call), got {call_count}"

    def test_shared_conn_cleared_after_exit(self):
        """After exiting connection(), _shared_conn is reset to None."""
        dfm, _, _ = _make_dfm()

        with dfm.connection():
            assert dfm._shared_conn is not None

        assert dfm._shared_conn is None


class TestSchemaToTargetTypeContract:
    """Asserts SCHEMA_TO_TARGET_TYPE in Python matches the CASE mapping in
    meta.usp_dfm_reset (04_usp_dfm_reset.sql).

    The stored procedure uses a CASE expression with a fallback ELSE that
    returns the schema name verbatim. The Python dict must cover at least all
    the CASE branches. The 'dfm' schema uses the ELSE fallback in SQL (→ 'dfm')
    and is explicitly mapped in Python for clarity.
    """

    SP_CASE_MAPPING: dict[str, str] = {
        "ext": "extract",
        "arc": "archive",
        "pre_stg": "pre_staging",
        "stg": "staging",
        "dim": "dimension",
        "fct": "fact",
        "map": "mapping",
        "bridge": "bridge",
        "fhir_stg": "fhir_staging",
        "fhir": "fhir_table",
        "fhir_server": "fhir_resource",
        "codesystem": "codesystem",
        "conceptmap": "conceptmap",
        "ref": "reference",
    }

    def test_python_covers_all_sp_branches(self):
        """Every CASE branch in the stored procedure must exist in the Python dict."""
        for schema, target_type in self.SP_CASE_MAPPING.items():
            assert schema in SCHEMA_TO_TARGET_TYPE, (
                f"Schema {schema!r} exists in usp_dfm_reset CASE but not in "
                f"SCHEMA_TO_TARGET_TYPE"
            )
            assert SCHEMA_TO_TARGET_TYPE[schema] == target_type, (
                f"Schema {schema!r} maps to {SCHEMA_TO_TARGET_TYPE[schema]!r} in Python "
                f"but {target_type!r} in usp_dfm_reset"
            )

    def test_sp_covers_all_python_keys(self):
        """Every Python key should be either in the SP CASE or use the ELSE fallback.

        The 'dfm' schema is the only one that relies on the ELSE branch, where
        the SP returns the schema name verbatim ('dfm' → 'dfm').
        """
        sp_keys = set(self.SP_CASE_MAPPING.keys())
        for schema, target_type in SCHEMA_TO_TARGET_TYPE.items():
            if schema not in sp_keys:
                assert schema == target_type, (
                    f"Schema {schema!r} is not in usp_dfm_reset CASE and does not "
                    f"match its target_type {target_type!r} (ELSE branch returns "
                    f"schema name verbatim, so they must be equal)"
                )

    def test_checkpoint_owners_are_subset_of_schema_map(self):
        """All checkpoint owner schemas must be in SCHEMA_TO_TARGET_TYPE."""
        for schema in CHECKPOINT_OWNER_SCHEMAS:
            assert (
                schema in SCHEMA_TO_TARGET_TYPE
            ), f"Checkpoint owner schema {schema!r} not in SCHEMA_TO_TARGET_TYPE"


# ===========================================================================
# fail_dfm_run resilience
# ===========================================================================


class TestFailDfmRunResilience:
    def test_swallows_db_error_and_logs_warning(self):
        """A dead connection in fail_dfm_run must not mask the original error."""
        mssql = MagicMock()

        @contextmanager
        def _boom():
            conn = MagicMock()
            cursor = MagicMock()
            cursor.execute.side_effect = RuntimeError("connection is closed")
            conn.cursor.return_value = cursor
            yield conn

        mssql.get_connection = _boom
        context = MagicMock()
        dfm = DfmManager(mssql, context)

        # Must not raise — the caller is in an except path re-raising the
        # original asset error.
        dfm.fail_dfm_run(42, error_message="original asset error")

        context.log.warning.assert_called_once()
        assert "42" in context.log.warning.call_args[0][0]

    def test_retries_on_fresh_connection_when_shared_conn_dead(self):
        """If the shared connection died, the UPDATE is retried on a fresh one."""
        dead_cursor = MagicMock()
        dead_cursor.execute.side_effect = RuntimeError("shared conn dead")
        dead_conn = MagicMock()
        dead_conn.cursor.return_value = dead_cursor

        fresh_cursor = FakeCursor()
        fresh_conn = FakeConnection(fresh_cursor)

        mssql = MagicMock()

        @contextmanager
        def _fresh():
            yield fresh_conn

        mssql.get_connection = _fresh
        context = MagicMock()
        dfm = DfmManager(mssql, context)
        dfm._shared_conn = dead_conn  # simulate an active connection() block

        dfm.fail_dfm_run(7, error_message="boom")

        assert len(fresh_cursor.executed) == 1
        assert "'failed'" in fresh_cursor.executed[0]
        assert fresh_cursor.executed_params[0] == ("boom", 7)
        context.log.warning.assert_not_called()


# ===========================================================================
# sweep_stale_runs
# ===========================================================================


class TestSweepStaleRuns:
    def test_marks_stale_running_rows_abandoned(self):
        dfm, cursor, _ = _make_dfm()
        cursor.rowcount = 3

        swept = dfm.sweep_stale_runs()

        assert swept == 3
        sql = cursor.executed[0]
        assert "'abandoned'" in sql
        assert "[status] = 'running'" in sql
        assert cursor.executed_params[0] == (DfmManager.STALE_RUN_HOURS,)

    def test_custom_threshold_is_bound_parameter(self):
        dfm, cursor, _ = _make_dfm()
        dfm.sweep_stale_runs(stale_hours=6)
        assert cursor.executed_params[0] == (6,)

    def test_start_dfm_run_sweeps_first(self):
        """start_dfm_run runs the sweeper before any other statement."""
        dfm, cursor, _ = _make_dfm(
            fetchone_results=[
                ("2025-01-01 00:00:00",),  # _ensure_state_row SELECT
                (42,),  # _insert_run_log OUTPUT
            ]
        )
        result = dfm.start_dfm_run("arc", "ifdb_encounter", dagster_run_id="run-1", mode="delta")

        assert result.log_id == 42
        assert "'abandoned'" in cursor.executed[0]

    def test_sweep_failure_does_not_block_start(self):
        """A failing sweep logs a warning and the run still starts."""

        class SweepFailCursor(FakeCursor):
            def execute(self, sql, params=None):
                if "'abandoned'" in sql:
                    raise RuntimeError("sweep failed")
                super().execute(sql, params)

        cursor = SweepFailCursor(fetchone_results=[("2025-01-01 00:00:00",), (42,)])
        conn = FakeConnection(cursor)
        mssql = MagicMock()

        @contextmanager
        def _get_conn():
            yield conn

        mssql.get_connection = _get_conn
        context = MagicMock()
        dfm = DfmManager(mssql, context)

        result = dfm.start_dfm_run("arc", "ifdb_encounter", dagster_run_id="run-1", mode="delta")

        assert result.log_id == 42
        assert result.checkpoint == "2025-01-01 00:00:00"
        context.log.warning.assert_called()
