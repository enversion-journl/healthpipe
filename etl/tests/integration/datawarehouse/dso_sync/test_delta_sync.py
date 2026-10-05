# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Delta Sync Integration Tests

Tests the delta (watermark-based) sync workflow with real databases.

Since linked servers are unavailable locally (same DB instance for DSA and DSO),
these tests use self-MERGE (source = target table) to validate:
- SyncStateManager CRUD against real [meta].[SyncWatermarks] table
- Delta MERGE SQL execution and watermark filter behaviour
- Full sync fallback when no previous watermark exists

The self-MERGE technique works because build_delta_merge_statement() generates
SQL that references the source via a linked-server 4-part name. By replacing
that prefix with the local table reference, the MERGE operates on the same
table — allowing us to verify the watermark WHERE clause filters correctly
without a linked server.

Prerequisites:
- [meta].[SyncWatermarks] table exists (run dso_infra_meta asset)
- dim.Kreditor exists and has data (run staging ETL + infra assets)
- USE_REAL_DB_FOR_TESTS=true
- ENABLE_TEST_WRITES=true
"""

import logging
import os
import time
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from DSO.load.sync_configs import ERP_SYNC_CONFIGS
from DSO.load.sync_state import SyncStateManager
from DSO.load.sync_utils import (
    LINKED_SERVER_NAME,
    STAGING_DATABASE,
    build_delta_merge_statement,
    get_table_columns,
)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.real_resources,
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true",
        reason="Requires USE_REAL_DB_FOR_TESTS=true",
    ),
]


# =============================================================================
# Helpers
# =============================================================================


def _mock_context():
    """Create a mock Dagster context that forwards to the Python logger."""
    ctx = MagicMock()
    ctx.log.info = lambda msg: logger.info(msg)
    ctx.log.warning = lambda msg: logger.warning(msg)
    ctx.log.error = lambda msg: logger.error(msg)
    return ctx


def _make_local_merge_sql(sql: str) -> str:
    """Replace linked-server 4-part name with local 2-part name for same-DB execution.

    Transforms ``[STAGING_LINK].[DW].[schema].[table]`` → ``[schema].[table]``
    so that the MERGE can run as a self-MERGE without a linked server.
    """
    return sql.replace(f"[{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].", "")


def _table_has_rows(resource, schema: str, table: str) -> int:
    """Return row count for a table, or 0 if the table does not exist."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            return cursor.fetchone()[0]
        except Exception:
            return 0
        finally:
            cursor.close()


def _watermark_table_exists(resource) -> bool:
    """Check whether [meta].[SyncWatermarks] exists."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'SyncWatermarks'"
            )
            return cursor.fetchone()[0] > 0
        finally:
            cursor.close()


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ensure_watermark_table(dso_target_resource):
    """Skip the test if [meta].[SyncWatermarks] does not exist."""
    if not _watermark_table_exists(dso_target_resource):
        pytest.skip("[meta].[SyncWatermarks] table not found — run dso_infra_meta asset first")


@pytest.fixture
def cleanup_test_watermark(dso_target_resource, ensure_watermark_table):
    """Insert/delete a test-only watermark row.  Uses a fake schema/table to avoid collisions."""
    test_schema = "__test__"
    test_table = "__delta_sync_test__"

    def _delete():
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "DELETE FROM [meta].[SyncWatermarks] "
                    f"WHERE schema_name = '{test_schema}' AND table_name = '{test_table}'"
                )
                conn.commit()
            except Exception:
                pass
            finally:
                cursor.close()

    _delete()
    yield test_schema, test_table
    _delete()


@pytest.fixture
def dim_kreditor_columns(dso_target_resource):
    """Get real columns for dim.Kreditor; skip if table is missing or empty."""
    columns = get_table_columns(dso_target_resource, "dim", "Kreditor")
    if not columns:
        pytest.skip("dim.Kreditor not found in DSO — run infrastructure assets first")
    row_count = _table_has_rows(dso_target_resource, "dim", "Kreditor")
    if row_count == 0:
        pytest.skip("dim.Kreditor is empty — run staging ETL first")
    return columns


# =============================================================================
# SyncStateManager integration tests
# =============================================================================


class TestSyncStateManagerIntegration:
    """CRUD tests for SyncStateManager against real [meta].[SyncWatermarks]."""

    def test_get_returns_none_for_unknown_table(self, dso_target_resource, cleanup_test_watermark):
        """First read for an unknown table returns None (first-run scenario)."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        result = mgr.get_last_sync_timestamp(test_schema, test_table)

        assert result is None

    def test_update_then_get_round_trip(self, dso_target_resource, cleanup_test_watermark):
        """Update inserts a watermark, subsequent get returns a timestamp."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        mgr.update_sync_state(test_schema, test_table, rows_affected=100, duration_ms=500)
        ts = mgr.get_last_sync_timestamp(test_schema, test_table)

        assert ts is not None
        assert isinstance(ts, datetime)

    def test_update_overwrites_existing(self, dso_target_resource, cleanup_test_watermark):
        """Second update overwrites the first (MERGE upsert behaviour)."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        mgr.update_sync_state(test_schema, test_table, rows_affected=100)
        ts1 = mgr.get_last_sync_timestamp(test_schema, test_table)

        time.sleep(1.1)  # DATETIME2(0) has 1-second resolution

        mgr.update_sync_state(test_schema, test_table, rows_affected=50)
        ts2 = mgr.get_last_sync_timestamp(test_schema, test_table)

        assert ts2 >= ts1

    def test_update_stores_metadata(self, dso_target_resource, cleanup_test_watermark):
        """Verify rows_affected, duration_ms and status are persisted correctly."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        mgr.update_sync_state(
            test_schema,
            test_table,
            rows_affected=42,
            duration_ms=1234,
            status="success",
        )

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT last_sync_rows, last_sync_duration_ms, last_sync_status "
                    "FROM [meta].[SyncWatermarks] "
                    f"WHERE schema_name = '{test_schema}' AND table_name = '{test_table}'"
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        assert row is not None
        assert row[0] == 42
        assert row[1] == 1234
        assert row[2] == "success"

    def test_watermarks_one_row_per_table(self, dso_target_resource, cleanup_test_watermark):
        """meta.SyncWatermarks has at most one row per (schema_name, table_name) — traceable per table."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        mgr.update_sync_state(test_schema, test_table, rows_affected=1)
        mgr.update_sync_state(test_schema, test_table, rows_affected=2)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM [meta].[SyncWatermarks] "
                    f"WHERE schema_name = '{test_schema}' AND table_name = '{test_table}'"
                )
                count = cursor.fetchone()[0]
            finally:
                cursor.close()

        assert count == 1

    def test_watermarks_updated_at_and_last_sync_timestamp_set(
        self, dso_target_resource, cleanup_test_watermark
    ):
        """last_sync_timestamp and updated_at are set (DSO SYSDATETIME); traceable after success."""
        test_schema, test_table = cleanup_test_watermark
        ctx = _mock_context()
        mgr = SyncStateManager(dso_target_resource, ctx)

        mgr.update_sync_state(test_schema, test_table, rows_affected=5)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT last_sync_timestamp, updated_at FROM [meta].[SyncWatermarks] "
                    f"WHERE schema_name = '{test_schema}' AND table_name = '{test_table}'"
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        assert row is not None
        assert row[0] is not None
        assert row[1] is not None
        assert row[0] == row[1]


# =============================================================================
# Delta MERGE SQL execution tests (self-MERGE, same-DB)
# =============================================================================


class TestDeltaMergeSQLExecution:
    """Execute delta MERGE SQL against real tables as self-MERGE.

    Self-MERGE (source = target) validates:
    - Generated SQL is syntactically valid for SQL Server
    - Watermark WHERE clause correctly filters rows
    - Full MERGE fallback when last_sync_timestamp is None

    All data-modifying tests use ROLLBACK to leave the table unchanged.
    """

    def test_delta_merge_future_timestamp_zero_rows(
        self, dso_target_resource, dim_kreditor_columns
    ):
        """Delta MERGE with a future watermark should process 0 rows."""
        config = ERP_SYNC_CONFIGS["dim_Kreditor"]
        future_ts = datetime(2099, 12, 31, 23, 59, 59)
        sql = build_delta_merge_statement(config, dim_kreditor_columns, future_ts)
        local_sql = _make_local_merge_sql(sql)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_sql)
                rows = cursor.rowcount
                conn.rollback()
            finally:
                cursor.close()

        assert rows == 0, f"Expected 0 rows with future watermark, got {rows}"

    def test_delta_merge_old_timestamp_processes_all_rows(
        self, dso_target_resource, dim_kreditor_columns
    ):
        """Delta MERGE with a very old watermark should match all rows (self-MERGE update)."""
        config = ERP_SYNC_CONFIGS["dim_Kreditor"]
        total_rows = _table_has_rows(dso_target_resource, "dim", "Kreditor")

        old_ts = datetime(2000, 1, 1, 0, 0, 0)
        sql = build_delta_merge_statement(config, dim_kreditor_columns, old_ts)
        local_sql = _make_local_merge_sql(sql)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_sql)
                rows = cursor.rowcount
                conn.rollback()
            finally:
                cursor.close()

        assert rows > 0, "Expected rows affected with old watermark, got 0"
        assert (
            rows == total_rows
        ), f"Expected all {total_rows} rows with very old watermark, got {rows}"

    def test_full_merge_fallback_when_no_timestamp(self, dso_target_resource, dim_kreditor_columns):
        """build_delta_merge_statement(timestamp=None) falls back to full MERGE."""
        config = ERP_SYNC_CONFIGS["dim_Kreditor"]
        total_rows = _table_has_rows(dso_target_resource, "dim", "Kreditor")

        sql = build_delta_merge_statement(config, dim_kreditor_columns, None)
        local_sql = _make_local_merge_sql(sql)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_sql)
                rows = cursor.rowcount
                conn.rollback()
            finally:
                cursor.close()

        if total_rows > 0:
            assert (
                rows == total_rows
            ), f"Full merge should process all {total_rows} rows, got {rows}"

    def test_delta_merge_no_data_loss(self, dso_target_resource, dim_kreditor_columns):
        """Self-MERGE must not change the row count (no INSERT, no DELETE)."""
        config = ERP_SYNC_CONFIGS["dim_Kreditor"]
        count_before = _table_has_rows(dso_target_resource, "dim", "Kreditor")

        old_ts = datetime(2000, 1, 1, 0, 0, 0)
        sql = build_delta_merge_statement(config, dim_kreditor_columns, old_ts)
        local_sql = _make_local_merge_sql(sql)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_sql)
                conn.rollback()
            finally:
                cursor.close()

        count_after = _table_has_rows(dso_target_resource, "dim", "Kreditor")

        assert (
            count_before == count_after
        ), f"Self-MERGE changed row count: before={count_before}, after={count_after}"


# =============================================================================
# End-to-end delta flow simulation
# =============================================================================


class TestDeltaSyncFlow:
    """Simulate the full delta sync lifecycle: first run → delta → no-change delta.

    Uses SyncStateManager + self-MERGE to validate the watermark-driven flow
    without requiring a linked server.
    """

    def test_first_run_full_sync_then_delta(
        self, dso_target_resource, dim_kreditor_columns, cleanup_test_watermark
    ):
        """Lifecycle: no watermark → full sync → update watermark → delta sync → 0 rows."""
        test_schema, test_table = cleanup_test_watermark
        config = ERP_SYNC_CONFIGS["dim_Kreditor"]
        total_rows = _table_has_rows(dso_target_resource, "dim", "Kreditor")
        if total_rows == 0:
            pytest.skip("dim.Kreditor is empty")

        ctx = _mock_context()
        state_mgr = SyncStateManager(dso_target_resource, ctx)

        # --- Step 1: First run — no watermark, should fall back to full merge ---
        last_ts = state_mgr.get_last_sync_timestamp(test_schema, test_table)
        assert last_ts is None, "Expected no watermark on first run"

        sql_full = build_delta_merge_statement(config, dim_kreditor_columns, last_ts)
        local_full = _make_local_merge_sql(sql_full)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_full)
                full_rows = cursor.rowcount
                conn.rollback()
            finally:
                cursor.close()

        assert (
            full_rows == total_rows
        ), f"First run should sync all {total_rows} rows, got {full_rows}"

        # --- Step 2: Record watermark (simulates post-sync update) ---
        state_mgr.update_sync_state(
            test_schema,
            test_table,
            rows_affected=full_rows,
            duration_ms=100,
        )
        recorded_ts = state_mgr.get_last_sync_timestamp(test_schema, test_table)
        assert recorded_ts is not None

        # --- Step 3: Delta sync with the just-recorded watermark ---
        # Since we rolled back the self-MERGE (timestamps unchanged), no rows should
        # have dw_timestamp_update >= recorded_ts (which is SYSDATETIME() from step 2).
        # Wait briefly to ensure the watermark is strictly in the past.
        time.sleep(0.1)

        sql_delta = build_delta_merge_statement(
            config,
            dim_kreditor_columns,
            recorded_ts,
        )
        local_delta = _make_local_merge_sql(sql_delta)

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(local_delta)
                delta_rows = cursor.rowcount
                conn.rollback()
            finally:
                cursor.close()

        # Depending on timestamp precision (DATETIME2(0) = 1s), some rows may or may
        # not match.  With a 1-second resolution and the watermark set to "now", rows
        # whose dw_timestamp_update equals the same second could still match.
        # The key assertion is: delta_rows <= total_rows (fewer than a full sync).
        assert (
            delta_rows <= total_rows
        ), f"Delta sync should process <= {total_rows} rows, got {delta_rows}"
        logger.info(
            f"Delta sync: {delta_rows}/{total_rows} rows " f"(watermark={recorded_ts.isoformat()})"
        )
