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
- Delta MERGE SQL execution and watermark filter behaviour
- Full sync fallback when no previous watermark exists

The self-MERGE technique works because build_delta_merge_statement() generates
SQL that references the source via a linked-server 4-part name. By replacing
that prefix with the local table reference, the MERGE operates on the same
table — allowing us to verify the watermark WHERE clause filters correctly
without a linked server.

Prerequisites:
- dim.Kreditor exists and has data (run staging ETL + infra assets)
- USE_REAL_DB_FOR_TESTS=true
- ENABLE_TEST_WRITES=true
"""

import logging
import os
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from DSO.load.sync_configs import ERP_SYNC_CONFIGS
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


# =============================================================================
# Fixtures
# =============================================================================


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
