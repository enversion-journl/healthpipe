# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for MSSQL insert path selection.

Tests validate:
- mssql-python connection detection via _is_mssql_python_conn()
- Insert path routing: bulkcopy > mssql-python executemany > pymssql fallback
- Chunk sizes: 100k for mssql-python, 2k for pymssql
- bulkcopy_conn parameter backward compatibility (defaults to None)
- bulkcopy_insert delegates to cursor.bulkcopy with correct args
- _streaming_executemany uses correct placeholders and chunk sizes per driver
"""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from DSI.connectors.mssql.bulk_copy import (
    BULKCOPY_TIMEOUT_SECONDS,
    bulkcopy_insert,
    bulkcopy_insert_with_retry,
    is_timeout_error,
)
from DSI.connectors.mssql.extractor import MssqlExtractor, _is_mssql_python_conn
from DSI.connectors.mssql.models import TableConfig

pytestmark = pytest.mark.unit


def _make_config(**overrides) -> TableConfig:
    defaults = dict(
        output_name="ext_test_table",
        source_schema="dbo",
        source_table="TestTable",
        target_table="test_table",
        description="Unit test table",
        is_full_load=True,
    )
    defaults.update(overrides)
    return TableConfig(**defaults)


def _mock_context():
    ctx = MagicMock()
    ctx.has_tag.return_value = False
    return ctx


# ============================================================================
# _is_mssql_python_conn detection
# ============================================================================


class TestIsMssqlPythonConn:
    def test_mssql_python_conn_detected(self):
        conn = MagicMock()
        conn.__class__.__module__ = "mssql_python.connection"
        assert _is_mssql_python_conn(conn) is True

    def test_pymssql_conn_not_detected(self):
        conn = MagicMock()
        conn.__class__.__module__ = "pymssql._pymssql"
        assert _is_mssql_python_conn(conn) is False

    def test_non_mssql_python_module_not_detected(self):
        conn = MagicMock()
        conn.__class__.__module__ = "some_other_driver.connection"
        assert _is_mssql_python_conn(conn) is False


# ============================================================================
# bulkcopy_insert
# ============================================================================


class TestBulkcopyInsert:
    def test_calls_bulkcopy_with_correct_args(self):
        ctx = _mock_context()
        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.return_value = {"rows_copied": 5}

        config = _make_config()
        column_names = ["col_a", "col_b"]
        data = [(1, "a", "SRC", datetime(2025, 1, 1), date(2025, 1, 1))] * 5

        result = bulkcopy_insert(ctx, bc_conn, config, column_names, data)

        assert result == 5
        bc_cursor.bulkcopy.assert_called_once_with(
            "ext.test_table",
            data,
            batch_size=0,
            timeout=BULKCOPY_TIMEOUT_SECONDS,
            table_lock=True,
            column_mappings=[
                "col_a",
                "col_b",
                "SourceSystem",
                "dw_load_timestamp",
                "dw_partition_date",
            ],
        )

    def test_raises_on_bulkcopy_failure(self):
        ctx = _mock_context()
        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.side_effect = RuntimeError("BCP fail")

        config = _make_config()
        with pytest.raises(RuntimeError, match="BCP fail"):
            bulkcopy_insert(ctx, bc_conn, config, ["a"], [(1,)])


# ============================================================================
# Streaming executemany — MssqlExtractor._streaming_executemany
# ============================================================================


class TestMssqlPythonExecutemanyInsert:
    def test_uses_question_mark_placeholders(self):
        ctx = _mock_context()
        target_conn = MagicMock()
        target_cursor = MagicMock()
        target_conn.cursor.return_value = target_cursor

        source_cursor = MagicMock()
        rows = [(1, "hello")] * 3
        source_cursor.fetchmany.side_effect = [rows, []]

        config = _make_config()
        load_ts = datetime(2025, 1, 1, 12, 0, 0)
        part_date = date(2025, 1, 1)

        extractor = MssqlExtractor(
            MagicMock(),
            target_conn,
            context=ctx,
            source_system="ILS",
        )
        result = extractor._streaming_executemany(
            source_cursor,
            target_conn,
            config,
            ["col_a", "col_b"],
            None,
            load_ts,
            part_date,
            placeholder="?",
            chunk_size=100_000,
        )

        assert result == 3
        insert_sql = target_cursor.executemany.call_args[0][0]
        assert "?" in insert_sql
        assert "%s" not in insert_sql

    def test_fetches_in_100k_chunks(self):
        ctx = _mock_context()
        target_conn = MagicMock()
        target_cursor = MagicMock()
        target_conn.cursor.return_value = target_cursor

        source_cursor = MagicMock()
        source_cursor.fetchmany.side_effect = [[(1,)], []]

        config = _make_config()
        extractor = MssqlExtractor(
            MagicMock(),
            target_conn,
            context=ctx,
            source_system="SydAX",
        )
        extractor._streaming_executemany(
            source_cursor,
            target_conn,
            config,
            ["col_a"],
            None,
            datetime.utcnow(),
            None,
            placeholder="?",
            chunk_size=100_000,
        )

        source_cursor.fetchmany.assert_any_call(100_000)

    def test_appends_etl_metadata_columns(self):
        ctx = _mock_context()
        target_conn = MagicMock()
        target_cursor = MagicMock()
        target_conn.cursor.return_value = target_cursor

        source_cursor = MagicMock()
        source_cursor.fetchmany.side_effect = [[(42,)], []]

        config = _make_config()
        load_ts = datetime(2025, 3, 1, 10, 0, 0)
        part_date = date(2025, 3, 1)

        extractor = MssqlExtractor(
            MagicMock(),
            target_conn,
            context=ctx,
            source_system="SydAX",
        )
        extractor._streaming_executemany(
            source_cursor,
            target_conn,
            config,
            ["val"],
            None,
            load_ts,
            part_date,
            placeholder="?",
            chunk_size=100_000,
        )

        rows_passed = target_cursor.executemany.call_args[0][1]
        assert len(rows_passed) == 1
        assert rows_passed[0] == ("42", "SydAX", load_ts, part_date)


# ============================================================================
# Pymssql executemany path
# ============================================================================


class TestPymssqlExecutemanyInsert:
    def _make_pymssql_conn(self):
        conn = MagicMock(spec=["cursor", "commit", "rollback"])
        cursor = MagicMock(spec=["execute", "executemany", "close", "fetchmany"])
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_pymssql_uses_percent_s_placeholders(self):
        ctx = _mock_context()
        conn, cursor = self._make_pymssql_conn()
        source_cursor = MagicMock()
        source_cursor.fetchmany.side_effect = [[(1,)], []]

        config = _make_config()
        extractor = MssqlExtractor(
            MagicMock(),
            conn,
            context=ctx,
            source_system="SydAX",
        )
        extractor._streaming_executemany(
            source_cursor,
            conn,
            config,
            ["col"],
            None,
            datetime.utcnow(),
            None,
            placeholder="%s",
            chunk_size=2_000,
        )

        insert_sql = cursor.executemany.call_args[0][0]
        assert "%s" in insert_sql
        assert "?" not in insert_sql

    def test_fetches_in_2k_chunks(self):
        ctx = _mock_context()
        conn, cursor = self._make_pymssql_conn()
        source_cursor = MagicMock()
        source_cursor.fetchmany.side_effect = [[(1,)], []]

        config = _make_config()
        extractor = MssqlExtractor(
            MagicMock(),
            conn,
            context=ctx,
            source_system="SydAX",
        )
        extractor._streaming_executemany(
            source_cursor,
            conn,
            config,
            ["col"],
            None,
            datetime.utcnow(),
            None,
            placeholder="%s",
            chunk_size=2_000,
        )

        source_cursor.fetchmany.assert_any_call(2_000)


# ============================================================================
# stream_extract_and_insert: insert path routing
# ============================================================================


class TestStreamExtractAndInsertRouting:
    """Verify stream_extract_and_insert selects the correct insert strategy."""

    def _setup_common_mocks(self):
        ctx = _mock_context()
        source_conn = MagicMock()
        target_conn = MagicMock()
        target_cursor = MagicMock()
        target_conn.cursor.return_value = target_cursor
        source_cursor = MagicMock()
        source_conn.cursor.return_value = source_cursor
        source_cursor.fetchmany.return_value = []
        source_cursor.description = [("col1",)]
        return ctx, source_conn, target_conn, source_cursor, target_cursor

    @patch("DSI.connectors.mssql.sql_builders.get_table_columns_info")
    def test_bulkcopy_conn_takes_priority(self, mock_cols):
        mock_cols.return_value = (
            [("col1", "int", True, None, None, None)],
            ["col1"],
        )
        ctx, source_conn, target_conn, source_cursor, target_cursor = self._setup_common_mocks()
        config = _make_config()

        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.return_value = {"rows_copied": 0}

        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=ctx,
            source_system="SydAX",
            bulkcopy_conn=bc_conn,
        )
        result = extractor.stream_extract_and_insert(config, partition_key=None)

        assert result.row_count == 0
        ctx.log.info.assert_any_call("Using mssql-python bulk-copy for insert")

    @patch("DSI.connectors.mssql.extractor._is_mssql_python_conn", return_value=True)
    @patch("DSI.connectors.mssql.sql_builders.get_table_columns_info")
    def test_mssql_python_executemany_when_no_bulkcopy(self, mock_cols, mock_detect):
        mock_cols.return_value = (
            [("col1", "int", True, None, None, None)],
            ["col1"],
        )
        ctx, source_conn, target_conn, source_cursor, target_cursor = self._setup_common_mocks()
        config = _make_config()

        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=ctx,
            source_system="SydAX",
        )
        result = extractor.stream_extract_and_insert(config, partition_key=None)

        assert result.row_count == 0
        ctx.log.info.assert_any_call("Using mssql-python executemany (100k-row chunks) for insert")

    @patch("DSI.connectors.mssql.extractor._is_mssql_python_conn", return_value=False)
    @patch("DSI.connectors.mssql.sql_builders.get_table_columns_info")
    def test_fallback_to_pymssql(self, mock_cols, mock_detect):
        mock_cols.return_value = (
            [("col1", "int", True, None, None, None)],
            ["col1"],
        )
        ctx, source_conn, target_conn, source_cursor, target_cursor = self._setup_common_mocks()
        config = _make_config()

        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=ctx,
            source_system="SydAX",
        )
        result = extractor.stream_extract_and_insert(config, partition_key=None)

        assert result.row_count == 0
        ctx.log.info.assert_any_call("Using pymssql executemany (2k-row chunks) for insert")

    @patch("DSI.connectors.mssql.extractor._is_mssql_python_conn", return_value=False)
    @patch("DSI.connectors.mssql.sql_builders.get_table_columns_info")
    def test_backward_compatible_signature(self, mock_cols, mock_detect):
        """Calling without bulkcopy_conn must still work (default=None)."""
        mock_cols.return_value = (
            [("col1", "int", True, None, None, None)],
            ["col1"],
        )
        ctx, source_conn, target_conn, source_cursor, target_cursor = self._setup_common_mocks()
        config = _make_config()

        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=ctx,
            source_system="SydAX",
        )
        result = extractor.stream_extract_and_insert(config, partition_key=None)

        assert result.target_table == "ext.test_table"
        assert result.row_count == 0


# ============================================================================
# Integration-level: data flows through correctly
# ============================================================================


class TestStreamExtractAndInsertDataFlow:
    """Verify data actually flows through insert paths with correct ETL metadata."""

    @patch("DSI.connectors.mssql.extractor._is_mssql_python_conn", return_value=False)
    @patch("DSI.connectors.mssql.sql_builders.get_table_columns_info")
    def test_rows_get_etl_metadata_appended(self, mock_cols, mock_detect):
        mock_cols.return_value = (
            [("val", "int", True, None, None, None)],
            ["val"],
        )
        ctx = _mock_context()
        source_conn = MagicMock()
        target_conn = MagicMock(spec=["cursor", "commit", "rollback"])
        target_cursor = MagicMock(spec=["execute", "executemany", "close", "fetchmany"])
        target_conn.cursor.return_value = target_cursor

        source_cursor = MagicMock()
        source_conn.cursor.return_value = source_cursor
        source_cursor.fetchmany.side_effect = [[(10,), (20,)], []]
        source_cursor.description = [("val",)]

        config = _make_config()

        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=ctx,
            source_system="TestSrc",
        )
        result = extractor.stream_extract_and_insert(config, partition_key="2025-03-01")

        assert result.row_count == 2
        rows_inserted = target_cursor.executemany.call_args[0][1]
        assert len(rows_inserted) == 2
        assert rows_inserted[0][0] == "10"
        assert rows_inserted[0][1] == "TestSrc"
        assert isinstance(rows_inserted[0][2], datetime)
        assert rows_inserted[0][3] == date(2025, 3, 1)


# ============================================================================
# is_timeout_error
# ============================================================================


class TestIsTimeoutError:
    def test_detects_deadline_elapsed(self):
        err = RuntimeError("Timeout Error: Elapsed: deadline has elapsed")
        assert is_timeout_error(err) is True

    def test_detects_timeout_expired(self):
        err = RuntimeError("Timeout Error: Timeout expired")
        assert is_timeout_error(err) is True

    def test_ignores_non_timeout_error(self):
        err = RuntimeError("BCP fail: column mismatch")
        assert is_timeout_error(err) is False

    def test_ignores_timeout_without_qualifier(self):
        err = RuntimeError("timeout occurred")
        assert is_timeout_error(err) is False


# ============================================================================
# bulkcopy_insert_with_retry
# ============================================================================


class TestBulkcopyInsertWithRetry:
    """Tests for the retry wrapper around bulkcopy_insert.

    The API:
    - ``bulkcopy_insert_with_retry`` tries the full chunk; on timeout it
      refreshes the connection and splits the chunk in half immediately
      (recursive). A same-size retry on the fresh connection happens only at
      the split floor.
    - Returns ``(rows_inserted, active_connection)``.
    """

    def _make_bc_conn(self, side_effects):
        """Create a mock bulkcopy connection with configurable side effects."""
        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.side_effect = side_effects
        return bc_conn, bc_cursor

    def _make_resource(self, bc_conn):
        """Create a mock mssql_target_resource that returns *bc_conn* on refresh."""
        resource = MagicMock()
        ctx_mgr = MagicMock()
        ctx_mgr.__enter__ = MagicMock(return_value=bc_conn)
        ctx_mgr.__exit__ = MagicMock(return_value=False)
        resource.get_connection.return_value = ctx_mgr
        return resource

    def _make_data(self, n):
        """Create n dummy rows."""
        return [(i, f"val_{i}", "SRC", datetime(2025, 1, 1), date(2025, 1, 1)) for i in range(n)]

    def test_succeeds_on_first_try(self):
        """No timeout — returns directly without retry."""
        ctx = _mock_context()
        bc_conn, bc_cursor = self._make_bc_conn([{"rows_copied": 10}])
        config = _make_config()
        data = self._make_data(10)

        count, conn = bulkcopy_insert_with_retry(ctx, bc_conn, config, ["a", "b"], data)

        assert count == 10
        assert conn is bc_conn
        assert bc_cursor.bulkcopy.call_count == 1

    def test_succeeds_on_fresh_connection_retry(self):
        """First call times out, fresh-connection retry succeeds."""
        ctx = _mock_context()
        timeout_err = RuntimeError("Timeout Error: Elapsed: deadline has elapsed")
        bc_conn, bc_cursor = self._make_bc_conn([timeout_err, {"rows_copied": 10}])
        config = _make_config()
        data = self._make_data(10)
        resource = self._make_resource(bc_conn)

        count, conn = bulkcopy_insert_with_retry(
            ctx, bc_conn, config, ["a", "b"], data, mssql_target_resource=resource
        )

        assert count == 10
        assert bc_cursor.bulkcopy.call_count == 2
        ctx.log.warning.assert_any_call("[rows=10] (current connection) Bulk-copy timed out.")

    def test_raises_when_chunk_too_small_to_split(self):
        """All retries timeout on small data (<= _MIN_SPLIT_ROWS) -> raises RuntimeError."""
        ctx = _mock_context()
        timeout_err = RuntimeError("Timeout Error: Elapsed: deadline has elapsed")
        bc_conn, bc_cursor = self._make_bc_conn([timeout_err] * 10)
        config = _make_config()
        data = self._make_data(10)
        resource = self._make_resource(bc_conn)

        with pytest.raises(RuntimeError, match="cannot split further"):
            bulkcopy_insert_with_retry(
                ctx, bc_conn, config, ["a", "b"], data, mssql_target_resource=resource
            )

    def test_non_timeout_error_propagates_immediately(self):
        """Non-timeout errors are not retried."""
        ctx = _mock_context()
        bc_conn, bc_cursor = self._make_bc_conn([RuntimeError("Column mismatch")])
        config = _make_config()
        data = self._make_data(10)

        with pytest.raises(RuntimeError, match="Column mismatch"):
            bulkcopy_insert_with_retry(ctx, bc_conn, config, ["a", "b"], data)

        assert bc_cursor.bulkcopy.call_count == 1

    def test_non_timeout_on_fresh_conn_retry_propagates(self):
        """If the fresh-connection retry raises a non-timeout error, it propagates."""
        ctx = _mock_context()
        timeout_err = RuntimeError("Timeout Error: Elapsed: deadline has elapsed")
        bc_conn, bc_cursor = self._make_bc_conn([timeout_err, RuntimeError("Connection lost")])
        config = _make_config()
        data = self._make_data(10)
        resource = self._make_resource(bc_conn)

        with pytest.raises(RuntimeError, match="Connection lost"):
            bulkcopy_insert_with_retry(
                ctx, bc_conn, config, ["a", "b"], data, mssql_target_resource=resource
            )

    def test_split_on_persistent_timeout(self):
        """Chunk that always times out at >500 rows gets split; sub-chunks succeed."""
        ctx = _mock_context()
        timeout_err = RuntimeError("Timeout Error: Timeout expired")

        def _side_effect(table, data, **kwargs):
            if len(data) > 500:
                raise timeout_err
            return {"rows_copied": len(data)}

        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.side_effect = _side_effect
        config = _make_config()
        data = self._make_data(1000)
        resource = self._make_resource(bc_conn)

        count, conn = bulkcopy_insert_with_retry(
            ctx, bc_conn, config, ["a", "b"], data, mssql_target_resource=resource
        )

        assert count == 1000

    def test_splittable_timeout_splits_without_retrying_same_size(self):
        """A splittable chunk that times out is split immediately; the same
        (wedge-prone) size is never re-attempted on the fresh connection."""
        ctx = _mock_context()
        timeout_err = RuntimeError("Timeout Error: Elapsed: deadline has elapsed")
        seen_sizes: list[int] = []

        def _side_effect(table, data, **kwargs):
            seen_sizes.append(len(data))
            if len(data) > 500:
                raise timeout_err
            return {"rows_copied": len(data)}

        bc_conn = MagicMock()
        bc_cursor = MagicMock()
        bc_conn.cursor.return_value = bc_cursor
        bc_cursor.bulkcopy.side_effect = _side_effect
        config = _make_config()
        data = self._make_data(1000)
        resource = self._make_resource(bc_conn)

        count, conn = bulkcopy_insert_with_retry(
            ctx, bc_conn, config, ["a", "b"], data, mssql_target_resource=resource
        )

        assert count == 1000
        # 1000-row size attempted exactly once, then split into 500 + 500.
        assert seen_sizes == [1000, 500, 500]
        assert seen_sizes.count(1000) == 1
        # Connection was refreshed before splitting.
        resource.get_connection.assert_called()
