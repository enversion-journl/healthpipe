# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for the NVARCHAR refactor of EtlWatermarkManager.

Covers the new behaviour added on 2026-04-27:
- advance_watermark() accepts an optional `value: str | None` parameter.
- get_watermark() returns str | None.
- log_start_run / log_complete_run accept str | None for watermark args.
- Default behaviour (no value passed to advance_watermark) still writes
  SYSDATETIME() server-side as an ISO datetime string.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock

from core.etl_watermark_manager import EtlWatermarkManager


def _create_mock_resource():
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
    ctx = MagicMock()
    ctx.log = MagicMock()
    ctx.run_id = "test-run-nvarchar"
    return ctx


# =============================================================================
# advance_watermark with explicit value
# =============================================================================


class TestAdvanceWatermarkExplicitValue:
    """advance_watermark(value=...) writes the supplied string verbatim."""

    def test_explicit_int_string_written_verbatim(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("archive", "arc", "unspsc_kmd_uddata", value="123456")

        sql = cursor.execute.call_args[0][0]
        # The literal value must appear in the MERGE SQL
        assert "N'123456'" in sql
        # And SYSDATETIME() must NOT replace it for value writes
        # (updated_at still uses SYSDATETIME, but the watermark_value column is the explicit value)
        # Look at the WHEN MATCHED UPDATE branch specifically.
        assert "watermark_value = N'123456'" in sql

    def test_explicit_value_in_insert_branch(self):
        """The WHEN NOT MATCHED INSERT must use the explicit value too."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("archive", "arc", "x", value="42")

        sql = cursor.execute.call_args[0][0]
        # Both branches reference the same value_expr.
        assert sql.count("N'42'") >= 2

    def test_default_value_uses_sysdatetime(self):
        """When value is omitted, the watermark is server-set to SYSDATETIME."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("fact", "fct", "Lagerbeholdning")

        sql = cursor.execute.call_args[0][0]
        # SYSDATETIME-based expression instead of an N'...' literal
        assert "SYSDATETIME()" in sql
        assert "watermark_value = CONVERT(NVARCHAR(50), SYSDATETIME(), 121)" in sql

    def test_explicit_value_escapes_single_quotes(self):
        """Single quotes in the value must be doubled to avoid SQL injection."""
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.advance_watermark("archive", "arc", "x", value="abc'def")

        sql = cursor.execute.call_args[0][0]
        # Doubled single quote escape, no raw "abc'def" in the SQL.
        assert "N'abc''def'" in sql


# =============================================================================
# get_watermark return type
# =============================================================================


class TestGetWatermarkReturnsString:
    def test_returns_string_value(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = ("987654",)

        mgr = EtlWatermarkManager(resource, ctx)
        result = mgr.get_watermark("archive", "arc", "unspsc_kmd_uddata")

        assert result == "987654"
        assert isinstance(result, str)

    def test_returns_none_for_missing_row(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = None

        mgr = EtlWatermarkManager(resource, ctx)
        result = mgr.get_watermark("archive", "arc", "unspsc_kmd_uddata")

        assert result is None


# =============================================================================
# log_start_run / log_complete_run accept str
# =============================================================================


class TestLogRunAcceptsStringWatermarks:
    def test_log_start_run_with_string_watermark_before(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("archive", "arc", "x", watermark_before="100", dagster_run_id="run-1")

        sql = cursor.execute.call_args[0][0]
        assert "N'100'" in sql

    def test_log_complete_run_with_string_watermark_after(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_complete_run(log_id=1, watermark_after="200", rows_affected=5)

        sql = cursor.execute.call_args[0][0]
        assert "N'200'" in sql

    def test_log_start_run_escapes_single_quotes(self):
        resource, _conn, cursor = _create_mock_resource()
        ctx = _create_mock_context()
        cursor.fetchone.return_value = (1,)

        mgr = EtlWatermarkManager(resource, ctx)
        mgr.log_start_run("archive", "arc", "x", watermark_before="a'b")

        sql = cursor.execute.call_args[0][0]
        assert "N'a''b'" in sql
