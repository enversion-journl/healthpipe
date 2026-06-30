# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for select_from_expr support on the MSSQL DFM factory path."""

from DSI.connectors.factory import _append_where_to_select_from_expr


def test_appends_where_when_no_where_present():
    expr = "SELECT * FROM [Finans].[Foo]"
    out = _append_where_to_select_from_expr(expr, "[Modified] > '2025-01-01'")
    assert out == "SELECT * FROM [Finans].[Foo] WHERE [Modified] > '2025-01-01'"


def test_appends_and_when_where_already_present():
    expr = "SELECT * FROM [Finans].[Foo] WHERE [ref_RECID] IS NOT NULL"
    out = _append_where_to_select_from_expr(expr, "[Modified] > '2025-01-01'")
    assert out == (
        "SELECT * FROM [Finans].[Foo] WHERE [ref_RECID] IS NOT NULL "
        "AND ([Modified] > '2025-01-01')"
    )


def test_case_insensitive_where_detection():
    expr = "SELECT * FROM [Finans].[Foo] where [x] = 1"
    out = _append_where_to_select_from_expr(expr, "[Modified] > '2025-01-01'")
    assert "AND ([Modified] > '2025-01-01')" in out


def test_empty_where_returns_expression_unchanged():
    expr = "SELECT * FROM [Finans].[Foo] WHERE [x] = 1"
    out = _append_where_to_select_from_expr(expr, "")
    assert out == expr
