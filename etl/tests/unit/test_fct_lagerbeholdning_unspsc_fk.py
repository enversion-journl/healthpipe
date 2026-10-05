# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct.Lagerbeholdning dw_ek_UNSPSC FK resolution (task 28041)."""

import sys

import DSA.SQL.fct.fct_Lagerbeholdning  # noqa: F401
import pytest

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.fct.fct_Lagerbeholdning"]
RESOLVE_SQL = _module.RESOLVE_SQL
MERGE_SQL = _module.MERGE_SQL


class TestUnspscFkSql:
    def test_resolve_reads_dw_ek_unspsc_from_dim_ilsvare(self):
        assert "di.[dw_ek_UNSPSC]" in RESOLVE_SQL
        assert "LEFT JOIN [dim].[ILSVare] di" in RESOLVE_SQL

    def test_resolve_does_not_join_map_unspsc_directly(self):
        assert "LEFT JOIN [map].[UNSPSC]" not in RESOLVE_SQL
        assert "clean_code_UNSPSC" not in RESOLVE_SQL

    def test_resolve_coalesces_unmapped_to_minus_one(self):
        assert "COALESCE(di.[dw_ek_UNSPSC], -1) AS dw_ek_UNSPSC" in RESOLVE_SQL

    def test_merge_sets_dw_ek_unspsc(self):
        assert "tgt.[dw_ek_UNSPSC] = src.[dw_ek_UNSPSC]" in MERGE_SQL
        assert "[dw_ek_UNSPSC]" in MERGE_SQL
        assert "src.[dw_ek_UNSPSC]" in MERGE_SQL
