# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct.Vareudlevering DW_EK_UNSPSC FK resolution."""

import sys

import DSA.SQL.fct.FctUpd_vareudlevering  # noqa: F401

_module = sys.modules["DSA.SQL.fct.FctUpd_vareudlevering"]
RESOLVE_SQL = _module.RESOLVE_SQL
MERGE_SQL = _module.MERGE_SQL


class TestUnspscFkSql:
    def test_resolve_reads_dw_ek_unspsc_from_dim_ilsvare(self):
        assert "di.[DW_EK_UNSPSC]" in RESOLVE_SQL
        assert "LEFT JOIN [dim].[IlsVare] di" in RESOLVE_SQL

    def test_resolve_does_not_join_map_unspsc_directly(self):
        assert "LEFT JOIN [map].[UNSPSC]" not in RESOLVE_SQL
        assert "CleanCode_UNSPSC" not in RESOLVE_SQL

    def test_resolve_coalesces_unmapped_to_minus_one(self):
        assert "COALESCE(di.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC" in RESOLVE_SQL

    def test_merge_sets_dw_ek_unspsc(self):
        assert "tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC" in MERGE_SQL
        assert "DW_EK_UNSPSC," in MERGE_SQL
        assert "src.DW_EK_UNSPSC" in MERGE_SQL
