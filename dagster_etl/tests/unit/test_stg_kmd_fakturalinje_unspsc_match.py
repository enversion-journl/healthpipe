# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for stg_kmd_fakturalinje_unspsc_match asset."""

import sys

import DSA.SQL.staging.stg_kmd_fakturalinje_unspsc_match  # noqa: F401

_module = sys.modules["DSA.SQL.staging.stg_kmd_fakturalinje_unspsc_match"]
BUILD_SQL = _module.BUILD_SQL
stg_kmd_fakturalinje_unspsc_match = (
    _module.stg_kmd_fakturalinje_unspsc_match.op.compute_fn.decorated_fn
)


class TestStgKmdFakturalinjeUnspscMatchSql:
    def test_truncates_and_inserts_into_staging_table(self):
        assert "TRUNCATE TABLE [stg].[kmd_fakturalinje_unspsc_match]" in BUILD_SQL
        assert "INSERT INTO [stg].[kmd_fakturalinje_unspsc_match]" in BUILD_SQL

    def test_reads_from_arc_tables(self):
        assert "[arc].[spend_analysis_lines_kmd_uddata]" in BUILD_SQL
        assert "[arc].[invoice_kmd_uddata]" in BUILD_SQL
        assert "[arc].[unspsc_kmd_uddata]" in BUILD_SQL
        assert "[map].[UNSPSC]" in BUILD_SQL

    def test_aggregates_on_invoice_line_sk(self):
        assert "KmdAggregated" in BUILD_SQL
        assert "GROUP BY\n        DW_SK_Invoice,\n        DW_SK_InvoiceLine" in BUILD_SQL
        assert "SUM(LineAmount)" in BUILD_SQL
        assert "SUM(InvoicedQuantity)" in BUILD_SQL

    def test_expands_invoice_sk_after_aggregation(self):
        assert "KmdExpanded" in BUILD_SQL
        assert "inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]" in BUILD_SQL
        assert "FROM KmdExpanded" in BUILD_SQL

    def test_does_not_recreate_table_or_index(self):
        assert "DROP TABLE" not in BUILD_SQL
        assert "CREATE NONCLUSTERED INDEX" not in BUILD_SQL

    def test_builds_cleancode_keys_and_tiers(self):
        assert "CleanCode_Kreditor" in BUILD_SQL
        assert "CleanCode_Faktura" in BUILD_SQL
        assert "MatchTier" in BUILD_SQL
        assert "KmdCandidatesTier1" in BUILD_SQL
        assert "KmdCandidatesTier2" in BUILD_SQL
        assert "KmdInvoiceTier3" in BUILD_SQL
        assert "UNION ALL" in BUILD_SQL
