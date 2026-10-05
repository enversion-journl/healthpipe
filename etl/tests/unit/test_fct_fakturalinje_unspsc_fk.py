# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct.Fakturalinje UNSPSC decoupling and apply-pass."""

import sys

import DSA.SQL.fct.fct_Fakturalinje  # noqa: F401
import DSA.SQL.ref.kmd_fakturalinje_unspsc_match  # noqa: F401
import pytest
from DSA.SQL.ref.kmd_fakturalinje_unspsc_match import build_match_rows_cte

pytestmark = pytest.mark.unit

_fct_module = sys.modules["DSA.SQL.fct.fct_Fakturalinje"]
RESOLVE_SQL = _fct_module.RESOLVE_SQL
MERGE_SQL = _fct_module.MERGE_SQL
MATCH_SQL = build_match_rows_cte()


class TestKmdUnspscMatchSql:
    def test_match_rows_cte_reads_from_arc_tables(self):
        assert "[arc].[kmd_uddata_spend_analysis_lines]" in MATCH_SQL
        assert "[arc].[kmd_uddata_invoice]" in MATCH_SQL
        assert "[arc].[kmd_uddata_unspsc]" in MATCH_SQL
        assert "[map].[UNSPSC]" in MATCH_SQL

    def test_builds_cleancode_kreditor_and_faktura(self):
        assert "clean_code_kreditor" in MATCH_SQL
        assert "clean_code_faktura" in MATCH_SQL
        assert "'CVR_'" in MATCH_SQL
        assert "'FAKTURA_'" in MATCH_SQL
        assert "KmdExpanded" in MATCH_SQL
        assert "REPLACE(inv.[InvoiceNumber], ' ', '')" in MATCH_SQL

    def test_expands_invoice_sk_after_aggregation(self):
        assert "KmdAggregated agg" in MATCH_SQL
        assert "inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]" in MATCH_SQL
        assert "FROM KmdExpanded" in MATCH_SQL

    def test_deduplicates_with_row_number(self):
        assert "ROW_NUMBER()" in MATCH_SQL
        assert "ORDER BY DW_ValidDate_Max DESC" in MATCH_SQL
        assert "WHERE rn = 1" in MATCH_SQL

    def test_inserts_three_match_tiers(self):
        assert "KmdCandidatesTier1" in MATCH_SQL
        assert "KmdCandidatesTier2" in MATCH_SQL
        assert "KmdInvoiceTier3" in MATCH_SQL
        assert "LineExtensionTotalAmount" in MATCH_SQL
        assert "HAVING COUNT(DISTINCT dw_ek_UNSPSC) = 1" in MATCH_SQL

    def test_match_aggregates_kmd_micro_lines(self):
        assert "KmdDeduped" in MATCH_SQL
        assert "KmdAggregated" in MATCH_SQL
        assert "SUM(LineAmount)" in MATCH_SQL
        assert "SUM(invoiced_quantity)" in MATCH_SQL
        assert "line_amount_sum" in MATCH_SQL
        assert "sal.[DW_SK_Invoice]" in MATCH_SQL
        assert "sal.[DW_SK_InvoiceLine]" in MATCH_SQL
        assert "sal.[DW_SK_SpendAnalysisLineItem]" in MATCH_SQL


class TestFakturalinjeUnspscResolveSql:
    def test_resolve_does_not_join_match_table(self):
        assert "kmd_fakturalinje_unspsc_match" not in RESOLVE_SQL

    def test_resolve_defaults_unspsc_to_minus_one(self):
        assert "CAST(-1 AS INT) AS dw_ek_UNSPSC" in RESOLVE_SQL

    def test_does_not_join_dim_kreditor_for_unspsc(self):
        assert "LEFT JOIN [dim].[Kreditor] dk" not in RESOLVE_SQL


class TestFakturalinjeUnspscMergeSql:
    def test_merge_does_not_update_dw_ek_unspsc_on_match(self):
        assert "tgt.dw_ek_UNSPSC = src.dw_ek_UNSPSC" not in MERGE_SQL

    def test_insert_sets_unspsc_minus_one_for_new_rows(self):
        assert "dw_ek_UNSPSC" in MERGE_SQL
        assert "src.dw_ek_UNSPSC" in MERGE_SQL

    def test_insert_sets_both_audit_timestamps(self):
        assert "dw_timestamp_insert, dw_timestamp_update" in MERGE_SQL
        assert (
            "CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))" in MERGE_SQL
        )
