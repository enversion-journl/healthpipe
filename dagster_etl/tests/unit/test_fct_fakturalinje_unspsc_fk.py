# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct.Fakturalinje DW_EK_UNSPSC FK resolution via KMD staging match."""

import sys

import DSA.SQL.fct.FctUpd_fakturalinje  # noqa: F401
import DSA.SQL.staging.stg_kmd_fakturalinje_unspsc_match  # noqa: F401

_fct_module = sys.modules["DSA.SQL.fct.FctUpd_fakturalinje"]
_stg_module = sys.modules["DSA.SQL.staging.stg_kmd_fakturalinje_unspsc_match"]
BUILD_SQL = _stg_module.BUILD_SQL
RESOLVE_SQL = _fct_module.RESOLVE_SQL
MERGE_SQL = _fct_module.MERGE_SQL


class TestKmdUnspscMatchSql:
    def test_populates_persistent_staging_table(self):
        assert "INSERT INTO [stg].[kmd_fakturalinje_unspsc_match]" in BUILD_SQL
        assert "TRUNCATE TABLE [stg].[kmd_fakturalinje_unspsc_match]" in BUILD_SQL

    def test_reads_from_arc_tables(self):
        assert "[arc].[spend_analysis_lines_kmd_uddata]" in BUILD_SQL
        assert "[arc].[invoice_kmd_uddata]" in BUILD_SQL
        assert "[arc].[unspsc_kmd_uddata]" in BUILD_SQL
        assert "[map].[UNSPSC]" in BUILD_SQL

    def test_builds_cleancode_kreditor_and_faktura(self):
        assert "CleanCode_Kreditor" in BUILD_SQL
        assert "CleanCode_Faktura" in BUILD_SQL
        assert "'CVR_'" in BUILD_SQL
        assert "'FAKTURA_'" in BUILD_SQL
        assert "KmdExpanded" in BUILD_SQL
        assert "REPLACE(inv.[InvoiceNumber], ' ', '')" in BUILD_SQL
        assert "LIKE 'DK[0-9]%'" in BUILD_SQL

    def test_expands_invoice_sk_after_aggregation(self):
        assert "KmdAggregated agg" in BUILD_SQL
        assert "inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]" in BUILD_SQL
        assert "FROM KmdExpanded" in BUILD_SQL

    def test_deduplicates_with_row_number(self):
        assert "ROW_NUMBER()" in BUILD_SQL
        assert "ORDER BY DW_ValidDate_Max DESC" in BUILD_SQL
        assert "WHERE rn = 1" in BUILD_SQL

    def test_inserts_three_match_tiers(self):
        assert "KmdCandidatesTier1" in BUILD_SQL
        assert "KmdCandidatesTier2" in BUILD_SQL
        assert "KmdInvoiceTier3" in BUILD_SQL
        assert "1 AS MatchTier" in BUILD_SQL
        assert "2 AS MatchTier" in BUILD_SQL
        assert "3 AS MatchTier" in BUILD_SQL
        assert "LineExtensionTotalAmount" in BUILD_SQL
        assert "HAVING COUNT(DISTINCT DW_EK_UNSPSC) = 1" in BUILD_SQL


class TestFakturalinjeUnspscResolveSql:
    def test_resolve_joins_staging_match_table_by_tier(self):
        assert "LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd1" in RESOLVE_SQL
        assert "LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd2" in RESOLVE_SQL
        assert "LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd3" in RESOLVE_SQL
        assert "kmd1.[MatchTier] = 1" in RESOLVE_SQL
        assert "kmd2.[MatchTier] = 2" in RESOLVE_SQL
        assert "kmd3.[MatchTier] = 3" in RESOLVE_SQL

    def test_does_not_join_dim_kreditor_for_unspsc(self):
        assert "LEFT JOIN [dim].[Kreditor] dk" not in RESOLVE_SQL

    def test_match_aggregates_kmd_micro_lines_in_staging(self):
        assert "KmdAggregated" in BUILD_SQL
        assert "SUM(LineAmount)" in BUILD_SQL
        assert "SUM(InvoicedQuantity)" in BUILD_SQL
        assert "LineAmount_Sum" in BUILD_SQL
        assert "sal.[DW_SK_Invoice]" in BUILD_SQL
        assert "sal.[DW_SK_InvoiceLine]" in BUILD_SQL
        assert "GROUP BY\n        DW_SK_Invoice,\n        DW_SK_InvoiceLine" in BUILD_SQL

    def test_resolve_matches_on_cleancodes_and_line_measures(self):
        assert "kmd1.[CleanCode_Kreditor] = src.[CleanCode_Kreditor]" in RESOLVE_SQL
        assert "kmd1.[CleanCode_Faktura] = src.[CleanCode_Faktura]" in RESOLVE_SQL
        assert "kmd1.[LineAmount_Sum]" in RESOLVE_SQL
        assert "src.LinjeBeloeb" in RESOLVE_SQL
        assert "kmd1.[InvoicedQuantity]" in RESOLVE_SQL
        assert "src.AntalEnhederFaktureret" in RESOLVE_SQL
        assert "kmd1.[UnitPrice]" in RESOLVE_SQL
        assert "src.BasePrice_EnhedsPris" in RESOLVE_SQL

    def test_resolve_tier3_uses_metadata_faktura_total(self):
        assert "kmd3.[InvoiceTotal]" in RESOLVE_SQL
        assert "src.[MetadataFakturaTotal]" in RESOLVE_SQL
        assert RESOLVE_SQL.index("src.[MetadataFakturaTotal]") < RESOLVE_SQL.index(
            "src.[HeaderFakturaTotal]"
        )
        assert "src.[LinjeBeloeb] IS NULL" in RESOLVE_SQL
        assert "src.[BasePrice_EnhedsPris] IS NULL" in RESOLVE_SQL

    def test_resolve_coalesces_unmapped_to_minus_one(self):
        assert (
            "COALESCE(kmd1.DW_EK_UNSPSC, kmd2.DW_EK_UNSPSC, kmd3.DW_EK_UNSPSC, -1) AS DW_EK_UNSPSC"
            in RESOLVE_SQL
        )


class TestFakturalinjeUnspscMergeSql:
    def test_merge_sets_dw_ek_unspsc(self):
        assert "tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC" in MERGE_SQL
        assert "DW_EK_UNSPSC" in MERGE_SQL
        assert "src.DW_EK_UNSPSC" in MERGE_SQL

    def test_insert_sets_both_audit_timestamps(self):
        assert "DW_Timestamp_Insert, DW_Timestamp_Update" in MERGE_SQL
        assert (
            "CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))" in MERGE_SQL
        )
