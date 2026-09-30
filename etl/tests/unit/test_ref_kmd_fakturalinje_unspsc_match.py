# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ref_kmd_fakturalinje_unspsc_match asset."""

import re
import sys
from pathlib import Path

import DSA.SQL.ref.kmd_fakturalinje_unspsc_match  # noqa: F401
import pytest
from DSA.SQL.common.unspsc_match_shared import MATCH_CURRENCY_SCALE, sql_match_currency
from DSA.SQL.ref.kmd_fakturalinje_unspsc_match import build_match_rows_cte, build_ref_merge_sql

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.ref.kmd_fakturalinje_unspsc_match"]
ref_kmd_fakturalinje_unspsc_match_asset = _module.ref_kmd_fakturalinje_unspsc_match

_DDL = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "ref"
    / "01_tables.sql"
).read_text(encoding="utf-8")

# Normalize whitespace so index definitions can be asserted layout-insensitively.
_DDL_NORM = re.sub(r"\s+", " ", _DDL)


class TestRefKmdFakturalinjeUnspscMatchSql:
    def test_merges_into_ref_table(self):
        merge_sql = build_ref_merge_sql(full_scope=True, checkpoint_param="1900-01-01")
        assert "MERGE [ref].[KMDFakturalinjeUNSPSCMatch]" in merge_sql
        assert "INTO #ref_matches" in merge_sql
        assert "TRUNCATE" not in merge_sql

    def test_depends_on_kmd_arc_only(self):
        dep_keys = {str(dep) for dep in ref_kmd_fakturalinje_unspsc_match_asset.dependency_keys}
        assert any("arc_kmd_uddata_spend_analysis_lines" in dep for dep in dep_keys)
        assert any("arc_kmd_uddata_invoice" in dep for dep in dep_keys)
        assert any("arc_kmd_uddata_unspsc" in dep for dep in dep_keys)
        assert not any("mysupply" in dep.lower() for dep in dep_keys)

    def test_asset_in_ref_faktura_group(self):
        spec = ref_kmd_fakturalinje_unspsc_match_asset.get_asset_spec()
        assert spec.group_name == "erp_ref"

    def test_reads_from_arc_tables(self):
        match_sql = build_match_rows_cte()
        assert "[arc].[kmd_uddata_spend_analysis_lines]" in match_sql
        assert "[arc].[kmd_uddata_invoice]" in match_sql
        assert "[arc].[kmd_uddata_unspsc]" in match_sql
        assert "[map].[UNSPSC]" in match_sql

    def test_dedupes_duplicate_sal_line_items_before_aggregate(self):
        match_sql = build_match_rows_cte()
        assert "KmdDeduped" in match_sql
        assert "DW_SK_SpendAnalysisLineItem" in match_sql
        assert "DW_EK_SpendAnalysisLine DESC" in match_sql
        assert "dedup_rn = 1" in match_sql
        assert "FROM KmdDeduped" in match_sql

    def test_aggregates_on_invoice_line_sk(self):
        match_sql = build_match_rows_cte()
        assert "KmdAggregated" in match_sql
        assert "GROUP BY\n        DW_SK_Invoice,\n        DW_SK_InvoiceLine" in match_sql
        assert f"ROUND(SUM(LineAmount), {MATCH_CURRENCY_SCALE})" in match_sql
        assert f"ROUND(MAX(unit_price), {MATCH_CURRENCY_SCALE})" in match_sql
        assert "SUM(invoiced_quantity)" in match_sql

    def test_rounds_invoice_total_at_build(self):
        match_sql = build_match_rows_cte()
        assert sql_match_currency("inv.[LineExtensionTotalAmount]") in match_sql

    def test_sql_match_currency_helper(self):
        assert sql_match_currency("src.[linje_beloeb]") == (
            f"ROUND(TRY_CONVERT(DECIMAL(18, 4), src.[linje_beloeb]), {MATCH_CURRENCY_SCALE})"
        )

    def test_expands_invoice_sk_after_aggregation(self):
        match_sql = build_match_rows_cte()
        assert "KmdExpanded" in match_sql
        assert "inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]" in match_sql
        assert "FROM KmdExpanded" in match_sql

    def test_builds_cleancode_keys_and_tiers(self):
        match_sql = build_match_rows_cte()
        assert "clean_code_kreditor" in match_sql
        assert "clean_code_faktura" in match_sql
        assert "match_tier" in match_sql
        assert "KmdCandidatesTier1" in match_sql
        assert "KmdCandidatesTier2" in match_sql
        assert "KmdInvoiceTier3" in match_sql
        assert "UNION ALL" in match_sql

    def test_delta_scope_uses_affected_kmd_invoices(self):
        merge_sql = build_ref_merge_sql(full_scope=False, checkpoint_param="2024-01-01")
        assert "AffectedInvoices" in merge_sql
        assert "dw_timestamp_update] > '2024-01-01'" in merge_sql
        assert "[arc].[mysupply_statistic_invoice_header]" not in merge_sql

    def test_full_scope_reads_all_kmd_match_rows(self):
        merge_sql = build_ref_merge_sql(full_scope=True, checkpoint_param="1900-01-01")
        assert "FROM MatchRows" in merge_sql
        assert "[arc].[mysupply_statistic_invoice_header]" not in merge_sql

    def test_soft_delete_on_merge(self):
        merge_sql = build_ref_merge_sql(full_scope=True, checkpoint_param="1900-01-01")
        assert "dw_is_deleted] = 1" in merge_sql


class TestRefKmdFakturalinjeUnspscMatchIndexes:
    """The apply-pass's #RefChangedKeys extract needs an UNFILTERED delta index.

    It must see soft-deleted rows (that is what falls a line back to -1, now that the
    separate demote pass is gone), so it cannot carry a dw_is_deleted predicate — and a
    filtered index is only usable when its predicate is implied by the query's. With
    only filtered indexes on the table the sole access path was the clustered PK, i.e.
    a full scan of the ref table on every run.
    """

    def test_delta_index_is_unfiltered_and_covering(self):
        assert (
            "CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta] "
            "ON [ref].[KMDFakturalinjeUNSPSCMatch] ([dw_timestamp_update]) "
            "INCLUDE ([clean_code_kreditor], [clean_code_faktura]);" in _DDL_NORM
        )

    def test_delta_index_carries_no_soft_delete_filter(self):
        # Guard the whole point of the index: a `WHERE dw_is_deleted = 0` filter here
        # would make it unusable for #RefChangedKeys and silently restore the scan.
        delta_block = _DDL_NORM.split(
            "CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta]"
        )[1].split(";")[0]
        assert "dw_is_deleted" not in delta_block

    def test_superseded_filtered_timestamp_index_is_gone(self):
        # Replaced, not supplemented: the unfiltered index is a superset, and this
        # table's only watermark reader is #RefChangedKeys, which cannot use a
        # filtered index. Keeping both would only cost write work.
        assert "IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp" not in _DDL

    def test_delta_index_name_cannot_collide_with_dso_sync_watermark_index(self):
        # infrastructure/assets/watermark_indexes.py generates IX_<schema>_<table>_Watermark
        # for every mode='watermark' sync-config table, and the database's default
        # collation is case-insensitive — so a "_watermark" suffix here would be the SAME
        # identifier as dso_sync's. This table is not in the sync config today, but a
        # case-only difference would make correctness depend on that staying true: the
        # NOT EXISTS guard would match dso_sync's index and silently skip creation,
        # restoring the full scan. Keep the suffix distinct by more than case.
        from infrastructure.assets.watermark_indexes import _index_name

        dso_sync_name = _index_name("ref", "KMDFakturalinjeUNSPSCMatch")
        declared = re.findall(r"\[?(IX_ref_KMDFakturalinjeUNSPSCMatch_\w+)\]?", _DDL)
        assert declared, "expected at least one declared index on the table"
        assert all(n.casefold() != dso_sync_name.casefold() for n in declared), (
            f"an index name collides case-insensitively with dso_sync's {dso_sync_name}: "
            f"{declared}"
        )

    def test_table_is_not_a_watermark_sync_table(self):
        # The premise of the test above. If this ever fails, ref.KMDFakturalinjeUNSPSCMatch
        # has entered the watermark sync config and dso_sync will start creating its own
        # index on the same column — revisit whether both are wanted.
        from DSO.load.sync_configs import get_watermark_sync_tables

        assert not [t for t in get_watermark_sync_tables() if t[1] == "KMDFakturalinjeUNSPSCMatch"]

    def test_tier_lookup_indexes_stay_filtered(self):
        # The tier joins in build_apply_resolve_sql DO filter dw_is_deleted = 0, so
        # these two stay filtered — narrower, and correct for those seeks.
        for name in (
            "UX_ref_KMDFakturalinjeUNSPSCMatch_keys",
            "IX_ref_KMDFakturalinjeUNSPSCMatch_lookup",
        ):
            block = _DDL_NORM.split(f"INDEX [{name}]")[1].split(";")[0]
            assert "WHERE [dw_is_deleted] = 0" in block


class TestUnfilteredDeltaIndexMigration:
    _MIGRATION = (
        Path(__file__).resolve().parents[2]
        / "infrastructure"
        / "ddl"
        / "migrations"
        / "20260826_unspsc_match_unfiltered_delta_index.sql"
    )

    def test_migration_exists(self):
        assert self._MIGRATION.is_file()

    def test_migration_creates_then_drops_guarded(self):
        sql = self._MIGRATION.read_text(encoding="utf-8")
        create_at = sql.index("CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta]")
        drop_at = sql.index("DROP INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp]")
        # Create before drop, so the table is never left without a watermark index.
        assert create_at < drop_at
        assert "NOT EXISTS" in sql[:create_at]

    def test_migration_index_definition_matches_base_ddl(self):
        sql = re.sub(r"\s+", " ", self._MIGRATION.read_text(encoding="utf-8"))
        assert (
            "CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_delta] "
            "ON [ref].[KMDFakturalinjeUNSPSCMatch] ([dw_timestamp_update]) "
            "INCLUDE ([clean_code_kreditor], [clean_code_faktura]);" in sql
        )
