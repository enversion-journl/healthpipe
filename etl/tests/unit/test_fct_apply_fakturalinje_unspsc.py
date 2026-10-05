# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for fct_apply_fakturalinje_unspsc afterburner."""

import sys

import DSA.SQL.fct.apply_fakturalinje_unspsc  # noqa: F401
import pytest
from DSA.SQL.common.unspsc_match_shared import sql_match_currency
from DSA.SQL.fct.apply_fakturalinje_unspsc import (
    APPLY_PASS_MIN_FAKTURA_DATE,
    build_apply_delta_sql,
    build_apply_resolve_sql,
)

pytestmark = pytest.mark.unit

_module = sys.modules["DSA.SQL.fct.apply_fakturalinje_unspsc"]
REFRESH_SQL = _module.REFRESH_SQL
RUN_STARTED_AT_SQL = _module.RUN_STARTED_AT_SQL
apply_asset = _module.fct_apply_fakturalinje_unspsc


class TestApplyDeltaSql:
    def test_delta_reads_matchkeys_watermark_not_the_fact(self):
        # The match-key table is the derived key set, so it is the ONLY watermark the
        # delta reads. Reading fct.Fakturalinje's watermark too would force a
        # disjunction across two tables that no index can cover.
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        assert "w.[dw_timestamp_update] > '2024-01-01'" in delta_sql
        assert "r.[dw_timestamp_update] > '2024-01-01'" in delta_sql
        assert "[fct].[Fakturalinje]" not in delta_sql
        assert "f.[dw_timestamp_update]" not in delta_sql
        assert "?" not in delta_sql

    def test_ref_change_trigger_fires_on_soft_delete(self):
        # The ref-change delta trigger must fire on ANY ref change — including
        # soft-deletions — so a line whose match was deleted is re-resolved and falls
        # back to -1 through REFRESH. #RefChangedKeys therefore filters on the
        # timestamp only, with NO dw_is_deleted filter (that filter would make a
        # deletion-only change invisible and leave a stale code on the line).
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        assert (
            "FROM [ref].[KMDFakturalinjeUNSPSCMatch] r\n"
            "WHERE r.[dw_timestamp_update] > '2024-01-01';"
        ) in delta_sql
        assert "r.[dw_is_deleted]" not in delta_sql

    def test_delta_correlates_both_kreditor_and_header_on_the_same_faktura(self):
        # Two separate UNION ALL legs, not one disjunction, so each can seek its own
        # index. The header leg must correlate clean_code_Kreditor_header against the
        # ref kreditor AND still match on clean_code_Faktura — mirroring kmdh*.
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        assert (
            "ON p.[clean_code_Kreditor] = c.[clean_code_kreditor]\n"
            "           AND p.[clean_code_Faktura] = c.[clean_code_faktura]"
        ) in delta_sql
        assert (
            "ON h.[clean_code_Kreditor_header] = c.[clean_code_kreditor]\n"
            "           AND h.[clean_code_Faktura] = c.[clean_code_faktura]"
        ) in delta_sql
        assert delta_sql.count("UNION ALL") == 2

    def test_delta_carries_every_match_key_the_tier_joins_need(self):
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        for col in (
            "mk.[dw_ek_Fakturalinje]",
            "mk.[clean_code_Kreditor]",
            "mk.[clean_code_Kreditor_header]",
            "mk.[clean_code_Faktura]",
            "mk.[linje_beloeb]",
            "mk.[enheds_pris]",
            "mk.[antal_enheder_faktureret]",
            "mk.[faktura_total]",
        ):
            assert col in delta_sql
        # SELECT * would drag the timestamps through a wide DISTINCT for nothing.
        assert "SELECT *" not in delta_sql

    def test_lower_bound_on_faktura_date_uses_denormalised_column(self):
        # Scope is applied in the delta, off the match-key table's own copy of the
        # date key — no join to fct.Fakturalinje just to read it.
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        assert APPLY_PASS_MIN_FAKTURA_DATE in delta_sql
        assert "mk.[dw_ek_Dato_faktura] >=" in delta_sql
        assert "mk.[dw_ek_Dato_faktura] = -1" in delta_sql

    def test_delta_is_indexed_for_the_resolve_join(self):
        delta_sql = build_apply_delta_sql(checkpoint_param="2024-01-01")
        assert "CREATE UNIQUE CLUSTERED INDEX [IX_delta_Fakturalinje] ON #delta" in delta_sql


class TestApplyResolveSql:
    def test_resolve_joins_ref_match_by_tier(self):
        resolve_sql = build_apply_resolve_sql()
        assert "LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd1" in resolve_sql
        assert "LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd2" in resolve_sql
        assert "LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd3" in resolve_sql
        assert "LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmdh1" in resolve_sql
        assert "kmd1.[match_tier] = 1" in resolve_sql
        assert "kmdh1.[clean_code_kreditor] = mk.[clean_code_Kreditor_header]" in resolve_sql

    def test_resolve_reads_the_delta_not_the_full_matchkeys_table(self):
        resolve_sql = build_apply_resolve_sql()
        assert "FROM #delta mk" in resolve_sql
        assert "FROM [ref].[FakturalinjeMatchKeys]" not in resolve_sql
        assert "[stg].[fct_Fakturalinje_mysupply]" not in resolve_sql
        assert "[fct].[Fakturalinje]" not in resolve_sql
        assert "mk.[linje_beloeb]" in resolve_sql
        assert "mk.[faktura_total]" in resolve_sql

    def test_resolve_rounds_currency_match_keys(self):
        resolve_sql = build_apply_resolve_sql()
        assert sql_match_currency("mk.[linje_beloeb]") in resolve_sql
        assert sql_match_currency("mk.[enheds_pris]") in resolve_sql
        assert sql_match_currency("mk.[faktura_total]") in resolve_sql

    def test_every_tier_resolves_to_live_rows_only(self):
        # This is what makes the separate demote pass unnecessary: a line whose match
        # was soft-deleted re-enters the delta, misses every tier join here, and
        # resolves to -1 — which REFRESH then writes back.
        resolve_sql = build_apply_resolve_sql()
        for alias in ("kmd1", "kmd2", "kmd3", "kmdh1", "kmdh2", "kmdh3"):
            assert f"{alias}.[dw_is_deleted] = 0" in resolve_sql

    def test_resolved_is_indexed_for_the_refresh_join(self):
        resolve_sql = build_apply_resolve_sql()
        assert "CREATE CLUSTERED INDEX [IX_UnspscResolved_Fakturalinje]" in resolve_sql


class TestApplyRefresh:
    def test_refresh_only_updates_changed_rows(self):
        assert "WHERE f.[dw_ek_UNSPSC] <> r.[dw_ek_UNSPSC]" in REFRESH_SQL
        assert "dw_timestamp_update" in REFRESH_SQL

    def test_refresh_is_the_only_write_pass(self):
        # DEMOTE_SQL is gone: REFRESH writes the -1 fallback itself, off the same
        # #UnspscResolved, so there is no second full-table pass.
        assert not hasattr(_module, "DEMOTE_SQL")


class TestApplyCheckpoint:
    def test_checkpoint_is_floored_with_a_margin(self):
        # Both watermark columns the delta reads are datetime2(0) written via a
        # ROUNDING cast, so a sub-second checkpoint can sit above an unprocessed
        # write. With no fact-side watermark and no demote pass left, such a row
        # would be skipped permanently — hence floor + one second of margin.
        assert "CAST(DATEADD(SECOND, -1, SYSDATETIME()) AS datetime2(0))" in RUN_STARTED_AT_SQL
        assert "SYSDATETIME(), 121" not in RUN_STARTED_AT_SQL


class TestApplyAsset:
    def test_apply_asset_deps(self):
        dep_keys = {str(dep) for dep in apply_asset.dependency_keys}
        assert any("fct_Fakturalinje" in dep for dep in dep_keys)
        assert any("ref_kmd_fakturalinje_unspsc_match" in dep for dep in dep_keys)
        assert any("ref_fakturalinje_matchkeys" in dep for dep in dep_keys)
        assert not any("stg_fct_Fakturalinje_mysupply" in dep for dep in dep_keys)
