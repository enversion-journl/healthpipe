# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the delta-driven stg_dim_Leverandoervare."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

from DSA.SQL.staging.stg_dim_Leverandoervare import _build_stg_sql

DSA_SQL = Path(__file__).resolve().parents[2] / "DSA" / "SQL"


def _read_dim_source() -> str:
    return (DSA_SQL / "staging" / "stg_dim_Leverandoervare.py").read_text(encoding="utf-8")


def test_null_checkpoint_reads_whole_hub():
    sql = _build_stg_sql(None)
    assert "dw_timestamp_update > '" not in sql
    assert "[stg].[pre_Leverandoervare_ILSVare_delta_entiteter]" in sql


def test_checkpoint_filters_hub_reads():
    sql = _build_stg_sql("2026-07-01")
    # praecis een hub-laesning filtrerer paa CP_Vare
    assert sql.count("> '2026-07-01'") == 1


def test_delta_comes_solely_from_hub_vare_rows():
    sql = _build_stg_sql(None)
    # Vare-delta-kompletheden (inkl. varestamdata-aendringer) ejes af hub'en —
    # ingen lokal ekspansion mod ils_pairs i delta_vare
    assert "ON p.raa_vnr = h.vnr" not in sql
    assert "h.clean_code_Kreditor IS NOT NULL" in sql


def test_source_flags_are_full_state_not_delta_state():
    sql = _build_stg_sql("2026-07-01")
    # flags afledes af kildens fulde billede for entiteten (ikke af hvilken
    # gren deltaet kom fra): ils via det materialiserede #ils_pairs (fuld
    # par-billede semi-joinet til delta-entiteterne, IKKE hub-timestamp-
    # filtreret), mysupply via raekke i det delta-scopede fulde fakturabillede
    assert "CAST(CASE WHEN ip.clean_code_Kreditor IS NOT NULL" in sql
    assert "LEFT JOIN #ils_pairs ip" in sql
    assert "CAST(CASE WHEN mys.clean_code_Kreditor IS NOT NULL" in sql


def test_ils_pairs_materialized_once_not_correlated_exists():
    sql = _build_stg_sql(None)
    # ils_pairs materialiseres EEN gang til en indekseret temp-tabel og probes
    # via join — ikke som korreleret EXISTS pr. output-raekke (som tvang fuld
    # gen-scanning af hele ILS-arkivet pr. raekke i #delta_vare).
    assert "INTO #ils_pairs" in sql
    assert "CREATE UNIQUE CLUSTERED INDEX IX_tmp_ils_pairs" in sql
    assert "CASE WHEN EXISTS" not in sql
    assert "FROM ils_pairs y" not in sql
    # semi-joinet til delta-entiteterne, saa arbejdet skalerer med deltaet
    assert "INNER JOIN #delta_vare dv" in sql


def test_no_ils_item_master_attributes():
    sql = _build_stg_sql(None)
    # ILS-navne bor paa dim.ILSVare (vnr-grain) og naas via bridgen — dim.Leverandoervare
    # maa ikke baere en kopi (varenavn_ils/ils_choice er bevidst fjernet)
    assert "varenavn_ils" not in sql
    assert "ils_choice" not in sql
    assert "[arc].[ils_vare]" not in sql


def test_no_vnr_denormalization_in_map_and_fct():
    import importlib
    import inspect

    from DSA.SQL.fct.fct_Vareudlevering import RESOLVE_SQL

    map_vare_module = importlib.import_module("DSA.SQL.map.map_Leverandoervare")

    # Vare <-> ILSVare-relationen bor i bridge.LeverandoervareILSVare. map.Leverandoervare har ingen
    # vnr-kolonne, saa en genindfoert reference vaelter map_Leverandoervare ved koersel, og
    # vnr-fallback i fct_Vareudlevering ville genindfoere *:1-denormaliseringen.
    assert "vnr" not in inspect.getsource(map_vare_module)
    assert "vare_vnr_fallback" not in RESOLVE_SQL
    assert "vnr_normalized" not in RESOLVE_SQL


def test_dim_ils_pairs_use_only_laftdstam_lvnr_slots():
    """laftds lvnr-kolonner er legacy — kun laftdstam-slots (##1 +
    alternativerne) maa give ILS-eksistens."""
    src = _read_dim_source()
    assert "LAFTDSTAM_ALT_LVNR_SLOTS" in src
    assert "LAFTD_ALT_LVNR_SLOTS" not in src

    from DSA.SQL.staging.stg_dim_Leverandoervare import _build_stg_sql

    sql = _build_stg_sql(None)
    assert "s.[lvnr##1]" in sql
    assert "[arc].[ils_laftd]" not in sql
