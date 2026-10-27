# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakt-guards for delta-loadet i AftaleVarePris-kaeden (OPS-28980).

Staging er delta-drevet fra fct.AftaleVarePris-checkpointet; disse tests
pinner de kontrakt-egenskaber der ville genindfoere full load eller
fejlagtig soft delete, hvis de forsvandt.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DSA_SQL = Path(__file__).resolve().parents[2] / "DSA" / "SQL"


def test_stg_fct_aftalevarepris_delta_is_checkpoint_gated():
    """Plausibel bug: checkpoint-filteret fjernes/glemmes -> full load vender tilbage."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        'checkpoint_from=("fct", "AftaleVarePris")' in src
    ), "Staging skal laese checkpointet fra ejeren fct.AftaleVarePris"
    assert (
        "INNER JOIN #delta_lines dl" in src
    ), "Backbone-laesningen skal gates af #delta_lines i delta-mode"


def test_stg_fct_aftalevarepris_delta_includes_time_flip():
    """Plausibel bug: dw_is_current gaar i staa for fremtidsdaterede prisversioner,
    hvis time-flip-signalet (prisversion traadt i kraft siden sidste checkpoint)
    fjernes fra delta-signalerne."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        "CAST(p.[start_] AS DATE) > @cp_date" in src
    ), "Time-flip-signalet skal sammenligne start_ mod @cp_date"


def test_stg_fct_aftalevarepris_dedups_all_natural_key_joins():
    """Plausibel bug: alle arc.ils_*-tabeller upsertes paa prrowid, ikke paa deres
    naturlige noegle. Flere rowids for samme (lnr, aftale, vnr) / (lnr, aftale) / lnr
    fan-out'er prisraekken til bk_prrowid-dubletter (bryder det unikke indeks).
    Alle tre join'ede tabeller skal derfor dedup'es til én raekke pr. naturlig noegle."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert (
        "PARTITION BY d.lnr, d.aftale, d.vnr" in src
    ), "laftd-join'et skal dedup'es (ROW_NUMBER rn=1) paa (lnr, aftale, vnr)"
    assert (
        "PARTITION BY h.lnr, h.aftale" in src
    ), "lafth-join'et skal dedup'es (ROW_NUMBER rn=1) paa (lnr, aftale)"
    assert "PARTITION BY lev.lnr" in src, "lev-join'et skal dedup'es (ROW_NUMBER rn=1) paa lnr"


def test_fct_aftalevarepris_predelete_sweep_is_line_scoped():
    """Sweep FOER MERGE, skopet paa LINJE-noeglerne (aldrig aftale-niveau —
    soesterlinjer restages ikke og maa ikke fejes)."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    assert "PRE_DELETE_SQL" in src and "SOFT_DELETE_SQL" not in src
    assert "src.bk_leverandoernummer = tgt.bk_leverandoernummer" in src
    assert "src.bk_aftale_nummer = tgt.bk_aftale_nummer" in src
    assert "src.bk_ils_varenummer = tgt.bk_ils_varenummer" in src
    assert src.index("PRE_DELETE_SQL = ") < src.index("def fct_AftaleVarePris")
    body = src[src.index("def fct_AftaleVarePris") :]
    assert body.index("PRE_DELETE_SQL") < body.index("MERGE_SQL"), "sweep skal koere FOER merge"


def test_fct_aftalevarepris_merges_on_bk_prrowid_alone():
    """Grain er bk_prrowid — ingen lvnr-komponent i noeglen."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    assert "ON tgt.bk_prrowid = src.bk_prrowid" in src
    assert "AND tgt.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris" not in src


def test_fct_aftalevarepris_resolves_all_four_slot_fks():
    """Plausibel bug: et slot-opslag genbruger et andet slots CleanCode."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    for n in range(1, 5):
        assert f"vare_map{n}.clean_code_Leverandoervare = src.clean_code_Leverandoervare_{n}" in src
        assert (
            f"COALESCE(vare_map{n}.dw_ek_Leverandoervare, -1) AS dw_ek_Leverandoervare_{n}" in src
        )


def test_stg_fct_aftalevarepris_slot_cleancodes_are_positional():
    """Slot N's CleanCode bygges af stam_lvnr{N} og maa ikke krydse slots
    (plausibel bug: copy-paste kobler slot 2 med stam_lvnr3)."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    for n in range(1, 5):
        alias = f"AS clean_code_Leverandoervare_{n}"
        assert alias in sql, f"slot {n} mangler"
        expr = sql[sql.rindex("CASE", 0, sql.index(alias)) : sql.index(alias)]
        assert f"pv.stam_lvnr{n}" in expr
        for other in range(1, 5):
            if other != n:
                assert f"pv.stam_lvnr{other}" not in expr, f"slot {n} laeser slot {other}"
        assert (
            f"LTRIM(RTRIM(pv.stam_lvnr{n})) <> ''" in expr
        ), "tomme slots maa ikke give haengende CVR_..._LEVVARE_-CleanCode"
    assert "OUTER APPLY" not in sql, "slots ligger som kolonner, aldrig som raekker"


def test_stg_fct_aftalevarepris_cleancode_is_base_format_only():
    """Entiteten er aftalelinjen — basisformat uden lvnr-komponent."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    alias = "AS clean_code_AftaleVarePris,"
    start = sql.rindex("'AFTALE_'", 0, sql.index(alias))
    expr = sql[start : sql.index(alias)]
    assert "'AFTALE_' +" in expr
    assert "'_VARE_' +" in expr
    assert "LEVVARE" not in expr, "clean_code_AftaleVarePris maa ikke baere en lvnr-komponent"


def test_stg_fct_aftalevarepris_lvnr_slots_come_from_laftdstam():
    """Slot-raakolonnerne laeses fra laftdstam; laftds lvnr-kolonner er legacy
    og indgaar ikke."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    for n in range(1, 5):
        assert f"stam.[lvnr##{n}] AS stam_lvnr{n}" in sql
    assert not re.search(r"\bd\.\[lvnr##", sql)


def test_stg_fct_aftalevarepris_dw_valid_to_is_lead_chained():
    """dw_valid_to = naeste versions dw_valid_from (LEAD vinder ubetinget);
    kildens slut kun som fallback paa sidste version. LEAD skal vaere
    COALESCE'ens FOERSTE argument — hvis armene nogensinde byttes om, ville
    kildens slut vinde ubetinget i stedet for LEAD."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert "LEAD(" in sql
    assert "PARTITION BY p.lnr, p.aftale, p.vnr" in sql
    # COALESCE skal wrappe LEAD(...) direkte som foerste argument (ikke som
    # fallback efter kildens slut).
    assert (
        "COALESCE(\n                LEAD(" in sql
    ), "LEAD skal vaere COALESCE'ens foerste argument for dw_valid_to"
    assert sql.index("LEAD(") < sql.index(
        "p.slut"
    ), "LEAD skal ligge foer kildens slut-fallback i dw_valid_to-udtrykket"


def test_stg_fct_aftalevarepris_dw_is_current_shares_lead_chain_ordering():
    """dw_is_current-vinduet skal ordne paa PRAECIS samme kaedede udtryk som
    LEAD-vinduet (blot DESC i stedet for ASC) + prrowid som tiebreaker —
    ellers kan de to vinduer i teorien uenes om hvilken version der er den
    sidste for linjen."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    # Optraeder 4 gange: dw_valid_from, LEAD's argument, LEAD's OVER (ORDER BY ...),
    # og dw_is_current's OVER (ORDER BY ... DESC, p.prrowid DESC).
    assert sql.count("DATEADD(SECOND, COALESCE(p.startklk, 0),") >= 4
    assert (
        "ORDER BY DATEADD(SECOND, COALESCE(p.startklk, 0),\n"
        "                             CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0))) DESC, p.prrowid DESC"
        in sql
    ), "dw_is_current skal ordne paa den kaedede DATEADD-DESC-variant med prrowid-tiebreaker"


def test_stg_fct_aftalevarepris_has_no_kreditorvarenummer_columns():
    """Fakten baerer ingen raa lvnr-strenge — nummeret naas via
    dim.Leverandoervare."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    assert "kreditorvarenummer" not in _build_staging_sql("")


def test_pre_stg_vareaftaler_does_not_build_aftalevarepris_cleancode():
    """pre_stg_Vareaftaler_ils maa ikke bygge clean_code_AftaleVarePris — to
    producenter af samme noegle er en desync-faelde, og stg_fct_AftaleVarePris_ils
    ejer formatet."""
    src = (DSA_SQL / "staging" / "pre_stg_Vareaftaler_ils.py").read_text(encoding="utf-8")
    assert "clean_code_AftaleVarePris" not in src


def test_stg_fct_aftalevarepris_backbone_is_laftd_left_join_pris():
    """Prisloese aftalelinjer skal med — backbone er laftd med LEFT JOIN til
    laftdpris, ikke omvendt."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert "LEFT JOIN" in sql[
        sql.index("[arc].[ils_laftd]") : sql.index("[arc].[ils_lafth]")
    ] or sql.index("[arc].[ils_laftd]") < sql.index(
        "[arc].[ils_laftdpris]"
    ), "laftd skal vaere backbone (foerst), laftdpris LEFT JOIN'et"
    assert "har_prisversion" in sql
    assert "'LAFTDPRIS_' + " in sql and "'LAFTD_' + " in sql, "bk_prrowid skal baere kildepraefiks"


def test_stg_fct_aftalevarepris_synthetic_rows_have_defined_validity():
    """Pinner de faktiske literals for den syntetiske gren, ikke kun at et
    p.prrowid IS NULL-tjek findes et sted: dw_is_current skal give 1, og
    dw_valid_from skal falde tilbage til linjens egen start_."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert (
        "WHEN p.prrowid IS NULL THEN 1" in sql
    ), "dw_is_current skal vaere 1 for syntetiske raekker (ingen konkurrerende versioner)"
    assert (
        "COALESCE(d.[start_], '1900-01-01')" in sql
    ), "dw_valid_from skal falde tilbage til linjens egen start_ paa syntetiske raekker"


def test_stg_fct_aftalevarepris_laftd_backbone_excludes_null_keys():
    """Med laftd som backbone skal NULL-noegle-filteret vaere eksplicit, ellers
    giver en NULL-vnr-laftd-raekke en syntetisk raekke med NULL
    clean_code_AftaleVarePris, som map-opslaget dropper og applierens
    fail-fast derefter fejler paa."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert (
        "WHERE d.lnr IS NOT NULL AND d.aftale IS NOT NULL AND d.vnr IS NOT NULL" in sql
    ), "laftd-vinder-subqueryen skal filtrere NULL-noegle-raekker foer ROW_NUMBER"


def test_stg_fct_aftalevarepris_delta_carries_full_line_history():
    """LOAD-BEARING for pre-delete-sweepen: staging skal medtage ALLE
    raekker for en beroert linje — delta-join'et gater paa linjenoeglerne,
    aldrig paa prisversions-/timestamp-niveau."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert "ON dl.lnr = d.lnr AND dl.aftale = d.aftale AND dl.vnr = d.vnr" in src
