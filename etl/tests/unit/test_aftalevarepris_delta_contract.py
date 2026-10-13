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
    """Spec 2026-08-31: sweep FOER MERGE, skopet paa LINJE-noeglerne (aldrig
    aftale-niveau — soesterlinjer restages ikke og maa ikke fejes)."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    assert "PRE_DELETE_SQL" in src and "SOFT_DELETE_SQL" not in src
    assert "src.bk_leverandoernummer = tgt.bk_leverandoernummer" in src
    assert "src.bk_aftale_nummer = tgt.bk_aftale_nummer" in src
    assert "src.bk_ils_varenummer = tgt.bk_ils_varenummer" in src
    assert src.index("PRE_DELETE_SQL = ") < src.index("def fct_AftaleVarePris")
    body = src[src.index("def fct_AftaleVarePris") :]
    assert body.index("PRE_DELETE_SQL") < body.index("MERGE_SQL"), "sweep skal koere FOER merge"


def test_fct_aftalevarepris_merges_on_composite_key():
    """Spec 2026-08-27: grain er (bk_prrowid, clean_code_AftaleVarePris)."""
    src = (DSA_SQL / "fct" / "fct_AftaleVarePris.py").read_text(encoding="utf-8")
    assert "ON tgt.bk_prrowid = src.bk_prrowid" in src
    assert "AND tgt.clean_code_AftaleVarePris = src.clean_code_AftaleVarePris" in src


def test_stg_fct_aftalevarepris_fans_out_lvnr_slots():
    """Spec 2026-08-27: prisversioner fanner ud pr. distinkt lvnr-CleanCode
    (laftdstam ##1-4 + laftd ##2-3) via OUTER APPLY, med kilder-provenans."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert "OUTER APPLY" in sql, "Fan-out'en skal vaere OUTER APPLY (bevarer no-lvnr-raekken)"
    for kilde in (
        "'laftdstam_1', 1",
        "'laftdstam_2', 2",
        "'laftdstam_3', 3",
        "'laftdstam_4', 4",
        "'laftd_2', 5",
        "'laftd_3', 6",
    ):
        assert kilde in sql, f"slot-kilde {kilde} mangler i unpivot'en"
    assert "STRING_AGG(slots.kilde" in sql, "kilder skal aggregeres pr. distinkt lvnr"
    assert "GROUP BY norm.clean_lvnr" in sql, "dedup skal ske paa normaliseret lvnr"


def test_stg_fct_aftalevarepris_extends_entity_cleancode():
    """Spec 2026-08-27: clean_code_AftaleVarePris udvides med _LEVVARE_{lvnr},
    saa forskellige lvnr-raekker aldrig deler dw_ek_AftaleVarePris.
    No-lvnr-raekken beholder basisformatet."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert "clean_code_AftaleVarePris_base" in sql
    assert "'_LEVVARE_' + lv.clean_lvnr" in sql
    assert (
        "ELSE '' END AS clean_code_AftaleVarePris" in sql
    ), "no-lvnr-raekken skal beholde basisformatet"


def test_stg_fct_aftalevarepris_windows_run_before_fanout():
    """Kritisk: LEAD/dw_is_current skal beregnes paa prisversions-aksen FOER
    fan-out'en — ellers peger LEAD paa samme versions soester-lvnr-raekke og
    dw_is_current rammer kun EN lvnr-raekke pr. linje."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    apply_start = sql.index("OUTER APPLY")
    assert sql.index("LEAD(") < apply_start, "LEAD skal ligge i pv (foer OUTER APPLY)"
    assert sql.index("AS dw_is_current") < apply_start, "dw_is_current skal ligge i pv"


def test_stg_fct_aftalevarepris_dw_valid_to_is_lead_chained():
    """Spec 2026-08-27: dw_valid_to = naeste versions dw_valid_from (LEAD vinder
    ubetinget); kildens slut kun som fallback paa sidste version. LEAD skal
    vaere COALESCE'ens FOERSTE argument — hvis armene nogensinde byttes om,
    ville kildens slut vinde ubetinget i stedet for LEAD."""
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
    """Final-review-rettelse: dw_is_current-vinduet skal ordne paa PRAECIS samme
    kaedede udtryk som LEAD-vinduet (blot DESC i stedet for ASC) + prrowid som
    tiebreaker — ellers kan de to vinduer i teorien uenes om hvilken version
    der er den sidste for linjen."""
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
    """Spec 2026-08-27: kreditorvarenummer1/2/3 er fjernet — raa lvnr-strenge
    bor i dim.Leverandoervare."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    assert "kreditorvarenummer" not in _build_staging_sql("")


def test_pre_stg_vareaftaler_does_not_build_aftalevarepris_cleancode():
    """Spec 2026-08-27: pre_vareaftaler's clean_code_AftaleVarePris havde ingen
    konsumenter og ville efter formatudvidelsen ligge i gammelt format —
    en desync-faelde. Den eneste producent er stg_fct_AftaleVarePris_ils."""
    src = (DSA_SQL / "staging" / "pre_stg_Vareaftaler_ils.py").read_text(encoding="utf-8")
    assert "clean_code_AftaleVarePris" not in src


def test_stg_fct_aftalevarepris_backbone_is_laftd_left_join_pris():
    """Spec 2026-08-31: prisloese aftalelinjer skal med — backbone er laftd
    med LEFT JOIN til laftdpris, ikke omvendt."""
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
    """Review-fund: den gamle backbone (p INNER JOIN d) udelukkede strukturelt
    NULL-noegle-linjer (en NULL-vnr-linje kan aldrig matche en laftdpris-raekke).
    Efter flippet til laftd-backbone skal filteret vaere eksplicit, ellers giver
    en NULL-vnr-laftd-raekke en syntetisk raekke med NULL
    clean_code_AftaleVarePris, som map-opslaget dropper og applierens
    fail-fast derefter fejler paa."""
    from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql

    sql = _build_staging_sql("")
    assert (
        "WHERE d.lnr IS NOT NULL AND d.aftale IS NOT NULL AND d.vnr IS NOT NULL" in sql
    ), "laftd-vinder-subqueryen skal filtrere NULL-noegle-raekker foer ROW_NUMBER"


def test_stg_fct_aftalevarepris_delta_carries_full_line_history():
    """LOAD-BEARING for pre-delete-sweepen (Task 2): staging skal medtage ALLE
    raekker for en beroert linje — delta-join'et gater paa linjenoeglerne,
    aldrig paa prisversions-/timestamp-niveau."""
    src = (DSA_SQL / "staging" / "stg_fct_AftaleVarePris_ils.py").read_text(encoding="utf-8")
    assert "ON dl.lnr = d.lnr AND dl.aftale = d.aftale AND dl.vnr = d.vnr" in src
