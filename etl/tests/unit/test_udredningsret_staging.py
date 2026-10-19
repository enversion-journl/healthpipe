# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for udredningsrets staging-lag (US-3).

SQL-byggerne i DSA/SQL/staging/udredningsret_staging_sql.py er rene
funktioner, saa clean_code-reglerne, markoergrupperne (SDS bilag 1),
delta-scope og borger-genopretning (U9), GDPR-garantien (CPR forlader
aldrig pre_stg), start/slut/attributmarkoer-valget (U2-U4), loebenummeret,
dato-noegler og SOR-opslag asserteres som tekst — uden database. De seks
assets koeres mod fakes (kun taellinger i metadata), og registreringen i
staging_assets og udredningsret_publish_job tjekkes mod den rigtige
asset-graf. Spec: docs/source_systems/udredningsret/udredningsret_model.md.
"""

import importlib
import re
from collections import deque
from contextlib import contextmanager
from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.common import dimension_lookup_temp_tables
from DSA.SQL.common.dato_dimension import dato_key
from DSA.SQL.staging import (
    drg_staging_sql,
    pre_stg_Helbredsforloeb,
    pre_stg_Udredningsforloeb,
    staging_assets,
    stg_dim_Forloebsmarkoer,
    stg_dim_UdredningsretEgenskaber,
    stg_fct_Udredningsforloeb,
    stg_fct_UdredningsretMarkoer,
    udredningsret_staging_sql,
)
from DSA.SQL.staging.udredningsret_kolonner import (
    AARSAG_TEKSTER,
    DIM_EGENSKABER_TABLE,
    DIM_FORLOEBSMARKOER_TABLE,
    DOWNSTREAM_CHECKPOINT_EJERE,
    EGENSKABER_ATTRIBUTTER,
    FCT_FORLOEB_TABLE,
    FCT_MARKOER_TABLE,
    FORLOEB_ATTRIBUT_NAVNE,
    FORLOEB_ATTRIBUTTER,
    FORLOEB_CLEAN_CODE_TYPE,
    FORLOEB_PREFIX,
    FORLOEBSMARKOER_CLEAN_CODE_TYPE,
    FORLOEBSMARKOER_PREFIX,
    HENVIST_KODER,
    HENVIST_LIKE,
    HF_CLEAN_CODE_TYPE,
    HF_PREFIX,
    HJERTE_TEKSTER,
    INDEN_30_TEKSTER,
    KLASSIFIKATION_ATTRIBUTTER,
    KLASSIFIKATION_CLEAN_CODE_TYPE,
    KLASSIFIKATION_PREFIX,
    MARKOER_CLEAN_CODE_TYPE,
    MARKOER_GRUPPE_TEKSTER,
    MARKOER_GRUPPER,
    MARKOER_PREFIX,
    MARKOER_SOR_ROLLER,
    NA_BORGER,
    OEVRIGE_GRUPPE,
    OEVRIGE_GRUPPE_TEKST,
    OPGOERELSE_KVARTAL_LIKE,
    OVERHOLDT_TEKSTER,
    PRE_FORLOEB_TABLE,
    PRE_HF_TABLE,
    PSYKIATRI_TEKSTER,
    REGION_TEKSTER,
    REGION_UKENDT_TEKST,
    SOR_ROLLER,
    UNKNOWN_BORGER,
    UNKNOWN_ORGANISATION,
    clean_code_forloebsmarkoer_sql,
    clean_code_helbredsforloeb_sql,
    clean_code_markoer_sql,
    clean_code_udredningsforloeb_sql,
    henvist_predicate_sql,
    klassifikation_clean_code_sql,
    markoer_gruppe_sql,
    markoer_gruppe_tekst_sql,
    opgoerelse_dato_sql,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    ARC_TABLE,
    build_borger_coverage_sql,
    build_grain_check_sql,
    build_pre_helbredsforloeb_sql,
    build_pre_udredningsforloeb_sql,
    build_sor_coverage_sql,
    build_stg_dim_egenskaber_sql,
    build_stg_dim_forloebsmarkoer_sql,
    build_stg_fct_markoer_sql,
    build_stg_fct_udredningsforloeb_sql,
    create_borger_cpr_lookup_tables_sql,
    create_organisation_sor_lookup_table_sql,
)
from DSI.csv_files.udredningsret_datasets import HENVIST_MARKOERER

pytestmark = pytest.mark.unit

# Asset-modulerne (til monkeypatch af DfmManager). Pakken re-eksporterer
# assettet under samme navn som modulet, saa attribut-import ville give
# AssetsDefinition — importlib gaar til sys.modules.
pre_hf_module = importlib.import_module("DSA.SQL.staging.pre_stg_Helbredsforloeb")
pre_forloeb_module = importlib.import_module("DSA.SQL.staging.pre_stg_Udredningsforloeb")
dim_forloebsmarkoer_module = importlib.import_module("DSA.SQL.staging.stg_dim_Forloebsmarkoer")
dim_egenskaber_module = importlib.import_module("DSA.SQL.staging.stg_dim_UdredningsretEgenskaber")
fct_forloeb_module = importlib.import_module("DSA.SQL.staging.stg_fct_Udredningsforloeb")
fct_markoer_module = importlib.import_module("DSA.SQL.staging.stg_fct_UdredningsretMarkoer")

CHECKPOINT = "2026-01-01 00:00:00"

# Spec 3.2 / 3.3 / 3.4 / 3.5 / 3.6 — de stagede kolonnesaet i raekkefoelge.
PRE_HF_KOLONNER = [
    "clean_code_Helbredsforloeb",
    "helbredsforloeb_noegle",
    "helbredsforloeb_id",
    "dw_ek_Borger",
    "borger_opslag",
    "foerste_markoer_kode",
    "foerste_markoer_dato",
    "foerste_enhed_sor",
    "foerste_forloebselement_uuid",
    "antal_markoerer",
    "antal_forloebselementer",
    "antal_udredningsforloeb",
    "dw_is_deleted",
    "dw_timestamp_update",
]
PRE_FORLOEB_KOLONNER = [
    "clean_code_Udredningsforloeb",
    "clean_code_Helbredsforloeb",
    "helbredsforloeb_noegle",
    "f_id",
    "pk",
    "loebenummer",
    "startdato",
    "start_markoer_kode",
    "start_markoer_uuid",
    "slutdato",
    "slut_markoer_kode",
    "slut_markoer_uuid",
    "seneste_markoer_dato",
    "foerste_forloebselement_uuid",
    "helbredsforloeb_id",
    *FORLOEB_ATTRIBUT_NAVNE,
    "indgaar_i_indikator",
    "antal_markoerer",
    "antal_udredningsplaner",
    "antal_afbrydelser",
    "dw_is_deleted",
    "dw_timestamp_update",
]
DIM_FORLOEBSMARKOER_KOLONNER = [
    "clean_code_Forloebsmarkoer",
    "kode",
    "gruppe",
    "gruppe_tekst",
    "tekst",
    "tekst_kilde",
    "dw_is_deleted",
    "dw_timestamp_update",
]
FCT_MARKOER_KOLONNER = [
    "clean_code_UdredningsretMarkoer",
    "clean_code_Udredningsforloeb",
    "clean_code_Helbredsforloeb",
    "clean_code_Forloebsmarkoer",
    "dw_ek_Borger",
    "borger_opslag",
    "forloeb_enhed_sor",
    "dw_ek_Organisation_forloeb",
    "markoer_kode",
    "markoer_dato",
    "dw_ek_Dato_markoer",
    "helbredsforloeb_id",
    "forloebselement_uuid",
    "markoer_uuid",
    "f_id",
    "pk",
    "indgaar_i_indikator_H",
    "indgaar_i_indikator_L_N",
    "sygehusregion",
    "bopaelsregion",
    "antal_kilderaekker",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
    "dw_timestamp_update",
]
FCT_FORLOEB_KOLONNER = [
    "clean_code_Udredningsforloeb",
    "clean_code_Helbredsforloeb",
    "clean_code_UdredningsretKlassifikation",
    "dw_ek_Borger",
    "borger_opslag",
    "forloeb_enhed_sor",
    "dw_ek_Organisation_forloeb",
    "kontakt_enhed_sor",
    "dw_ek_Organisation_kontakt",
    "dw_ek_Dato_start",
    "dw_ek_Dato_slut",
    "dw_ek_Dato_opgoerelse",
    "udredningstid_dage",
    "indgaar_i_indikator",
    "overholdt",
    "inden_30_dage",
    "alder_ved_henvisning",
    "aktionsdiagnose_kode",
    "diagnosekontakt_uuid",
    "helbredsforloeb_id",
    "antal_markoerer",
    "antal_udredningsplaner",
    "antal_afbrydelser",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
    "dw_timestamp_update",
]
DIM_EGENSKABER_KOLONNER = [
    "clean_code_Udredningsforloeb",
    "clean_code_UdredningsretKlassifikation",
    *EGENSKABER_ATTRIBUTTER,
    "dw_is_deleted",
    "dw_timestamp_update",
]

# (bygger, maaltabel, forventede kolonner) for de seks stagede tabeller
STAGEDE_TABELLER = [
    (lambda: build_pre_helbredsforloeb_sql(None), PRE_HF_TABLE, PRE_HF_KOLONNER),
    (build_pre_udredningsforloeb_sql, PRE_FORLOEB_TABLE, PRE_FORLOEB_KOLONNER),
    (build_stg_dim_forloebsmarkoer_sql, DIM_FORLOEBSMARKOER_TABLE, DIM_FORLOEBSMARKOER_KOLONNER),
    (build_stg_fct_markoer_sql, FCT_MARKOER_TABLE, FCT_MARKOER_KOLONNER),
    (build_stg_fct_udredningsforloeb_sql, FCT_FORLOEB_TABLE, FCT_FORLOEB_KOLONNER),
    (build_stg_dim_egenskaber_sql, DIM_EGENSKABER_TABLE, DIM_EGENSKABER_KOLONNER),
]
STAGEDE_IDS = [
    "pre_hf",
    "pre_forloeb",
    "dim_forloebsmarkoer",
    "fct_markoer",
    "fct_forloeb",
    "dim_egenskaber",
]


# ---------------------------------------------------------------------------
# Hjaelpere: parse den renderede SQL
# ---------------------------------------------------------------------------


def _norm(sql: str) -> str:
    """Whitespace-normaliseret SQL, saa multi-linje-udtryk kan asserteres som én streng."""
    return " ".join(sql.split())


def _select_liste(sql: str, table: str) -> str:
    """SELECT-listen der foeder `INTO <table>` (kommentarlinjer fjernet).

    Den ydre SELECT staar altid alene paa en linje i byggerne; CTE'ernes
    indre SELECT'er er indrykkede og rammes derfor ikke af rindex.
    """
    into = sql.index(f"\nINTO {table}")
    start = sql.rindex("\nSELECT\n", 0, into) + len("\nSELECT\n")
    linjer = sql[start:into].splitlines()
    return "\n".join(ln for ln in linjer if not ln.strip().startswith("--"))


def _split_topniveau(tekst: str) -> list[str]:
    """Split paa kommaer uden for parenteser (SQL-udtryk kan selv indeholde kommaer)."""
    dele: list[str] = []
    dybde = 0
    aktuel: list[str] = []
    for ch in tekst:
        if ch == "(":
            dybde += 1
        elif ch == ")":
            dybde -= 1
        if ch == "," and dybde == 0:
            dele.append("".join(aktuel))
            aktuel = []
        else:
            aktuel.append(ch)
    dele.append("".join(aktuel))
    return [d.strip() for d in dele if d.strip()]


def _stagede_kolonner(sql: str, table: str) -> list[str]:
    """Kolonnenavnene i den staged tabel, i SELECT-raekkefoelge (alias, ellers sidste [navn])."""
    navne = []
    for udtryk in _split_topniveau(_select_liste(sql, table)):
        alias = re.search(r"AS \[(\w+)\]$", udtryk)
        navne.append(alias.group(1) if alias else re.findall(r"\[(\w+)\]", udtryk)[-1])
    return navne


def _into_maal(sql: str) -> list[str]:
    """SELECT INTO-maal (stg- og temp-tabeller), der foedes af en kolonneliste.

    #scope bygges af en én-linjes `SELECT DISTINCT noegle INTO #scope` uden
    kolonneliste og springes over — den kan ikke baere andet end noeglen.
    """
    maal = []
    for m in re.finditer(r"\nINTO (\[stg\]\.\[\w+\]|#\w+)", sql):
        if sql.rfind("\nSELECT\n", 0, m.start()) > sql.rfind(";", 0, m.start()):
            maal.append(m.group(1))
    return maal


def _when_grene(case_sql: str) -> list[tuple[str, str]]:
    """(praedikat, resultat) for hver WHEN-gren i en CASE."""
    return re.findall(r"WHEN (.*?) THEN N?'([^']+)'", case_sql)


def _bredde(sql_type: str) -> int:
    return int(re.search(r"\((\d+)\)", sql_type).group(1))


def _gruppenavne() -> list[str]:
    return [g for g, _, _ in MARKOER_GRUPPER]


# ---------------------------------------------------------------------------
# (1) clean_codes: praefikser, bredder og VARCHAR (spec 2)
# ---------------------------------------------------------------------------


def test_clean_code_praefikser_er_spec_2s():
    """Praefikserne er kontrakt mod map-laget (og U9's SUBSTRING i delta-scopet)."""
    assert HF_PREFIX == "UDREDHF_"
    assert FORLOEB_PREFIX == "UDREDFORL_"
    assert MARKOER_PREFIX == "UDREDMRK_"
    assert KLASSIFIKATION_PREFIX == "UDREDKLASS_"
    assert FORLOEBSMARKOER_PREFIX == "FORLOEBSMARKOER_"


def test_clean_code_helbredsforloeb_er_praefiks_plus_noegle_i_varchar_80():
    sql = clean_code_helbredsforloeb_sql("n.[helbredsforloeb_noegle]")
    assert sql == "CAST('UDREDHF_' + n.[helbredsforloeb_noegle] AS VARCHAR(80))"
    assert len(HF_PREFIX) + 64 <= _bredde(HF_CLEAN_CODE_TYPE) == 80


def test_clean_code_udredningsforloeb_bruger_startdato_uden_bindestreger_og_loebenummer():
    sql = clean_code_udredningsforloeb_sql("n.[hf]", "n.[startdato]", "n.[loebenummer]")
    assert sql.startswith("CAST('UDREDFORL_' + n.[hf] + '_' ")
    # yyyymmdd: bindestregerne fjernes, NULL-startdato holder koden ikke-NULL
    assert "+ REPLACE(ISNULL(n.[startdato], ''), '-', '') + '_' " in sql
    assert sql.endswith("+ CAST(n.[loebenummer] AS VARCHAR(10)) AS VARCHAR(100))")
    # 10 + 64 + '_' + 8 + '_' + loebenummer (<= 10 cifre) skal kunne vaere i typen
    assert len(FORLOEB_PREFIX) + 64 + 1 + 8 + 1 + 10 <= _bredde(FORLOEB_CLEAN_CODE_TYPE)


def test_clean_code_markoer_er_praecis_arkivets_mergenoegle():
    """(markoer_noegle, loebenummer) er arc's PK — saa fact-raekke og arc-raekke er 1:1
    og en soft-delete i arc retirerer den samme fact-raekke."""
    sql = clean_code_markoer_sql("a.[markoer_noegle]", "a.[loebenummer]")
    assert sql == (
        "CAST('UDREDMRK_' + a.[markoer_noegle] + '_' "
        "+ CAST(a.[loebenummer] AS VARCHAR(10)) AS VARCHAR(100))"
    )
    assert len(MARKOER_PREFIX) + 64 + 1 + 10 <= _bredde(MARKOER_CLEAN_CODE_TYPE)


def test_clean_code_forloebsmarkoer_normaliserer_koden():
    sql = clean_code_forloebsmarkoer_sql("k.[kode]")
    assert sql == "CAST('FORLOEBSMARKOER_' + UPPER(LTRIM(RTRIM(k.[kode]))) AS VARCHAR(50))"
    assert len(FORLOEBSMARKOER_PREFIX) + 20 <= _bredde(FORLOEBSMARKOER_CLEAN_CODE_TYPE)


def test_alle_clean_codes_er_ascii_varchar_og_uden_cpr():
    """Spec 2: ASCII og VARCHAR (kun klassifikationen er NVARCHAR som DRG's DDL)."""
    for sql in (
        clean_code_helbredsforloeb_sql("x"),
        clean_code_udredningsforloeb_sql("x", "y", "z"),
        clean_code_markoer_sql("x", "y"),
        clean_code_forloebsmarkoer_sql("x"),
    ):
        assert re.search(r"AS VARCHAR\(\d+\)\)$", sql), sql
        assert "NVARCHAR" not in sql
        assert "cpr" not in sql.lower()
    assert klassifikation_clean_code_sql("p").endswith(f"AS {KLASSIFIKATION_CLEAN_CODE_TYPE})")
    assert KLASSIFIKATION_CLEAN_CODE_TYPE == "NVARCHAR(100)"
    assert len(KLASSIFIKATION_PREFIX) + 64 <= 100


# ---------------------------------------------------------------------------
# (2) Markoergrupper (SDS bilag 1, spec 3.3)
# ---------------------------------------------------------------------------

# Repraesentative koder pr. gruppe -> det tekstfragment i gruppens praedikat,
# der faanger dem. Fragmentet maa KUN staa i den gruppe, saa foerste-match-
# reglen ikke kan skjule en kode, der er kopieret ind i to grupper.
REPRAESENTANTER = {
    "henvist_udredning": {
        "AFV01A1": "'AFV01A1'",
        "AFV01A1A": "'AFV01A1A'",
        "AFA01A": "'AFA01A'",
        "AFD01A": "'AFD01A'",
        "AFB12A": "LIKE 'AFB[0-9][0-9]A'",
    },
    "udredning_start": {"AFV01B": "= 'AFV01B'"},
    "endeligt_udredt": {"AFX01A3": "LIKE 'AFX01A%'", "AFX01C": "= 'AFX01C'"},
    "udredningsplan": {"AFX01D1A": "LIKE 'AFX01D1%'", "AFX01D4": "'AFX01D4'"},
    "afbrudt": {"AFV01X4": "'AFV01X4'"},
    "kraeftpakke_slut_beslutning": {
        "AFB12C1": "LIKE 'AFB[0-9][0-9]C%'",
        "AFD01X9": "'AFD01X9'",
        "AFB12X2": "LIKE 'AFB[0-9][0-9]X[29]'",
    },
    "behandling_start": {"AFV01F": "= 'AFV01F'"},
}


def test_markoergrupper_staar_i_spec_3_3s_prioriterede_raekkefoelge():
    assert _gruppenavne() == [
        "henvist_udredning",
        "udredning_start",
        "endeligt_udredt",
        "udredningsplan",
        "afbrudt",
        "kraeftpakke_slut_beslutning",
        "behandling_start",
    ]
    assert OEVRIGE_GRUPPE == "oevrige" and OEVRIGE_GRUPPE_TEKST == "Øvrige"
    assert MARKOER_GRUPPE_TEKSTER == {
        "henvist_udredning": "Henvist til udredning",
        "udredning_start": "Udredning start",
        "endeligt_udredt": "Endeligt udredt",
        "udredningsplan": "Udredningsplan",
        "afbrudt": "Afbrudt",
        "kraeftpakke_slut_beslutning": "Kræftpakke slut/beslutning",
        "behandling_start": "Behandling start",
        "oevrige": "Øvrige",
    }


def test_henvist_koder_deles_med_datasaettet():
    """Ekstrakt, arkiv og staging maa ikke vaere uenige om, hvad 'henvist' er."""
    assert HENVIST_KODER == HENVIST_MARKOERER == ("AFV01A1", "AFV01A1A", "AFA01A", "AFD01A")
    assert HENVIST_LIKE == "AFB[0-9][0-9]A"


def test_markoer_gruppe_case_render_grupperne_i_orden_med_oevrige_som_else():
    case = markoer_gruppe_sql("k")
    grene = _when_grene(case)
    assert [g for _, g in grene] == _gruppenavne()
    assert case.rstrip().endswith("ELSE 'oevrige' END")
    for praedikat, _ in grene:
        assert praedikat.startswith("k "), praedikat  # kolonneudtrykket bruges i hver gren


def test_hver_repraesentativ_kode_fanges_af_sin_gruppe_og_kun_den():
    grene = dict((g, p) for p, g in _when_grene(markoer_gruppe_sql("k")))
    for gruppe, koder in REPRAESENTANTER.items():
        for kode, fragment in koder.items():
            assert fragment in grene[gruppe], f"{kode} ({fragment}) mangler i {gruppe}"
            andre = [g for g, p in grene.items() if g != gruppe and fragment in p]
            assert not andre, f"{kode} ({fragment}) staar ogsaa i {andre} — foerste match vinder"


def test_henvist_vinder_over_kraeftpakke_for_afb_koderne():
    """AFB12A er henvisning, AFB12C1/AFB12X2 er kraeftpakke-slut: begge LIKE-moenstre
    starter med AFB, saa henvist SKAL evalueres foerst og moenstrene maa ikke overlappe."""
    navne = _gruppenavne()
    assert navne.index("henvist_udredning") < navne.index("kraeftpakke_slut_beslutning")
    henvist = {g: p for p, g in _when_grene(markoer_gruppe_sql("k"))}["henvist_udredning"]
    assert "LIKE 'AFB[0-9][0-9]A'" in henvist
    assert "AFB[0-9][0-9]A%" not in henvist  # et %-suffiks ville sluge AFB12A1/AFB12AX


def test_henvist_predicate_er_parentesiseret_og_identisk_med_foerste_gren():
    """Praedikatet indeholder OR og bruges under AND/CASE — uden parentes ville
    'x AND a OR b' binde forkert."""
    praedikat = henvist_predicate_sql("[start_markoer_kode]")
    assert praedikat.startswith("(") and praedikat.endswith(")")
    foerste, _ = _when_grene(markoer_gruppe_sql("[start_markoer_kode]"))[0]
    assert praedikat == f"({foerste})"
    assert "[start_markoer_kode] IN ('AFV01A1', 'AFV01A1A', 'AFA01A', 'AFD01A')" in praedikat


def test_gruppetekst_case_bruger_samme_grene_og_n_literaler():
    """Dansk tekst med ae/oe (Kraeftpakke, Oevrige) kraever N'' — ellers mojibake."""
    tekst_case = markoer_gruppe_tekst_sql("k")
    gruppe_case = markoer_gruppe_sql("k")
    assert [p for p, _ in _when_grene(tekst_case)] == [p for p, _ in _when_grene(gruppe_case)]
    assert [t for _, t in _when_grene(tekst_case)] == [t for _, t, _ in MARKOER_GRUPPER]
    assert "THEN N'Kræftpakke slut/beslutning'" in tekst_case
    assert tekst_case.rstrip().endswith("ELSE N'Øvrige' END")


# ---------------------------------------------------------------------------
# (3) Klassifikationens clean_code (spec 2 + 3.6)
# ---------------------------------------------------------------------------


def test_klassifikation_attributter_er_spec_3_6s_otte_i_raekkefoelge():
    assert KLASSIFIKATION_ATTRIBUTTER == (
        "overholdt",
        "aarsag",
        "inden_30_dage",
        "psykiatri",
        "hjerteomraade",
        "forloebslabel",
        "bopaelsregion_forloeb",
        "speciale",
    )


def test_klassifikation_clean_code_hasher_praecis_de_otte_laengde_praefikset():
    """Uden laengde-praefiks ville ('AB','C') og ('A','BC') hashe ens."""
    sql = klassifikation_clean_code_sql("p")
    assert sql.startswith("CAST('UDREDKLASS_' + CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT(")
    assert sql.count("CONCAT(LEN(") == 8 == len(KLASSIFIKATION_ATTRIBUTTER)
    positioner = [sql.index(f"p.[{col}]") for col in KLASSIFIKATION_ATTRIBUTTER]
    assert positioner == sorted(positioner)  # samme orden som tuplen -> stabil hash
    # ingen andre kolonner i hash-inputtet (fx f_id ville give en ny noegle pr. omnummerering)
    assert set(re.findall(r"p\.\[(\w+)\]", sql)) == set(KLASSIFIKATION_ATTRIBUTTER)
    # NULL og blank kollapser, saa 'uoplyst' er én klassifikation
    assert sql.count("ISNULL(UPPER(LTRIM(RTRIM(p.[") == 2 * 8


def test_fct_og_dim_egenskaber_bruger_identisk_klassifikationsudtryk():
    """map.UdredningsretKlassifikation mintes fra dim-staging og opsloges fra facten —
    to forskellige udtryk ville give facten -1 paa alle raekker."""
    udtryk = klassifikation_clean_code_sql("p") + " AS [clean_code_UdredningsretKlassifikation]"
    assert udtryk in build_stg_fct_udredningsforloeb_sql()
    assert udtryk in build_stg_dim_egenskaber_sql()


# ---------------------------------------------------------------------------
# (4) Opgoerelseskvartal -> dato (spec 3.6)
# ---------------------------------------------------------------------------


def test_opgoerelse_dato_mapper_kvartal_1_til_4_og_falder_tilbage_til_blank():
    sql = opgoerelse_dato_sql("p.[q]")
    assert OPGOERELSE_KVARTAL_LIKE == "[1-4]K[0-9][0-9]" and "%" not in OPGOERELSE_KVARTAL_LIKE
    assert sql.startswith("CASE WHEN p.[q] LIKE '[1-4]K[0-9][0-9]' THEN ")
    # '2K26' -> '20' + '26' + '-' + '04' + '-01'
    assert "'20' + SUBSTRING(p.[q], 3, 2) + '-' + CASE SUBSTRING(p.[q], 1, 1) " in sql
    assert "WHEN '1' THEN '01' WHEN '2' THEN '04' WHEN '3' THEN '07' ELSE '10' END + '-01'" in sql
    # tom streng (ikke NULL) -> dato_key's BLANK, aldrig UNKNOWN
    assert sql.endswith("+ '-01' ELSE '' END")


def test_fct_forloeb_opgoerelsesdato_gaar_gennem_dato_key():
    sql = build_stg_fct_udredningsforloeb_sql()
    assert (
        dato_key(opgoerelse_dato_sql("p.[opgoerelseskvartal]")) + " AS [dw_ek_Dato_opgoerelse]"
        in sql
    )


# ---------------------------------------------------------------------------
# (5) Delta-scope og borger-genopretning (spec 3.1, U9)
# ---------------------------------------------------------------------------


def test_fuld_load_scoper_alle_noegler_uden_checkpoint_og_uden_fact_opslag():
    sql = build_pre_helbredsforloeb_sql(None)
    assert re.search(
        r"SELECT DISTINCT a\.\[helbredsforloeb_noegle\]\s+INTO #scope\s+"
        r"FROM \[arc\]\.\[udredningsret_mont\] AS a;",
        sql,
    )
    assert "dw_timestamp_update] >" not in sql
    assert "[fct].[Udredningsforloeb]" not in sql and "#scope_kandidater" not in sql


def test_delta_scoper_paa_arc_tidsstempel_inkl_soft_slettede():
    sql = build_pre_helbredsforloeb_sql(CHECKPOINT)
    assert f"WHERE a.[dw_timestamp_update] > '{CHECKPOINT}'" in sql
    # gren (1): en soft-slettet raekke skal ogsaa traekke sit helbredsforloeb i scope
    gren_1 = sql.split("INSERT INTO #scope_kandidater")[1].split(";")[0]
    assert "dw_is_deleted" not in gren_1
    assert "JOIN #scope AS s ON s.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]" in sql


def test_delta_har_guarded_borger_genopretning_fra_facten():
    sql = build_pre_helbredsforloeb_sql(CHECKPOINT)
    assert "IF OBJECT_ID(N'[fct].[Udredningsforloeb]', 'U') IS NOT NULL" in sql
    assert "AND OBJECT_ID(N'[map].[Helbredsforloeb]', 'U') IS NOT NULL" in sql
    gren_2 = sql.split("BEGIN")[1].split("END;")[0]
    assert "FROM [fct].[Udredningsforloeb] AS f" in gren_2
    assert "JOIN [map].[Helbredsforloeb] AS m" in gren_2
    assert "f.[dw_is_deleted] = 0" in gren_2  # kun levende fact-raekker
    assert f"f.[dw_ek_Borger] = {UNKNOWN_BORGER}" in gren_2 and UNKNOWN_BORGER == -1
    # noeglen genskabes fra clean_code'en: 'UDREDHF_' (8 tegn) + 64 tegn noegle
    assert "SUBSTRING(m.[clean_code_Helbredsforloeb], 9, 64) AS CHAR(64)" in gren_2
    assert "LEFT(m.[clean_code_Helbredsforloeb], 8) = 'UDREDHF_'" in gren_2


def test_checkpoint_vaerdi_escapes():
    sql = build_pre_helbredsforloeb_sql("2026' OR 1=1 --")
    assert "2026' OR 1=1" not in sql
    assert "> '2026'' OR 1=1 --'" in sql


def test_pre_hf_asset_laeser_alle_downstream_ejere_og_bruger_min(monkeypatch, ctx):
    """Delta-vinduet er MIN over ejerne: en ejer, der haenger bagud, bestemmer vinduet."""
    cps = {
        ("dim", "UdredningsretEgenskaber"): "2026-03-01 00:00:00",
        ("dim", "UdredningsretKlassifikation"): "2026-03-01 00:00:00",
        ("dim", "Forloebsmarkoer"): "2026-04-01 00:00:00",
        ("fct", "Udredningsforloeb"): "2026-02-01 00:00:00",
        ("fct", "UdredningsretMarkoer"): "2026-03-01 00:00:00",
    }
    dfm = _dfm(monkeypatch, pre_hf_module, get_checkpoint=lambda s, t: cps[(s, t)])
    mssql = _FakeMssql(fetchone=[(14, 2)], fetchall=[[("cpr", 11), ("ukendt", 2), ("tom", 1)]])

    result = pre_stg_Helbredsforloeb(ctx, mssql, RunModeConfig())

    assert {c.args for c in dfm.get_checkpoint.call_args_list} == set(DOWNSTREAM_CHECKPOINT_EJERE)
    assert len(DOWNSTREAM_CHECKPOINT_EJERE) == 5
    # borger-opslaget skal ligge FOER pre-SQL'en paa samme forbindelse (#borger_current_cpr joines)
    assert mssql.executed[0] == create_borger_cpr_lookup_tables_sql()
    assert mssql.executed[1] == build_pre_helbredsforloeb_sql("2026-02-01 00:00:00")
    assert "> '2026-02-01 00:00:00'" in mssql.executed[1]
    assert _md(result, "checkpoint") == "2026-02-01 00:00:00"
    assert _md(result, "row_count") == 14
    assert _md(result, "helbredsforloeb_uden_levende_markoerer") == 2
    assert _md(result, "borger_uoploeste") == 2 and _md(result, "borger_via_tidligere_cpr") == 0
    dfm.complete_dfm_run.assert_called_once_with(117, rows_affected=14)


def test_pre_hf_asset_koerer_fuld_load_naar_en_ejer_mangler_checkpoint(monkeypatch, ctx):
    """U11: én ejer uden checkpoint (fejlet/aldrig koert) tvinger fuld load — DRG's regel
    (spring None over) ville krympe vinduet til de oevrige ejeres checkpoint, saa den
    haltende ejer aldrig fik sine raekker og dens kontroltotal fejlede hver koersel."""
    cps = {
        ("dim", "UdredningsretEgenskaber"): "2026-03-01 00:00:00",
        ("dim", "UdredningsretKlassifikation"): "2026-03-01 00:00:00",
        ("dim", "Forloebsmarkoer"): "2026-03-01 00:00:00",
        ("fct", "Udredningsforloeb"): "2026-03-01 00:00:00",
        ("fct", "UdredningsretMarkoer"): None,
    }
    _dfm(monkeypatch, pre_hf_module, get_checkpoint=lambda s, t: cps[(s, t)])
    mssql = _FakeMssql(fetchone=[(1, 0)], fetchall=[[("cpr", 1)]])

    result = pre_stg_Helbredsforloeb(ctx, mssql, RunModeConfig())

    assert mssql.executed[1] == build_pre_helbredsforloeb_sql(None)
    assert "dw_timestamp_update] >" not in mssql.executed[1]
    assert _md(result, "checkpoint") == "full load"


@pytest.mark.parametrize("mode, cp", [("full_load", "2026-03-01 00:00:00"), ("delta", None)])
def test_pre_hf_asset_koerer_fuld_load_ved_full_load_eller_uden_checkpoints(
    monkeypatch, ctx, mode, cp
):
    _dfm(monkeypatch, pre_hf_module, get_checkpoint=lambda s, t: cp)
    mssql = _FakeMssql(fetchone=[(1, 0)], fetchall=[[("cpr", 1)]])

    result = pre_stg_Helbredsforloeb(ctx, mssql, RunModeConfig(mode=mode))

    assert mssql.executed[1] == build_pre_helbredsforloeb_sql(None)
    assert _md(result, "checkpoint") == "full load"


# ---------------------------------------------------------------------------
# (6) GDPR: CPR forlader aldrig pre_stg
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bygger, table, _kolonner", STAGEDE_TABELLER, ids=STAGEDE_IDS)
def test_ingen_stg_select_liste_naevner_cpr(bygger, table, _kolonner):
    sql = bygger()
    for maal in _into_maal(sql):
        liste = _select_liste(sql, maal)
        if maal.startswith("[stg]"):
            assert "cpr" not in liste.lower(), f"{maal} har CPR i SELECT-listen"
        # temp-tabellen #hf_raekker afleder dw_ek_Borger af CPR, men projicerer den aldrig
        assert "AS [CPR]" not in liste and "AS [cpr]" not in liste, maal
    assert not [n for n in _stagede_kolonner(sql, table) if "cpr" in n.lower()]


def test_kun_pre_hf_roerer_cpr_og_kun_i_opslag_og_tomhedstest():
    for bygger in (
        build_pre_udredningsforloeb_sql,
        build_stg_dim_forloebsmarkoer_sql,
        build_stg_fct_markoer_sql,
        build_stg_fct_udredningsforloeb_sql,
        build_stg_dim_egenskaber_sql,
        build_borger_coverage_sql,
    ):
        assert "cpr" not in bygger().lower(), bygger.__name__
    sql = build_pre_helbredsforloeb_sql(CHECKPOINT)
    # hver forekomst af a.[CPR] er enten en IS NULL-test eller et trimmet join-/tomhedsudtryk
    forekomster = len(re.findall(r"a\.\[CPR\]", sql))
    assert forekomster > 0
    assert forekomster == sql.count("LTRIM(RTRIM(a.[CPR]))") + sql.count("a.[CPR] IS NULL")
    assert "b.[cpr] = LTRIM(RTRIM(a.[CPR]))" in sql and "bt.[cpr] = LTRIM(RTRIM(a.[CPR]))" in sql
    # ingen laengdecast paa leverancesiden (en 11+-tegns vaerdi maa ikke trunkeres til et match)
    assert "b.[cpr] = CAST(" not in sql and "bt.[cpr] = CAST(" not in sql


def test_borger_opslag_med_tidligere_cpr_fallback_og_sentineler():
    sql = build_pre_helbredsforloeb_sql(None)
    assert f"COALESCE(b.[dw_ek_Borger], bt.[dw_ek_Borger], {UNKNOWN_BORGER})" in sql
    assert f"THEN {NA_BORGER}" in sql and NA_BORGER == -2  # blank CPR -> N/A, ikke UNKNOWN
    for udfald in ("'tom'", "'cpr'", "'tidligere_cpr'", "'ukendt'"):
        assert udfald in sql
    # noeglen tages over levende raekker, ellers over de slettede (retireringsraekken peger rigtigt)
    assert (
        "COALESCE(MIN(CASE WHEN n.[dw_is_deleted] = 0 THEN n.[dw_ek_Borger] END), "
        "MIN(n.[dw_ek_Borger])) AS [dw_ek_Borger]"
    ) in sql
    assert "[borger_opslag], COUNT(*) AS [antal]" in build_borger_coverage_sql()


# ---------------------------------------------------------------------------
# Stagede kolonnesaet og batch-form
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bygger, table, kolonner", STAGEDE_TABELLER, ids=STAGEDE_IDS)
def test_stagede_kolonner_er_specs_i_raekkefoelge(bygger, table, kolonner):
    sql = bygger()
    assert _stagede_kolonner(sql, table) == kolonner
    assert len(set(kolonner)) == len(kolonner)
    for navn in kolonner:
        assert not set(navn) & set("æøåÆØÅ"), navn


@pytest.mark.parametrize("bygger, table, kolonner", STAGEDE_TABELLER, ids=STAGEDE_IDS)
def test_hver_bygger_er_en_batch_med_drop_select_into_og_unikt_grain_index(bygger, table, kolonner):
    sql = bygger()
    assert "\nGO\n" not in sql
    assert sql.index(f"DROP TABLE IF EXISTS {table};") < sql.index(f"\nINTO {table}")
    grain = kolonner[0]
    assert re.search(
        rf"CREATE UNIQUE CLUSTERED INDEX \[IX_\w+_PK\]\s+ON {re.escape(table)}\(\[{grain}\]\);", sql
    ), f"grain paa {grain} haandhaeves ikke fysisk"


def test_pre_hf_foerste_markoer_taellere_og_slettemarkering():
    sql = build_pre_helbredsforloeb_sql(None)
    # foerste LEVENDE markoer i arkivets noegleorden
    assert (
        "CASE WHEN r.[dw_is_deleted] = 0 THEN ROW_NUMBER() OVER ( PARTITION BY "
        "r.[helbredsforloeb_noegle], r.[dw_is_deleted] ORDER BY r.[dato], r.[kode], "
        "r.[enhed_sor], r.[forloebselement_uuid] ) END AS [rn_foerste]"
    ) in _norm(sql)
    live = "n.[dw_is_deleted] = 0"
    assert f"CAST(MAX(CASE WHEN {live} THEN n.[helbredsforloeb_id] END) AS NVARCHAR(50))" in sql
    assert f"SUM(CASE WHEN {live} THEN 1 ELSE 0 END) AS [antal_markoerer]" in sql
    assert f"COUNT(DISTINCT CASE WHEN {live} THEN n.[forloebselement_uuid] END)" in sql
    assert f"COUNT(DISTINCT CASE WHEN {live} THEN n.[f_id] END) AS [antal_udredningsforloeb]" in sql
    assert "NULLIF(LTRIM(RTRIM(a.[F_ID])), '') AS [f_id]" in sql  # blank F_ID taeller ikke
    # alle markoerer soft-slettede -> dw_is_deleted = 1, men raekken staged'es (spec 6)
    assert f"CASE WHEN SUM(CASE WHEN {live} THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END AS BIT" in sql
    assert "WHERE a.[dw_is_deleted] = 0" not in sql
    # tidsstemplet daekker ALLE raekker, saa en ren sletning ogsaa bevaeger delta-vinduet
    assert "MAX(n.[dw_timestamp_update]) AS [dw_timestamp_update]" in sql


# ---------------------------------------------------------------------------
# (7) pre_Udredningsforloeb: grain, start/slut/attribut, loebenummer (spec 3.3)
# ---------------------------------------------------------------------------


def test_pre_forloeb_laeser_levende_raekker_med_f_id_i_scope():
    sql = build_pre_udredningsforloeb_sql()
    assert "WHERE a.[dw_is_deleted] = 0" in sql
    assert "AND a.[F_ID] IS NOT NULL AND LTRIM(RTRIM(a.[F_ID])) <> ''" in sql
    assert (
        f"EXISTS (SELECT 1 FROM {PRE_HF_TABLE} AS h "
        "WHERE h.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle])"
    ) in _norm(sql)
    assert "CAST(0 AS BIT) AS [dw_is_deleted]" in sql  # forsvundne forloeb via scope, spec 6
    assert markoer_gruppe_sql("LTRIM(RTRIM(a.[FORLOEBSMARKOER]))") in sql


def test_pre_forloeb_vinduer_partitionerer_paa_helbredsforloeb_og_f_id():
    sql = _norm(build_pre_udredningsforloeb_sql())
    assert sql.count("PARTITION BY m.[helbredsforloeb_noegle], m.[f_id]") == 2
    assert "GROUP BY r.[helbredsforloeb_noegle], r.[f_id]" in sql


def test_forloeb_markoerer_bevarer_f_id_og_bruger_nonclustered_opslagsindeks():
    """CHAR(64) + NVARCHAR(500) fits the 1,700-byte nonclustered limit."""
    sql = build_pre_udredningsforloeb_sql()
    assert "CAST(LTRIM(RTRIM(a.[F_ID])) AS NVARCHAR(500)) AS [f_id]," in sql
    assert "CREATE NONCLUSTERED INDEX [IX_forloeb_markoerer]" in sql
    assert "ON #forloeb_markoerer([helbredsforloeb_noegle], [f_id])" in sql


def test_startmarkoer_er_tidligste_henvist_ellers_tidligste_af_alle():
    sql = _norm(build_pre_udredningsforloeb_sql())
    assert (
        "ORDER BY CASE WHEN m.[gruppe] = 'henvist_udredning' THEN 0 ELSE 1 END, "
        "m.[dato], m.[kode], m.[markoer_uuid], m.[loebenummer] ) AS [rn_start]"
    ) in sql
    for kol in ("dato", "kode", "markoer_uuid"):
        assert f"MAX(CASE WHEN r.[rn_start] = 1 THEN r.[{kol}] END)" in sql


def test_slutmarkoer_er_mrk_h_uden_for_henvist_ellers_seneste_endeligt_udredt_ellers_ingen():
    sql = _norm(build_pre_udredningsforloeb_sql())
    prioritet = (
        "CASE WHEN m.[mrk_h_1] = 1 AND m.[gruppe] <> 'henvist_udredning' THEN 0 "
        "WHEN m.[gruppe] = 'endeligt_udredt' THEN 1 ELSE 2 END"
    )
    assert f"{prioritet} AS [slut_prioritet]" in sql
    # attributmarkoer: slutprioritet foerst, saa seneste dato (U2/U3); ties kode, uuid
    # og til sidst arkivets loebenummer — to arc-raekker af samme markoer (10.531
    # maalt) er ens paa alt andet, og pk/attributterne staar IKKE i ORDER BY, saa
    # uden den sidste noegle ville valget af attributmarkoer vaere motorafhaengigt
    assert (
        f"ORDER BY {prioritet}, m.[dato] DESC, m.[kode], m.[markoer_uuid], m.[loebenummer] ) "
        "AS [rn_attribut]"
    ) in sql
    assert "a.[loebenummer]," in sql  # baeres ind i #forloeb_markoerer
    # slut-kolonnerne saettes kun, naar attributmarkoeren faktisk er en slutmarkoer
    slut = "r.[rn_attribut] = 1 AND r.[slut_prioritet] < 2"
    for kol in ("dato", "kode", "markoer_uuid"):
        assert f"MAX(CASE WHEN {slut} THEN r.[{kol}] END)" in sql
    assert "MAX(r.[dato]) AS [seneste_markoer_dato]" in sql  # baeres separat (U2)


def test_attributter_laeses_fra_attributmarkoeren_og_types_pr_spec():
    sql = build_pre_udredningsforloeb_sql()
    for navn, kilde, sql_type in FORLOEB_ATTRIBUTTER:
        assert f"NULLIF(LTRIM(RTRIM(a.[{kilde}])), '') AS [{navn}]" in sql
        assert f"MAX(CASE WHEN r.[rn_attribut] = 1 THEN r.[{navn}] END) AS [{navn}]" in sql
        assert f"CAST(n.[{navn}] AS {sql_type}) AS [{navn}]" in sql
    assert "MAX(CASE WHEN r.[rn_attribut] = 1 THEN r.[pk] END) AS [pk]" in sql


def test_forloeb_attributter_er_spec_3_3s_nitten():
    assert FORLOEB_ATTRIBUTTER == (
        ("aktionsdiagnose_kode", "C_ADIAG", "NVARCHAR(20)"),
        ("diagnosekontakt_uuid", "C_ADIAG_KONTAKT", "NVARCHAR(36)"),
        ("kontakt_enhed_sor", "ANSVARLIG_ENHED_KONTAKT", "NVARCHAR(25)"),
        ("forloeb_enhed_sor", "ANSVARLIG_ENHED_FORLOB", "NVARCHAR(25)"),
        ("speciale", "Speciale_ansvarlig_enhed_kontakt", "NVARCHAR(100)"),
        ("alder_raa", "ALDER", "NVARCHAR(10)"),
        ("bopaelsregion_forloeb", "C_BOREGION_F", "NVARCHAR(10)"),
        ("bopaelsregion", "C_BOREGION", "NVARCHAR(10)"),
        ("sygehusregion", "C_SGHREGION", "NVARCHAR(10)"),
        ("psykiatri", "PSYKIATRI", "NVARCHAR(5)"),
        ("hjerteomraade", "MRK_C", "NVARCHAR(5)"),
        ("udredningstid_raa", "H", "NVARCHAR(10)"),
        ("inden_30_dage", "MRK_ST_H", "NVARCHAR(5)"),
        ("overholdt", "MRK_ST_L", "NVARCHAR(5)"),
        ("aarsag", "MRK_ST_N", "NVARCHAR(5)"),
        ("mrk_l_n", "MRK_L_N", "NVARCHAR(5)"),
        ("opgoerelseskvartal", "OPGPER", "NVARCHAR(10)"),
        ("opgoerelsesmaaned", "OPGMAANED", "NVARCHAR(10)"),
        ("forloebslabel", "FORLOEBSLABEL", "NVARCHAR(20)"),
    )
    assert len(FORLOEB_ATTRIBUT_NAVNE) == 19 == len(set(FORLOEB_ATTRIBUT_NAVNE))


def test_indgaar_i_indikator_kraever_henvist_og_ikke_henvist_mrk_h():
    """De 11 forloeb med kun én MRK_H-markoer indgaar ikke (som i kilden)."""
    sql = _norm(build_pre_udredningsforloeb_sql())
    assert (
        "CASE WHEN SUM(CASE WHEN r.[mrk_h_1] = 1 AND r.[er_henvist] = 1 THEN 1 ELSE 0 END) > 0 "
        "AND SUM(CASE WHEN r.[mrk_h_1] = 1 AND r.[er_henvist] = 0 THEN 1 ELSE 0 END) > 0 "
        "THEN 1 ELSE 0 END AS [indgaar_i_indikator]"
    ) in sql
    assert "CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END AS [mrk_h_1]" in sql


def test_taellere_dannes_i_pre_stg_fra_arkivet():
    """U6: forloebsfacten maa aldrig laese markoerfacten."""
    sql = build_pre_udredningsforloeb_sql()
    assert "COUNT(*) AS [antal_markoerer]" in sql
    assert (
        "SUM(CASE WHEN r.[gruppe] = 'udredningsplan' THEN 1 ELSE 0 END) AS [antal_udredningsplaner]"
        in sql
    )
    assert "SUM(CASE WHEN r.[gruppe] = 'afbrudt' THEN 1 ELSE 0 END) AS [antal_afbrydelser]" in sql
    assert (
        FCT_MARKOER_TABLE not in sql
        and FCT_MARKOER_TABLE not in build_stg_fct_udredningsforloeb_sql()
    )


def test_gruppenavne_i_builderen_findes_i_markoergrupperne():
    """Byggeren sammenligner [gruppe] med private literaler — et omdoebt gruppenavn i
    MARKOER_GRUPPER ville ellers stille goere start/slut/taellere tomme."""
    sql = build_pre_udredningsforloeb_sql()
    brugte = set(re.findall(r"\[gruppe\] (?:=|<>) '(\w+)'", sql))
    assert brugte == {"henvist_udredning", "endeligt_udredt", "udredningsplan", "afbrudt"}
    assert brugte <= set(_gruppenavne())


def test_loebenummer_bevarer_historik_og_tildeler_nye_over_hoejeste_reservation():
    sql = _norm(build_pre_udredningsforloeb_sql())
    assert "MAX([loebenummer]) AS [sidste_loebenummer]" in sql
    assert "COALESCE(f.[gemt_loebenummer], ISNULL(h.[sidste_loebenummer], 0)" in sql
    assert "FROM [map].[Udredningsforloeb] AS m" in sql
    assert "LEFT JOIN [dim].[UdredningsretEgenskaber] AS d" in sql
    assert "h.[start_markoer_uuid] = f.[start_markoer_uuid]" in sql
    assert "h.[antal_foer] = 1 AND f.[antal_nu] = 1" in sql
    assert "Ambiguous udredningsforloeb identity" in sql
    assert "MIN(r.[forloebselement_uuid]) AS [foerste_forloebselement_uuid]" in sql


def test_uuid_kolonner_normaliseres_til_lowercase_blank_til_null():
    sql = build_pre_udredningsforloeb_sql()
    assert "NULLIF(LOWER(LTRIM(RTRIM(a.[OBJEKTID_FORLOBSMARKOR]))), '') AS [markoer_uuid]" in sql
    assert (
        "NULLIF(LOWER(LTRIM(RTRIM(a.[OBJEKTID_FORLOBELEMENT]))), '') AS [forloebselement_uuid]"
        in sql
    )


# ---------------------------------------------------------------------------
# (8) fct-byggere: dato-noegler, sentineler, SOR, LEFT JOIN til forloebet
# ---------------------------------------------------------------------------


def test_fct_forloeb_dato_noegler_med_blank_for_manglende_slutmarkoer():
    sql = build_stg_fct_udredningsforloeb_sql()
    assert dato_key("p.[startdato]") + " AS [dw_ek_Dato_start]" in sql
    # U2: slutdato NULL -> ISNULL('') -> BLANK 99999998, aldrig UNKNOWN
    assert dato_key("ISNULL(p.[slutdato], '')") + " AS [dw_ek_Dato_slut]" in sql
    assert dato_key("p.[slutdato]") + " AS [dw_ek_Dato_slut]" not in sql
    assert "TRY_CONVERT(DATETIME2, p.[startdato]) AS [dw_haendelsestidspunkt]" in sql


def test_fct_forloeb_udredningstid_kun_i_indikatoren_og_bit_flag_null_ved_blank():
    sql = _norm(build_stg_fct_udredningsforloeb_sql())
    assert (
        "CASE WHEN p.[indgaar_i_indikator] = 1 THEN TRY_CONVERT(INT, p.[udredningstid_raa]) END "
        "AS [udredningstid_dage]"
    ) in sql
    for kol in ("overholdt", "inden_30_dage"):
        assert f"CAST(CASE p.[{kol}] WHEN '1' THEN 1 WHEN '0' THEN 0 END AS BIT) AS [{kol}]" in sql
    assert "ELSE 0 END AS BIT) AS [overholdt]" not in sql  # blank er 'ikke i opgoerelsen', ikke 0
    assert "TRY_CONVERT(SMALLINT, p.[alder_raa]) AS [alder_ved_henvisning]" in sql


def test_sor_roller_er_de_to_besluttede_og_markoeren_kun_forloebsenheden():
    assert SOR_ROLLER == (
        ("forloeb_enhed_sor", "dw_ek_Organisation_forloeb"),
        ("kontakt_enhed_sor", "dw_ek_Organisation_kontakt"),
    )
    assert MARKOER_SOR_ROLLER == (("forloeb_enhed_sor", "dw_ek_Organisation_forloeb"),)


def test_fct_forloeb_oploeser_begge_sor_roller_hver_med_eget_org_alias():
    sql = build_stg_fct_udredningsforloeb_sql()
    for i, (raw, key) in enumerate(SOR_ROLLER):
        assert f"p.[{raw}],\n    COALESCE(o{i}.[dw_ek_Organisation], -1) AS [{key}]" in sql
        assert (
            f"LEFT JOIN #organisation_sor AS o{i} ON o{i}.[kode] = p.[{raw}] AND p.[{raw}] IS NOT NULL"
            in sql
        )
    assert UNKNOWN_ORGANISATION == -1
    assert "o1.[kode] = p.[kontakt_enhed_sor]" in sql  # ikke o0 to gange
    assert (
        f"JOIN {PRE_HF_TABLE} AS h\n  ON h.[helbredsforloeb_noegle] = p.[helbredsforloeb_noegle]"
        in sql
    )
    assert "h.[dw_ek_Borger],\n    h.[borger_opslag]" in sql  # borger fra scope-tabellen, ikke CPR


def test_fct_markoer_left_joiner_forloebet_og_stager_soft_slettede():
    sql = build_stg_fct_markoer_sql()
    assert (
        f"LEFT JOIN {PRE_FORLOEB_TABLE} AS u\n  ON u.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]"
        "\n AND u.[f_id] = LTRIM(RTRIM(a.[F_ID]))"
    ) in sql
    assert f"JOIN {PRE_FORLOEB_TABLE} AS u" in sql and f"INNER JOIN {PRE_FORLOEB_TABLE}" not in sql
    assert "u.[clean_code_Udredningsforloeb]," in sql  # NULL uden F_ID -> -1 downstream (U5)
    # levende OG soft-slettede raekker: intet dw_is_deleted-filter, flaget staged'es igennem
    assert "dw_is_deleted] = 0" not in sql
    assert "a.[dw_is_deleted],\n    a.[dw_timestamp_update]" in sql
    assert (
        f"JOIN {PRE_HF_TABLE} AS h\n  ON h.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]"
        in sql
    )
    assert f"FROM {ARC_TABLE} AS a" in sql and ARC_TABLE == "[arc].[udredningsret_mont]"


def test_fct_markoer_clean_codes_sentineler_og_maal():
    sql = build_stg_fct_markoer_sql()
    assert (
        clean_code_markoer_sql("a.[markoer_noegle]", "a.[loebenummer]")
        + " AS [clean_code_UdredningsretMarkoer]"
        in sql
    )
    assert "h.[clean_code_Helbredsforloeb]," in sql
    # blank markoerkode -> NULL clean_code (-1 downstream), ellers samme bygger som dimensionen
    assert (
        "CASE WHEN NULLIF(LTRIM(RTRIM(a.[FORLOEBSMARKOER])), '') IS NULL THEN NULL\n         ELSE "
        + clean_code_forloebsmarkoer_sql("a.[FORLOEBSMARKOER]")
        + " END AS [clean_code_Forloebsmarkoer]"
    ) in sql
    assert "h.[dw_ek_Borger],\n    h.[borger_opslag]" in sql
    assert "COALESCE(o0.[dw_ek_Organisation], -1) AS [dw_ek_Organisation_forloeb]" in sql
    assert (
        "LEFT JOIN #organisation_sor AS o0\n  ON o0.[kode] = LTRIM(RTRIM(a.[ANSVARLIG_ENHED_FORLOB]))"
        in sql
    )
    assert "AS NVARCHAR(25)) AS [forloeb_enhed_sor]" in sql  # raa kode ved siden af noeglen
    assert dato_key("a.[FORLOEBSMARKOER_DATO]") + " AS [dw_ek_Dato_markoer]" in sql
    assert (
        "CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END AS BIT) AS [indgaar_i_indikator_H]"
        in sql
    )
    assert (
        "CASE WHEN LTRIM(RTRIM(a.[MRK_L_N])) = '1' THEN 1 ELSE 0 END AS BIT) AS [indgaar_i_indikator_L_N]"
        in sql
    )
    assert "a.[antal_kilderaekker]," in sql
    assert "TRY_CONVERT(DATETIME2, a.[FORLOEBSMARKOER_DATO]) AS [dw_haendelsestidspunkt]" in sql


def test_dim_forloebsmarkoer_er_global_med_sks_tekst_og_ukendt_fallback():
    sql = build_stg_dim_forloebsmarkoer_sql()
    assert PRE_HF_TABLE not in sql  # ikke delta-scoped (433 koder)
    assert "WHERE a.[dw_is_deleted] = 0" in sql
    assert "AND LTRIM(RTRIM(a.[FORLOEBSMARKOER])) <> ''" in sql
    # gruppering, [kode] og clean_code deler ét udtryk: under en case-sensitiv
    # collation ville 'afv01b'/'AFV01B' ellers give to raekker med samme clean_code
    assert "SELECT UPPER(LTRIM(RTRIM(a.[FORLOEBSMARKOER]))) AS [kode]" in sql
    assert "GROUP BY UPPER(LTRIM(RTRIM(a.[FORLOEBSMARKOER])))" in sql
    assert "GROUP BY LTRIM(RTRIM(a.[FORLOEBSMARKOER]))" not in sql
    assert "LEFT JOIN [codesystem].[SKSConcept] AS s ON s.[code] = k.[kode]" in sql
    assert "INNER JOIN [codesystem]" not in sql  # ukendte koder mintes altid (U7)
    assert "CAST(ISNULL(s.[display], N'Ukendt') AS NVARCHAR(500)) AS [tekst]" in sql
    assert (
        "CASE WHEN s.[code] IS NULL THEN 'ukendt' ELSE 'SKS' END AS NVARCHAR(30)) AS [tekst_kilde]"
        in sql
    )
    assert f"CAST({markoer_gruppe_sql('k.[kode]')} AS NVARCHAR(40)) AS [gruppe]" in sql
    assert f"CAST({markoer_gruppe_tekst_sql('k.[kode]')} AS NVARCHAR(100)) AS [gruppe_tekst]" in sql
    assert clean_code_forloebsmarkoer_sql("k.[kode]") + " AS [clean_code_Forloebsmarkoer]" in sql


def test_dim_egenskaber_laeser_kun_pre_forloeb_uden_borger():
    sql = build_stg_dim_egenskaber_sql()
    assert f"FROM {PRE_FORLOEB_TABLE} AS p;" in sql
    assert "JOIN" not in sql
    assert "dw_ek_Borger" not in sql  # borgernoeglen hoerer til fct-siden


def test_grain_check_og_sor_coverage_sql():
    assert (
        build_grain_check_sql(PRE_FORLOEB_TABLE, "clean_code_Udredningsforloeb")
        == "SELECT COUNT(*), COUNT(DISTINCT [clean_code_Udredningsforloeb]) FROM [stg].[pre_Udredningsforloeb]"
    )
    to_roller = build_sor_coverage_sql(FCT_FORLOEB_TABLE, SOR_ROLLER)
    assert to_roller.count("UNION ALL") == 1 and to_roller.rstrip().endswith(";")
    for raw, key in SOR_ROLLER:
        assert f"'{key}' AS [rolle]" in to_roller
        assert f"SUM(CASE WHEN [{raw}] <> '' AND [{key}] = -1" in to_roller
    for kol in ("tom_kode", "udfyldt_uden_match", "matchet", "ialt"):
        assert f"AS [{kol}]" in to_roller
    en_rolle = build_sor_coverage_sql(FCT_MARKOER_TABLE, MARKOER_SOR_ROLLER)
    assert "UNION ALL" not in en_rolle and "dw_ek_Organisation_kontakt" not in en_rolle
    assert f"FROM {FCT_MARKOER_TABLE}" in en_rolle


# ---------------------------------------------------------------------------
# (9) Attributlister og tekster (spec 3.6, 4.2, 7)
# ---------------------------------------------------------------------------


def test_egenskaber_attributter_er_spec_3_6s_tyve_i_raekkefoelge():
    assert EGENSKABER_ATTRIBUTTER == (
        "f_id",
        "pk",
        "start_markoer_kode",
        "start_markoer_uuid",
        "slut_markoer_kode",
        "slut_markoer_uuid",
        "seneste_markoer_dato",
        "foerste_forloebselement_uuid",
        "helbredsforloeb_id",
        "forloeb_enhed_sor",
        "kontakt_enhed_sor",
        "aktionsdiagnose_kode",
        "diagnosekontakt_uuid",
        "opgoerelseskvartal",
        "opgoerelsesmaaned",
        "sygehusregion",
        "bopaelsregion",
        "speciale",
        "forloebslabel",
        "mrk_l_n",
    )


def test_attributlisterne_findes_alle_i_pre_forloeb():
    """En attribut, pre_stg ikke danner, ville foerst fejle mod databasen."""
    stagede = set(PRE_FORLOEB_KOLONNER)
    assert set(EGENSKABER_ATTRIBUTTER) <= stagede
    assert set(KLASSIFIKATION_ATTRIBUTTER) <= stagede
    # opslags-input, raa maal og noegler hoerer ikke i klassifikationen
    for navn in ("f_id", "pk", "udredningstid_raa", "alder_raa", "helbredsforloeb_noegle"):
        assert navn not in KLASSIFIKATION_ATTRIBUTTER


def test_klassifikationstekster_daekker_raa_vaerdier_og_blank():
    assert set(AARSAG_TEKSTER) == {"0", "1", "2", "3", "4", "5", "6", "7", "8", ""}
    assert AARSAG_TEKSTER[""] == "Ikke i opgørelsen" and AARSAG_TEKSTER["0"] == "Uoplyst"
    assert AARSAG_TEKSTER["5"].startswith("Ikke overholdt") and AARSAG_TEKSTER["1"].startswith(
        "Udredt"
    )
    assert OVERHOLDT_TEKSTER == {"1": "Overholdt", "0": "Ikke overholdt", "": "Ikke i opgørelsen"}
    assert INDEN_30_TEKSTER == {
        "1": "Inden for 30 dage",
        "0": "Over 30 dage",
        "": "Ikke i opgørelsen",
    }
    assert PSYKIATRI_TEKSTER == {"1": "Psykiatri", "0": "Somatik", "": "Ukendt"}
    assert HJERTE_TEKSTER == {"1": "Hjerteområdet", "0": "Uden for hjerteområdet", "": "Ukendt"}
    assert REGION_TEKSTER == {
        "1081": "Region Nordjylland",
        "1082": "Region Midtjylland",
        "1083": "Example Region",
        "1084": "Region Hovedstaden",
        "1085": "Region Sjælland",
    }
    assert REGION_UKENDT_TEKST == "Ukendt"


# ---------------------------------------------------------------------------
# (10) Registrering: assets, deps, jobs
# ---------------------------------------------------------------------------

FORVENTEDE_ASSETS = {
    "udredningsret/pre_stg_Helbredsforloeb": "udredningsret_pre_staging",
    "udredningsret/pre_stg_Udredningsforloeb": "udredningsret_pre_staging",
    "udredningsret/stg_dim_Forloebsmarkoer": "udredningsret_staging",
    "udredningsret/stg_fct_UdredningsretMarkoer": "udredningsret_staging",
    "udredningsret/stg_fct_Udredningsforloeb": "udredningsret_staging",
    "udredningsret/stg_dim_UdredningsretEgenskaber": "udredningsret_staging",
}
# US-3's, US-4's, US-5's og US-6's grupper — hele publish-selektionen.
PUBLISH_GRUPPER = (
    "udredningsret_pre_staging",
    "udredningsret_staging",
    "udredningsret_map",
    "udredningsret_dimension",
    "udredningsret_fact",
    "udredningsret_dso_sync",
)
# US-4's og US-5's map/dim/fct-assets. De hedder som resten af repoets dwh-lag
# (uden domaenepraefiks), saa de kan ikke findes paa noeglens foerste led som
# staging-assets'ene — derfor den eksplicitte liste.
FORVENTEDE_DWH_ASSETS = {
    "map_Helbredsforloeb",
    "map_Udredningsforloeb",
    "map_UdredningsretKlassifikation",
    "map_Forloebsmarkoer",
    "map_UdredningsretMarkoer",
    "dim_UdredningsretEgenskaber",
    "dim_UdredningsretKlassifikation",
    "dim_Forloebsmarkoer",
    "fct_Udredningsforloeb",
    "fct_UdredningsretMarkoer",
}
# US-6's DSO-sync-assets. Navnet er dso_sync_<config-noegle i smaa bogstaver>,
# saa en omdoebt config-noegle ville flytte asset'et her uden at fejle andre
# steder.
FORVENTEDE_DSO_SYNC_ASSETS = {
    "dso_sync_dim_udredningsretegenskaber",
    "dso_sync_dim_udredningsretklassifikation",
    "dso_sync_dim_forloebsmarkoer",
    "dso_sync_fct_udredningsforloeb",
    "dso_sync_fct_udredningsretmarkoer",
}
INGEST_GRUPPER = ("udredningsret_extract", "udredningsret_archive", "udredningsret_borger")


def _registrerede() -> dict[str, dg.AssetsDefinition]:
    return {a.key.to_user_string(): a for a in staging_assets if a.key.path[0] == "udredningsret"}


def test_de_seks_assets_er_registreret_med_udredningsret_praefiks_og_grupper():
    reg = _registrerede()
    assert {k: a.group_names_by_key[a.key] for k, a in reg.items()} == FORVENTEDE_ASSETS
    for k, a in reg.items():
        tags = a.tags_by_key[a.key]
        assert tags["domain"] == "udredningsret"
        assert tags["source"] == "udredningsret" and tags["entity"]
        assert tags["layer"] == ("pre_stg" if "/pre_stg_" in k else "stg")
        assert a.op.tags.get("dagster/compute_kind") == "mssql", k
    from DSA.SQL.staging import __all__ as eksporteret

    for navn in FORVENTEDE_ASSETS:
        assert navn.split("/")[1] in eksporteret


def test_asset_deps_foelger_laesningerne():
    """En manglende dep lader Dagster koere et asset foer den tabel, det laeser, er bygget."""
    deps = {
        k: {d.to_user_string() for d in a.asset_deps[a.key]} for k, a in _registrerede().items()
    }
    pre_hf = "udredningsret/pre_stg_Helbredsforloeb"
    pre_forloeb = "udredningsret/pre_stg_Udredningsforloeb"
    assert deps[pre_hf] == {"arc_udredningsret_mont", "dim_Borger"}
    assert deps[pre_forloeb] == {pre_hf}
    assert deps["udredningsret/stg_dim_Forloebsmarkoer"] == {"arc_udredningsret_mont", "cs_sks"}
    # markoerfacten laeser arc direkte (FROM [arc].[udredningsret_mont]); forloebsfacten
    # laeser kun pre_*
    assert deps["udredningsret/stg_fct_UdredningsretMarkoer"] == {
        "arc_udredningsret_mont",
        pre_hf,
        pre_forloeb,
        "dim_Organisation",
    }
    assert deps["udredningsret/stg_fct_Udredningsforloeb"] == {
        pre_hf,
        pre_forloeb,
        "dim_Organisation",
    }
    assert deps["udredningsret/stg_dim_UdredningsretEgenskaber"] == {pre_forloeb}


def test_publish_og_ingest_job_deler_ingen_grupper():
    from jobs import all_jobs
    from jobs.udredningsret_jobs import udredningsret_ingest_job, udredningsret_publish_job

    assert udredningsret_publish_job.name == "udredningsret_publish_job"
    assert udredningsret_publish_job in all_jobs and udredningsret_ingest_job in all_jobs
    publish = str(udredningsret_publish_job.selection)
    ingest = str(udredningsret_ingest_job.selection)
    for gruppe in PUBLISH_GRUPPER:
        assert gruppe in publish and gruppe not in ingest, gruppe
    for gruppe in INGEST_GRUPPER:
        assert gruppe in ingest and gruppe not in publish, gruppe


def test_publish_job_oploeser_alle_enogtyve_udredningsret_assets_i_asset_grafen():
    """AssetSelection.groups() oploeser en ukendt gruppe til tom selektion uden fejl —
    derfor tjekkes mod den rigtige graf, og ingest maa ikke ramme staging.

    Seks staging-assets (US-3) + fire maps, tre dimensioner og forloebsfacten
    (US-4) + markoerens map og fact (US-5), som foejer sig til de samme to
    grupper, + de fem DSO-sync-assets (US-6). Tallet er med vilje eksakt: en
    gruppe, der falder ud af selektionen, ville ellers bare give et mindre
    publish-job uden en fejl."""
    from definitions import defs
    from jobs.udredningsret_jobs import udredningsret_ingest_job, udredningsret_publish_job

    graph = defs.resolve_asset_graph()
    publish = {k.to_user_string() for k in udredningsret_publish_job.selection.resolve(graph)}
    ingest = {k.to_user_string() for k in udredningsret_ingest_job.selection.resolve(graph)}
    forventet = set(FORVENTEDE_ASSETS) | FORVENTEDE_DWH_ASSETS | FORVENTEDE_DSO_SYNC_ASSETS
    assert len(forventet) == 21
    assert publish == forventet
    assert not forventet & ingest
    assert "arc_udredningsret_mont" not in publish


# ---------------------------------------------------------------------------
# (11) Faelles opslags-SQL (U10)
# ---------------------------------------------------------------------------


def test_drg_og_udredningsret_deler_opslags_sql_fra_common():
    assert (
        drg_staging_sql.create_borger_cpr_lookup_tables_sql
        is dimension_lookup_temp_tables.create_borger_cpr_lookup_tables_sql
    )
    assert (
        drg_staging_sql.create_organisation_sor_lookup_table_sql
        is dimension_lookup_temp_tables.create_organisation_sor_lookup_table_sql
    )
    assert (
        udredningsret_staging_sql.create_borger_cpr_lookup_tables_sql
        is dimension_lookup_temp_tables.create_borger_cpr_lookup_tables_sql
    )
    assert (
        udredningsret_staging_sql.create_organisation_sor_lookup_table_sql
        is dimension_lookup_temp_tables.create_organisation_sor_lookup_table_sql
    )
    lookup = create_borger_cpr_lookup_tables_sql()
    assert "INTO #borger_current_cpr\n" in lookup and "INTO #borger_previous_cpr\n" in lookup
    assert "INTO #organisation_sor\n" in create_organisation_sor_lookup_table_sql()


# ---------------------------------------------------------------------------
# Assets mod fakes: DFM-identiteter, raekkefoelge, grain-fejl (kun taellinger)
# ---------------------------------------------------------------------------


class _FakeMssql:
    """MSSQLResource-stand-in: logger SQL og afspiller scriptede fetch-resultater."""

    def __init__(self, fetchone=(), fetchall=()):
        self.executed: list[str] = []
        self.commits = 0
        self._one = deque(fetchone)
        self._all = deque(fetchall)

    def get_connection(self):
        res = self

        class _Cursor:
            def execute(self, sql, *p):
                res.executed.append(sql)

            def fetchone(self):
                return res._one.popleft()

            def fetchall(self):
                return res._all.popleft()

            def nextset(self):
                return None

            def close(self):
                pass

        class _Conn:
            def cursor(self):
                return _Cursor()

            def commit(self):
                res.commits += 1

        @contextmanager
        def cm():
            yield _Conn()

        return cm()


@pytest.fixture
def ctx():
    """Asset-kontekst med ephemeral instance, der lukkes deterministisk efter testen
    (ellers efterlades sqlite-forbindelser til GC ved process-exit)."""
    with dg.build_asset_context() as context:
        yield context


def _dfm(monkeypatch, module, get_checkpoint=None) -> MagicMock:
    dfm = MagicMock()
    dfm.start_dfm_run.return_value = MagicMock(log_id=117, checkpoint=None)
    if get_checkpoint is not None:
        dfm.get_checkpoint.side_effect = get_checkpoint
    monkeypatch.setattr(module, "DfmManager", lambda *a, **k: dfm)
    return dfm


def _md(result: dg.MaterializeResult, key: str):
    v = result.metadata[key]
    return v.value if hasattr(v, "value") else v


ASSET_KOERSLER = [
    (
        pre_stg_Udredningsforloeb,
        pre_forloeb_module,
        ("pre_stg", "pre_Udredningsforloeb"),
        [build_pre_udredningsforloeb_sql()],
        dict(fetchone=[(5, 5), (5, 3, 1, 2, 0)]),
        5,
        {
            "indikatorforloeb": 3,
            "forloeb_uden_slutmarkoer": 1,
            "forloeb_uden_henvist_markoer": 2,
            "loebenummer_over_1": 0,
        },
    ),
    (
        stg_dim_Forloebsmarkoer,
        dim_forloebsmarkoer_module,
        ("stg", "dim_Forloebsmarkoer"),
        [build_stg_dim_forloebsmarkoer_sql()],
        dict(fetchone=[(433, 2)], fetchall=[[("afbrudt", 3), ("oevrige", 400)]]),
        433,
        {"koder_uden_sks_tekst": 2, "antal_pr_gruppe": {"afbrudt": 3, "oevrige": 400}},
    ),
    (
        stg_fct_UdredningsretMarkoer,
        fct_markoer_module,
        ("stg", "fct_UdredningsretMarkoer"),
        [create_organisation_sor_lookup_table_sql(), build_stg_fct_markoer_sql()],
        dict(
            fetchone=[(100, 100), (100, 14, 3, 101, 98)],
            fetchall=[[("dw_ek_Organisation_forloeb", 5, 1, 94, 100)]],
        ),
        100,
        {
            "markoerer_uden_forloeb": 14,
            "soft_slettede": 3,
            "sum_antal_kilderaekker": 101,
            "sum_antal_kilderaekker_levende": 98,
            "sor_udfyldt_uden_match_ialt": 1,
        },
    ),
    (
        stg_fct_Udredningsforloeb,
        fct_forloeb_module,
        ("stg", "fct_Udredningsforloeb"),
        [create_organisation_sor_lookup_table_sql(), build_stg_fct_udredningsforloeb_sql()],
        dict(
            fetchone=[(5, 5), (5, 3, 2, 1)],
            fetchall=[
                [
                    ("dw_ek_Organisation_forloeb", 0, 0, 5, 5),
                    ("dw_ek_Organisation_kontakt", 1, 0, 4, 5),
                ]
            ],
        ),
        5,
        {"indikatorforloeb": 3, "dato_slut_blank": 2, "dato_opgoerelse_blank": 1},
    ),
    (
        stg_dim_UdredningsretEgenskaber,
        dim_egenskaber_module,
        ("stg", "dim_UdredningsretEgenskaber"),
        [build_stg_dim_egenskaber_sql()],
        dict(fetchone=[(5,)]),
        5,
        {},
    ),
]


@pytest.mark.parametrize(
    "asset_fn, module, identitet, forventet_sql, fakes, raekker, metadata",
    ASSET_KOERSLER,
    ids=[k[2][1] for k in ASSET_KOERSLER],
)
def test_asset_logger_dfm_identitet_koerer_sql_i_orden_og_rapporterer_taellinger(
    monkeypatch, ctx, asset_fn, module, identitet, forventet_sql, fakes, raekker, metadata
):
    dfm = _dfm(monkeypatch, module)
    mssql = _FakeMssql(**fakes)

    result = asset_fn(ctx, mssql, RunModeConfig(mode="delta"))

    args, kwargs = dfm.start_dfm_run.call_args
    assert args == identitet and kwargs["mode"] == "delta"
    # opslags-temp-tabellerne bygges FOER den SQL, der joiner dem; commit efter SELECT INTO
    assert mssql.executed[: len(forventet_sql)] == forventet_sql
    assert mssql.commits == 1
    dfm.complete_dfm_run.assert_called_once_with(117, rows_affected=raekker)
    dfm.fail_dfm_run.assert_not_called()
    assert _md(result, "row_count") == raekker
    for key, vaerdi in metadata.items():
        assert _md(result, key) == vaerdi, key


def test_pre_hf_asset_logger_dfm_identitet(monkeypatch, ctx):
    """Identiteten ("pre_stg","pre_Helbredsforloeb") er den, DFMRunLog og en evt. sensor slaar op paa."""
    dfm = _dfm(monkeypatch, pre_hf_module, get_checkpoint=lambda s, t: None)

    pre_stg_Helbredsforloeb(ctx, _FakeMssql(fetchone=[(1, 0)], fetchall=[[]]), RunModeConfig())

    args, kwargs = dfm.start_dfm_run.call_args
    assert args == ("pre_stg", "pre_Helbredsforloeb") and kwargs["mode"] == "delta"
    dfm.complete_dfm_run.assert_called_once_with(117, rows_affected=1)


@pytest.mark.parametrize(
    "asset_fn, module, table",
    [
        (pre_stg_Udredningsforloeb, pre_forloeb_module, PRE_FORLOEB_TABLE),
        (stg_fct_UdredningsretMarkoer, fct_markoer_module, FCT_MARKOER_TABLE),
        (stg_fct_Udredningsforloeb, fct_forloeb_module, FCT_FORLOEB_TABLE),
    ],
    ids=["pre_forloeb", "fct_markoer", "fct_forloeb"],
)
def test_grain_brud_fejler_med_taellinger_og_markerer_dfm_koerslen_fejlet(
    monkeypatch, ctx, asset_fn, module, table
):
    dfm = _dfm(monkeypatch, module)
    mssql = _FakeMssql(fetchone=[(5, 4)])

    with pytest.raises(ValueError, match=r"Grain violation") as exc:
        asset_fn(ctx, mssql, RunModeConfig())

    besked = str(exc.value)
    assert table in besked and "5 rows" in besked and "4 distinct" in besked
    assert "UDRED" not in besked  # ingen clean_code-vaerdi (og dermed ingen noegle) i fejlen
    dfm.fail_dfm_run.assert_called_once()
    assert "Grain violation" in dfm.fail_dfm_run.call_args.kwargs["error_message"]
    dfm.complete_dfm_run.assert_not_called()


def test_pre_hf_asset_fejl_markerer_dfm_koerslen_og_kaster_videre(monkeypatch, ctx):
    dfm = _dfm(monkeypatch, pre_hf_module, get_checkpoint=lambda s, t: None)

    class _Boom(_FakeMssql):
        def get_connection(self):
            raise RuntimeError("connection refused")

    with pytest.raises(RuntimeError, match="connection refused"):
        pre_stg_Helbredsforloeb(ctx, _Boom(), RunModeConfig())

    dfm.fail_dfm_run.assert_called_once_with(117, error_message="connection refused")
    dfm.complete_dfm_run.assert_not_called()
