# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for udredningsrets map/dim/fct-lag (US-4).

SQL'en i map-, dim- og fact-modulerne er rendret én gang ved import, saa den
kan asserteres som tekst uden database — praecis som test_drg_dwh.py goer for
DRG. Den statiske DDL holdes til de samme konstanter (kolonnelister OG typer),
saa mapping, staging, dimensions-SQL og DDL ikke kan drive fra hinanden:
kolonnetuplerne i dim-modulerne er udledt af udredningsret_kolonner.py, og
attributtypernes facit er de CAST'er, staging-byggerne rent faktisk skriver.

Spec: docs/source_systems/udredningsret/udredningsret_model.md (afsnit 2, 4, 6
og 7; beslutning U2-U13).
"""

import importlib
import re
from pathlib import Path

import pytest
from DSA.SQL.common.dato_dimension import DATO_BLANK_KEY
from DSA.SQL.dim.dim_Forloebsmarkoer import ATTRIBUTTER as FORLOEBSMARKOER_ATTRIBUTTER
from DSA.SQL.dim.dim_Forloebsmarkoer import INSERT_SQL as FORLOEBSMARKOER_INSERT_SQL
from DSA.SQL.dim.dim_Forloebsmarkoer import MERGE_SQL as FORLOEBSMARKOER_MERGE_SQL
from DSA.SQL.dim.dim_Forloebsmarkoer import RESOLVE_SQL as FORLOEBSMARKOER_RESOLVE_SQL
from DSA.SQL.dim.dim_Forloebsmarkoer import UNMAPPED_SQL as FORLOEBSMARKOER_UNMAPPED_SQL
from DSA.SQL.dim.dim_UdredningsretEgenskaber import ATTRIBUTTER as EGENSKABER_DIM_ATTRIBUTTER
from DSA.SQL.dim.dim_UdredningsretEgenskaber import INSERT_SQL as EGENSKABER_INSERT_SQL
from DSA.SQL.dim.dim_UdredningsretEgenskaber import MERGE_SQL as EGENSKABER_MERGE_SQL
from DSA.SQL.dim.dim_UdredningsretEgenskaber import RESOLVE_SQL as EGENSKABER_RESOLVE_SQL
from DSA.SQL.dim.dim_UdredningsretEgenskaber import UNMAPPED_SQL as EGENSKABER_UNMAPPED_SQL
from DSA.SQL.dim.dim_UdredningsretKlassifikation import (
    ATTRIBUTTER as KLASSIFIKATION_DIM_ATTRIBUTTER,
)
from DSA.SQL.dim.dim_UdredningsretKlassifikation import (
    BOPAELSREGION_TEKSTER,
    FORLOEBSLABEL_REFRESH_SQL,
    KATALOG_TEKST_ATTRIBUT,
    KOLONNER,
    TEKST_REGLER,
)
from DSA.SQL.dim.dim_UdredningsretKlassifikation import INSERT_SQL as KLASSIFIKATION_INSERT_SQL
from DSA.SQL.dim.dim_UdredningsretKlassifikation import MERGE_SQL as KLASSIFIKATION_MERGE_SQL
from DSA.SQL.dim.dim_UdredningsretKlassifikation import RESOLVE_SQL as KLASSIFIKATION_RESOLVE_SQL
from DSA.SQL.dim.dim_UdredningsretKlassifikation import UNMAPPED_SQL as KLASSIFIKATION_UNMAPPED_SQL
from DSA.SQL.fct.fct_Udredningsforloeb import (
    FACT_COLUMNS,
    FK_DIAGNOSTIK_SQL,
    FK_ROLES,
    GRAIN_CHECK_SQL,
    KONTROLTOTAL_SQL,
    RETIRE_SQL,
    STAGED_COLUMNS,
    TEMP_TABLE,
)
from DSA.SQL.fct.fct_Udredningsforloeb import INSERT_SQL as FCT_INSERT_SQL
from DSA.SQL.fct.fct_Udredningsforloeb import MERGE_SQL as FCT_MERGE_SQL
from DSA.SQL.fct.fct_Udredningsforloeb import RESOLVE_SQL as FCT_RESOLVE_SQL
from DSA.SQL.fct.fct_Udredningsforloeb import UNMAPPED_SQL as FCT_UNMAPPED_SQL
from DSA.SQL.map.map_Forloebsmarkoer import INSERT_SQL as MAP_FORLOEBSMARKOER_SQL
from DSA.SQL.map.map_Helbredsforloeb import INSERT_SQL as MAP_HF_SQL
from DSA.SQL.map.map_Udredningsforloeb import INSERT_SQL as MAP_FORLOEB_SQL
from DSA.SQL.map.map_UdredningsretKlassifikation import INSERT_SQL as MAP_KLASSIFIKATION_SQL
from DSA.SQL.staging.udredningsret_kolonner import (
    DIM_EGENSKABER_TABLE,
    DIM_FORLOEBSMARKOER_TABLE,
    DOWNSTREAM_CHECKPOINT_EJERE,
    EGENSKABER_ATTRIBUTTER,
    FCT_FORLOEB_DWH_TABLE,
    FCT_FORLOEB_TABLE,
    FORLOEB_CLEAN_CODE_TYPE,
    FORLOEBSMARKOER_CLEAN_CODE_TYPE,
    HF_CLEAN_CODE_TYPE,
    KLASSIFIKATION_ATTRIBUTTER,
    KLASSIFIKATION_CLEAN_CODE_TYPE,
    KLASSIFIKATION_TEKST_KOLONNER,
    MAP_FORLOEB_TABLE,
    MAP_FORLOEBSMARKOER_TABLE,
    MAP_HF_TABLE,
    MAP_KLASSIFIKATION_TABLE,
    PRE_FORLOEB_TABLE,
    PRE_HF_TABLE,
    REGION_TEKSTER,
    REGION_UKENDT_TEKST,
    UKENDT_TEKST,
    henvist_predicate_sql,
    tekst_case_sql,
)
from DSA.SQL.staging.udredningsret_staging_sql import (
    ARC_TABLE,
    build_pre_udredningsforloeb_sql,
    build_stg_dim_forloebsmarkoer_sql,
)

pytestmark = pytest.mark.unit

_ETL_DIR = Path(__file__).resolve().parents[2]
_DDL_DIR = _ETL_DIR / "infrastructure" / "ddl" / "tables" / "udredningsret"

AUDIT_KOLONNER = ["dw_timestamp_insert", "dw_timestamp_update"]


def _ddl_tekst(ddl_file: str) -> str:
    return (_DDL_DIR / ddl_file).read_text(encoding="utf-8")


def _ddl_kolonner(ddl_file: str, table: str) -> list[str]:
    """Kolonnenavne i én CREATE TABLE-blok, i DDL-orden."""
    match = re.search(
        rf"CREATE TABLE \[\w+\]\.\[{table}\]\((.*?)\n    \) ON \[PRIMARY\]",
        _ddl_tekst(ddl_file),
        re.S,
    )
    assert match, f"CREATE TABLE {table} ikke fundet i {ddl_file}"
    return re.findall(r"^\s+\[(\w+)\]", match.group(1), re.M)


def _stagede_typer(sql: str) -> dict[str, str]:
    """navn -> CAST-type for hver `CAST(... AS <TYPE>) AS [navn]` i en bygger.

    Staging-SQL'ens egne CAST'er er facit for dimensionernes kolonnetyper: en
    smallere DDL-kolonne ville afkorte vaerdien i MERGE'n uden en fejl.
    """
    return {navn: typ for typ, navn in re.findall(r"AS (N?VARCHAR\(\d+\))\) AS \[(\w+)\]", sql)}


PRE_FORLOEB_TYPER = _stagede_typer(build_pre_udredningsforloeb_sql())
DIM_FORLOEBSMARKOER_TYPER = _stagede_typer(build_stg_dim_forloebsmarkoer_sql())

# (entitet, INSERT_SQL, map-tabel, kildetabel, clean_code-type) — D2
MAPS = (
    ("Helbredsforloeb", MAP_HF_SQL, MAP_HF_TABLE, PRE_HF_TABLE, HF_CLEAN_CODE_TYPE),
    (
        "Udredningsforloeb",
        MAP_FORLOEB_SQL,
        MAP_FORLOEB_TABLE,
        DIM_EGENSKABER_TABLE,
        FORLOEB_CLEAN_CODE_TYPE,
    ),
    (
        "UdredningsretKlassifikation",
        MAP_KLASSIFIKATION_SQL,
        MAP_KLASSIFIKATION_TABLE,
        DIM_EGENSKABER_TABLE,
        KLASSIFIKATION_CLEAN_CODE_TYPE,
    ),
    (
        "Forloebsmarkoer",
        MAP_FORLOEBSMARKOER_SQL,
        MAP_FORLOEBSMARKOER_TABLE,
        DIM_FORLOEBSMARKOER_TABLE,
        FORLOEBSMARKOER_CLEAN_CODE_TYPE,
    ),
)
MAP_IDS = [m[0] for m in MAPS]

# (dimension, PK, clean_code, attributkolonner, UNMAPPED, RESOLVE, MERGE, INSERT, map, kilde)
DIMS = (
    (
        "UdredningsretEgenskaber",
        "dw_ek_Udredningsforloeb",
        "clean_code_Udredningsforloeb",
        EGENSKABER_DIM_ATTRIBUTTER,
        EGENSKABER_UNMAPPED_SQL,
        EGENSKABER_RESOLVE_SQL,
        EGENSKABER_MERGE_SQL,
        EGENSKABER_INSERT_SQL,
        MAP_FORLOEB_TABLE,
        DIM_EGENSKABER_TABLE,
    ),
    (
        "UdredningsretKlassifikation",
        "dw_ek_UdredningsretKlassifikation",
        "clean_code_UdredningsretKlassifikation",
        KOLONNER,
        KLASSIFIKATION_UNMAPPED_SQL,
        KLASSIFIKATION_RESOLVE_SQL,
        KLASSIFIKATION_MERGE_SQL,
        KLASSIFIKATION_INSERT_SQL,
        MAP_KLASSIFIKATION_TABLE,
        DIM_EGENSKABER_TABLE,
    ),
    (
        "Forloebsmarkoer",
        "dw_ek_Forloebsmarkoer",
        "clean_code_Forloebsmarkoer",
        FORLOEBSMARKOER_ATTRIBUTTER,
        FORLOEBSMARKOER_UNMAPPED_SQL,
        FORLOEBSMARKOER_RESOLVE_SQL,
        FORLOEBSMARKOER_MERGE_SQL,
        FORLOEBSMARKOER_INSERT_SQL,
        MAP_FORLOEBSMARKOER_TABLE,
        DIM_FORLOEBSMARKOER_TABLE,
    ),
)
DIM_IDS = [d[0] for d in DIMS]


# ---------------------------------------------------------------------------
# (1) DDL holdes til konstanterne — kolonner OG typer
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entitet, _sql, _tabel, _kilde, clean_code_type", MAPS, ids=MAP_IDS)
def test_map_ddl_er_dw_ek_clean_code_og_audit(entitet, _sql, _tabel, _kilde, clean_code_type):
    """map.* baerer KUN dw_ek, clean_code og audit (MR-protokol 2.3)."""
    assert _ddl_kolonner("02_maps.sql", entitet) == [
        f"dw_ek_{entitet}",
        f"clean_code_{entitet}",
        *AUDIT_KOLONNER,
    ]
    tekst = _ddl_tekst("02_maps.sql")
    assert f"[dw_ek_{entitet}] INT IDENTITY(1,1) NOT NULL" in tekst
    # clean_code-typen er stagingens egen: en smallere map-kolonne ville
    # afkorte noeglen og kollapse to forskellige forloeb til ét dw_ek
    assert f"[clean_code_{entitet}] {clean_code_type} NOT NULL" in tekst
    assert f"CONSTRAINT [PK_Map_{entitet}] PRIMARY KEY CLUSTERED ([clean_code_{entitet}] ASC)" in (
        tekst
    )
    assert f"[IX_Map_{entitet}_DW_EK]" in tekst


def test_dim_egenskaber_ddl_matcher_attributtuplen():
    assert EGENSKABER_DIM_ATTRIBUTTER == EGENSKABER_ATTRIBUTTER
    assert _ddl_kolonner("03_dims.sql", "UdredningsretEgenskaber") == [
        "dw_ek_Udredningsforloeb",
        "clean_code_Udredningsforloeb",
        *EGENSKABER_ATTRIBUTTER,
        *AUDIT_KOLONNER,
    ]


def test_dim_egenskaber_ddl_typer_matcher_stagingens_casts():
    tekst = _ddl_tekst("03_dims.sql")
    assert f"[clean_code_Udredningsforloeb] {FORLOEB_CLEAN_CODE_TYPE} NULL" in tekst
    for navn in EGENSKABER_ATTRIBUTTER:
        staged = PRE_FORLOEB_TYPER.get(navn)
        assert staged, f"{navn} castes ikke i build_pre_udredningsforloeb_sql"
        assert f"[{navn}] {staged} NULL" in tekst, f"{navn}: DDL-typen matcher ikke {staged}"


def test_dim_klassifikation_kolonnetuple_er_attribut_plus_tekst():
    """Hver attribut faar sin tekstkolonne lige efter sig — undtagen speciale,
    der er fritekst fra leverancen og forklarer sig selv (D3)."""
    assert KLASSIFIKATION_DIM_ATTRIBUTTER == KLASSIFIKATION_ATTRIBUTTER
    forventet: list[str] = []
    for attribut in KLASSIFIKATION_ATTRIBUTTER:
        forventet.append(attribut)
        if attribut in KLASSIFIKATION_TEKST_KOLONNER:
            forventet.append(f"{attribut}_tekst")
    assert list(KOLONNER) == forventet
    assert "speciale_tekst" not in KOLONNER
    assert KOLONNER[-1] == "speciale"
    # hver tekstkolonne har praecis én kilde: en dict-regel eller kataloget
    assert set(TEKST_REGLER) | {KATALOG_TEKST_ATTRIBUT} == set(KLASSIFIKATION_TEKST_KOLONNER)
    assert KATALOG_TEKST_ATTRIBUT not in TEKST_REGLER


def test_dim_klassifikation_ddl_matcher_kolonnetuplen():
    assert _ddl_kolonner("03_dims.sql", "UdredningsretKlassifikation") == [
        "dw_ek_UdredningsretKlassifikation",
        "clean_code_UdredningsretKlassifikation",
        *KOLONNER,
        *AUDIT_KOLONNER,
    ]


def test_dim_klassifikation_ddl_typer_matcher_staging_og_tekstregler():
    tekst = _ddl_tekst("03_dims.sql")
    assert f"[clean_code_UdredningsretKlassifikation] {KLASSIFIKATION_CLEAN_CODE_TYPE} NULL" in (
        tekst
    )
    for attribut in KLASSIFIKATION_ATTRIBUTTER:
        staged = PRE_FORLOEB_TYPER[attribut]
        assert f"[{attribut}] {staged} NULL" in tekst, attribut
    # tekstkolonnernes DDL-type ER resolve-SQL'ens CAST-maal: er de uenige,
    # afkortes teksten stiltiende i MERGE'n
    for attribut, (_tekster, _else, sql_type) in TEKST_REGLER.items():
        assert f"[{attribut}_tekst] {sql_type} NULL" in tekst, attribut
    assert "[forloebslabel_tekst] NVARCHAR(500) NULL" in tekst


def test_dim_forloebsmarkoer_ddl_matcher_attributtuplen_og_stagingens_typer():
    assert _ddl_kolonner("03_dims.sql", "Forloebsmarkoer") == [
        "dw_ek_Forloebsmarkoer",
        "clean_code_Forloebsmarkoer",
        *FORLOEBSMARKOER_ATTRIBUTTER,
        *AUDIT_KOLONNER,
    ]
    tekst = _ddl_tekst("03_dims.sql")
    assert f"[clean_code_Forloebsmarkoer] {FORLOEBSMARKOER_CLEAN_CODE_TYPE} NULL" in tekst
    for navn in FORLOEBSMARKOER_ATTRIBUTTER:
        staged = DIM_FORLOEBSMARKOER_TYPER.get(navn)
        assert staged, f"{navn} castes ikke i build_stg_dim_forloebsmarkoer_sql"
        assert f"[{navn}] {staged} NULL" in tekst, f"{navn}: DDL-typen matcher ikke {staged}"


def test_fct_ddl_matcher_fact_kolonnerne():
    assert _ddl_kolonner("04_facts.sql", "Udredningsforloeb") == [
        "dw_ek_Udredningsforloeb",
        *FACT_COLUMNS,
        *AUDIT_KOLONNER,
    ]


def test_fct_kolonner_er_stagede_plus_de_tre_map_noegler():
    """Facten baerer praecis stagingens maal plus de tre noegler, den selv
    oploeser — en ny stg-kolonne skal foeres ind bevidst, ikke glemmes."""
    assert set(FACT_COLUMNS) == set(STAGED_COLUMNS) | {
        "clean_code_Udredningsforloeb",
        "dw_ek_Helbredsforloeb",
        "dw_ek_UdredningsretKlassifikation",
    }
    # delt PK med transaktionsdimensionen: facten har INGEN egenskabsnoegle
    assert "dw_ek_UdredningsretEgenskaber" not in FACT_COLUMNS
    assert "dw_ek_UdredningsretEgenskaber" not in _ddl_kolonner("04_facts.sql", "Udredningsforloeb")
    # staging-bogholderi hoerer ikke hjemme i facten
    for forbudt in ("borger_opslag", "forloeb_enhed_sor", "kontakt_enhed_sor"):
        assert forbudt not in FACT_COLUMNS


def test_fct_ddl_har_sentinel_defaults_og_noeglens_indekser():
    tekst = _ddl_tekst("04_facts.sql")
    for rolle in FK_ROLES:
        assert (
            f"[{rolle}] INT NOT NULL CONSTRAINT [DF_Udredningsforloeb_{rolle}] DEFAULT -1" in tekst
        ), rolle
    assert (
        "[indgaar_i_indikator] BIT NOT NULL "
        "CONSTRAINT [DF_Udredningsforloeb_indgaar_i_indikator] DEFAULT 0" in tekst
    )
    assert (
        "[dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_Udredningsforloeb_dw_is_deleted] DEFAULT 0"
        in tekst
    )
    assert (
        "CONSTRAINT [PK_Fct_Udredningsforloeb] PRIMARY KEY CLUSTERED "
        "([dw_ek_Udredningsforloeb] ASC)" in tekst
    )
    # clean_code'en er unik pr. levende raekke — filtreret, saa NULL ikke kolliderer
    assert (
        "CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_Udredningsforloeb_clean_code]" in tekst
        and "WHERE [clean_code_Udredningsforloeb] IS NOT NULL" in tekst
    )
    for indeks in ("Borger", "Helbredsforloeb", "Klassifikation", "Dato_Start", "Watermark"):
        assert f"[IX_Fct_Udredningsforloeb_{indeks}]" in tekst, indeks


def test_seeds_giver_map_og_dim_en_sentinel_men_facten_ingen():
    tekst = _ddl_tekst("05_seeds.sql")
    for entitet, _sql, _tabel, _kilde, _type in MAPS:
        assert f"SET IDENTITY_INSERT [map].[{entitet}] ON" in tekst, entitet
        assert f"WHERE [dw_ek_{entitet}] = -1" in tekst, entitet
    for dim, pk, *_ in DIMS:
        assert f"INSERT INTO [dim].[{dim}] (" in tekst, dim
        assert f"FROM [dim].[{dim}] WHERE [{pk}] = -1" in tekst, dim
    # sentinel-teksterne, saa en rapport aldrig viser en tom celle for -1
    assert "[overholdt_tekst], [aarsag_tekst]" in tekst
    assert f"-1, N'UNKNOWN', N'{UKENDT_TEKST}', N'{UKENDT_TEKST}'" in tekst
    assert "[clean_code_Forloebsmarkoer], [tekst]" in tekst
    assert f"-1, 'UNKNOWN', N'{UKENDT_TEKST}'," in tekst
    # facten har ingen sentinel-raekke
    assert "[fct].[Udredningsforloeb]" not in tekst


# ---------------------------------------------------------------------------
# (2) map er append-only
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entitet, sql, tabel, kilde, _type", MAPS, ids=MAP_IDS)
def test_maps_er_append_only_fra_den_besluttede_kilde(entitet, sql, tabel, kilde, _type):
    assert f"INSERT INTO {tabel} (" in sql
    assert f"FROM {kilde} src" in sql, f"{entitet} laeser ikke {kilde}"
    assert "SELECT DISTINCT" in sql
    assert "NOT EXISTS" in sql
    # ordgraenser: en kolonne som dw_is_deleted maa ikke udloese tjekket
    assert not re.search(r"\bMERGE\b|\bUPDATE\b|\bDELETE\b", sql, re.I)
    # dw_ek tildeles af IDENTITY — map'en skriver den aldrig selv
    assert f"dw_ek_{entitet}" not in sql
    # tomme og manglende clean_codes minter ingen noegle
    assert f"src.[clean_code_{entitet}] IS NOT NULL" in sql
    assert f"src.[clean_code_{entitet}] <> ''" in sql


def test_map_helbredsforloeb_minter_ogsaa_helt_soft_slettede_forloeb():
    """D2: ogsaa et helbredsforloeb uden levende markoerer skal have sin noegle
    — facten bruger den til at retirere de forloeb, der forsvandt med dem
    (spec afsnit 6). Et dw_is_deleted-filter her ville braekke retireringen."""
    assert "dw_is_deleted" not in MAP_HF_SQL


# ---------------------------------------------------------------------------
# (3) Dimensioner: UNMAPPED-vaern, EXCEPT-change detection, aldrig retirering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dim, pk, clean_code, _kolonner, unmapped, _resolve, _merge, _insert, map_tabel, kilde",
    DIMS,
    ids=DIM_IDS,
)
def test_dim_unmapped_vaern_taeller_stagede_raekker_uden_map(
    dim, pk, clean_code, _kolonner, unmapped, _resolve, _merge, _insert, map_tabel, kilde
):
    """RESOLVE INNER JOIN'er map'en, saa en umappet raekke ville forsvinde
    stiltiende — vaernet taeller dem foerst og fejler haardt."""
    assert f"FROM {kilde} ds" in unmapped
    assert f"LEFT JOIN {map_tabel} m" in unmapped
    assert f"WHERE m.[{pk}] IS NULL" in unmapped
    assert f"ON m.[{clean_code}] = ds.[{clean_code}]" in unmapped


@pytest.mark.parametrize(
    "dim, pk, clean_code, kolonner, _unmapped, resolve, merge, insert, map_tabel, _kilde",
    DIMS,
    ids=DIM_IDS,
)
def test_dim_merge_bruger_except_change_detection_og_retirerer_aldrig(
    dim, pk, clean_code, kolonner, _unmapped, resolve, merge, insert, map_tabel, _kilde
):
    assert "WHEN MATCHED AND EXISTS (" in merge
    assert "EXCEPT" in merge
    assert "WHEN NOT MATCHED BY SOURCE" not in merge  # dimensioner retirerer aldrig
    matched = merge.split("WHEN NOT MATCHED")[0]
    # EXCEPT-sammenligningen og UPDATE SET skal asserteres HVER for sig: "tgt.[x] =
    # src.[x]" indeholder baade "src.[x]" og "tgt.[x]", saa en samlet soegning i
    # hele WHEN MATCHED-blokken ville vaere opfyldt af UPDATE SET alene og lade en
    # haandtunet EXCEPT-liste slippe igennem.
    sammenligning, adskiller, opdatering = matched.partition("THEN\n    UPDATE SET")
    assert adskiller, f"{dim}: WHEN MATCHED har ingen UPDATE SET"
    for kolonne in (clean_code, *kolonner):
        assert f"src.[{kolonne}]" in sammenligning, f"{dim}.{kolonne} mangler i change detection"
        assert f"tgt.[{kolonne}]" in sammenligning, f"{dim}.{kolonne} mangler i change detection"
        assert f"tgt.[{kolonne}] = src.[{kolonne}]" in opdatering, f"{dim}.{kolonne} opdateres ikke"
    # bulk-stien skal indsaette praecis de samme kolonner som MERGE'ns INSERT
    assert insert.count(f"[{clean_code}]") >= 1
    for kolonne in kolonner:
        assert f"[{kolonne}]" in insert, f"{dim}.{kolonne} mangler i bulk-INSERT"
    assert "WITH (TABLOCK)" in insert
    assert f"INNER JOIN {map_tabel} m" in resolve


@pytest.mark.parametrize(
    "dim, pk, _clean_code, _kolonner, _unmapped, resolve, _merge, _insert, _map_tabel, _kilde",
    DIMS,
    ids=DIM_IDS,
)
def test_dim_resolve_giver_temp_tabellen_clustered_noegle_foer_merge(
    dim, pk, _clean_code, _kolonner, _unmapped, resolve, _merge, _insert, _map_tabel, _kilde
):
    """MR-protokol 2.3: #Temp skal have en clustered noegle FOER MERGE.

    Uden den skal resolve-saettet sorteres eller hashes ved hver koersel, foer
    MERGE'n kan matche mod maalets clustered PK — og egenskabsdimensionen baerer
    samme kardinalitet som facten. Noeglen skal staa EFTER SELECT ... INTO
    (tabellen findes ikke foer) og maa ikke vaere unik, saa en dublet naar frem
    til MERGE'n i stedet for at doe som en fejlet indeksbygning — praecis som
    factens egen temp-tabel.
    """
    assert f"CREATE CLUSTERED INDEX [IX_{dim}]\n    ON #{dim}([{pk}]);" in resolve, dim
    assert "CREATE UNIQUE CLUSTERED INDEX" not in resolve, dim
    assert resolve.index(f"INTO #{dim}") < resolve.index("CREATE CLUSTERED INDEX"), dim


# ---------------------------------------------------------------------------
# (4) dim.UdredningsretKlassifikation: kilde, tekster, katalog-refresh
# ---------------------------------------------------------------------------


def test_klassifikation_grupperer_paa_staged_noegle_og_laeser_attributter_fra_pre():
    """U12: kun forloebslabel og speciale findes paa stg.dim_UdredningsretEgenskaber,
    saa de seks oevrige attributter hentes fra stg.pre_Udredningsforloeb joinet paa
    den forloebs-clean_code, de to tabeller deler (samme grain, én raekke hver)."""
    assert f"FROM {DIM_EGENSKABER_TABLE} ds" in KLASSIFIKATION_RESOLVE_SQL
    assert f"INNER JOIN {PRE_FORLOEB_TABLE} p" in KLASSIFIKATION_RESOLVE_SQL
    assert (
        "ON p.[clean_code_Udredningsforloeb] = ds.[clean_code_Udredningsforloeb]"
        in KLASSIFIKATION_RESOLVE_SQL
    )
    assert (
        "GROUP BY ds.[clean_code_UdredningsretKlassifikation]" in KLASSIFIKATION_RESOLVE_SQL
    ), "medlemmet skal grupperes paa den STAGEDE klassifikationsnoegle"
    for attribut in KLASSIFIKATION_ATTRIBUTTER:
        assert f"MAX(p.[{attribut}]) AS [{attribut}]" in KLASSIFIKATION_RESOLVE_SQL, attribut


def test_klassifikation_tekstcase_daekker_hver_noegle_og_sin_else_gren():
    for attribut, (tekster, else_sql, sql_type) in TEKST_REGLER.items():
        case_sql = tekst_case_sql(f"grp.[{attribut}]", tekster, else_sql)
        assert (
            f"CAST({case_sql} AS {sql_type}) AS [{attribut}_tekst]" in KLASSIFIKATION_RESOLVE_SQL
        ), attribut
        # hver noegle i dict'en har sin gren, og opslaget kollapser NULL og blank
        assert f"CASE ISNULL(LTRIM(RTRIM(grp.[{attribut}])), '')" in KLASSIFIKATION_RESOLVE_SQL
        for noegle, tekst in tekster.items():
            assert f"WHEN '{noegle}' THEN N'{tekst}'" in KLASSIFIKATION_RESOLVE_SQL, (
                attribut,
                noegle,
            )
        assert f"ELSE {else_sql} END" in KLASSIFIKATION_RESOLVE_SQL, attribut


def test_bopaelsregion_gentager_en_ukendt_regionskode_men_kalder_blank_ukendt():
    """Blank er 'Ukendt'; enhver anden ukendt kode gentages i stedet for at
    blive skjult bag 'Ukendt' (forbrugerreglen i spec 4.2)."""
    assert BOPAELSREGION_TEKSTER == {**REGION_TEKSTER, "": REGION_UKENDT_TEKST}
    tekster, else_sql, _type = TEKST_REGLER["bopaelsregion_forloeb"]
    assert tekster[""] == REGION_UKENDT_TEKST
    assert else_sql == "grp.[bopaelsregion_forloeb]"
    # de oevrige regler falder derimod tilbage til 'Ukendt'
    for attribut, (_t, andet_else, _s) in TEKST_REGLER.items():
        if attribut != "bopaelsregion_forloeb":
            assert andet_else == f"N'{UKENDT_TEKST}'", attribut


def test_forloebslabel_tekst_er_et_katalogopslag_med_left_join():
    """Uden katalograekke bliver teksten NULL — medlemmet forsvinder ikke."""
    assert "LEFT JOIN [codesystem].[SKSConcept] s" in KLASSIFIKATION_RESOLVE_SQL
    assert (
        "ON s.[code] = UPPER(LTRIM(RTRIM(grp.[forloebslabel])))" in KLASSIFIKATION_RESOLVE_SQL
    ), "koden skal normaliseres foer opslaget, ikke overlades til kollationen"
    assert "s.[display] AS [forloebslabel_tekst]" in KLASSIFIKATION_RESOLVE_SQL


def test_forloebslabel_refreshes_globalt_og_idempotent():
    """forloebslabel_tekst er katalog-sourced, saa en katalog-kun-revision skal
    naa ALLE medlemmer — ikke kun dem i den aktuelle delta-batch."""
    assert "UPDATE k" in FORLOEBSLABEL_REFRESH_SQL
    assert "[dim].[UdredningsretKlassifikation] k" in FORLOEBSLABEL_REFRESH_SQL
    assert "[codesystem].[SKSConcept] s" in FORLOEBSLABEL_REFRESH_SQL
    # ingen binding til #temp/delta-batchen — det er en global opdatering
    assert "#UdredningsretKlassifikation" not in FORLOEBSLABEL_REFRESH_SQL
    # kun aendrede tekster roeres (idempotent), og aldrig sentinel-medlemmet
    assert "ISNULL(k.[forloebslabel_tekst]" in FORLOEBSLABEL_REFRESH_SQL
    assert "k.[dw_ek_UdredningsretKlassifikation] <> -1" in FORLOEBSLABEL_REFRESH_SQL


# ---------------------------------------------------------------------------
# (5) fct.Udredningsforloeb: resolve, grain, retirering, kontroltotal
# ---------------------------------------------------------------------------


def test_fct_unmapped_taeller_alle_tre_map_opslag():
    for map_tabel, pk in (
        (MAP_FORLOEB_TABLE, "dw_ek_Udredningsforloeb"),
        (MAP_HF_TABLE, "dw_ek_Helbredsforloeb"),
        (MAP_KLASSIFIKATION_TABLE, "dw_ek_UdredningsretKlassifikation"),
    ):
        assert f"LEFT JOIN {map_tabel} m" in FCT_UNMAPPED_SQL, map_tabel
        assert f"WHERE m.[{pk}] IS NULL" in FCT_UNMAPPED_SQL, pk
    assert FCT_UNMAPPED_SQL.count(f"FROM {FCT_FORLOEB_TABLE} s") == 3


def test_fct_resolve_inner_joiner_alle_tre_maps_og_baerer_stagingen_uaendret():
    for map_tabel, clean_code in (
        (MAP_FORLOEB_TABLE, "clean_code_Udredningsforloeb"),
        (MAP_HF_TABLE, "clean_code_Helbredsforloeb"),
        (MAP_KLASSIFIKATION_TABLE, "clean_code_UdredningsretKlassifikation"),
    ):
        assert f"INNER JOIN {map_tabel}" in FCT_RESOLVE_SQL, map_tabel
        assert f"src.[{clean_code}]" in FCT_RESOLVE_SQL, clean_code
    # alle tre er vaernede af UNMAPPED_SQL, saa der er ingen LEFT JOIN/sentinel-sti
    assert "LEFT JOIN" not in FCT_RESOLVE_SQL
    assert "COALESCE" not in FCT_RESOLVE_SQL
    for kolonne in STAGED_COLUMNS:
        assert f"src.[{kolonne}]" in FCT_RESOLVE_SQL, kolonne
    # stagingens eget bogholderi og de raa SOR-koder foelger ikke med
    for forbudt in ("borger_opslag", "forloeb_enhed_sor", "kontakt_enhed_sor", "dw_timestamp"):
        assert forbudt not in FCT_RESOLVE_SQL, forbudt
    assert f"INTO {TEMP_TABLE}" in FCT_RESOLVE_SQL


def test_fct_grain_check_maaler_paa_forloebets_clean_code():
    assert (
        GRAIN_CHECK_SQL
        == f"SELECT COUNT(*), COUNT(DISTINCT [clean_code_Udredningsforloeb]) FROM {TEMP_TABLE}"
    )
    # det clustrede indeks paa temp-tabellen er bevidst IKKE unikt, saa dubletten
    # naar frem til taellingen i stedet for at doe som en indeksbygning
    assert "CREATE CLUSTERED INDEX" in FCT_RESOLVE_SQL
    assert "CREATE UNIQUE CLUSTERED INDEX" not in FCT_RESOLVE_SQL


def test_fct_merge_except_daekker_alle_fact_kolonner_inkl_genoplivning():
    matched = FCT_MERGE_SQL.split("WHEN NOT MATCHED")[0]
    # EXCEPT-blokken og UPDATE SET asserteres hver for sig — ellers ville "tgt.[x] =
    # src.[x]" i UPDATE SET alene opfylde alle tre assertions, og en EXCEPT-liste
    # uden fx dw_is_deleted ville aldrig blive fanget her.
    sammenligning, adskiller, opdatering = matched.partition("THEN\n    UPDATE SET")
    assert adskiller, "WHEN MATCHED har ingen UPDATE SET"
    for kolonne in FACT_COLUMNS:
        assert f"src.[{kolonne}]" in sammenligning, f"{kolonne} mangler i EXCEPT-kilden"
        assert f"tgt.[{kolonne}]" in sammenligning, f"{kolonne} mangler i EXCEPT-maalet"
        assert f"tgt.[{kolonne}] = src.[{kolonne}]" in opdatering, f"{kolonne} opdateres ikke"
    # WHEN MATCHED skriver dw_is_deleted fra kilden: en clean_code, der kommer
    # igen, genoplives i stedet for at blive staaende retireret — baade som
    # aendringsdetektion og som faktisk opdatering
    assert "src.[dw_is_deleted]" in sammenligning and "tgt.[dw_is_deleted]" in sammenligning
    assert "tgt.[dw_is_deleted] = src.[dw_is_deleted]" in opdatering
    assert "ON tgt.[dw_ek_Udredningsforloeb] = src.[dw_ek_Udredningsforloeb]" in FCT_MERGE_SQL
    for kolonne in FACT_COLUMNS:
        assert f"[{kolonne}]" in FCT_INSERT_SQL, kolonne
    assert "WITH (TABLOCK)" in FCT_INSERT_SQL


def test_fct_retire_scoper_paa_de_stagede_helbredsforloeb_og_udelader_batchen():
    """Spec afsnit 6: retireringen rammer KUN de helbredsforloeb, dette run har
    staget, og kun de forloeb, der ikke er i batchen. Uden scope ville et delta
    retirere hele facten; uden NOT EXISTS ville det retirere den igen og igen."""
    assert f"FROM {FCT_FORLOEB_DWH_TABLE} AS f" in RETIRE_SQL
    assert f"INNER JOIN {MAP_HF_TABLE} AS m" in RETIRE_SQL
    assert "ON m.[dw_ek_Helbredsforloeb] = f.[dw_ek_Helbredsforloeb]" in RETIRE_SQL
    assert f"INNER JOIN {PRE_HF_TABLE} AS h" in RETIRE_SQL
    assert "ON h.[clean_code_Helbredsforloeb] = m.[clean_code_Helbredsforloeb]" in RETIRE_SQL
    assert "SET f.[dw_is_deleted] = 1" in RETIRE_SQL
    assert "WHERE f.[dw_is_deleted] = 0" in RETIRE_SQL
    assert f"SELECT 1 FROM {TEMP_TABLE} AS s" in RETIRE_SQL
    assert "s.[dw_ek_Udredningsforloeb] = f.[dw_ek_Udredningsforloeb]" in RETIRE_SQL
    # ingen haarde sletninger nogen steder
    assert not re.search(r"\bDELETE\b", RETIRE_SQL, re.I)


def test_fct_kontroltotal_grupperer_arkivet_paa_helbredsforloeb_og_f_id():
    """U13: grainet er (helbredsforloeb_noegle, F_ID)-par — ikke COUNT(DISTINCT
    F_ID), som ville fejle et korrekt forloeb, hvis SDS genbrugte et F_ID paa
    tvaers af to helbredsforloeb."""
    assert f"FROM {ARC_TABLE} AS a" in KONTROLTOTAL_SQL
    assert "GROUP BY m.[helbredsforloeb_noegle], m.[f_id]" in KONTROLTOTAL_SQL
    # levende skive paa begge sider, og kun arkivraekker med et F_ID
    assert "a.[dw_is_deleted] = 0" in KONTROLTOTAL_SQL
    assert "a.[F_ID] IS NOT NULL AND LTRIM(RTRIM(a.[F_ID])) <> ''" in KONTROLTOTAL_SQL
    assert f"FROM {FCT_FORLOEB_DWH_TABLE}\n    WHERE [dw_is_deleted] = 0" in KONTROLTOTAL_SQL
    assert "ISNULL(SUM(CAST([indgaar_i_indikator] AS INT)), 0)" in KONTROLTOTAL_SQL
    for kolonne in ("[fct_antal]", "[arc_antal]", "[fct_indikator]", "[arc_indikator]"):
        assert kolonne in KONTROLTOTAL_SQL, kolonne


def test_fct_kontroltotal_bruger_pre_stgs_egen_indikatorregel():
    """Kontroltotalen skal genskabe pre_stg'ens indikatorafgraensning ord for
    ord — ellers sammenligner den to forskellige definitioner og fejler paa
    korrekt data. Begge sider bruger den delte henvist-praedikat-helper og
    den samme ELSE 0-form, saa en NULL markoerkode taeller som ikke-henvist."""
    pre_sql = build_pre_udredningsforloeb_sql()
    mrk_h = "CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END"
    assert mrk_h in KONTROLTOTAL_SQL and mrk_h in pre_sql
    henvist = henvist_predicate_sql("LTRIM(RTRIM(a.[FORLOEBSMARKOER]))")
    assert henvist in KONTROLTOTAL_SQL
    # pre_stg render det samme praedikat som foerste gren i markoer_gruppe_sql
    assert henvist[1:-1] in pre_sql
    assert f"CASE WHEN {henvist} THEN 1 ELSE 0 END" in KONTROLTOTAL_SQL
    assert (
        "MAX(CASE WHEN m.[mrk_h_1] = 1 AND m.[er_henvist] = 1 THEN 1 ELSE 0 END)"
        in KONTROLTOTAL_SQL
    )
    assert (
        "MAX(CASE WHEN m.[mrk_h_1] = 1 AND m.[er_henvist] = 0 THEN 1 ELSE 0 END)"
        in KONTROLTOTAL_SQL
    )


def test_fct_kontroltotal_metadata_er_den_maalte_forskel_og_fejlen_peger_paa_afhjaelpningen():
    """Review 2026-09-13: `kontroltotal_afvigelser` var hardcodet til 0 og bar
    derfor intet signal — tallet skal komme fra de to tal, checket faktisk
    sammenlignede, saa metadataen ikke kan komme ud af trit med kontrollen,
    hvis dens grain eller haardhed nogensinde aendres.

    Fejlbeskeden skal samtidig naevne den hyppigste aarsag (SDS har omgrupperet
    markoerer mellem F_ID uden anden indholdsaendring, U1/U15) og afhjaelpningen
    (`mode=full_load`): kontroltotalen fejler FOER `set_checkpoint`, saa hver
    eneste natkoersel fejler paa det samme, indtil nogen griber ind — og den,
    der bliver kaldt ud, skal ikke laese koden for at komme videre."""
    kilde = Path(importlib.import_module("DSA.SQL.fct.fct_Udredningsforloeb").__file__).read_text(
        encoding="utf-8"
    )
    assert '"kontroltotal_afvigelser": arc_antal - fct_antal,' in kilde
    assert '"kontroltotal_afvigelser": 0' not in kilde
    # implicit sammensatte strengliteraler samles, saa testen overlever en ombrydning
    samlet = re.sub(r'"\s*\n\s*f?"', "", kilde)
    assert (
        "Hyppigste aarsag: SDS har omgrupperet markoerer mellem F_ID uden anden "
        "indholdsaendring (U1). Afhjaelpning: koer udredningsret_publish_job med "
        "mode=full_load." in samlet
    )
    # afvigelsen er fortsat en HAARD fejl, ikke en advarsel
    assert 'error_message="kontroltotal afviger"' in kilde
    assert "raise dg.Failure(" in kilde


def test_fct_diagnostik_daekker_alle_fk_roller_sentineler_og_blank():
    for rolle in FK_ROLES:
        assert f"SUM(CASE WHEN [{rolle}] = -1 THEN 1 ELSE 0 END) AS [{rolle}]" in FK_DIAGNOSTIK_SQL
    # strukturelt tomt CPR taelles for sig, ikke som et opslagsmiss
    assert "[dw_ek_Borger] = -2" in FK_DIAGNOSTIK_SQL
    # de to datoer, der lovligt lander paa BLANK (U2), taelles paa deres egen noegle
    assert f"[dw_ek_Dato_slut] = {DATO_BLANK_KEY}" in FK_DIAGNOSTIK_SQL
    assert f"[dw_ek_Dato_opgoerelse] = {DATO_BLANK_KEY}" in FK_DIAGNOSTIK_SQL
    assert "WHERE [dw_is_deleted] = 0" in FK_DIAGNOSTIK_SQL


# ---------------------------------------------------------------------------
# (6) GDPR og lagdeling: dwh-laget laeser aldrig ext og naevner aldrig CPR
# ---------------------------------------------------------------------------

ALLE_DWH_SQL = {
    "map_Helbredsforloeb.INSERT_SQL": MAP_HF_SQL,
    "map_Udredningsforloeb.INSERT_SQL": MAP_FORLOEB_SQL,
    "map_UdredningsretKlassifikation.INSERT_SQL": MAP_KLASSIFIKATION_SQL,
    "map_Forloebsmarkoer.INSERT_SQL": MAP_FORLOEBSMARKOER_SQL,
    "dim_UdredningsretEgenskaber.RESOLVE_SQL": EGENSKABER_RESOLVE_SQL,
    "dim_UdredningsretEgenskaber.MERGE_SQL": EGENSKABER_MERGE_SQL,
    "dim_UdredningsretEgenskaber.INSERT_SQL": EGENSKABER_INSERT_SQL,
    "dim_UdredningsretKlassifikation.RESOLVE_SQL": KLASSIFIKATION_RESOLVE_SQL,
    "dim_UdredningsretKlassifikation.MERGE_SQL": KLASSIFIKATION_MERGE_SQL,
    "dim_UdredningsretKlassifikation.INSERT_SQL": KLASSIFIKATION_INSERT_SQL,
    "dim_UdredningsretKlassifikation.FORLOEBSLABEL_REFRESH_SQL": FORLOEBSLABEL_REFRESH_SQL,
    "dim_Forloebsmarkoer.RESOLVE_SQL": FORLOEBSMARKOER_RESOLVE_SQL,
    "dim_Forloebsmarkoer.MERGE_SQL": FORLOEBSMARKOER_MERGE_SQL,
    "dim_Forloebsmarkoer.INSERT_SQL": FORLOEBSMARKOER_INSERT_SQL,
    "fct_Udredningsforloeb.UNMAPPED_SQL": FCT_UNMAPPED_SQL,
    "fct_Udredningsforloeb.RESOLVE_SQL": FCT_RESOLVE_SQL,
    "fct_Udredningsforloeb.MERGE_SQL": FCT_MERGE_SQL,
    "fct_Udredningsforloeb.INSERT_SQL": FCT_INSERT_SQL,
    "fct_Udredningsforloeb.RETIRE_SQL": RETIRE_SQL,
    "fct_Udredningsforloeb.KONTROLTOTAL_SQL": KONTROLTOTAL_SQL,
    "fct_Udredningsforloeb.FK_DIAGNOSTIK_SQL": FK_DIAGNOSTIK_SQL,
}


@pytest.mark.parametrize("navn", sorted(ALLE_DWH_SQL), ids=sorted(ALLE_DWH_SQL))
def test_dwh_sql_naevner_aldrig_cpr_og_laeser_aldrig_ext(navn):
    """Husregel: dwh-laget laeser KUN stg/map/codesystem/arc (arc alene til
    kontroltotalen), og hverken CPR-kolonner eller CPR-vaerdier findes her."""
    sql = ALLE_DWH_SQL[navn]
    assert "cpr" not in sql.lower(), navn
    assert "[ext]." not in sql, navn


def test_kun_facten_laeser_arkivet_og_kun_til_kontroltotalen():
    for navn, sql in ALLE_DWH_SQL.items():
        if ARC_TABLE in sql:
            assert navn == "fct_Udredningsforloeb.KONTROLTOTAL_SQL", navn


# ---------------------------------------------------------------------------
# (7) tekst_case_sql: rendering, escaping og NULL/blank-kollaps
# ---------------------------------------------------------------------------


def test_tekst_case_sql_render_alle_grene_og_kollapser_null_og_blank():
    sql = tekst_case_sql("p.[flag]", {"1": "Ja", "": "Ikke i opgørelsen"}, "N'Ukendt'")
    assert sql == (
        "CASE ISNULL(LTRIM(RTRIM(p.[flag])), '')\n"
        "    WHEN '1' THEN N'Ja'\n"
        "    WHEN '' THEN N'Ikke i opgørelsen'\n"
        "    ELSE N'Ukendt' END"
    )


def test_tekst_case_sql_dobler_apostroffer_i_baade_noegle_og_tekst():
    """En apostrof i en SDS-tekst maa ikke kunne bryde ud af sin literal."""
    sql = tekst_case_sql("p.[a]", {"O'1": "patientens ønske"}, "p.[a]")
    assert "WHEN 'O''1' THEN N'patientens ønske'" in sql
    sql2 = tekst_case_sql("p.[a]", {"1": "d'Angleterre"}, "N'Ukendt'")
    assert "THEN N'd''Angleterre'" in sql2
    # ELSE-grenen er SQL, ikke en Python-streng, og citeres derfor ikke
    assert sql.endswith("ELSE p.[a] END")


# ---------------------------------------------------------------------------
# (8) Registrering, grupper, deps og checkpoint-ejerskab
# ---------------------------------------------------------------------------

FORVENTEDE_GRUPPER = {
    "map_Helbredsforloeb": "udredningsret_map",
    "map_Udredningsforloeb": "udredningsret_map",
    "map_UdredningsretKlassifikation": "udredningsret_map",
    "map_Forloebsmarkoer": "udredningsret_map",
    "dim_UdredningsretEgenskaber": "udredningsret_dimension",
    "dim_UdredningsretKlassifikation": "udredningsret_dimension",
    "dim_Forloebsmarkoer": "udredningsret_dimension",
    "fct_Udredningsforloeb": "udredningsret_fact",
}

FORVENTEDE_DEPS = {
    "map_Helbredsforloeb": {"udredningsret/pre_stg_Helbredsforloeb"},
    "map_Udredningsforloeb": {"udredningsret/stg_dim_UdredningsretEgenskaber"},
    "map_UdredningsretKlassifikation": {"udredningsret/stg_dim_UdredningsretEgenskaber"},
    "map_Forloebsmarkoer": {"udredningsret/stg_dim_Forloebsmarkoer"},
    "dim_UdredningsretEgenskaber": {
        "map_Udredningsforloeb",
        "udredningsret/stg_dim_UdredningsretEgenskaber",
    },
    "dim_UdredningsretKlassifikation": {
        "map_UdredningsretKlassifikation",
        "udredningsret/stg_dim_UdredningsretEgenskaber",
        "udredningsret/pre_stg_Udredningsforloeb",
        "cs_sks",
    },
    "dim_Forloebsmarkoer": {"map_Forloebsmarkoer", "udredningsret/stg_dim_Forloebsmarkoer"},
    "fct_Udredningsforloeb": {
        "udredningsret/stg_fct_Udredningsforloeb",
        "udredningsret/pre_stg_Helbredsforloeb",
        "map_Udredningsforloeb",
        "map_Helbredsforloeb",
        "map_UdredningsretKlassifikation",
        "arc_udredningsret_mont",
    },
}


def _udredningsret_assets() -> dict:
    from DSA.SQL.dim import all_assets as dim_assets
    from DSA.SQL.fct import all_assets as fct_assets
    from DSA.SQL.map import all_assets as map_assets

    alle = {a.key.to_user_string(): a for a in (*map_assets, *dim_assets, *fct_assets)}
    return {navn: alle[navn] for navn in FORVENTEDE_GRUPPER if navn in alle}


def test_assets_er_registreret_med_udredningsret_grupper_og_tags():
    reg = _udredningsret_assets()
    assert set(reg) == set(FORVENTEDE_GRUPPER), "et asset mangler i map/dim/fct all_assets"
    for navn, asset in reg.items():
        assert asset.group_names_by_key[asset.key] == FORVENTEDE_GRUPPER[navn], navn
        tags = asset.tags_by_key[asset.key]
        assert tags["domain"] == "udredningsret", navn
        assert tags["module"] == "datawarehouse", navn
        forventet_lag = {"map": "maps", "dim": "dimensions", "fct": "facts"}[navn.split("_")[0]]
        assert tags["layer"] == forventet_lag, navn
        if navn.startswith("dim_"):
            assert tags["scd"] == "type1", navn
        assert asset.op.tags.get("dagster/compute_kind") == "mssql", navn


def test_asset_deps_foelger_laesningerne():
    """En manglende dep lader Dagster koere et asset foer den tabel, det laeser,
    er bygget — fx facten foer map'en, der minter dens noegler."""
    for navn, asset in _udredningsret_assets().items():
        deps = {d.to_user_string() for d in asset.asset_deps[asset.key]}
        assert deps == FORVENTEDE_DEPS[navn], navn


# (schema, tabel) -> modulet, der ejer checkpointet. US-5 leverer det femte par.
CHECKPOINT_EJERE = {
    ("dim", "UdredningsretEgenskaber"): "DSA.SQL.dim.dim_UdredningsretEgenskaber",
    ("dim", "UdredningsretKlassifikation"): "DSA.SQL.dim.dim_UdredningsretKlassifikation",
    ("dim", "Forloebsmarkoer"): "DSA.SQL.dim.dim_Forloebsmarkoer",
    ("fct", "Udredningsforloeb"): "DSA.SQL.fct.fct_Udredningsforloeb",
}
US5_EJER = ("fct", "UdredningsretMarkoer")


def test_downstream_checkpoint_ejere_matcher_de_assets_der_saetter_dem():
    """Delta-vinduet i pre_stg_Helbredsforloeb er MIN over DOWNSTREAM_CHECKPOINT_EJERE.
    Saetter et asset et checkpoint under en anden identitet end den, listen naevner,
    faar vinduet aldrig at vide, at ejeren er bagud, og raekker gaar tabt.

    Det femte par (fct.UdredningsretMarkoer) faar sit asset i US-5. Indtil da har
    det ingen ejer, saa `None if any(...)` (U11) tvinger fuld load hver koersel —
    bevidst valgt frem for at fjerne parret og risikere, at det glemmes i US-5."""
    for (schema, tabel), modulnavn in CHECKPOINT_EJERE.items():
        kilde = Path(importlib.import_module(modulnavn).__file__).read_text(encoding="utf-8")
        assert f'dfm.set_checkpoint("{schema}", "{tabel}"' in kilde, (schema, tabel)
        assert f'dfm.get_checkpoint("{schema}", "{tabel}")' in kilde, (schema, tabel)
        assert "config.should_advance_checkpoint" in kilde, modulnavn
    assert set(DOWNSTREAM_CHECKPOINT_EJERE) == set(CHECKPOINT_EJERE) | {US5_EJER}
    assert US5_EJER not in CHECKPOINT_EJERE


def test_maps_saetter_aldrig_checkpoint():
    """Maps er append-only og log-only: et checkpoint her ville flytte
    delta-vinduet, foer dimensionerne og facten havde laest raekkerne."""
    for entitet, *_ in MAPS:
        modul = importlib.import_module(f"DSA.SQL.map.map_{entitet}")
        assert "set_checkpoint" not in Path(modul.__file__).read_text(encoding="utf-8"), entitet


# ---------------------------------------------------------------------------
# (9) DQA: facten er registreret som subject
# ---------------------------------------------------------------------------


def test_fct_er_registreret_som_dqa_subject_med_alle_fk_roller():
    """MR-protokol 2.9: en fact SKAL staa i dqa's _FACTS, ellers har den hverken
    sentinel_rate paa sine FK-roller eller ugentlig referentiel kontrol.

    Specs udledes af lineage-YAML'en, saa testen holder samtidig fast i, at
    dw_ek_Helbredsforloeb er FK-None (gruppenoegle uden dimension) og derfor
    taelles i sentinel_rate, men IKKE faar en referential_integrity-spec, mens
    alle oevrige roller faar begge dele.
    """
    from dqa.config.dwh_subjects import DWH_SUBJECTS

    subjects = [s for s in DWH_SUBJECTS if s.table == "fct.Udredningsforloeb"]
    assert len(subjects) == 1, "facten mangler i (eller staar dobbelt i) _FACTS"
    subject = subjects[0]
    assert subject.identity_column == "dw_ek_Udredningsforloeb"

    sentinel = [c for c in subject.checks if c.check_type == "sentinel_rate"]
    assert len(sentinel) == 1
    assert tuple(sorted(sentinel[0].params["columns"])) == tuple(sorted(FK_ROLES))

    ri_kolonner = {
        c.params["column"] for c in subject.checks if c.check_type == "referential_integrity"
    }
    assert "dw_ek_Helbredsforloeb" not in ri_kolonner  # FK-None: ingen dimension
    assert ri_kolonner == set(FK_ROLES) - {"dw_ek_Helbredsforloeb"}
