# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for udredningsrets markoer-lag (US-5, T5.3).

Markoerfacten er haendelsesfacten under fct.Udredningsforloeb: én raekke pr.
leveret markoer, noeglet paa arkivets EGEN mergenoegle, saa fact-raekken og
arkivraekken er 1:1. Det er praecis den 1:1-relation, der goer, at facten
hverken har eller maa have en retireringsrutine — en markoer kan kun retireres
gennem sin egen stagede dw_is_deleted.

SQL'en i map- og fact-modulet rendres én gang ved import, saa den kan
asserteres som tekst uden database — som test_udredningsret_dwh.py (US-4) og
test_drg_dwh.py goer. Den statiske DDL holdes til modulets kolonnetupler OG
til stagingens egne CAST'er, saa staging, fact-SQL og DDL ikke kan drive fra
hinanden.

Spec: docs/source_systems/udredningsret/udredningsret_model.md (afsnit 2, 3.5,
4.1-4.3, 6 og 9; beslutning U5, U6, U7).
"""

import importlib
import re
from pathlib import Path

import pytest
from DSA.SQL.fct.fct_UdredningsretMarkoer import (
    FACT_COLUMNS,
    FK_DIAGNOSTIK_SQL,
    FK_ROLES,
    GRAIN_CHECK_SQL,
    GRAIN_COLUMN,
    KONTROLTOTAL_SQL,
    STAGED_COLUMNS,
    TAELLER_KONTROL_SQL,
    TARGET_HAS_ROWS_SQL,
    TEMP_TABLE,
)
from DSA.SQL.fct.fct_UdredningsretMarkoer import INSERT_SQL as FCT_INSERT_SQL
from DSA.SQL.fct.fct_UdredningsretMarkoer import MERGE_SQL as FCT_MERGE_SQL
from DSA.SQL.fct.fct_UdredningsretMarkoer import RESOLVE_SQL as FCT_RESOLVE_SQL
from DSA.SQL.fct.fct_UdredningsretMarkoer import UNMAPPED_SQL as FCT_UNMAPPED_SQL
from DSA.SQL.map.map_UdredningsretMarkoer import INSERT_SQL as MAP_MARKOER_SQL
from DSA.SQL.staging.udredningsret_kolonner import (
    DOWNSTREAM_CHECKPOINT_EJERE,
    FCT_FORLOEB_DWH_TABLE,
    FCT_MARKOER_DWH_TABLE,
    FCT_MARKOER_TABLE,
    MAP_FORLOEB_TABLE,
    MAP_FORLOEBSMARKOER_TABLE,
    MAP_HF_TABLE,
    MAP_MARKOER_TABLE,
    MARKOER_CLEAN_CODE_TYPE,
)
from DSA.SQL.staging.udredningsret_staging_sql import ARC_TABLE, build_stg_fct_markoer_sql

pytestmark = pytest.mark.unit

_ETL_DIR = Path(__file__).resolve().parents[2]
_DDL_DIR = _ETL_DIR / "infrastructure" / "ddl" / "tables" / "udredningsret"

AUDIT_KOLONNER = ["dw_timestamp_insert", "dw_timestamp_update"]
ENTITET = "UdredningsretMarkoer"
PK = f"dw_ek_{ENTITET}"
CLEAN_CODE = f"clean_code_{ENTITET}"

MARKOER_MODUL = importlib.import_module("DSA.SQL.fct.fct_UdredningsretMarkoer")
MARKOER_KILDE = Path(MARKOER_MODUL.__file__).read_text(encoding="utf-8")

# Al SQL, modulerne rendrer — GDPR- og lagdelingstjekkene koerer over dem alle,
# saa en ny konstant ikke kan slippe uden om reglerne.
MARKOER_SQL = {
    "map_UdredningsretMarkoer.INSERT_SQL": MAP_MARKOER_SQL,
    "fct_UdredningsretMarkoer.UNMAPPED_SQL": FCT_UNMAPPED_SQL,
    "fct_UdredningsretMarkoer.RESOLVE_SQL": FCT_RESOLVE_SQL,
    "fct_UdredningsretMarkoer.GRAIN_CHECK_SQL": GRAIN_CHECK_SQL,
    "fct_UdredningsretMarkoer.TARGET_HAS_ROWS_SQL": TARGET_HAS_ROWS_SQL,
    "fct_UdredningsretMarkoer.MERGE_SQL": FCT_MERGE_SQL,
    "fct_UdredningsretMarkoer.INSERT_SQL": FCT_INSERT_SQL,
    "fct_UdredningsretMarkoer.KONTROLTOTAL_SQL": KONTROLTOTAL_SQL,
    "fct_UdredningsretMarkoer.FK_DIAGNOSTIK_SQL": FK_DIAGNOSTIK_SQL,
    "fct_UdredningsretMarkoer.TAELLER_KONTROL_SQL": TAELLER_KONTROL_SQL,
}


def _ddl_tekst(ddl_file: str) -> str:
    return (_DDL_DIR / ddl_file).read_text(encoding="utf-8")


def _ddl_blok(ddl_file: str, table: str) -> str:
    """Kolonnedelen af ÉN CREATE TABLE-blok.

    Typeassertionerne skal koere mod netop denne blok og ikke mod hele filen:
    fct.Udredningsforloeb og fct.UdredningsretMarkoer deler kolonnenavnet
    helbredsforloeb_id, saa en soegning over hele 04_facts.sql ville blive
    opfyldt af soeskendefactens linje, ogsaa hvis markoerfactens var skrumpet.
    """
    match = re.search(
        rf"CREATE TABLE \[\w+\]\.\[{table}\]\((.*?)\n    \) ON \[PRIMARY\]",
        _ddl_tekst(ddl_file),
        re.S,
    )
    assert match, f"CREATE TABLE {table} ikke fundet i {ddl_file}"
    return match.group(1)


def _ddl_kolonner(ddl_file: str, table: str) -> list[str]:
    """Kolonnenavne i én CREATE TABLE-blok, i DDL-orden."""
    return re.findall(r"^\s+\[(\w+)\]", _ddl_blok(ddl_file, table), re.M)


def _stagede_typer(sql: str) -> dict[str, str]:
    """navn -> CAST-type for hver `CAST(... AS <TYPE>) AS [navn]` i en bygger.

    Stagingens egne CAST'er er facit for factens kolonnetyper: en smallere
    DDL-kolonne ville afkorte vaerdien i MERGE'n uden en fejl.
    """
    return {navn: typ for typ, navn in re.findall(r"AS (N?VARCHAR\(\d+\))\) AS \[(\w+)\]", sql)}


STAGING_SQL = build_stg_fct_markoer_sql()
STAGEDE_TYPER = _stagede_typer(STAGING_SQL)


# ---------------------------------------------------------------------------
# (1) DDL holdes til konstanterne — kolonner OG typer
# ---------------------------------------------------------------------------


def test_map_ddl_er_dw_ek_clean_code_og_audit():
    """map.* baerer KUN dw_ek, clean_code og audit (MR-protokol 2.3), og
    clean_code-typen er stagingens egen: en smallere map-kolonne ville afkorte
    noeglen og kollapse to forskellige markoerer til ét dw_ek."""
    assert _ddl_kolonner("02_maps.sql", ENTITET) == [PK, CLEAN_CODE, *AUDIT_KOLONNER]
    tekst = _ddl_tekst("02_maps.sql")
    assert f"[{PK}] INT IDENTITY(1,1) NOT NULL" in tekst
    assert f"[{CLEAN_CODE}] {MARKOER_CLEAN_CODE_TYPE} NOT NULL" in tekst
    assert f"CONSTRAINT [PK_Map_{ENTITET}] PRIMARY KEY CLUSTERED ([{CLEAN_CODE}] ASC)" in tekst
    assert f"[IX_Map_{ENTITET}_DW_EK]" in tekst


def test_fct_ddl_matcher_fact_kolonnerne():
    assert _ddl_kolonner("04_facts.sql", ENTITET) == [PK, *FACT_COLUMNS, *AUDIT_KOLONNER]


def test_fct_kolonner_er_stagede_plus_de_tre_map_noegler():
    """Facten baerer praecis stagingens maal plus de tre noegler, den selv
    oploeser — en ny stg-kolonne skal foeres ind bevidst, ikke glemmes."""
    assert set(FACT_COLUMNS) == set(STAGED_COLUMNS) | {
        CLEAN_CODE,
        "dw_ek_Udredningsforloeb",
        "dw_ek_Helbredsforloeb",
        "dw_ek_Forloebsmarkoer",
    }
    # markoerens egen noegle staar foran de stagede kolonner, saa DDL-ordenen
    # kan udledes af tuplen alene
    assert FACT_COLUMNS[:4] == (
        CLEAN_CODE,
        "dw_ek_Udredningsforloeb",
        "dw_ek_Helbredsforloeb",
        "dw_ek_Forloebsmarkoer",
    )
    # hver staget kolonne findes faktisk i stagingens SELECT — ellers ville
    # RESOLVE'n fejle paa en kolonne, der ikke findes
    for kolonne in STAGED_COLUMNS:
        assert f"[{kolonne}]" in STAGING_SQL, f"{kolonne} stages ikke af build_stg_fct_markoer_sql"
    # koden og datoen ligger i dim.Forloebsmarkoer og dim.Dato — ingen
    # degenererede kopier i facten (spec 4.2)
    for forbudt in ("markoer_kode", "markoer_dato", "f_id", "pk", "borger_opslag"):
        assert forbudt not in FACT_COLUMNS, forbudt
        assert forbudt not in _ddl_kolonner("04_facts.sql", ENTITET), forbudt


def test_fct_ddl_typer_matcher_stagingens_casts_og_clean_code_typen():
    # KUN markoerfactens egen blok: helbredsforloeb_id staar med samme type paa
    # soeskendefacten i den samme fil, saa en filbred soegning ville vaere sand,
    # selv om markoerfactens kolonne var smallere end stagingens CAST
    tekst = _ddl_blok("04_facts.sql", ENTITET)
    # clean_code'en er NULLable paa facten (som paa forloebsfacten), men samme
    # bredde som stagingens og map'ens
    assert f"[{CLEAN_CODE}] {MARKOER_CLEAN_CODE_TYPE} NULL" in tekst
    for navn in ("helbredsforloeb_id", "forloebselement_uuid", "markoer_uuid"):
        assert f"[{navn}] {STAGEDE_TYPER[navn]} NULL" in tekst, navn
    for navn in ("sygehusregion", "bopaelsregion"):
        assert f"[{navn}] {STAGEDE_TYPER[navn]} NULL" in tekst, navn
    assert STAGEDE_TYPER["helbredsforloeb_id"] == "NVARCHAR(50)"
    assert STAGEDE_TYPER["markoer_uuid"] == "NVARCHAR(36)"


def test_fct_ddl_har_sentinel_defaults_maal_og_noeglens_indekser():
    tekst = _ddl_tekst("04_facts.sql")
    for rolle in FK_ROLES:
        assert (
            f"[{rolle}] INT NOT NULL CONSTRAINT [DF_{ENTITET}_{rolle}] DEFAULT -1" in tekst
        ), rolle
    for flag in ("indgaar_i_indikator_H", "indgaar_i_indikator_L_N"):
        assert f"[{flag}] BIT NOT NULL CONSTRAINT [DF_{ENTITET}_{flag}] DEFAULT 0" in tekst, flag
    # kontroltotalens maal: en manglende raekke i leverancen maa ikke kunne
    # gemme sig bag en NULL
    assert (
        f"[antal_kilderaekker] INT NOT NULL CONSTRAINT [DF_{ENTITET}_antal_kilderaekker] DEFAULT 1"
        in tekst
    )
    assert (
        f"[dw_is_deleted] BIT NOT NULL CONSTRAINT [DF_{ENTITET}_dw_is_deleted] DEFAULT 0" in tekst
    )
    assert f"CONSTRAINT [PK_Fct_{ENTITET}] PRIMARY KEY CLUSTERED ([{PK}] ASC)" in tekst
    # clean_code'en er unik pr. raekke — filtreret, saa NULL ikke kolliderer
    assert (
        f"CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_{ENTITET}_clean_code]" in tekst
        and f"WHERE [{CLEAN_CODE}] IS NOT NULL" in tekst
    )
    for indeks in (
        "Udredningsforloeb",
        "Helbredsforloeb",
        "Forloebsmarkoer",
        "Borger",
        "Dato_Markoer",
        "Watermark",
    ):
        assert f"[IX_Fct_{ENTITET}_{indeks}]" in tekst, indeks


def test_seeds_giver_map_en_sentinel_men_facten_ingen():
    tekst = _ddl_tekst("05_seeds.sql")
    assert f"SET IDENTITY_INSERT [map].[{ENTITET}] ON" in tekst
    assert f"WHERE [{PK}] = -1" in tekst
    # sentinel-vaerdien asserteres INDE i markoerens egen seed-blok: praecis
    # samme "-1, 'UNKNOWN'" staar i fire soeskende-seeds i den samme fil, saa en
    # filbred soegning ville vaere sand, ogsaa hvis markoerens egen vaerdi drev
    seed_blok = re.search(
        rf"SET IDENTITY_INSERT \[map\]\.\[{ENTITET}\] ON;(.*?)"
        rf"SET IDENTITY_INSERT \[map\]\.\[{ENTITET}\] OFF;",
        tekst,
        re.S,
    )
    assert seed_blok, f"seed-blokken for map.{ENTITET} mangler"
    assert "-1, 'UNKNOWN'" in seed_blok.group(1)
    # facten har ingen sentinel-raekke: enhver raekke i maalet vaelger MERGE-stien
    assert f"[fct].[{ENTITET}]" not in tekst


# ---------------------------------------------------------------------------
# (2) map er append-only og minter ogsaa de soft-slettede markoerer
# ---------------------------------------------------------------------------


def test_map_er_append_only_fra_markoerfactens_staging():
    assert f"INSERT INTO {MAP_MARKOER_TABLE} (" in MAP_MARKOER_SQL
    assert f"FROM {FCT_MARKOER_TABLE} src" in MAP_MARKOER_SQL
    assert "SELECT DISTINCT" in MAP_MARKOER_SQL
    assert "NOT EXISTS" in MAP_MARKOER_SQL
    # ordgraenser: en kolonne som dw_is_deleted maa ikke udloese tjekket
    assert not re.search(r"\bMERGE\b|\bUPDATE\b|\bDELETE\b", MAP_MARKOER_SQL, re.I)
    # dw_ek tildeles af IDENTITY — map'en skriver den aldrig selv
    assert PK not in MAP_MARKOER_SQL
    # tomme og manglende clean_codes minter ingen noegle
    assert f"src.[{CLEAN_CODE}] IS NOT NULL" in MAP_MARKOER_SQL
    assert f"src.[{CLEAN_CODE}] <> ''" in MAP_MARKOER_SQL


def test_map_minter_ogsaa_soft_slettede_markoerer():
    """D3/spec afsnit 6: en retireret markoer skal BEHOLDE sit dw_ek, saa den
    genoplives paa den SAMME fact-raekke, naar leverancen bringer den tilbage.
    Et dw_is_deleted-filter her ville minte et nyt dw_ek ved genoplivning og
    efterlade den gamle raekke retireret for evigt."""
    assert "dw_is_deleted" not in MAP_MARKOER_SQL


def test_map_saetter_aldrig_checkpoint():
    """Maps er append-only og log-only: et checkpoint her ville flytte
    delta-vinduet, foer facten havde laest raekkerne."""
    modul = importlib.import_module("DSA.SQL.map.map_UdredningsretMarkoer")
    assert "set_checkpoint" not in Path(modul.__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# (3) UNMAPPED-vaernet: to INNER-opslag, to sentinel-opslag
# ---------------------------------------------------------------------------

# (map-tabel, map'ens PK, opslaget er INNER JOIN i RESOLVE'n)
MAP_OPSLAG = (
    (MAP_MARKOER_TABLE, PK, CLEAN_CODE, True),
    (MAP_HF_TABLE, "dw_ek_Helbredsforloeb", "clean_code_Helbredsforloeb", True),
    (MAP_FORLOEB_TABLE, "dw_ek_Udredningsforloeb", "clean_code_Udredningsforloeb", False),
    (MAP_FORLOEBSMARKOER_TABLE, "dw_ek_Forloebsmarkoer", "clean_code_Forloebsmarkoer", False),
)
OPSLAG_IDS = [m[0] for m in MAP_OPSLAG]


def _unmapped_gren(map_tabel: str) -> str:
    """Den ENE tael-gren i UNMAPPED_SQL, der slaar op i en bestemt map."""
    grene = [
        g for g in FCT_UNMAPPED_SQL.split("(SELECT COUNT(*)") if f"LEFT JOIN {map_tabel} m" in g
    ]
    assert len(grene) == 1, f"{map_tabel} slaas op i {len(grene)} grene, forventede én"
    return grene[0]


@pytest.mark.parametrize("map_tabel, map_pk, clean_code, _inner", MAP_OPSLAG, ids=OPSLAG_IDS)
def test_unmapped_taeller_alle_fire_map_opslag(map_tabel, map_pk, clean_code, _inner):
    assert f"LEFT JOIN {map_tabel} m" in FCT_UNMAPPED_SQL, map_tabel
    assert f"ON m.[{clean_code}] = s.[{clean_code}]" in FCT_UNMAPPED_SQL, clean_code
    assert f"m.[{map_pk}] IS NULL" in FCT_UNMAPPED_SQL, map_pk
    assert FCT_UNMAPPED_SQL.count(f"FROM {FCT_MARKOER_TABLE} s") == 4


def test_unmapped_begraenser_sentinel_opslagene_til_ikke_null_clean_codes():
    """D5.1: NULL er den LOVLIGE sentinel-situation paa forloeb (ingen F_ID, U5)
    og paa markoerkoden (blank kode), saa de to tael-grene skal se bort fra den.
    Uden IS NOT NULL-graenen ville hver eneste sentinel-raekke fejle koerslen —
    og testen ville aldrig naa ud over en tom leverance."""
    for clean_code in ("clean_code_Udredningsforloeb", "clean_code_Forloebsmarkoer"):
        assert f"WHERE s.[{clean_code}] IS NOT NULL" in FCT_UNMAPPED_SQL, clean_code
    # de to INNER-opslag har derimod INGEN IS NOT NULL-gren: en markoer uden
    # egen noegle eller uden helbredsforloeb er en fejl, ikke en sentinel
    assert FCT_UNMAPPED_SQL.count("IS NOT NULL") == 2
    for clean_code in (CLEAN_CODE, "clean_code_Helbredsforloeb"):
        assert f"WHERE s.[{clean_code}] IS NOT NULL" not in FCT_UNMAPPED_SQL, clean_code


def test_unmapped_vaerner_markoerkoden_kun_paa_de_levende_stagede_raekker():
    """Gren (d) maa KUN taelle levende stagede raekker, fordi det er hele den
    population, map.Forloebsmarkoer overhovedet kan daekke: map'en fyldes fra
    stg.dim_Forloebsmarkoer, der bygges over arkivets LEVENDE raekker alene
    (spec 3.4), mens denne staging bevidst baerer levende OG soft-slettede
    raekker (spec 3.5) og danner et clean_code for enhver ikke-blank kode.

    En kode, der paa koerselstidspunktet kun findes paa soft-slettede raekker,
    kan derfor aldrig mintes — og uden graenen ville vaernet hard-faile paa en
    tilstand, INGEN map-koersel kan rette (map'en er append-only, saa et nyt
    forsoeg aendrer intet). Den retirerede markoer lander i stedet paa
    COALESCE(-1); raekken er alligevel slettet, og vaernet gaelder fuldt ud
    igen, saa snart koden leveres levende.

    De tre andre grene har og skal IKKE have graenen — asymmetrien er selve
    beviset: (a) fyldes fra denne stagingtabel selv, (b) fra
    stg.pre_Helbredsforloeb, som ogsaa stager et helbredsforloeb helt uden
    levende raekker, og (c)'s clean_code laeses fra stg.pre_Udredningsforloeb,
    netop den tabel map.Udredningsforloeb mintes fra.
    """
    assert "s.[dw_is_deleted] = 0" in _unmapped_gren(MAP_FORLOEBSMARKOER_TABLE)
    for map_tabel in (MAP_MARKOER_TABLE, MAP_HF_TABLE, MAP_FORLOEB_TABLE):
        assert "dw_is_deleted" not in _unmapped_gren(map_tabel), map_tabel
    assert FCT_UNMAPPED_SQL.count("dw_is_deleted") == 1


def test_unmapped_fejlbesked_naevner_bade_map_og_foraeldreloese():
    """Beskeden skal kunne handles paa: hvilken map skal koeres, og at en
    staget foraelder SKAL findes (0 foraeldreloese), aldrig blive stille -1.

    Assertionerne koerer mod den UDTRUKNE besked, ikke mod modulteksten: alle
    fire map-navne staar ogsaa i asset'ets deps, og "0 foraeldreloese" staar i
    modulets docstring — en filbred soegning ville derfor vaere sand, ogsaa hvis
    beskeden var tom for handlingsanvisning.
    """
    besked = re.search(
        r"if markoer_unmapped \+.*?raise ValueError\(\s*(.*?)\n\s*\)", MARKOER_KILDE, re.S
    )
    assert besked, "UNMAPPED-vaernet rejser ingen ValueError"
    tekst = besked.group(1)
    for map_navn in (
        "map_UdredningsretMarkoer",
        "map_Helbredsforloeb",
        "map_Forloebsmarkoer",
        "map_Udredningsforloeb",
    ):
        assert map_navn in tekst, map_navn
    assert "0 foraeldreloese" in tekst
    assert "-1" in tekst
    # ... og vaernet skal udloeses af ALLE fire taellere. Falder én ud af
    # summen, forsvinder dens invariant lydloest — droppes forloeb_unmapped, er
    # "0 foraeldreloese" ikke laengere haandhaevet, og den foraeldreloese
    # markoer falder stille paa COALESCE(-1) i RESOLVE'n
    assert re.search(
        r"if markoer_unmapped \+ hf_unmapped \+ forloeb_unmapped \+ kode_unmapped > 0:",
        MARKOER_KILDE,
    ), "UNMAPPED-vaernet summerer ikke alle fire taellere"


# ---------------------------------------------------------------------------
# (4) RESOLVE: INNER for de vaernede noegler, LEFT + COALESCE for sentinelerne
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("map_tabel, map_pk, clean_code, inner", MAP_OPSLAG, ids=OPSLAG_IDS)
def test_resolve_joiner_inner_hvor_noeglen_er_krav_og_left_hvor_minus_1_er_lovlig(
    map_tabel, map_pk, clean_code, inner
):
    """D5.2: markoerens egen noegle og helbredsforloebet er INNER JOINs (vaernet
    ovenfor fanger et miss), mens forloebet og markoerkoden er LEFT JOINs med
    COALESCE(-1) — NULL er dér den lovlige sentinel-situation."""
    join = "INNER JOIN" if inner else "LEFT JOIN"
    assert f"{join} {map_tabel}" in FCT_RESOLVE_SQL, map_tabel
    assert f"src.[{clean_code}]" in FCT_RESOLVE_SQL, clean_code
    if inner:
        assert f"COALESCE({map_pk}" not in FCT_RESOLVE_SQL
    else:
        assert re.search(
            rf"COALESCE\(\w+\.\[{map_pk}\], -1\) AS \[{map_pk}\]", FCT_RESOLVE_SQL
        ), map_pk


def test_resolve_baerer_stagingen_uaendret_og_lader_bogholderiet_blive():
    for kolonne in STAGED_COLUMNS:
        assert f"src.[{kolonne}]" in FCT_RESOLVE_SQL, kolonne
    # stagingens eget bogholderi, de raa SOR-koder og de degenererede kopier
    # foelger ikke med i facten
    for forbudt in (
        "borger_opslag",
        "forloeb_enhed_sor",
        "markoer_kode",
        "markoer_dato",
        "[f_id]",
        "[pk]",
        "dw_timestamp",
    ):
        assert forbudt not in FCT_RESOLVE_SQL, forbudt
    assert f"FROM {FCT_MARKOER_TABLE} src" in FCT_RESOLVE_SQL
    assert f"INTO {TEMP_TABLE}" in FCT_RESOLVE_SQL


def test_resolve_giver_temp_tabellen_clustered_noegle_foer_merge():
    """MR-protokol 2.3: #Temp skal have en clustered noegle FOER MERGE, ellers
    skal resolve-saettet sorteres eller hashes ved hver koersel, foer MERGE'n
    kan matche mod maalets clustered PK — og saettet baerer 8,2 mio. raekker.
    Noeglen maa IKKE vaere unik, saa en dublet naar frem til grain-taellingen i
    stedet for at doe som en fejlet indeksbygning."""
    assert f"CREATE CLUSTERED INDEX [IX_{ENTITET}_Facts]\n    ON {TEMP_TABLE}([{PK}]);" in (
        FCT_RESOLVE_SQL
    )
    assert "CREATE UNIQUE CLUSTERED INDEX" not in FCT_RESOLVE_SQL
    assert FCT_RESOLVE_SQL.index(f"INTO {TEMP_TABLE}") < FCT_RESOLVE_SQL.index(
        "CREATE CLUSTERED INDEX"
    )


def test_grain_check_maaler_paa_markoerens_clean_code():
    assert GRAIN_COLUMN == CLEAN_CODE
    assert GRAIN_CHECK_SQL == f"SELECT COUNT(*), COUNT(DISTINCT [{CLEAN_CODE}]) FROM {TEMP_TABLE}"
    # D5.3: ordet "grain" skal staa i SELVE fejlbeskeden, for det er den, vagten
    # soeger paa i koerselsloggen. Modulteksten alene ville allerede vaere
    # opfyldt af asset-taggen "grain" og af metadatanoeglen af samme navn
    besked = re.search(
        r"if total_rows != distinct_codes:\s*raise ValueError\(\s*(.*?)\n\s*\)",
        MARKOER_KILDE,
        re.S,
    )
    assert besked, "grain-tjekket rejser ingen ValueError"
    assert "grain" in besked.group(1)


# ---------------------------------------------------------------------------
# (5) Load-stier: tomt maal -> TABLOCK, ellers MERGE med EXCEPT
# ---------------------------------------------------------------------------


def test_merge_except_daekker_alle_fact_kolonner_inkl_genoplivning():
    matched = FCT_MERGE_SQL.split("WHEN NOT MATCHED")[0]
    # EXCEPT-blokken og UPDATE SET asserteres hver for sig — ellers ville
    # "tgt.[x] = src.[x]" i UPDATE SET alene opfylde alle tre assertions, og en
    # EXCEPT-liste uden fx dw_is_deleted ville aldrig blive fanget her.
    sammenligning, adskiller, opdatering = matched.partition("THEN\n    UPDATE SET")
    assert adskiller, "WHEN MATCHED har ingen UPDATE SET"
    assert "EXCEPT" in sammenligning
    for kolonne in FACT_COLUMNS:
        assert f"src.[{kolonne}]" in sammenligning, f"{kolonne} mangler i EXCEPT-kilden"
        assert f"tgt.[{kolonne}]" in sammenligning, f"{kolonne} mangler i EXCEPT-maalet"
        assert f"tgt.[{kolonne}] = src.[{kolonne}]" in opdatering, f"{kolonne} opdateres ikke"
    # D5.4: WHEN MATCHED skriver dw_is_deleted fra kilden, saa en markoer,
    # leverancen dropper, retireres — og en, den bringer tilbage, genoplives
    assert "src.[dw_is_deleted]" in sammenligning and "tgt.[dw_is_deleted]" in sammenligning
    assert "tgt.[dw_is_deleted] = src.[dw_is_deleted]" in opdatering
    assert f"ON tgt.[{PK}] = src.[{PK}]" in FCT_MERGE_SQL
    assert "tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))" in FCT_MERGE_SQL


def test_bulk_insert_skriver_praecis_de_samme_kolonner_som_merge():
    for kolonne in (PK, *FACT_COLUMNS, *AUDIT_KOLONNER):
        assert f"[{kolonne}]" in FCT_INSERT_SQL, kolonne
    assert "WITH (TABLOCK)" in FCT_INSERT_SQL
    assert f"FROM {TEMP_TABLE} src" in FCT_INSERT_SQL
    # facten har ingen sentinel-raekke, saa ENHVER raekke vaelger MERGE-stien
    assert f"EXISTS (SELECT 1 FROM {FCT_MARKOER_DWH_TABLE})" in TARGET_HAS_ROWS_SQL
    assert "<> -1" not in TARGET_HAS_ROWS_SQL


def test_markoerfacten_har_ingen_retireringsrutine():
    """D5.5 — invarianten: hver markoer findes 1:1 i arkivet, staging baerer de
    soft-slettede raekker med i sit scope, og arkivet hard-sletter aldrig. En
    markoer kan derfor KUN retireres gennem sin egen stagede dw_is_deleted, og
    et ekstra retirerings-UPDATE ville retirere raekker, som leverancen stadig
    indeholder. Forloebsfacten har modsat en RETIRE_SQL, fordi et forloeb er en
    GRUPPERING, der kan forsvinde, uden at nogen arkivraekke forsvinder."""
    # DSA.SQL.fct.__init__ binder asset-objektet til samme navn som submodulet,
    # saa modulet skal hentes eksplicit — ikke med `from DSA.SQL.fct import ...`
    forloeb_modul = importlib.import_module("DSA.SQL.fct.fct_Udredningsforloeb")

    assert not hasattr(MARKOER_MODUL, "RETIRE_SQL")
    assert hasattr(forloeb_modul, "RETIRE_SQL"), "soeskende-facten har mistet sin retirering"
    for navn, sql in MARKOER_SQL.items():
        # ingen SQL saetter dw_is_deleted til 1 — kun MERGE'n skriver den, og
        # kun fra kilden
        assert not re.search(r"\[dw_is_deleted\]\s*=\s*1\b", sql), navn
        assert not re.search(r"\bDELETE\b", sql), navn
        if navn != "fct_UdredningsretMarkoer.MERGE_SQL":
            assert not re.search(r"\bUPDATE\b", sql), navn
    # MERGE'n retirerer heller ikke selv: ingen WHEN NOT MATCHED BY SOURCE
    assert "WHEN NOT MATCHED BY SOURCE" not in FCT_MERGE_SQL
    assert FCT_MERGE_SQL.count("UPDATE SET") == 1
    # og asset'et rapporterer ingen retireringstaeller, der kunne foregoegle en
    assert "retireret" not in MARKOER_KILDE


# ---------------------------------------------------------------------------
# (6) Kontroltotal, FK-diagnostik og taellerkontrol
# ---------------------------------------------------------------------------


def test_kontroltotal_holder_hele_facten_op_mod_hele_arkivet():
    """D5.7: grainet er 1:1 med arkivraekken, saa raekketallet kraever ingen
    gruppering — og antal_kilderaekker baerer leverancens raa raekketal, saa
    summen skal matche paa begge sider. Begge sider er den LEVENDE skive, og
    sammenligningen spaender hele tabellen, saa den holder paa tvaers af
    delta-batches."""
    assert f"FROM {ARC_TABLE} AS a" in KONTROLTOTAL_SQL
    assert f"FROM {FCT_MARKOER_DWH_TABLE}" in KONTROLTOTAL_SQL
    assert "a.[dw_is_deleted] = 0" in KONTROLTOTAL_SQL
    assert KONTROLTOTAL_SQL.count("[dw_is_deleted] = 0") == 2
    # ingen gruppering og ingen delta-afgraensning
    assert "GROUP BY" not in KONTROLTOTAL_SQL
    assert TEMP_TABLE not in KONTROLTOTAL_SQL
    # SUM over INT loeber over ved 2^31 paa en stor nok leverance
    assert KONTROLTOTAL_SQL.count("SUM(CAST(") == 2
    assert "[antal_kilderaekker] AS BIGINT" in KONTROLTOTAL_SQL
    for kolonne in ("[fct_antal]", "[arc_antal]", "[fct_kilderaekker]", "[arc_kilderaekker]"):
        assert kolonne in KONTROLTOTAL_SQL, kolonne
    # afvigelsen er en HAARD fejl med den danske tekst, driftsvagten soeger paa
    assert "kontroltotalen holder ikke" in MARKOER_KILDE
    assert "dg.Failure(" in MARKOER_KILDE
    assert re.search(
        r"if fct_antal != arc_antal or fct_kilderaekker != arc_kilderaekker:", MARKOER_KILDE
    ), "kontroltotalen sammenlignes ikke paa begge tal"


def test_fk_diagnostik_daekker_alle_roller_blank_cpr_og_designsentinelen():
    assert FK_ROLES == (
        "dw_ek_Udredningsforloeb",
        "dw_ek_Helbredsforloeb",
        "dw_ek_Forloebsmarkoer",
        "dw_ek_Borger",
        "dw_ek_Organisation_forloeb",
        "dw_ek_Dato_markoer",
    )
    # rollerne er praecis factens FK-kolonner — hverken flere eller faerre
    assert set(FK_ROLES) == {c for c in FACT_COLUMNS if c.startswith("dw_ek_")}
    for rolle in FK_ROLES:
        assert f"SUM(CASE WHEN [{rolle}] = -1 THEN 1 ELSE 0 END) AS [{rolle}]" in FK_DIAGNOSTIK_SQL
    # strukturelt tomt CPR taelles for sig, ikke som et opslagsmiss
    assert "[dw_ek_Borger] = -2" in FK_DIAGNOSTIK_SQL
    assert "WHERE [dw_is_deleted] = 0" in FK_DIAGNOSTIK_SQL
    # D5.8: markoerer uden forloeb rapporteres under deres EGET navn, fordi -1
    # dér er design (U5) og ikke en uoploest noegle
    assert '"markoerer_uden_forloeb": uden_forloeb' in MARKOER_KILDE
    assert 'uden_forloeb = fk_ukendt["dw_ek_Udredningsforloeb"]' in MARKOER_KILDE


def test_taellerkontrollen_er_metadata_og_aldrig_en_haard_fejl():
    """D5.9/beslutning U6: antal_markoerer taelles i pre_stg direkte fra
    arkivet, saa de to tal er bygget af hver sin kilde med vilje. Enighed er
    beviset for, at begge laesninger beskriver de samme raekker — men den maa
    ALDRIG faelde koerslen, foer US-6 har gjort den til et DQA-subject."""
    assert f"FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0" in TAELLER_KONTROL_SQL
    assert "SUM(CAST([antal_markoerer] AS BIGINT))" in TAELLER_KONTROL_SQL
    assert (
        f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE}\n"
        "     WHERE [dw_is_deleted] = 0 AND [dw_ek_Udredningsforloeb] <> -1" in TAELLER_KONTROL_SQL
    )
    # ingen gren rejser paa taellerne
    for variabel in ("taeller_afvigelse", "forloeb_taeller", "markoerer_med_forloeb"):
        assert not re.search(rf"if\b[^\n]*\b{variabel}\b", MARKOER_KILDE), variabel
        assert f'"{variabel}"' in MARKOER_KILDE or f"{variabel}:" in MARKOER_KILDE
    # facten fejler praecis ét sted: kontroltotalen
    assert MARKOER_KILDE.count("dg.Failure(") == 1


# ---------------------------------------------------------------------------
# (7) GDPR og lagdeling
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("navn", sorted(MARKOER_SQL), ids=sorted(MARKOER_SQL))
def test_markoer_sql_naevner_aldrig_cpr_og_laeser_aldrig_ext(navn):
    """Husregel: dwh-laget laeser KUN stg/map/arc (arc alene til
    kontroltotalen), og hverken CPR-kolonner eller CPR-vaerdier findes her."""
    sql = MARKOER_SQL[navn]
    assert "cpr" not in sql.lower(), navn
    assert "[ext]." not in sql, navn


def test_kun_kontroltotalen_laeser_arkivet():
    for navn, sql in MARKOER_SQL.items():
        if ARC_TABLE in sql:
            assert navn == "fct_UdredningsretMarkoer.KONTROLTOTAL_SQL", navn


# ---------------------------------------------------------------------------
# (8) Registrering, grupper, deps og checkpoint-ejerskab
# ---------------------------------------------------------------------------

FORVENTEDE_GRUPPER = {
    "map_UdredningsretMarkoer": "udredningsret_map",
    "fct_UdredningsretMarkoer": "udredningsret_fact",
}

FORVENTEDE_DEPS = {
    "map_UdredningsretMarkoer": {"udredningsret/stg_fct_UdredningsretMarkoer"},
    "fct_UdredningsretMarkoer": {
        "udredningsret/stg_fct_UdredningsretMarkoer",
        "map_UdredningsretMarkoer",
        "map_Helbredsforloeb",
        "map_Udredningsforloeb",
        "map_Forloebsmarkoer",
        "arc_udredningsret_mont",
        # taellerkontrollen laeser fct.Udredningsforloeb, og begge facts ligger i
        # gruppen udredningsret_fact: uden kanten er de sidestillede i grafen
        "fct_Udredningsforloeb",
    },
}


def _markoer_assets() -> dict:
    from DSA.SQL.fct import all_assets as fct_assets
    from DSA.SQL.map import all_assets as map_assets

    alle = {a.key.to_user_string(): a for a in (*map_assets, *fct_assets)}
    return {navn: alle[navn] for navn in FORVENTEDE_GRUPPER if navn in alle}


def test_assets_er_registreret_i_de_bestaaende_udredningsret_grupper():
    """D1: ingen ny asset-gruppe, saa udredningsret_publish_job's selektion er
    uaendret — men begge assets SKAL staa i map/fct all_assets, ellers koerer
    de aldrig."""
    reg = _markoer_assets()
    assert set(reg) == set(FORVENTEDE_GRUPPER), "et asset mangler i map/fct all_assets"
    for navn, asset in reg.items():
        assert asset.group_names_by_key[asset.key] == FORVENTEDE_GRUPPER[navn], navn
        tags = asset.tags_by_key[asset.key]
        assert tags["domain"] == "udredningsret", navn
        assert tags["module"] == "datawarehouse", navn
        assert tags["layer"] == {"map": "maps", "fct": "facts"}[navn.split("_")[0]], navn
        assert asset.op.tags.get("dagster/compute_kind") == "mssql", navn


def test_asset_deps_foelger_laesningerne():
    """En manglende dep lader Dagster koere et asset foer den tabel, det laeser,
    er bygget — fx facten foer map'en, der minter dens noegler, foer arkivet,
    kontroltotalen holder den op imod, eller foer fct.Udredningsforloeb, hvis
    antal_markoerer taellerkontrollen sammenligner med. Den sidste kant er
    noedvendig, netop fordi begge facts ligger i gruppen udredningsret_fact:
    selektionen i udredningsret_publish_job ordner dem ikke, det goer grafen."""
    for navn, asset in _markoer_assets().items():
        deps = {d.to_user_string() for d in asset.asset_deps[asset.key]}
        assert deps == FORVENTEDE_DEPS[navn], navn


def test_facten_ejer_det_femte_downstream_checkpoint():
    """Delta-vinduet i pre_stg_Helbredsforloeb er MIN over
    DOWNSTREAM_CHECKPOINT_EJERE. Saetter facten sit checkpoint under en anden
    identitet end den, listen naevner, faar vinduet aldrig at vide, at ejeren er
    bagud, og raekker gaar tabt (U11)."""
    ejer = ("fct", ENTITET)
    assert ejer in DOWNSTREAM_CHECKPOINT_EJERE
    assert f'dfm.set_checkpoint("fct", "{ENTITET}"' in MARKOER_KILDE
    assert f'dfm.get_checkpoint("fct", "{ENTITET}")' in MARKOER_KILDE
    assert "config.should_advance_checkpoint" in MARKOER_KILDE
    # ... og under sin egen DFM-identitet, saa koerselsloggen kan skelnes fra
    # forloebsfactens
    assert f'dfm.start_dfm_run(\n        "fct", "{ENTITET}"' in MARKOER_KILDE


# ---------------------------------------------------------------------------
# (9) DQA: facten er registreret som subject med sin sentinel-cap
# ---------------------------------------------------------------------------


def test_fct_er_registreret_som_dqa_subject_med_alle_fk_roller_og_cap():
    """MR-protokol 2.9: en fact SKAL staa i dqa's _FACTS, ellers har den hverken
    sentinel_rate paa sine FK-roller eller ugentlig referentiel kontrol.

    Specs udledes af lineage-YAML'en, saa testen holder samtidig fast i, at
    dw_ek_Helbredsforloeb er FK-None (gruppenoegle uden dimension) og derfor
    taelles i sentinel_rate, men IKKE faar en referential_integrity-spec.
    Cappen paa dw_ek_Udredningsforloeb (U5-designsentinelen) skal vaere sat,
    for uden en cap koerer sentinel_rate paa INFO og kan aldrig flage."""
    from dqa.config.dwh_subjects import DWH_SUBJECTS
    from dqa.model import Severity

    subjects = [s for s in DWH_SUBJECTS if s.table == f"fct.{ENTITET}"]
    assert len(subjects) == 1, "facten mangler i (eller staar dobbelt i) _FACTS"
    subject = subjects[0]
    assert subject.identity_column == PK

    sentinel = [c for c in subject.checks if c.check_type == "sentinel_rate"]
    assert len(sentinel) == 1
    assert tuple(sorted(sentinel[0].params["columns"])) == tuple(sorted(FK_ROLES))
    assert sentinel[0].params["max_rate"] == {"dw_ek_Udredningsforloeb": 0.20}
    assert sentinel[0].severity is Severity.WARN, "uden WARN kan en cap aldrig flage"

    ri_kolonner = {
        c.params["column"] for c in subject.checks if c.check_type == "referential_integrity"
    }
    assert "dw_ek_Helbredsforloeb" not in ri_kolonner  # FK-None: ingen dimension
    assert ri_kolonner == set(FK_ROLES) - {"dw_ek_Helbredsforloeb"}
