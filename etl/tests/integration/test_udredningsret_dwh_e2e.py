# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E-integrationstest for udredningsrets map/dim/fct-lag (US-4 og
US-5) mod devcontainer-MSSQL.

Bygger paa staging-testens fixtures og hjaelpere (test_udredningsret_staging_e2e,
som selv bygger paa ingest-testens syntetiske leverancer), saa hele kaeden
koeres med arkivets EGNE noegler:

    leverance (tar.bz2) -> extract -> arc -> pre_stg -> stg
        -> map (5) -> dim (3) -> fct.Udredningsforloeb
                              -> fct.UdredningsretMarkoer

Verificerer (spec: docs/source_systems/udredningsret/udredningsret_model.md,
afsnit 4 og 6; beslutning U2-U13):

1. FULD INDLAESNING: facten har én raekke pr. stagede forloeb, deler PK med
   dim.UdredningsretEgenskaber (praecis én egenskabsraekke pr. fact-raekke),
   klassifikationsdimensionen har ét medlem pr. attributkombination med de
   rendrede danske tekster (inkl. 'Ikke i opgørelsen' for blanke flag og
   SKS-teksten paa forloebslabel), dim.Forloebsmarkoer baerer bilag 1-grupperne
   inkl. 'oevrige' og 'Ukendt' for en kode uden SKS-raekke, og kontroltotalen
   mod arkivet er groen.
1b. MARKOERFACTEN EFTER FULD INDLAESNING (US-5): én raekke pr. arkivraekke,
   SUM(antal_kilderaekker) = arkivets, markoerer uden F_ID paa sentinel -1
   mens hver markoer MED F_ID rammer en levende forloebsraekke (0
   foraeldreloese), og taellerkontrollen (SUM(antal_markoerer) paa
   forloebsfacten) er enig med markoerraekkerne, der har et forloeb.
2. DELTA efter leverance B (F_ID omnummereret, OBJEKTID genudstedt, én markoer
   soft-slettet): dw_ek_Udredningsforloeb er UAENDRET, dim-attributterne er
   overskrevet (Type 1), fact-raekkerne opdateret in place, kontroltotal groen.
2b. GENUDSTEDT OBJEKTID (US-5): markoernoeglen er bygget paa markoer_noegle,
   som IKKE indeholder OBJEKTID, saa de to genudstedte markoerer opdaterer
   markoer_uuid paa deres EGNE raekker uden at minte nye; den markoer,
   leverance B droppede, staar retireret med dw_is_deleted = 1.
3. AENDRET INDIKATORATTRIBUT direkte i arkivet: nyt klassifikationsmedlem,
   factens FK skifter, forloebsnoeglen roeres ikke.
4. RETIRERING + GENOPLIVNING: et forloeb, hvis markoerer alle soft-slettes,
   faar dw_is_deleted = 1 (aldrig DELETE) og kommer tilbage med 0, naar
   arkivraekken lever igen. Kontroltotalen er groen i begge tilstande.
5. NYT OPGOERELSESKVARTAL: dw_ek_Dato_opgoerelse flytter fra BLANK til
   kvartalets foerste dag.
6. GLOBAL KATALOG-REFRESH: en revideret SKSConcept-tekst naar ALLE medlemmer —
   ogsaa dem, der ikke er i den aktuelle delta-batch.
7. KONTROLTOTAL-BRUD: arkivet aendres uden gen-staging -> dg.Failure.
8. GRAIN-BRUD: to stg-raekker med samme clean_code -> ValueError.
8b. GENOPLIVNING AF EN MARKOER (US-5): den retirerede markoer kommer tilbage i
   arkivet og genoplives paa den SAMME fact-raekke (samme
   dw_ek_UdredningsretMarkoer) — facten har ingen retireringsrutine, saa
   MERGE'ns dw_is_deleted fra kilden er den eneste vej ind og ud.
8c. KONTROLTOTAL-BRUD PAA MARKOERFACTEN (US-5): en arkivraekke soft-slettes
   uden gen-staging -> dg.Failure.
9. GDPR: hverken kolonnenavne eller CPR-vaerdier i nogen af de ti
   map/dim/fct-tabeller.

Testene deler modul-scoped DB-tilstand og koerer i definitionsraekkefoelge.
Kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true (devcontainer).
"""

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.common.dato_dimension import DATO_BLANK_KEY
from DSA.SQL.dim.dim_Forloebsmarkoer import dim_Forloebsmarkoer
from DSA.SQL.dim.dim_UdredningsretEgenskaber import dim_UdredningsretEgenskaber
from DSA.SQL.dim.dim_UdredningsretKlassifikation import dim_UdredningsretKlassifikation
from DSA.SQL.fct.fct_Udredningsforloeb import fct_Udredningsforloeb
from DSA.SQL.fct.fct_UdredningsretMarkoer import fct_UdredningsretMarkoer
from DSA.SQL.map.map_Forloebsmarkoer import map_Forloebsmarkoer
from DSA.SQL.map.map_Helbredsforloeb import map_Helbredsforloeb
from DSA.SQL.map.map_Udredningsforloeb import map_Udredningsforloeb
from DSA.SQL.map.map_UdredningsretKlassifikation import map_UdredningsretKlassifikation
from DSA.SQL.map.map_UdredningsretMarkoer import map_UdredningsretMarkoer
from DSA.SQL.staging.udredningsret_kolonner import (
    AARSAG_TEKSTER,
    DIM_EGENSKABER_DWH_TABLE,
    DIM_EGENSKABER_TABLE,
    DIM_FORLOEBSMARKOER_DWH_TABLE,
    DIM_KLASSIFIKATION_DWH_TABLE,
    FCT_FORLOEB_DWH_TABLE,
    FCT_FORLOEB_TABLE,
    FCT_MARKOER_DWH_TABLE,
    FCT_MARKOER_TABLE,
    FORLOEB_PREFIX,
    INDEN_30_TEKSTER,
    MAP_FORLOEB_TABLE,
    MAP_FORLOEBSMARKOER_TABLE,
    MAP_HF_TABLE,
    MAP_KLASSIFIKATION_TABLE,
    MAP_MARKOER_TABLE,
    MARKOER_PREFIX,
    OEVRIGE_GRUPPE,
    OEVRIGE_GRUPPE_TEKST,
    OVERHOLDT_TEKSTER,
    UKENDT_TEKST,
)

# Genbruger staging-testens fixtures og hjaelpere (og, gennem dem,
# ingest-testens leverancebyggere), saa arkivet er bygget af den rigtige
# extract+archive og noeglerne er dets egne.
from tests.integration.test_udredningsret_staging_e2e import (  # noqa: F401
    A_FILE,
    B_FILE,
    CPR_W,
    CPR_X,
    CPR_Y,
    CPR_Z,
    ELEM_1,
    ELEM_2,
    ELEM_4,
    KODE_UDEN_SKS,
    OBJ,
    OBJ_F_REISSUED,
    OBJ_UDEN_FID,
    build_delivery,
    delivery_dir,
    hf_noegle,
    koer_kaeden,
    kolonner,
    markoer,
    raekker,
    require_real_environment,
    rows_a_staging,
    rows_b_staging,
    run_archive,
    run_extract,
    run_gate,
    run_sql,
    saet_downstream_checkpoints,
    scalar,
    staging_db,
    udred_db,
    vent_paa_nyt_db_sekund,
)

pytestmark = pytest.mark.integration

# Ekstra syntetisk helbredsforloeb oven paa staging-testens leverancer: én
# markoer UDEN F_ID med en kode uden for SDS' bilag 1, saa 'oevrige'-grenen og
# 'Ukendt'-teksten i dim.Forloebsmarkoer faktisk bliver ramt. Uden F_ID danner
# den intet forloeb og roerer derfor hverken facten eller kontroltotalen.
ELEM_7 = "e7e7e7e7-0000-0000-0000-000000000007"
OBJ_OEVRIGE = "ffffffff-0000-0000-0000-000000000106"
KODE_OEVRIGE = "AZZ99X9"

FORLOEBSLABEL = "ALAL90"  # markoer() saetter FORLOEBSLABEL paa hver raekke
FORLOEBSLABEL_TEKST = "Almen udredning"
FORLOEBSLABEL_REVIDERET = "Almen udredning (revideret)"

# (tabel, noeglekolonne) — noeglen er None for facterne, der ingen sentinel har
DWH_TABELLER = (
    (FCT_FORLOEB_DWH_TABLE, None),
    (FCT_MARKOER_DWH_TABLE, None),
    (DIM_EGENSKABER_DWH_TABLE, "dw_ek_Udredningsforloeb"),
    (DIM_KLASSIFIKATION_DWH_TABLE, "dw_ek_UdredningsretKlassifikation"),
    (DIM_FORLOEBSMARKOER_DWH_TABLE, "dw_ek_Forloebsmarkoer"),
    (MAP_FORLOEB_TABLE, "dw_ek_Udredningsforloeb"),
    (MAP_KLASSIFIKATION_TABLE, "dw_ek_UdredningsretKlassifikation"),
    (MAP_FORLOEBSMARKOER_TABLE, "dw_ek_Forloebsmarkoer"),
    (MAP_MARKOER_TABLE, "dw_ek_UdredningsretMarkoer"),
    (MAP_HF_TABLE, "dw_ek_Helbredsforloeb"),
)

TILSTAND: dict[str, object] = {}


# ---------------------------------------------------------------------------
# Leverancer og hjaelpere
# ---------------------------------------------------------------------------


def _oevrig_markoer() -> list[list[str]]:
    return [markoer("600", ELEM_7, OBJ_OEVRIGE, "", "", CPR_W, KODE_OEVRIGE, "2026-07-01")]


def rows_a_dwh() -> list[list[str]]:
    return rows_a_staging() + _oevrig_markoer()


def rows_b_dwh() -> list[list[str]]:
    return rows_b_staging() + _oevrig_markoer()


def md(result: dg.MaterializeResult, key: str):
    vaerdi = result.metadata[key]
    return vaerdi.value if hasattr(vaerdi, "value") else vaerdi


def koer_dwh(resource, mode: str = "delta") -> dict[str, dg.MaterializeResult]:
    """Materialiser map -> dim -> fct oven paa den stagede batch."""
    cfg = RunModeConfig(mode=mode)
    resultater = {}
    for navn, fn in (
        ("map_Helbredsforloeb", map_Helbredsforloeb),
        ("map_Udredningsforloeb", map_Udredningsforloeb),
        ("map_UdredningsretKlassifikation", map_UdredningsretKlassifikation),
        ("map_Forloebsmarkoer", map_Forloebsmarkoer),
        ("map_UdredningsretMarkoer", map_UdredningsretMarkoer),
        ("dim_UdredningsretEgenskaber", dim_UdredningsretEgenskaber),
        ("dim_UdredningsretKlassifikation", dim_UdredningsretKlassifikation),
        ("dim_Forloebsmarkoer", dim_Forloebsmarkoer),
        ("fct_Udredningsforloeb", fct_Udredningsforloeb),
        # markoerfacten sidst: dens taellerkontrol laeser fct.Udredningsforloeb
        # og er derfor kun meningsfuld, naar forloebsfacten er bygget i samme
        # koersel (metadata, aldrig en haard fejl). Raekkefoelgen her er den
        # samme, som asset-grafen paalaegger i jobbet — markoerfacten har en dep
        # paa forloebsfacten netop af den grund
        ("fct_UdredningsretMarkoer", fct_UdredningsretMarkoer),
    ):
        resultater[navn] = fn(dg.build_asset_context(), resource, cfg)
    return resultater


def arc_watermark(resource) -> str:
    return scalar(
        resource,
        "SELECT CONVERT(NVARCHAR(50), MAX(dw_timestamp_update), 121) FROM arc.udredningsret_mont",
    )


def soft_delete_markoer(resource, markoer_uuid: str, slettet: int, roer_tidsstempel: bool) -> None:
    """Saet dw_is_deleted paa én arkivraekke, som archive_merge ville goere det."""
    stempel = ", dw_timestamp_update = SYSDATETIME()" if roer_tidsstempel else ""
    run_sql(
        resource,
        f"UPDATE arc.udredningsret_mont SET dw_is_deleted = ?{stempel} "
        "WHERE LOWER(OBJEKTID_FORLOBSMARKOR) = ?",
        slettet,
        markoer_uuid.lower(),
    )


def fact_raekke(resource, clean_code: str, kolonner_sql: str) -> tuple:
    raekke = raekker(
        resource,
        f"SELECT {kolonner_sql} FROM {FCT_FORLOEB_DWH_TABLE} "
        "WHERE [clean_code_Udredningsforloeb] = ?",
        clean_code,
    )
    assert len(raekke) == 1, f"{len(raekke)} fact-raekker for {clean_code[:20]}..."
    return raekke[0]


def klassifikation(resource, dw_ek: int) -> dict[str, str]:
    navne = (
        "overholdt",
        "overholdt_tekst",
        "aarsag",
        "aarsag_tekst",
        "inden_30_dage",
        "inden_30_dage_tekst",
        "psykiatri_tekst",
        "hjerteomraade_tekst",
        "forloebslabel",
        "forloebslabel_tekst",
        "bopaelsregion_forloeb_tekst",
        "speciale",
    )
    vaerdier = raekker(
        resource,
        f"SELECT {', '.join(f'[{n}]' for n in navne)} FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
        "WHERE [dw_ek_UdredningsretKlassifikation] = ?",
        dw_ek,
    )[0]
    return dict(zip(navne, vaerdier, strict=True))


def levende_fact_raekker(resource) -> int:
    return scalar(
        resource, f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0"
    )


def arc_forloebsgrupper(resource) -> int:
    """Antal (helbredsforloeb_noegle, F_ID)-par paa levende arkivraekker med F_ID —
    factens korn, opgjort af testen selv.

    Asset'ets kontroltotal-metadata er en literal nul, der kun naas, naar
    kontrollen holder (ellers er der kastet dg.Failure), saa en assertion paa den
    kan ikke fejle paa nogen datatilstand. Dette opgoer kan: det er skrevet
    uafhaengigt af KONTROLTOTAL_SQL og holder facten op mod arkivet direkte.
    """
    return scalar(
        resource,
        "SELECT COUNT(*) FROM ("
        "SELECT [helbredsforloeb_noegle], LTRIM(RTRIM([F_ID])) AS [f_id] "
        "FROM arc.udredningsret_mont "
        "WHERE [dw_is_deleted] = 0 "
        "  AND [F_ID] IS NOT NULL AND LTRIM(RTRIM([F_ID])) <> '' "
        "GROUP BY [helbredsforloeb_noegle], LTRIM(RTRIM([F_ID]))) AS g",
    )


def hf1_clean_code(resource) -> str:
    return f"{FORLOEB_PREFIX}{hf_noegle(resource, ELEM_1)}_20260105_1"


# --- markoerfacten (US-5) ------------------------------------------------


def markoer_fact_raekke(resource, markoer_uuid: str, kolonner_sql: str) -> tuple:
    """Den ENE fact-raekke, der baerer dette markoer-UUID."""
    fundne = raekker(
        resource,
        f"SELECT {kolonner_sql} FROM {FCT_MARKOER_DWH_TABLE} WHERE [markoer_uuid] = ?",
        markoer_uuid.lower(),
    )
    assert len(fundne) == 1, f"{len(fundne)} markoer-fact-raekker for {markoer_uuid}"
    return fundne[0]


def markoer_fact_opgoerelse(resource) -> tuple[int, int, int]:
    """(alle raekker, levende raekker, kilderaekke-sum over de levende)."""
    return raekker(
        resource,
        "SELECT COUNT(*), "
        "SUM(CASE WHEN [dw_is_deleted] = 0 THEN 1 ELSE 0 END), "
        "ISNULL(SUM(CASE WHEN [dw_is_deleted] = 0 "
        "THEN CAST([antal_kilderaekker] AS BIGINT) END), 0) "
        f"FROM {FCT_MARKOER_DWH_TABLE}",
    )[0]


def arc_levende_markoerer(resource) -> tuple[int, int]:
    """(levende arkivraekker, deres kilderaekke-sum) — markoerfactens korn,
    opgjort af testen selv.

    Asset'ets kontroltotal-metadata er en literal nul, der kun naas, naar
    kontrollen holder (ellers er der kastet dg.Failure), saa en assertion paa
    den kan ikke fejle paa nogen datatilstand. Dette opgoer kan: det er skrevet
    uafhaengigt af KONTROLTOTAL_SQL og holder facten op mod arkivet direkte.
    """
    return raekker(
        resource,
        "SELECT COUNT(*), ISNULL(SUM(CAST(antal_kilderaekker AS BIGINT)), 0) "
        "FROM arc.udredningsret_mont WHERE dw_is_deleted = 0",
    )[0]


def markoerer_med_forloeb(resource) -> int:
    return scalar(
        resource,
        f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} "
        "WHERE [dw_is_deleted] = 0 AND [dw_ek_Udredningsforloeb] <> -1",
    )


def forloebsfactens_markoertaeller(resource) -> int:
    return scalar(
        resource,
        f"SELECT ISNULL(SUM(CAST([antal_markoerer] AS BIGINT)), 0) "
        f"FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0",
    )


def markoerer_uden_levende_forloeb(resource) -> int:
    """Levende markoerraekker med et forloeb, der IKKE findes som levende
    fact-raekke — '0 foraeldreloese' opgjort mod facten selv."""
    return scalar(
        resource,
        f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} m "
        f"LEFT JOIN {FCT_FORLOEB_DWH_TABLE} f "
        "ON f.[dw_ek_Udredningsforloeb] = m.[dw_ek_Udredningsforloeb] "
        "AND f.[dw_is_deleted] = 0 "
        "WHERE m.[dw_is_deleted] = 0 AND m.[dw_ek_Udredningsforloeb] <> -1 "
        "AND f.[dw_ek_Udredningsforloeb] IS NULL",
    )


# ---------------------------------------------------------------------------
# Fixture: tomt dwh-lag (sentinelerne bevares) + SKS-tekst for forloebslabel
# ---------------------------------------------------------------------------


def _ryd_dwh_laget(res) -> None:
    """Toem map/dim/fct; -1-sentinelerne fra 05_seeds.sql bevares."""
    for tabel, noegle in DWH_TABELLER:
        if noegle is None:
            run_sql(res, f"DELETE FROM {tabel}")
        else:
            run_sql(res, f"DELETE FROM {tabel} WHERE [{noegle}] <> -1")


@pytest.fixture(scope="module")
def dwh_db(staging_db):  # noqa: F811
    """Toem map/dim/fct og seed ALAL90 i SKS-kataloget — og ryd op igen bagefter.

    Oprydningen er ikke kosmetik: map.Helbredsforloeb har en unik clean_code, og
    de oevrige udredningsret-moduler bygger deres arkiv af de SAMME syntetiske
    leverancer og faar derfor de samme helbredsforloebsnoegler. Uden oprydningen
    ville staging-testens borger-genopretning (test 10) kollidere med en noegle,
    dette modul havde mintet."""
    res = staging_db
    _ryd_dwh_laget(res)
    run_sql(res, "DELETE FROM [codesystem].[SKSConcept] WHERE [code] = ?", FORLOEBSLABEL)
    run_sql(
        res,
        "INSERT INTO [codesystem].[SKSConcept] ([code],[display],[dw_timestamp_insert],"
        "[dw_timestamp_update]) VALUES (?, ?, SYSDATETIME(), SYSDATETIME())",
        FORLOEBSLABEL,
        FORLOEBSLABEL_TEKST,
    )

    yield res

    _ryd_dwh_laget(res)
    run_sql(res, "DELETE FROM [codesystem].[SKSConcept] WHERE [code] = ?", FORLOEBSLABEL)


# ---------------------------------------------------------------------------
# 1. Fuld indlaesning: delt PK, klassifikationstekster, kodedimension, kontroltotal
# ---------------------------------------------------------------------------


def test_1_fuld_indlaesning_bygger_facten_og_dimensionerne(
    dwh_db,
    delivery_dir,  # noqa: F811
):
    build_delivery(delivery_dir, A_FILE, "20260830", "2026-08-30", rows_a_dwh())
    assert run_extract(dwh_db, delivery_dir).metadata["besluttet"] == "indlaest"
    run_archive(dwh_db)
    run_gate(dwh_db)
    koer_kaeden(dwh_db, mode="full_load")
    resultater = koer_dwh(dwh_db, mode="full_load")

    stagede_forloeb = scalar(dwh_db, f"SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE}")
    assert stagede_forloeb == 6
    assert scalar(dwh_db, f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE}") == stagede_forloeb
    assert md(resultater["fct_Udredningsforloeb"], "rows_affected") == stagede_forloeb
    assert md(resultater["fct_Udredningsforloeb"], "retireret") == 0
    # kontroltotalen holder: asset'et fejler ved afvigelse, saa metadataens nul er
    # en konstant — testen taeller derfor selv arkivets forloebsgrupper
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == stagede_forloeb
    assert md(resultater["fct_Udredningsforloeb"], "indikatorforloeb") == 1
    assert (
        scalar(
            dwh_db, f"SELECT SUM(CAST([indgaar_i_indikator] AS INT)) FROM {FCT_FORLOEB_DWH_TABLE}"
        )
        == 1
    )

    # delt PK: praecis én egenskabsraekke pr. fact-raekke, og ingen forloebsnoegle
    # uden dimension
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} f JOIN {DIM_EGENSKABER_DWH_TABLE} d "
            "ON d.[dw_ek_Udredningsforloeb] = f.[dw_ek_Udredningsforloeb]",
        )
        == stagede_forloeb
    )
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} f LEFT JOIN {DIM_EGENSKABER_DWH_TABLE} d "
            "ON d.[dw_ek_Udredningsforloeb] = f.[dw_ek_Udredningsforloeb] "
            "WHERE d.[dw_ek_Udredningsforloeb] IS NULL",
        )
        == 0
    )
    # ingen FK peger paa et medlem, der ikke findes
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} f "
            f"LEFT JOIN {DIM_KLASSIFIKATION_DWH_TABLE} k "
            "ON k.[dw_ek_UdredningsretKlassifikation] = f.[dw_ek_UdredningsretKlassifikation] "
            "WHERE k.[dw_ek_UdredningsretKlassifikation] IS NULL",
        )
        == 0
    )

    clean_code = hf1_clean_code(dwh_db)
    TILSTAND["hf1_clean_code"] = clean_code
    (
        dw_ek,
        dw_ek_hf,
        dw_ek_borger,
        dw_ek_klass,
        dato_start,
        dato_slut,
        dato_opg,
        udredningstid,
        indikator,
        overholdt,
        inden_30,
        alder,
        antal_markoerer,
        antal_planer,
        slettet,
    ) = fact_raekke(
        dwh_db,
        clean_code,
        "[dw_ek_Udredningsforloeb], [dw_ek_Helbredsforloeb], [dw_ek_Borger], "
        "[dw_ek_UdredningsretKlassifikation], [dw_ek_Dato_start], [dw_ek_Dato_slut], "
        "[dw_ek_Dato_opgoerelse], [udredningstid_dage], [indgaar_i_indikator], [overholdt], "
        "[inden_30_dage], [alder_ved_henvisning], [antal_markoerer], [antal_udredningsplaner], "
        "[dw_is_deleted]",
    )
    assert dw_ek > 0 and dw_ek_hf > 0 and dw_ek_klass > 0
    assert dw_ek_borger > 0  # CPR_X er seedet i dim.Borger
    assert (dato_start, dato_slut, dato_opg) == (20260105, 20260201, 20260101)
    assert (udredningstid, indikator, overholdt, inden_30, alder) == (27, True, True, True, 52)
    assert (antal_markoerer, antal_planer, slettet) == (5, 2, False)
    TILSTAND["hf1_dw_ek"] = dw_ek
    TILSTAND["hf1_klass_a"] = dw_ek_klass

    # klassifikationen: HF1's udfyldte flag mod de fem oevrige forloebs blanke
    medlemmer = scalar(
        dwh_db,
        f"SELECT COUNT(*) FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
        "WHERE [dw_ek_UdredningsretKlassifikation] <> -1",
    )
    assert medlemmer == 2, "ét medlem pr. attributkombination"
    hf1_klass = klassifikation(dwh_db, dw_ek_klass)
    assert hf1_klass["overholdt"] == "1"
    assert hf1_klass["overholdt_tekst"] == OVERHOLDT_TEKSTER["1"]
    assert hf1_klass["aarsag_tekst"] == AARSAG_TEKSTER["1"]
    assert hf1_klass["inden_30_dage_tekst"] == INDEN_30_TEKSTER["1"]
    # blanke vaerdier uden for indikatoren: 'Ukendt', ikke tom celle
    assert hf1_klass["psykiatri_tekst"] == UKENDT_TEKST
    assert hf1_klass["hjerteomraade_tekst"] == UKENDT_TEKST
    assert hf1_klass["bopaelsregion_forloeb_tekst"] == UKENDT_TEKST
    # forloebslabel_tekst kommer fra SKS-kataloget, ikke fra en dict
    assert (hf1_klass["forloebslabel"], hf1_klass["forloebslabel_tekst"]) == (
        FORLOEBSLABEL,
        FORLOEBSLABEL_TEKST,
    )
    assert (
        md(resultater["dim_UdredningsretKlassifikation"], "members_with_forloebslabel_tekst") == 2
    )

    # HF2 har ingen slutmarkoer og ingen indikatorflag -> 'Ikke i opgørelsen'
    hf2_klass_ek = fact_raekke(
        dwh_db,
        f"{FORLOEB_PREFIX}{hf_noegle(dwh_db, ELEM_2)}_20260301_1",
        "[dw_ek_UdredningsretKlassifikation]",
    )[0]
    hf2_klass = klassifikation(dwh_db, hf2_klass_ek)
    assert hf2_klass["overholdt"] is None
    assert hf2_klass["overholdt_tekst"] == OVERHOLDT_TEKSTER[""] == "Ikke i opgørelsen"
    assert hf2_klass["aarsag_tekst"] == AARSAG_TEKSTER[""]
    assert hf2_klass["inden_30_dage_tekst"] == INDEN_30_TEKSTER[""]
    assert hf2_klass_ek != dw_ek_klass
    TILSTAND["blank_klass"] = hf2_klass_ek

    # HF2's forloeb er aabent: BLANK-sentinel og NULL-maal (U2)
    assert fact_raekke(
        dwh_db,
        f"{FORLOEB_PREFIX}{hf_noegle(dwh_db, ELEM_2)}_20260301_1",
        "[dw_ek_Dato_slut], [dw_ek_Dato_opgoerelse], [udredningstid_dage], [overholdt]",
    ) == (DATO_BLANK_KEY, DATO_BLANK_KEY, None, None)

    # kodedimensionen: bilag 1-grupperne, SKS-teksten og 'oevrige'/'Ukendt'
    dim_koder = {
        r[0]: r[1:]
        for r in raekker(
            dwh_db,
            "SELECT [kode], [gruppe], [gruppe_tekst], [tekst], [tekst_kilde] "
            f"FROM {DIM_FORLOEBSMARKOER_DWH_TABLE} WHERE [dw_ek_Forloebsmarkoer] <> -1",
        )
    }
    assert set(dim_koder) == {
        "AFV01A1",
        "AFV01B",
        "AFX01C",
        "AFX01D4",
        "AFV01X2",
        KODE_UDEN_SKS,
        KODE_OEVRIGE,
    }
    assert dim_koder["AFV01A1"][:2] == ("henvist_udredning", "Henvist til udredning")
    assert dim_koder["AFV01A1"][2:] == ("Henvist til udredning", "SKS")
    # en kode uden for bilag 1 er et NORMALT medlem — aldrig en haard fejl (U7)
    assert dim_koder[KODE_OEVRIGE] == (
        OEVRIGE_GRUPPE,
        OEVRIGE_GRUPPE_TEKST,
        UKENDT_TEKST,
        "ukendt",
    )
    assert dim_koder[KODE_UDEN_SKS][2:] == (UKENDT_TEKST, "ukendt")

    # map'ene er mintet fra staging, og hver clean_code én gang
    assert (
        scalar(dwh_db, f"SELECT COUNT(*) FROM {MAP_HF_TABLE} WHERE [dw_ek_Helbredsforloeb] <> -1")
        == 6
    )
    assert md(resultater["map_Udredningsforloeb"], "new_rows") == 6
    assert md(resultater["map_UdredningsretKlassifikation"], "new_rows") == 2

    TILSTAND["resultater_a"] = resultater
    TILSTAND["cp_efter_a"] = arc_watermark(dwh_db)


# ---------------------------------------------------------------------------
# 1b. Markoerfacten efter fuld indlaesning (US-5)
# ---------------------------------------------------------------------------


def test_1b_markoerfacten_er_en_til_en_med_arkivet(dwh_db):
    """Markoerfactens korn ER arkivets mergenoegle, saa facten og arkivet er
    1:1 pr. konstruktion: lige mange levende raekker og samme
    SUM(antal_kilderaekker), som gendanner leverancens raa raekketal (b og c er
    fuldstaendigt identiske kilderaekker, arkivet har foldet sammen til én).

    De to markoerer uden F_ID (HF1's behandlingsstart og 'oevrige'-markoeren)
    staar paa sentinel -1: design, ikke et opslagsfald (U5). Hver markoer MED
    F_ID rammer derimod en levende forloebsraekke — '0 foraeldreloese'.
    """
    resultater = TILSTAND["resultater_a"]
    markoer = resultater["fct_UdredningsretMarkoer"]

    stagede_markoerer = scalar(dwh_db, f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}")
    assert stagede_markoerer == 14  # 15 kilderaekker -> 14 arkivraekker (b+c foldet)
    assert md(markoer, "staging_batch") == stagede_markoerer
    # bulk-stien (tomt maal) skal have indsat HELE batchen
    assert md(markoer, "rows_affected") == stagede_markoerer

    alle, levende, kilderaekker = markoer_fact_opgoerelse(dwh_db)
    assert (alle, levende) == (stagede_markoerer, stagede_markoerer)
    # kontroltotalen holder: asset'et fejler ved afvigelse, saa metadataens nul
    # er en konstant — testen opgoer derfor selv arkivets levende skive
    assert (levende, kilderaekker) == arc_levende_markoerer(dwh_db) == (14, 15)
    assert md(markoer, "total_live_rows") == levende
    assert md(markoer, "sum_antal_kilderaekker") == kilderaekker

    # map'en minter én noegle pr. staget markoer, og hver fact-raekke har sin
    assert md(resultater["map_UdredningsretMarkoer"], "new_rows") == stagede_markoerer
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} f LEFT JOIN {MAP_MARKOER_TABLE} m "
            "ON m.[clean_code_UdredningsretMarkoer] = f.[clean_code_UdredningsretMarkoer] "
            "WHERE m.[dw_ek_UdredningsretMarkoer] IS NULL",
        )
        == 0
    )
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} "
            f"WHERE [clean_code_UdredningsretMarkoer] NOT LIKE '{MARKOER_PREFIX}%'",
        )
        == 0
    )

    # sentinel-stien: praecis de to markoerer uden F_ID
    assert md(markoer, "markoerer_uden_forloeb") == 2
    for uuid in (OBJ_UDEN_FID, OBJ_OEVRIGE):
        dw_ek_forloeb, dw_ek_markoerkode, dw_ek_hf = markoer_fact_raekke(
            dwh_db,
            uuid,
            "[dw_ek_Udredningsforloeb], [dw_ek_Forloebsmarkoer], [dw_ek_Helbredsforloeb]",
        )
        assert dw_ek_forloeb == -1, uuid
        # markoerkoden og helbredsforloebet er derimod oploest: kun forloebet
        # mangler, og kun fordi markoeren ikke har noget F_ID
        assert dw_ek_markoerkode > 0, uuid
        assert dw_ek_hf > 0, uuid
    # ingen markoerkode er blank i leverancerne, saa den rolle har ingen -1
    assert md(markoer, "ukendt_dw_ek_Forloebsmarkoer") == 0

    # 0 foraeldreloese: hver markoer med et forloeb rammer en levende fact-raekke
    assert markoerer_uden_levende_forloeb(dwh_db) == 0
    assert markoerer_med_forloeb(dwh_db) == levende - 2 == 12

    # taellerkontrollen (U6): forloebsfactens antal_markoerer er talt i pre_stg
    # direkte fra arkivet — enighed viser, at de to laesninger af 'et forloebs
    # markoerer' beskriver de samme raekker
    assert forloebsfactens_markoertaeller(dwh_db) == markoerer_med_forloeb(dwh_db) == 12
    assert md(markoer, "antal_markoerer_forloebsfact") == 12
    assert md(markoer, "markoerer_med_forloeb") == 12
    assert md(markoer, "taeller_afvigelse") == 0

    # HF1's slutmarkoer: indikatorflaget og de degenererede egenskaber baeres
    # igennem, mens koden og datoen kun findes som noegler
    TILSTAND["slut_dw_ek"], indikator_h, forloebselement = markoer_fact_raekke(
        dwh_db,
        OBJ["d"],
        "[dw_ek_UdredningsretMarkoer], [indgaar_i_indikator_H], [forloebselement_uuid]",
    )
    assert indikator_h is True
    assert forloebselement == ELEM_1
    TILSTAND["plan_dw_ek"] = markoer_fact_raekke(dwh_db, OBJ["f"], "[dw_ek_UdredningsretMarkoer]")[
        0
    ]
    TILSTAND["afbrudt_dw_ek"] = markoer_fact_raekke(
        dwh_db, OBJ["i"], "[dw_ek_UdredningsretMarkoer]"
    )[0]
    TILSTAND["markoer_alle_a"] = alle


# ---------------------------------------------------------------------------
# 2. Delta efter leverance B: noeglestabilitet og Type 1-overskrivning
# ---------------------------------------------------------------------------


def test_2_delta_efter_leverance_b_holder_noeglen_og_overskriver_attributterne(
    dwh_db,
    delivery_dir,  # noqa: F811
):
    cp = TILSTAND["cp_efter_a"]
    vent_paa_nyt_db_sekund(dwh_db, cp)
    build_delivery(delivery_dir, B_FILE, "20260906", "2026-09-06", rows_b_dwh())
    assert run_extract(dwh_db, delivery_dir).metadata["kandidater"] == 1
    assert run_archive(dwh_db).metadata["rows_soft_deleted"] == 1
    run_gate(dwh_db)

    saet_downstream_checkpoints(dwh_db, cp)
    koer_kaeden(dwh_db, mode="delta")
    resultater = koer_dwh(dwh_db)

    clean_code = TILSTAND["hf1_clean_code"]
    assert hf1_clean_code(dwh_db) == clean_code, "clean_code'en aendrede sig ved omnummereringen"
    dw_ek, dw_ek_klass, antal_markoerer, antal_planer, alder = fact_raekke(
        dwh_db,
        clean_code,
        "[dw_ek_Udredningsforloeb], [dw_ek_UdredningsretKlassifikation], [antal_markoerer], "
        "[antal_udredningsplaner], [alder_ved_henvisning]",
    )
    assert dw_ek == TILSTAND["hf1_dw_ek"], "dw_ek skal vaere stabil paa tvaers af leverancer"
    # leverance B's genudstedte slutmarkoer baerer hverken ALDER eller MRK_ST_*,
    # saa maalene opdateres in place og klassifikationen falder til blank-medlemmet
    assert (antal_markoerer, antal_planer, alder) == (6, 3, None)
    assert dw_ek_klass == TILSTAND["blank_klass"]

    # Type 1: egenskabsdimensionen baerer SDS' nye F_ID paa den SAMME noegle
    assert raekker(
        dwh_db,
        f"SELECT [f_id], [clean_code_Udredningsforloeb] FROM {DIM_EGENSKABER_DWH_TABLE} "
        "WHERE [dw_ek_Udredningsforloeb] = ?",
        dw_ek,
    ) == [("9001", clean_code)]

    # facten voksede ikke: de tre scopede helbredsforloeb blev opdateret in place
    assert scalar(dwh_db, f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE}") == 6
    assert levende_fact_raekker(dwh_db) == 6
    assert md(resultater["fct_Udredningsforloeb"], "retireret") == 0
    assert md(resultater["fct_Udredningsforloeb"], "total_live_rows") == 6
    assert arc_forloebsgrupper(dwh_db) == 6

    # dimensioner retirerer aldrig: AFV01X2 forsvandt fra staging, men bliver staaende
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {DIM_FORLOEBSMARKOER_DWH_TABLE} WHERE [kode] = 'AFV01X2'",
        )
        == 1
    )
    assert (
        scalar(
            dwh_db,
            f"SELECT [gruppe] FROM {DIM_FORLOEBSMARKOER_DWH_TABLE} WHERE [kode] = 'AFX01D1'",
        )
        == "udredningsplan"
    )
    TILSTAND["resultater_b"] = resultater
    TILSTAND["cp_efter_b"] = arc_watermark(dwh_db)


# ---------------------------------------------------------------------------
# 2b. Genudstedt OBJEKTID og retireret markoer (US-5)
# ---------------------------------------------------------------------------


def test_2b_genudstedt_objektid_opdaterer_markoeren_uden_ny_raekke(dwh_db):
    """clean_code_UdredningsretMarkoer er bygget paa markoer_noegle =
    SHA2_256(helbredsforloeb, kode, dato) + arkivets loebenummer — SDS' eget
    OBJEKTID_FORLOBSMARKOR indgaar IKKE. Leverance B genudsteder to markoerer
    (slutmarkoeren d -> j og den ene af de to enslydende udredningsplaner
    f -> OBJ_F_REISSUED, hvis nye UUID sorterer FOERST), og begge skal derfor
    opdatere markoer_uuid paa deres EGEN raekke i stedet for at minte en ny.

    Samtidig forsvandt HF3's afbrydelse fra leverancen: arkivet soft-slettede
    den, og markoerfacten retirerer den paa den samme raekke — uden en
    retireringsrutine, alene fordi MERGE'n skriver dw_is_deleted fra kilden.
    """
    markoer = TILSTAND["resultater_b"]["fct_UdredningsretMarkoer"]

    for gammel, ny, gemt in (
        (OBJ["d"], OBJ["j"], "slut_dw_ek"),
        (OBJ["f"], OBJ_F_REISSUED, "plan_dw_ek"),
    ):
        assert (
            scalar(
                dwh_db,
                f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} WHERE [markoer_uuid] = ?",
                gammel.lower(),
            )
            == 0
        ), f"{gammel} lever stadig: det genudstedte UUID mintede en ny raekke"
        assert markoer_fact_raekke(dwh_db, ny, "[dw_ek_UdredningsretMarkoer]") == (
            TILSTAND[gemt],
        ), ny

    # kun den nye udredningsplan (AFX01D1) er kommet til — de to genudstedte
    # markoerer voksede ikke facten
    alle, levende, kilderaekker = markoer_fact_opgoerelse(dwh_db)
    assert alle == TILSTAND["markoer_alle_a"] + 1 == 15

    # den droppede markoer er retireret, ikke slettet, og paa sin egen noegle
    assert markoer_fact_raekke(
        dwh_db, OBJ["i"], "[dw_ek_UdredningsretMarkoer], [dw_is_deleted]"
    ) == (TILSTAND["afbrudt_dw_ek"], True)

    assert (levende, kilderaekker) == arc_levende_markoerer(dwh_db) == (14, 15)
    assert md(markoer, "total_live_rows") == levende

    # HF1's forloeb voksede med den nye plan: markoerfacten og forloebsfactens
    # egen taeller er enige om praecis hvilke raekker, der hoerer til forloebet
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} "
            "WHERE [dw_is_deleted] = 0 AND [dw_ek_Udredningsforloeb] = ?",
            TILSTAND["hf1_dw_ek"],
        )
        == 6
    )
    assert markoerer_uden_levende_forloeb(dwh_db) == 0
    assert forloebsfactens_markoertaeller(dwh_db) == markoerer_med_forloeb(dwh_db) == 12
    assert md(markoer, "taeller_afvigelse") == 0
    assert md(markoer, "markoerer_uden_forloeb") == 2


# ---------------------------------------------------------------------------
# 3. Aendret indikatorattribut: nyt klassifikationsmedlem, samme forloebsnoegle
# ---------------------------------------------------------------------------


def test_3_aendret_indikatorattribut_giver_nyt_klassifikationsmedlem(dwh_db):
    """Klassifikationen er hashet over de otte attributter, saa en revideret
    MRK_ST-vaerdi minter et NYT medlem — facten skifter FK, men forloebets egen
    noegle roeres ikke."""
    cp = TILSTAND["cp_efter_b"]
    vent_paa_nyt_db_sekund(dwh_db, cp)
    run_sql(
        dwh_db,
        "UPDATE arc.udredningsret_mont SET MRK_ST_L = '0', MRK_ST_N = '5', MRK_ST_H = '0', "
        "dw_timestamp_update = SYSDATETIME() WHERE LOWER(OBJEKTID_FORLOBSMARKOR) = ?",
        OBJ["j"].lower(),
    )
    foer_medlemmer = scalar(
        dwh_db,
        f"SELECT COUNT(*) FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
        "WHERE [dw_ek_UdredningsretKlassifikation] <> -1",
    )

    saet_downstream_checkpoints(dwh_db, cp)
    koer_kaeden(dwh_db, mode="delta")
    resultater = koer_dwh(dwh_db)

    assert md(resultater["map_UdredningsretKlassifikation"], "new_rows") == 1
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
            "WHERE [dw_ek_UdredningsretKlassifikation] <> -1",
        )
        == foer_medlemmer + 1
    )
    dw_ek, dw_ek_klass = fact_raekke(
        dwh_db,
        TILSTAND["hf1_clean_code"],
        "[dw_ek_Udredningsforloeb], [dw_ek_UdredningsretKlassifikation]",
    )
    assert dw_ek == TILSTAND["hf1_dw_ek"], "forloebsnoeglen maa ikke roeres af en attributaendring"
    assert dw_ek_klass not in (TILSTAND["hf1_klass_a"], TILSTAND["blank_klass"])
    nyt = klassifikation(dwh_db, dw_ek_klass)
    assert (nyt["overholdt"], nyt["aarsag"], nyt["inden_30_dage"]) == ("0", "5", "0")
    assert nyt["overholdt_tekst"] == OVERHOLDT_TEKSTER["0"]
    assert nyt["aarsag_tekst"] == AARSAG_TEKSTER["5"]
    assert nyt["inden_30_dage_tekst"] == INDEN_30_TEKSTER["0"]
    # gamle medlemmer bliver staaende — dimensioner retirerer ikke
    assert klassifikation(dwh_db, TILSTAND["hf1_klass_a"])["overholdt"] == "1"
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == 6
    TILSTAND["cp_efter_attribut"] = arc_watermark(dwh_db)


# ---------------------------------------------------------------------------
# 4. Retirering og genoplivning (spec afsnit 6)
# ---------------------------------------------------------------------------


def test_4_forsvundet_forloeb_retireres_og_genoplives(dwh_db):
    """HF4's eneste markoer soft-slettes i arkivet: helbredsforloebet kommer i
    scope uden forloeb, og fact-raekken markeres dw_is_deleted = 1 i stedet for
    at blive slettet. Kommer arkivraekken tilbage, genopliver MERGE'n raekken."""
    cp = TILSTAND["cp_efter_attribut"]
    hf4 = hf_noegle(dwh_db, ELEM_4)
    hf4_clean_code = f"{FORLOEB_PREFIX}{hf4}_20260501_1"
    assert fact_raekke(dwh_db, hf4_clean_code, "[dw_is_deleted]") == (False,)

    vent_paa_nyt_db_sekund(dwh_db, cp)
    run_sql(
        dwh_db,
        "UPDATE arc.udredningsret_mont SET dw_is_deleted = 1, dw_timestamp_update = SYSDATETIME() "
        "WHERE helbredsforloeb_noegle = ?",
        hf4,
    )
    saet_downstream_checkpoints(dwh_db, cp)
    koer_kaeden(dwh_db, mode="delta")
    # HF4 er i scope (arkivraekken blev roert), men danner intet forloeb laengere;
    # de oevrige scopede helbredsforloeb (borger-genopretningen, U9) gør det
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} " "WHERE [clean_code_Udredningsforloeb] = ?",
            hf4_clean_code,
        )
        == 0
    )
    resultater = koer_dwh(dwh_db)

    assert md(resultater["fct_Udredningsforloeb"], "retireret") == 1
    assert fact_raekke(dwh_db, hf4_clean_code, "[dw_is_deleted]") == (True,)
    # raekken er retireret, ikke slettet — og de oevrige forloeb er urørte
    assert scalar(dwh_db, f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE}") == 6
    assert levende_fact_raekker(dwh_db) == 5
    assert md(resultater["fct_Udredningsforloeb"], "total_live_rows") == 5
    # forloebet er ogsaa vaek i arkivet: retireringen fulgte kilden
    assert arc_forloebsgrupper(dwh_db) == 5

    # genoplivning: arkivraekken lever igen -> MERGE'n skriver dw_is_deleted = 0
    cp2 = arc_watermark(dwh_db)
    vent_paa_nyt_db_sekund(dwh_db, cp2)
    run_sql(
        dwh_db,
        "UPDATE arc.udredningsret_mont SET dw_is_deleted = 0, dw_timestamp_update = SYSDATETIME() "
        "WHERE helbredsforloeb_noegle = ?",
        hf4,
    )
    saet_downstream_checkpoints(dwh_db, cp2)
    koer_kaeden(dwh_db, mode="delta")
    resultater = koer_dwh(dwh_db)

    assert fact_raekke(dwh_db, hf4_clean_code, "[dw_is_deleted]") == (False,)
    assert md(resultater["fct_Udredningsforloeb"], "retireret") == 0
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == 6
    TILSTAND["cp_efter_genoplivning"] = arc_watermark(dwh_db)


# ---------------------------------------------------------------------------
# 5. Nyt opgoerelseskvartal
# ---------------------------------------------------------------------------


def test_5_nyt_opgoerelseskvartal_flytter_datonoeglen(dwh_db):
    """OPGPER '2K26' -> kvartalets foerste dag; uden OPGPER staar noeglen paa
    BLANK-sentinelen (U2), ikke paa UKENDT."""
    cp = TILSTAND["cp_efter_genoplivning"]
    clean_code = TILSTAND["hf1_clean_code"]
    assert fact_raekke(dwh_db, clean_code, "[dw_ek_Dato_opgoerelse]") == (DATO_BLANK_KEY,)

    vent_paa_nyt_db_sekund(dwh_db, cp)
    run_sql(
        dwh_db,
        "UPDATE arc.udredningsret_mont SET OPGPER = '2K26', dw_timestamp_update = SYSDATETIME() "
        "WHERE LOWER(OBJEKTID_FORLOBSMARKOR) = ?",
        OBJ["j"].lower(),
    )
    saet_downstream_checkpoints(dwh_db, cp)
    koer_kaeden(dwh_db, mode="delta")
    resultater = koer_dwh(dwh_db)

    dw_ek, dato_opg = fact_raekke(
        dwh_db, clean_code, "[dw_ek_Udredningsforloeb], [dw_ek_Dato_opgoerelse]"
    )
    assert dato_opg == 20260401
    assert dw_ek == TILSTAND["hf1_dw_ek"]
    assert (
        scalar(
            dwh_db,
            f"SELECT [opgoerelseskvartal] FROM {DIM_EGENSKABER_DWH_TABLE} "
            "WHERE [dw_ek_Udredningsforloeb] = ?",
            dw_ek,
        )
        == "2K26"
    )
    assert md(resultater["fct_Udredningsforloeb"], "total_live_rows") == 6
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == 6
    TILSTAND["cp_efter_kvartal"] = arc_watermark(dwh_db)


# ---------------------------------------------------------------------------
# 6. Global katalog-refresh af forloebslabel_tekst
# ---------------------------------------------------------------------------


def test_6_revideret_sks_tekst_naar_alle_medlemmer(dwh_db):
    """forloebslabel_tekst er katalog-sourced: en revision skal naa ogsaa de
    medlemmer, hvis forloeb IKKE er i den aktuelle delta-batch. Kun det sidste
    kvartals-delta staar i staging, saa uden den globale refresh ville de
    oevrige medlemmer beholde den gamle tekst."""
    medlemmer = scalar(
        dwh_db,
        f"SELECT COUNT(*) FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
        "WHERE [dw_ek_UdredningsretKlassifikation] <> -1",
    )
    assert medlemmer >= 3
    # leverance A's oprindelige medlem har ingen stagede forloeb laengere (HF1
    # skiftede medlem i test 3), saa MERGE'n kan IKKE naa det — kun den globale
    # refresh kan
    udenfor_batch = TILSTAND["hf1_klass_a"]
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {DIM_EGENSKABER_TABLE} s "
            f"JOIN {DIM_KLASSIFIKATION_DWH_TABLE} k ON k.[clean_code_UdredningsretKlassifikation] "
            "= s.[clean_code_UdredningsretKlassifikation] "
            "WHERE k.[dw_ek_UdredningsretKlassifikation] = ?",
            udenfor_batch,
        )
        == 0
    )
    assert klassifikation(dwh_db, udenfor_batch)["forloebslabel_tekst"] == FORLOEBSLABEL_TEKST
    run_sql(
        dwh_db,
        "UPDATE [codesystem].[SKSConcept] SET [display] = ? WHERE [code] = ?",
        FORLOEBSLABEL_REVIDERET,
        FORLOEBSLABEL,
    )

    resultat = dim_UdredningsretKlassifikation(dg.build_asset_context(), dwh_db, RunModeConfig())

    assert md(resultat, "forloebslabel_refreshed") >= 1
    assert (
        klassifikation(dwh_db, udenfor_batch)["forloebslabel_tekst"] == FORLOEBSLABEL_REVIDERET
    ), "et medlem uden for delta-batchen fik ikke den reviderede katalogtekst"
    assert (
        scalar(
            dwh_db,
            f"SELECT COUNT(*) FROM {DIM_KLASSIFIKATION_DWH_TABLE} "
            "WHERE [dw_ek_UdredningsretKlassifikation] <> -1 AND [forloebslabel_tekst] = ?",
            FORLOEBSLABEL_REVIDERET,
        )
        == medlemmer
    )
    # idempotent: en koersel mere roerer ingen raekker
    resultat = dim_UdredningsretKlassifikation(dg.build_asset_context(), dwh_db, RunModeConfig())
    assert md(resultat, "forloebslabel_refreshed") == 0

    run_sql(
        dwh_db,
        "UPDATE [codesystem].[SKSConcept] SET [display] = ? WHERE [code] = ?",
        FORLOEBSLABEL_TEKST,
        FORLOEBSLABEL,
    )
    dim_UdredningsretKlassifikation(dg.build_asset_context(), dwh_db, RunModeConfig())
    assert (
        klassifikation(dwh_db, TILSTAND["hf1_klass_a"])["forloebslabel_tekst"]
        == FORLOEBSLABEL_TEKST
    )


# ---------------------------------------------------------------------------
# 7. Kontroltotal-brud opdages
# ---------------------------------------------------------------------------


def test_7_kontroltotal_brud_fejler_koerslen(dwh_db):
    """Arkivet aendres UDEN gen-staging: facten er nu uenig med arkivet om,
    hvor mange levende forloeb der findes, og koerslen skal fejle."""
    markoer_uuid = scalar(
        dwh_db,
        "SELECT TOP 1 OBJEKTID_FORLOBSMARKOR FROM arc.udredningsret_mont "
        "WHERE dw_is_deleted = 0 AND F_ID = '5006'",
    )
    assert markoer_uuid
    soft_delete_markoer(dwh_db, markoer_uuid, slettet=1, roer_tidsstempel=False)
    try:
        with pytest.raises(dg.Failure, match="kontroltotalen holder ikke"):
            fct_Udredningsforloeb(dg.build_asset_context(), dwh_db, RunModeConfig())
    finally:
        # uden tidsstempel-bump: staging er stadig i sync med den gamle tilstand
        soft_delete_markoer(dwh_db, markoer_uuid, slettet=0, roer_tidsstempel=False)

    # genskabt konsistens -> asset'et koerer igennem igen
    resultat = fct_Udredningsforloeb(dg.build_asset_context(), dwh_db, RunModeConfig())
    assert md(resultat, "total_live_rows") == 6
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == 6


# ---------------------------------------------------------------------------
# 8. Grain-brud opdages
# ---------------------------------------------------------------------------


def test_8_grain_brud_fejler_koerslen(dwh_db):
    """To stg-raekker med samme clean_code skal faelde fact-koerslen. Stagingens
    eget unikke indeks forhindrer dubletten ad normal vej, saa testen fjerner
    vaernet for at bevise, at fact-laget har sit eget."""
    run_sql(dwh_db, f"DROP INDEX [IX_fct_Udredningsforloeb_PK] ON {FCT_FORLOEB_TABLE}")
    run_sql(dwh_db, f"INSERT INTO {FCT_FORLOEB_TABLE} SELECT TOP 1 * FROM {FCT_FORLOEB_TABLE}")
    try:
        with pytest.raises(ValueError, match="grain"):
            fct_Udredningsforloeb(dg.build_asset_context(), dwh_db, RunModeConfig())
    finally:
        run_sql(
            dwh_db,
            "WITH d AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY [clean_code_Udredningsforloeb] "
            f"ORDER BY (SELECT NULL)) AS rn FROM {FCT_FORLOEB_TABLE}) DELETE FROM d WHERE rn > 1",
        )
        run_sql(
            dwh_db,
            "CREATE UNIQUE CLUSTERED INDEX [IX_fct_Udredningsforloeb_PK] "
            f"ON {FCT_FORLOEB_TABLE}([clean_code_Udredningsforloeb])",
        )
    # facten er uroert af det fejlede forsoeg
    assert levende_fact_raekker(dwh_db) == 6


# ---------------------------------------------------------------------------
# 8b. Genoplivning af en retireret markoer (US-5)
# ---------------------------------------------------------------------------


def test_8b_retireret_markoer_genoplives_paa_samme_fact_raekke(dwh_db):
    """HF3's afbrydelse blev retireret af leverance B (test 2b). Kommer
    arkivraekken tilbage, skal den genoplive paa den SAMME fact-raekke.

    Det er map'ens ansvar: den minter noegler for levende OG soft-slettede
    stagede markoerer, saa den retirerede markoer beholder sit dw_ek.

    De to assertions beviser hver sit. Det uaendrede dw_ek viser, at raekken er
    den samme — ikke en ny, der lever ved siden af en evigt retireret. At
    map'en ikke minter en eneste ny noegle viser, at markoerens clean_code er
    STABIL paa tvaers af leverancer: drev det (fx fordi noglen kom til at
    indeholde en vaerdi, leverancen kan skifte), ville genoplivningen minte en
    ny noegle her. Den stroengere invariant — at map'en ogsaa minter en markoer,
    der ER soft-slettet, foerste gang den stages — kan dette scenarie ikke naa,
    fordi OBJ["i"] fik sin noegle i test 1, mens den var levende, og map'en er
    append-only; den er daekket statisk af
    tests/unit/test_udredningsret_markoer.py::test_map_minter_ogsaa_soft_slettede_markoerer.
    """
    assert markoer_fact_raekke(
        dwh_db, OBJ["i"], "[dw_ek_UdredningsretMarkoer], [dw_is_deleted]"
    ) == (TILSTAND["afbrudt_dw_ek"], True)
    alle_foer, levende_foer, _ = markoer_fact_opgoerelse(dwh_db)

    cp = arc_watermark(dwh_db)
    vent_paa_nyt_db_sekund(dwh_db, cp)
    soft_delete_markoer(dwh_db, OBJ["i"], slettet=0, roer_tidsstempel=True)
    saet_downstream_checkpoints(dwh_db, cp)
    koer_kaeden(dwh_db, mode="delta")
    resultater = koer_dwh(dwh_db)

    assert markoer_fact_raekke(
        dwh_db, OBJ["i"], "[dw_ek_UdredningsretMarkoer], [dw_is_deleted]"
    ) == (TILSTAND["afbrudt_dw_ek"], False)
    assert (
        md(resultater["map_UdredningsretMarkoer"], "new_rows") == 0
    ), "genoplivningen mintede en ny noegle i stedet for at genbruge den retirerede"

    alle, levende, kilderaekker = markoer_fact_opgoerelse(dwh_db)
    assert alle == alle_foer, "genoplivningen tilfoejede en raekke"
    assert levende == levende_foer + 1
    assert (levende, kilderaekker) == arc_levende_markoerer(dwh_db) == (15, 16)
    assert md(resultater["fct_UdredningsretMarkoer"], "total_live_rows") == levende

    # afbrydelsen hoerer til HF3's forloeb, saa baade markoerfacten og
    # forloebsfactens egen taeller tager den med igen
    assert markoerer_uden_levende_forloeb(dwh_db) == 0
    assert forloebsfactens_markoertaeller(dwh_db) == markoerer_med_forloeb(dwh_db) == 13
    assert md(resultater["fct_UdredningsretMarkoer"], "taeller_afvigelse") == 0
    # forloebene selv er uroerte: HF3 havde allerede sin henvisning
    assert levende_fact_raekker(dwh_db) == arc_forloebsgrupper(dwh_db) == 6


# ---------------------------------------------------------------------------
# 8c. Kontroltotal-brud paa markoerfacten (US-5)
# ---------------------------------------------------------------------------


def test_8c_kontroltotal_brud_fejler_markoerfacten(dwh_db):
    """Arkivet aendres UDEN gen-staging: facten er nu uenig med arkivet om,
    hvor mange levende markoerer der findes, og koerslen skal fejle."""
    soft_delete_markoer(dwh_db, OBJ_OEVRIGE, slettet=1, roer_tidsstempel=False)
    try:
        with pytest.raises(dg.Failure, match="kontroltotalen holder ikke"):
            fct_UdredningsretMarkoer(dg.build_asset_context(), dwh_db, RunModeConfig())
    finally:
        # uden tidsstempel-bump: staging er stadig i sync med den gamle tilstand
        soft_delete_markoer(dwh_db, OBJ_OEVRIGE, slettet=0, roer_tidsstempel=False)

    # genskabt konsistens -> asset'et koerer igennem igen
    resultat = fct_UdredningsretMarkoer(dg.build_asset_context(), dwh_db, RunModeConfig())
    assert md(resultat, "total_live_rows") == 15
    _alle, levende, kilderaekker = markoer_fact_opgoerelse(dwh_db)
    assert (levende, kilderaekker) == arc_levende_markoerer(dwh_db) == (15, 16)


# ---------------------------------------------------------------------------
# 9. GDPR: hverken kolonnenavne eller CPR-vaerdier i dwh-laget
# ---------------------------------------------------------------------------


def test_9_ingen_cpr_i_map_dim_eller_fct(dwh_db):
    tabelnavne = ", ".join(f"'{t.split('.')[1].strip('[]')}'" for t, _ in DWH_TABELLER)
    assert (
        scalar(
            dwh_db,
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA IN ('map','dim','fct') "
            f"AND TABLE_NAME IN ({tabelnavne}) AND COLUMN_NAME LIKE '%cpr%'",
        )
        == 0
    )
    # vaerdibaseret: et CPR gemt under et andet kolonnenavn ville slippe forbi
    # navnescannet — scan alle tekstkolonner for de syntetiske CPR-vaerdier
    for tabel, _noegle in DWH_TABELLER:
        for navn, data_type, _ in kolonner(dwh_db, tabel):
            if data_type not in ("nvarchar", "varchar", "char", "nchar"):
                continue
            for cpr in (CPR_X, CPR_Y, CPR_Z, CPR_W):
                assert (
                    scalar(
                        dwh_db,
                        f"SELECT COUNT(*) FROM {tabel} WHERE [{navn}] LIKE '%' + ? + '%'",
                        cpr,
                    )
                    == 0
                ), f"CPR-vaerdi fundet i {tabel}.{navn}"
