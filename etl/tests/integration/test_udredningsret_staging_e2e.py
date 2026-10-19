# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E-integrationstest for udredningsret-staging-kaeden (US-3, T3.6)
mod devcontainer-MSSQL.

Bygger paa ingest-testens syntetiske leverancer og hjaelpere
(test_udredningsret_ingest_e2e: build_delivery, rows_a/rows_b, markoer,
run_extract/run_archive/run_gate, fixtures udred_db/delivery_dir), saa
arkivets noegler er arkivets EGNE, og koerer de seks staging-assets direkte
(pre_stg_Helbredsforloeb -> pre_stg_Udredningsforloeb -> stg_dim_Forloebsmarkoer
-> stg_fct_UdredningsretMarkoer -> stg_fct_Udredningsforloeb ->
stg_dim_UdredningsretEgenskaber) som test_drg_dwh_e2e goer for DRG.

Verificerer (spec: docs/source_systems/udredningsret/udredningsret_model.md):

1. GRAIN: COUNT = COUNT DISTINCT paa noeglen i alle seks stg-tabeller, og det
   fysiske unikke indeks findes; kolonnelister og -typer foelger spec 3.2-3.6
   (INFORMATION_SCHEMA); antal helbredsforloeb og forloeb matcher arc.
2. HELBREDSFORLOEB + BORGER: CPR_X -> seedet noegle via cpr_nummer, CPR_Y via
   tidligere_cpr, CPR_Z og CPR_W -> -1 (ukendt); foerste markoer og taellere.
   (Tom CPR -> -2 kan ikke naas gennem den rigtige extract: dens formatvaern
   afviser CPR, der ikke er 10 cifre, saa reglen daekkes kun af unit-tests.)
3. FORLOEBSREGLER: HF1's forloeb starter paa henvisningen og slutter paa
   MRK_H-slutmarkoeren; attributter (ALDER, OPGPER, MRK_ST_*) laeses fra
   slutmarkoeren; indgaar_i_indikator = 1 og udredningstid_dage typet. Et
   forloeb uden slutmarkoer: slutdato NULL -> dw_ek_Dato_slut = BLANK
   (99999998), udredningstid NULL, indgaar_i_indikator = 0. Afbrudt forloeb
   taeller afbrydelser. dw_ek_Dato_opgoerelse fra OPGPER '1K26' -> 2026-01-01.
   SOR pr. rolle: SOR_1 -> seedet noegle, SOR_2 -> -1 med raa kode bevaret,
   tom kontaktenhed -> -1. Klassifikations-clean_code er ens i fct og dim.
   HF5 har to forloeb med SAMME startdato: det ene uden henvist-markoer starter
   paa sin tidligste markoer (U4) og slutter paa 'endeligt udredt'; loebenummer
   er 1/2 i orden efter foerste forloebselement-UUID og clean_code-suffikset
   '_1'/'_2' (spec 2).
4. MARKOERFACT: én raekke pr. arc-raekke; markoeren UDEN F_ID har
   clean_code_Udredningsforloeb NULL (sentinel-stien, U5); alle markoerer MED
   F_ID rammer en pre_Udredningsforloeb-raekke (0 foraeldreloese);
   antal_kilderaekker og dato-noegle fra arc.
5. DIM_FORLOEBSMARKOER: grupper efter SDS' bilag 1, SKS-tekst joinet, kode uden
   SKS-raekke -> 'Ukendt'/'ukendt'.
6. GDPR: ingen kolonne med 'cpr' i navnet og ingen af de syntetiske
   CPR-vaerdier i nogen tekstkolonne i nogen stg-tabel (vaerdiscan).
7. DELTA efter leverance B (F_ID omnummereret 5001 -> 9001, OBJEKTID
   genudstedt, én markoer soft-slettet): kun de aendrede helbredsforloeb er i
   scope (det uaendrede HF4 er ikke), clean_code_Udredningsforloeb for HF1 er
   IDENTISK med leverance A's trods omnummereringen, den soft-slettede
   markoer stages med dw_is_deleted = 1 og beholder sin clean_code, og
   raekketallet matcher arc for de scopede helbredsforloeb.
8. Checkpoints i fremtiden (2099) -> 0 raekker i de delta-scopede tabeller.
9. RETIRERINGSKANALEN (spec 3.1 sidste punkt, 3.2, 3.5, 6): et helbredsforloeb,
   hvis markoerer ALLE soft-slettes i arc, kommer i delta-scope og stages med
   dw_is_deleted = 1, 0 forloeb, taellere 0, borger over de slettede raekker;
   markoerfacten baerer dets raekke med dw_is_deleted = 1 og
   clean_code_Udredningsforloeb NULL (gruppen findes ikke laengere).
10. BORGER-GENOPRETNING (U9): delta-scopets anden gren traekker et
    helbredsforloeb ind alene fordi dets levende fct.Udredningsforloeb-raekke
    staar med dw_ek_Borger = -1 (CHAR(64)-noeglen genskabes fra
    map.Helbredsforloeb's clean_code); loest borger eller retireret fact-raekke
    -> ude igen.

Testene deler modul-scoped DB-tilstand og koerer i definitionsraekkefoelge.
Kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true (devcontainer).
"""

import time

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from DSA.SQL.common.dato_dimension import DATO_BLANK_KEY
from DSA.SQL.staging.pre_stg_Helbredsforloeb import pre_stg_Helbredsforloeb
from DSA.SQL.staging.pre_stg_Udredningsforloeb import pre_stg_Udredningsforloeb
from DSA.SQL.staging.stg_dim_Forloebsmarkoer import stg_dim_Forloebsmarkoer
from DSA.SQL.staging.stg_dim_UdredningsretEgenskaber import stg_dim_UdredningsretEgenskaber
from DSA.SQL.staging.stg_fct_Udredningsforloeb import stg_fct_Udredningsforloeb
from DSA.SQL.staging.stg_fct_UdredningsretMarkoer import stg_fct_UdredningsretMarkoer
from DSA.SQL.staging.udredningsret_kolonner import (
    DIM_EGENSKABER_TABLE,
    DIM_FORLOEBSMARKOER_TABLE,
    DOWNSTREAM_CHECKPOINT_EJERE,
    EGENSKABER_ATTRIBUTTER,
    FCT_FORLOEB_TABLE,
    FCT_MARKOER_TABLE,
    FORLOEB_ATTRIBUT_NAVNE,
    FORLOEB_PREFIX,
    FORLOEBSMARKOER_PREFIX,
    HF_PREFIX,
    KLASSIFIKATION_PREFIX,
    MARKOER_PREFIX,
    PRE_FORLOEB_TABLE,
    PRE_HF_TABLE,
)
from DSI.csv_files.udredningsret_datasets import UDREDNINGSRET_MONT

# Genbruger ingest-testens fixtures, hjaelpere og syntetiske leverancer, saa
# arkivet er bygget af den rigtige extract+archive og noeglerne er dets egne.
from tests.integration.test_udredningsret_ingest_e2e import (  # noqa: F401
    A_FILE,
    B_FILE,
    CPR_X,
    CPR_Y,
    CPR_Z,
    ELEM_1,
    ELEM_2,
    ELEM_3,
    OBJ,
    OBJ_F_REISSUED,
    SOR_1,
    SOR_2,
    build_delivery,
    delivery_dir,
    markoer,
    require_real_environment,
    rows,
    rows_a,
    rows_b,
    run_archive,
    run_extract,
    run_gate,
    scalar,
    udred_db,
)

pytestmark = pytest.mark.integration

# Seedede dim-noegler (hoeje vaerdier; ryddes i teardown). Andre end DRG-testens.
EK_BORGER_X = 9100001
EK_BORGER_Y = 9100002
EK_ORG_1 = 9100003

# Ekstra syntetiske raekker oven paa ingest-testens leverancer
ELEM_4 = "e4e4e4e4-0000-0000-0000-000000000004"
ELEM_5 = "e5e5e5e5-0000-0000-0000-000000000005"  # HF5, forloeb 5005 (sorterer foer ELEM_6)
ELEM_6 = "e6e6e6e6-0000-0000-0000-000000000006"  # HF5, forloeb 5006
OBJ_UDEN_FID = "ffffffff-0000-0000-0000-000000000101"  # HF1-markoer uden F_ID
OBJ_HF4 = "ffffffff-0000-0000-0000-000000000102"  # HF4: ukendt CPR, uaendret i A og B
OBJ_HF5_START = "ffffffff-0000-0000-0000-000000000103"  # 5005: udredning start (ingen henvist)
OBJ_HF5_SLUT = "ffffffff-0000-0000-0000-000000000104"  # 5005: endeligt udredt uden MRK_H
OBJ_HF5_HENVIST = "ffffffff-0000-0000-0000-000000000105"  # 5006: henvisning samme dag
CPR_W = "0404044444"  # fjerde syntetiske borger (HF4 og HF5), findes ikke i dim.Borger

# Koder i leverancerne og deres SKS-tekst; AFV01F faar bevidst INGEN raekke.
SKS_TEKSTER = {
    "AFV01A1": "Henvist til udredning",
    "AFV01B": "Udredning start",
    "AFX01C": "Endeligt udredt",
    "AFX01D4": "Udredningsplan pga. manglende kapacitet",
    "AFV01X2": "Udredning afbrudt",
    "AFX01D1": "Udredningsplan pga. faglige aarsager",
}
KODE_UDEN_SKS = "AFV01F"

STG_TABELLER = (
    PRE_HF_TABLE,
    PRE_FORLOEB_TABLE,
    DIM_FORLOEBSMARKOER_TABLE,
    FCT_MARKOER_TABLE,
    FCT_FORLOEB_TABLE,
    DIM_EGENSKABER_TABLE,
)
NOEGLE_PR_TABEL = {
    PRE_HF_TABLE: "clean_code_Helbredsforloeb",
    PRE_FORLOEB_TABLE: "clean_code_Udredningsforloeb",
    DIM_FORLOEBSMARKOER_TABLE: "clean_code_Forloebsmarkoer",
    FCT_MARKOER_TABLE: "clean_code_UdredningsretMarkoer",
    FCT_FORLOEB_TABLE: "clean_code_Udredningsforloeb",
    DIM_EGENSKABER_TABLE: "clean_code_Udredningsforloeb",
}

# Forventede kolonnelister i raekkefoelge (spec 3.2-3.6)
KOLONNER_PRE_HF = (
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
)
KOLONNER_PRE_FORLOEB = (
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
)
KOLONNER_DIM_FORLOEBSMARKOER = (
    "clean_code_Forloebsmarkoer",
    "kode",
    "gruppe",
    "gruppe_tekst",
    "tekst",
    "tekst_kilde",
    "dw_is_deleted",
    "dw_timestamp_update",
)
KOLONNER_FCT_MARKOER = (
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
)
KOLONNER_FCT_FORLOEB = (
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
)
KOLONNER_DIM_EGENSKABER = (
    "clean_code_Udredningsforloeb",
    "clean_code_UdredningsretKlassifikation",
    *EGENSKABER_ATTRIBUTTER,
    "dw_is_deleted",
    "dw_timestamp_update",
)
FORVENTEDE_KOLONNER = {
    PRE_HF_TABLE: KOLONNER_PRE_HF,
    PRE_FORLOEB_TABLE: KOLONNER_PRE_FORLOEB,
    DIM_FORLOEBSMARKOER_TABLE: KOLONNER_DIM_FORLOEBSMARKOER,
    FCT_MARKOER_TABLE: KOLONNER_FCT_MARKOER,
    FCT_FORLOEB_TABLE: KOLONNER_FCT_FORLOEB,
    DIM_EGENSKABER_TABLE: KOLONNER_DIM_EGENSKABER,
}
# (tabel, kolonne) -> (data_type, character_maximum_length) for de typebaerende kolonner
FORVENTEDE_TYPER = {
    (PRE_HF_TABLE, "clean_code_Helbredsforloeb"): ("varchar", 80),
    (PRE_HF_TABLE, "helbredsforloeb_noegle"): ("char", 64),
    (PRE_HF_TABLE, "dw_ek_Borger"): ("int", None),
    (PRE_HF_TABLE, "dw_is_deleted"): ("bit", None),
    (PRE_FORLOEB_TABLE, "clean_code_Udredningsforloeb"): ("varchar", 100),
    (PRE_FORLOEB_TABLE, "loebenummer"): ("int", None),
    (PRE_FORLOEB_TABLE, "indgaar_i_indikator"): ("bit", None),
    (DIM_FORLOEBSMARKOER_TABLE, "clean_code_Forloebsmarkoer"): ("varchar", 50),
    (DIM_FORLOEBSMARKOER_TABLE, "tekst"): ("nvarchar", 500),
    (FCT_MARKOER_TABLE, "clean_code_UdredningsretMarkoer"): ("varchar", 100),
    (FCT_MARKOER_TABLE, "dw_ek_Dato_markoer"): ("int", None),
    (FCT_MARKOER_TABLE, "indgaar_i_indikator_H"): ("bit", None),
    (FCT_MARKOER_TABLE, "dw_haendelsestidspunkt"): ("datetime2", None),
    (FCT_FORLOEB_TABLE, "clean_code_UdredningsretKlassifikation"): ("nvarchar", 100),
    (FCT_FORLOEB_TABLE, "dw_ek_Dato_slut"): ("int", None),
    (FCT_FORLOEB_TABLE, "udredningstid_dage"): ("int", None),
    (FCT_FORLOEB_TABLE, "overholdt"): ("bit", None),
    (FCT_FORLOEB_TABLE, "alder_ved_henvisning"): ("smallint", None),
    (DIM_EGENSKABER_TABLE, "clean_code_UdredningsretKlassifikation"): ("nvarchar", 100),
}

# Tilstand delt paa tvaers af testene (fyldes i test_1/test_4, laeses i test_7)
TILSTAND: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Syntetiske raekker: ingest-testens leverancer + staging-specifikke tilfaelde
# ---------------------------------------------------------------------------

_KOL = {c: i for i, c in enumerate(UDREDNINGSRET_MONT.source_columns)}


def _saet(rows_: list[list[str]], obj: str, **values: str) -> None:
    """Saet kildekolonner paa den raekke, hvis OBJEKTID_FORLOBSMARKOR er `obj`."""
    for r in rows_:
        if r[_KOL["OBJEKTID_FORLOBSMARKOR"]] == obj:
            for kolonne, vaerdi in values.items():
                r[_KOL[kolonne]] = vaerdi
            return
    raise AssertionError(f"ingen raekke med OBJEKTID {obj}")


def _ekstra_raekker() -> list[list[str]]:
    """Ens i A og B: HF1-markoer UDEN F_ID (behandling start), HF4 (kun henvisning) og
    HF5 med to forloeb, der starter SAMME dag: 5005 uden henvist-markoer (udredning
    start + endeligt udredt uden MRK_H) og 5006 med kun en henvisning."""
    return [
        markoer("100", ELEM_1, OBJ_UDEN_FID, "", "", CPR_X, KODE_UDEN_SKS, "2026-02-15"),
        markoer("400", ELEM_4, OBJ_HF4, "5004_1", "5004", CPR_W, "AFV01A1", "2026-05-01"),
        markoer("500", ELEM_5, OBJ_HF5_START, "5005_1", "5005", CPR_W, "AFV01B", "2026-06-01"),
        markoer("500", ELEM_5, OBJ_HF5_SLUT, "5005_2", "5005", CPR_W, "AFX01C", "2026-06-10"),
        markoer("500", ELEM_6, OBJ_HF5_HENVIST, "5006_1", "5006", CPR_W, "AFV01A1", "2026-06-01"),
    ]


def rows_a_staging() -> list[list[str]]:
    """Leverance A + slutmarkoerens indikatorattributter (ALDER, OPGPER, MRK_ST_*)
    saa 'attributter fra slutmarkoeren' kan skelnes fra planmarkoerernes ALDER 50/51."""
    r = rows_a()
    _saet(r, OBJ["d"], ALDER="52", OPGPER="1K26", MRK_ST_L="1", MRK_ST_H="1", MRK_ST_N="1")
    return r + _ekstra_raekker()


def rows_b_staging() -> list[list[str]]:
    """Leverance B (omnummereret, genudstedt, én markoer vaek) + de uaendrede ekstra raekker."""
    return rows_b() + _ekstra_raekker()


# ---------------------------------------------------------------------------
# Hjaelpere
# ---------------------------------------------------------------------------


def raekker(resource, sql: str, *params) -> list[tuple]:
    """Som ingest-testens rows(), men som tuples: driverens Row-objekter
    sammenligner ikke lig med tuples, og deres slices er lister."""
    return [tuple(r) for r in rows(resource, sql, *params)]


def run_sql(resource, sql: str, *params) -> None:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            conn.commit()
        finally:
            cursor.close()


def hf_noegle(resource, elem_uuid: str) -> str:
    """Arkivets helbredsforloeb_noegle for forloebselementet (stabil paa tvaers af A/B)."""
    noegler = rows(
        resource,
        "SELECT DISTINCT helbredsforloeb_noegle FROM arc.udredningsret_mont "
        "WHERE LOWER(OBJEKTID_FORLOBELEMENT) = ?",
        elem_uuid.lower(),
    )
    assert len(noegler) == 1, f"{len(noegler)} helbredsforloeb for forloebselement"
    return noegler[0][0]


def koer_kaeden(resource, mode: str) -> dict[str, dg.MaterializeResult]:
    """Materialiser de seks staging-assets i afhaengighedsorden."""
    cfg = RunModeConfig(mode=mode)
    resultater = {}
    for navn, fn in (
        ("pre_stg_Helbredsforloeb", pre_stg_Helbredsforloeb),
        ("pre_stg_Udredningsforloeb", pre_stg_Udredningsforloeb),
        ("stg_dim_Forloebsmarkoer", stg_dim_Forloebsmarkoer),
        ("stg_fct_UdredningsretMarkoer", stg_fct_UdredningsretMarkoer),
        ("stg_fct_Udredningsforloeb", stg_fct_Udredningsforloeb),
        ("stg_dim_UdredningsretEgenskaber", stg_dim_UdredningsretEgenskaber),
    ):
        resultater[navn] = fn(dg.build_asset_context(), resource, cfg)
    return resultater


def saet_downstream_checkpoints(resource, value: str) -> None:
    dfm = DfmManager(resource, dg.build_asset_context())
    for schema, table in DOWNSTREAM_CHECKPOINT_EJERE:
        dfm.set_checkpoint(schema, table, value=value)


def slet_downstream_checkpoints(resource) -> None:
    for schema, table in DOWNSTREAM_CHECKPOINT_EJERE:
        run_sql(
            resource,
            "DELETE FROM [meta].[DFMState] WHERE [schema_name] = ? AND [table_name] = ?",
            schema,
            table,
        )


def kolonner(resource, tabel: str) -> list[tuple]:
    """(navn, data_type, max_length) i ordinal raekkefoelge for '[schema].[tabel]'."""
    return rows(
        resource,
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' = ? ORDER BY ORDINAL_POSITION",
        tabel,
    )


def vent_paa_nyt_db_sekund(resource, efter: str) -> None:
    """Arkivet stempler DATETIME2(0): vent til DB-uret er forbi `efter`, saa
    leverance B's raekker med sikkerhed faar et stoerre dw_timestamp_update."""
    for _ in range(60):
        if scalar(
            resource,
            "SELECT CASE WHEN CAST(SYSDATETIME() AS DATETIME2(0)) > CAST(? AS DATETIME2(0)) "
            "THEN 1 ELSE 0 END",
            efter,
        ):
            return
        time.sleep(0.2)
    raise AssertionError("DB-uret rykkede sig ikke")


# ---------------------------------------------------------------------------
# Fixture: dim-opslag, SKS-tekster og rene checkpoints oven paa udred_db
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def staging_db(udred_db):  # noqa: F811
    """Seed dim.Borger / dim.Organisation / codesystem.SKSConcept og ryd staging-tilstand."""
    from infrastructure.utils.ddl_loader import execute_sql_files_from_directory

    res = udred_db
    if scalar(res, "SELECT OBJECT_ID('[codesystem].[SKSConcept]')") is None:
        execute_sql_files_from_directory(res, "tables/codesystem")

    alle_koder = tuple(SKS_TEKSTER) + (KODE_UDEN_SKS,)
    kode_pladser = ", ".join("?" for _ in alle_koder)
    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for t in STG_TABELLER:
                cursor.execute(f"DROP TABLE IF EXISTS {t}")
            # Borger-genopretning (U9) laeser fct.Udredningsforloeb, hvis den findes:
            # rester fra en tidligere koersel mod et nu droppet arc maa ikke
            # udvide delta-scopet.
            cursor.execute(
                "IF OBJECT_ID('[fct].[Udredningsforloeb]', 'U') IS NOT NULL "
                "DELETE FROM [fct].[Udredningsforloeb]"
            )
            cursor.execute(
                "DELETE FROM [dim].[Borger] WHERE [dw_ek_Borger] IN (?, ?)",
                EK_BORGER_X,
                EK_BORGER_Y,
            )
            cursor.execute(
                "DELETE FROM [dim].[Organisation] WHERE [dw_ek_Organisation] = ?", EK_ORG_1
            )
            cursor.execute(
                "INSERT INTO [dim].[Borger] ([dw_sk_Borger],[dw_ek_Borger],[cpr_nummer],"
                "[dw_valid_from],[dw_valid_to]) VALUES (?, ?, ?, '1900-01-01', '9999-12-31')",
                EK_BORGER_X,
                EK_BORGER_X,
                CPR_X,
            )
            cursor.execute(
                "INSERT INTO [dim].[Borger] "
                "([dw_sk_Borger],[dw_ek_Borger],[cpr_nummer],[tidligere_cpr],"
                "[dw_valid_from],[dw_valid_to]) VALUES (?, ?, ?, ?, '1900-01-01', '9999-12-31')",
                EK_BORGER_Y,
                EK_BORGER_Y,
                "9999999998",  # nuvaerende CPR — leverancen baerer den TIDLIGERE
                CPR_Y,
            )
            cursor.execute(
                "INSERT INTO [dim].[Organisation] ([dw_ek_Organisation],[kode],[navn]) "
                "VALUES (?, ?, ?)",
                EK_ORG_1,
                SOR_1,
                "Teststedet",
            )
            cursor.execute(
                f"DELETE FROM [codesystem].[SKSConcept] WHERE [code] IN ({kode_pladser})",
                *alle_koder,
            )
            for kode, tekst in SKS_TEKSTER.items():
                cursor.execute(
                    "INSERT INTO [codesystem].[SKSConcept] ([code],[display],"
                    "[dw_timestamp_insert],[dw_timestamp_update]) "
                    "VALUES (?, ?, SYSDATETIME(), SYSDATETIME())",
                    kode,
                    tekst,
                )
            conn.commit()
        finally:
            cursor.close()
    slet_downstream_checkpoints(res)

    yield res

    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM [dim].[Borger] WHERE [dw_ek_Borger] IN (?, ?)",
                EK_BORGER_X,
                EK_BORGER_Y,
            )
            cursor.execute(
                "DELETE FROM [dim].[Organisation] WHERE [dw_ek_Organisation] = ?", EK_ORG_1
            )
            cursor.execute(
                f"DELETE FROM [codesystem].[SKSConcept] WHERE [code] IN ({kode_pladser})",
                *alle_koder,
            )
            conn.commit()
        finally:
            cursor.close()
    slet_downstream_checkpoints(res)


# ---------------------------------------------------------------------------
# 1. Fuld indlaesning: kaeden koerer, grain, kolonner, kontroltal mod arc
# ---------------------------------------------------------------------------


def test_1_fuld_indlaesning_grain_og_kolonner(staging_db, delivery_dir, monkeypatch):  # noqa: F811
    build_delivery(delivery_dir, A_FILE, "20260830", "2026-08-30", rows_a_staging())
    assert run_extract(staging_db, delivery_dir, monkeypatch).metadata["besluttet"] == "indlaest"
    run_archive(staging_db)
    run_gate(staging_db)
    # 14 ext-raekker -> 13 arc-raekker (b+c er identiske), 5 helbredsforloeb
    assert (
        scalar(staging_db, "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE dw_is_deleted = 0")
        == 13
    )

    resultater = koer_kaeden(staging_db, mode="full_load")
    assert resultater["pre_stg_Helbredsforloeb"].metadata["checkpoint"] == "full load"

    for tabel in STG_TABELLER:
        noegle = NOEGLE_PR_TABEL[tabel]
        antal, distinkte = raekker(
            staging_db, f"SELECT COUNT(*), COUNT(DISTINCT [{noegle}]) FROM {tabel}"
        )[0]
        assert antal > 0, f"{tabel} er tom"
        assert antal == distinkte, f"{tabel}: grain brudt ({antal} vs {distinkte})"
        unikke_indeks = scalar(
            staging_db,
            "SELECT COUNT(*) FROM sys.indexes i "
            "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "WHERE i.object_id = OBJECT_ID(?) AND i.is_unique = 1 AND c.name = ?",
            tabel,
            noegle,
        )
        assert unikke_indeks == 1, f"{tabel}: intet unikt indeks paa {noegle}"
        # kolonnelisten foelger spec'en (navne OG raekkefoelge)
        faktiske = kolonner(staging_db, tabel)
        assert tuple(navn for navn, _, _ in faktiske) == FORVENTEDE_KOLONNER[tabel], tabel
        for navn, data_type, max_len in faktiske:
            forventet = FORVENTEDE_TYPER.get((tabel, navn))
            if forventet is not None:
                assert (data_type, max_len) == forventet, f"{tabel}.{navn}: {data_type}({max_len})"

    # kontroltal mod arc: helbredsforloeb og forloeb (distinkte ikke-blanke F_ID)
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_HF_TABLE}") == scalar(
        staging_db, "SELECT COUNT(DISTINCT helbredsforloeb_noegle) FROM arc.udredningsret_mont"
    )
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_HF_TABLE}") == 5
    forloeb_i_arc = scalar(
        staging_db,
        "SELECT COUNT(DISTINCT F_ID) FROM arc.udredningsret_mont "
        "WHERE dw_is_deleted = 0 AND LTRIM(RTRIM(F_ID)) <> ''",
    )
    assert forloeb_i_arc == 6
    for tabel in (PRE_FORLOEB_TABLE, FCT_FORLOEB_TABLE, DIM_EGENSKABER_TABLE):
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {tabel}") == forloeb_i_arc, tabel
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}") == 13
    md_markoer = resultater["stg_fct_UdredningsretMarkoer"].metadata
    assert md_markoer["row_count"] == 13
    # ingen soft-slettede endnu: begge kilderaekke-summer er arkivets (b+c taeller 2)
    assert (
        md_markoer["sum_antal_kilderaekker"] == md_markoer["sum_antal_kilderaekker_levende"] == 14
    )
    # U4-diagnostikken og loebenummer > 1 rammer praecis HF5's to forloeb
    md_forloeb = resultater["pre_stg_Udredningsforloeb"].metadata
    assert md_forloeb["forloeb_uden_henvist_markoer"] == 1
    assert md_forloeb["loebenummer_over_1"] == 1

    # praefikser og laengder paa clean_codes
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {PRE_HF_TABLE} "
            f"WHERE [clean_code_Helbredsforloeb] NOT LIKE '{HF_PREFIX}%' "
            f"OR LEN([clean_code_Helbredsforloeb]) <> {len(HF_PREFIX) + 64}",
        )
        == 0
    )
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {PRE_FORLOEB_TABLE} "
            f"WHERE [clean_code_Udredningsforloeb] NOT LIKE '{FORLOEB_PREFIX}%'",
        )
        == 0
    )
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} "
            f"WHERE [clean_code_UdredningsretMarkoer] NOT LIKE '{MARKOER_PREFIX}%'",
        )
        == 0
    )

    TILSTAND["cp_efter_a"] = scalar(
        staging_db,
        "SELECT CONVERT(NVARCHAR(50), MAX(dw_timestamp_update), 121) FROM arc.udredningsret_mont",
    )


# ---------------------------------------------------------------------------
# 2. Helbredsforloeb og borger-opslag
# ---------------------------------------------------------------------------


def test_2_helbredsforloeb_og_borger_opslag(staging_db):
    hf1, hf2, hf3, hf4 = (hf_noegle(staging_db, e) for e in (ELEM_1, ELEM_2, ELEM_3, ELEM_4))
    pr_hf = {
        r[0]: r[1:]
        for r in raekker(
            staging_db,
            "SELECT helbredsforloeb_noegle, dw_ek_Borger, borger_opslag, foerste_markoer_kode, "
            "foerste_markoer_dato, foerste_enhed_sor, foerste_forloebselement_uuid, "
            "antal_markoerer, antal_forloebselementer, antal_udredningsforloeb, "
            f"dw_is_deleted, helbredsforloeb_id FROM {PRE_HF_TABLE}",
        )
    }
    # CPR_X findes som cpr_nummer, CPR_Y kun som tidligere_cpr, CPR_Z og CPR_W slet ikke
    assert pr_hf[hf1][:2] == (EK_BORGER_X, "cpr")
    assert pr_hf[hf2][:2] == (EK_BORGER_Y, "tidligere_cpr")
    assert pr_hf[hf3][:2] == (-1, "ukendt")
    assert pr_hf[hf4][:2] == (-1, "ukendt")
    # foerste levende markoer og taellere: HF1 har 6 arc-raekker (a, b+c, d, e, f og
    # markoeren uden F_ID), ét forloebselement og ét udredningsforloeb
    assert pr_hf[hf1][2:7] == ("AFV01A1", "2026-01-05", SOR_1, ELEM_1, 6)
    assert pr_hf[hf1][7:] == (1, 1, False, "100")
    assert pr_hf[hf2][2:5] == ("AFV01A1", "2026-03-01", SOR_2)
    assert pr_hf[hf3][7:10] == (1, 1, False)
    assert pr_hf[hf4][7:10] == (1, 1, False)


# ---------------------------------------------------------------------------
# 3. Udredningsforloeb: start/slut/attributter, sentinels, SOR pr. rolle
# ---------------------------------------------------------------------------


def test_3_forloebsregler_datonoegler_og_sor(staging_db):
    hf1, hf2, hf3 = (hf_noegle(staging_db, e) for e in (ELEM_1, ELEM_2, ELEM_3))

    # HF1: start = henvisningen (a), slut = MRK_H-slutmarkoeren (d, AFX01C), attributter fra d
    p1 = raekker(
        staging_db,
        "SELECT f_id, loebenummer, startdato, start_markoer_kode, start_markoer_uuid, slutdato, "
        "slut_markoer_kode, slut_markoer_uuid, seneste_markoer_dato, alder_raa, "
        "opgoerelseskvartal, overholdt, inden_30_dage, aarsag, udredningstid_raa, "
        "indgaar_i_indikator, antal_markoerer, antal_udredningsplaner, antal_afbrydelser, "
        f"clean_code_Udredningsforloeb FROM {PRE_FORLOEB_TABLE} WHERE helbredsforloeb_noegle = ?",
        hf1,
    )
    assert len(p1) == 1
    (
        f_id,
        loebenummer,
        startdato,
        start_kode,
        start_uuid,
        slutdato,
        slut_kode,
        slut_uuid,
        seneste,
        alder,
        opgper,
        overholdt,
        inden_30,
        aarsag,
        udredningstid,
        indikator,
        n_markoerer,
        n_planer,
        n_afbrydelser,
        clean_code,
    ) = p1[0]
    assert (f_id, loebenummer) == ("5001", 1)
    assert (startdato, start_kode, start_uuid) == ("2026-01-05", "AFV01A1", OBJ["a"])
    assert (slutdato, slut_kode, slut_uuid) == ("2026-02-01", "AFX01C", OBJ["d"])
    assert seneste == "2026-02-01"
    # ALDER 52 er slutmarkoerens — ikke planmarkoerernes 50/51
    assert (alder, opgper, overholdt, inden_30, aarsag, udredningstid) == (
        "52",
        "1K26",
        "1",
        "1",
        "1",
        "27",
    )
    assert indikator is True
    # markoeren uden F_ID taeller IKKE med i forloebets 5 (a, b+c, d, e, f)
    assert (n_markoerer, n_planer, n_afbrydelser) == (5, 2, 0)
    assert clean_code == f"{FORLOEB_PREFIX}{hf1}_20260105_1"
    TILSTAND["hf1_clean_code_a"] = clean_code

    f1 = raekker(
        staging_db,
        "SELECT dw_ek_Borger, forloeb_enhed_sor, dw_ek_Organisation_forloeb, kontakt_enhed_sor, "
        "dw_ek_Organisation_kontakt, dw_ek_Dato_start, dw_ek_Dato_slut, dw_ek_Dato_opgoerelse, "
        "udredningstid_dage, indgaar_i_indikator, overholdt, inden_30_dage, alder_ved_henvisning, "
        "antal_markoerer, antal_udredningsplaner, antal_afbrydelser, "
        "CONVERT(NVARCHAR(10), dw_haendelsestidspunkt, 23), clean_code_UdredningsretKlassifikation "
        f"FROM {FCT_FORLOEB_TABLE} WHERE clean_code_Udredningsforloeb = ?",
        clean_code,
    )[0]
    assert f1[:5] == (EK_BORGER_X, SOR_1, EK_ORG_1, None, -1)  # tom kontaktenhed -> -1
    assert f1[5:8] == (20260105, 20260201, 20260101)  # OPGPER '1K26' -> kvartalets foerste dag
    assert f1[8:13] == (27, True, True, True, 52)
    assert f1[13:17] == (5, 2, 0, "2026-01-05")
    klass = f1[17]
    assert klass.startswith(KLASSIFIKATION_PREFIX) and len(klass) == len(KLASSIFIKATION_PREFIX) + 64
    # klassifikationen er den samme noegle i fct og dim (delt PK nedstroems)
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} f JOIN {DIM_EGENSKABER_TABLE} d "
            "ON d.clean_code_Udredningsforloeb = f.clean_code_Udredningsforloeb "
            "WHERE d.clean_code_UdredningsretKlassifikation <> f.clean_code_UdredningsretKlassifikation",
        )
        == 0
    )

    # HF2: kun henvisning -> ingen slutmarkoer -> BLANK-sentinel, udredningstid NULL, ikke i
    # indikatoren; SOR_2 findes ikke i dim.Organisation -> -1 med raa kode bevaret;
    # ingen OPGPER -> BLANK
    p2 = raekker(
        staging_db,
        "SELECT slutdato, slut_markoer_kode, indgaar_i_indikator, antal_markoerer "
        f"FROM {PRE_FORLOEB_TABLE} WHERE helbredsforloeb_noegle = ?",
        hf2,
    )
    assert p2 == [(None, None, False, 1)]
    f2 = raekker(
        staging_db,
        "SELECT dw_ek_Borger, forloeb_enhed_sor, dw_ek_Organisation_forloeb, dw_ek_Dato_start, "
        "dw_ek_Dato_slut, dw_ek_Dato_opgoerelse, udredningstid_dage, indgaar_i_indikator, "
        f"overholdt, inden_30_dage FROM {FCT_FORLOEB_TABLE} WHERE clean_code_Helbredsforloeb = ?",
        f"{HF_PREFIX}{hf2}",
    )
    assert f2 == [
        (EK_BORGER_Y, SOR_2, -1, 20260301, DATO_BLANK_KEY, DATO_BLANK_KEY, None, False, None, None)
    ]

    # HF3: henvisning + afbrydelse (AFV01X2 er ikke en afslutning) -> aabent forloeb,
    # afbrydelsen taelles, attributmarkoeren er den seneste (i)
    p3 = raekker(
        staging_db,
        "SELECT slutdato, seneste_markoer_dato, antal_markoerer, antal_afbrydelser, "
        f"antal_udredningsplaner, pk FROM {PRE_FORLOEB_TABLE} WHERE helbredsforloeb_noegle = ?",
        hf3,
    )
    assert p3 == [(None, "2026-04-20", 2, 1, 0, "5003_2")]
    assert (
        scalar(
            staging_db,
            f"SELECT dw_ek_Dato_slut FROM {FCT_FORLOEB_TABLE} WHERE clean_code_Helbredsforloeb = ?",
            f"{HF_PREFIX}{hf3}",
        )
        == DATO_BLANK_KEY
    )

    # egenskabsdimensionen baerer de raa SOR-koder og markoerkoderne
    assert raekker(
        staging_db,
        "SELECT start_markoer_kode, slut_markoer_kode, forloeb_enhed_sor, kontakt_enhed_sor, "
        f"opgoerelseskvartal, forloebslabel FROM {DIM_EGENSKABER_TABLE} "
        "WHERE clean_code_Udredningsforloeb = ?",
        clean_code,
    ) == [("AFV01A1", "AFX01C", SOR_1, None, "1K26", "ALAL90")]

    # HF5: to forloeb med SAMME startdato. 5005 har ingen henvist-markoer og starter
    # paa sin tidligste markoer (U4, AFV01B); slutter paa 'endeligt udredt' uden
    # MRK_H (prioritet 1). 5006 er kun en henvisning. loebenummer foelger foerste
    # forloebselement-UUID (ELEM_5 < ELEM_6), aldrig F_ID, og staar i clean_code.
    hf5 = hf_noegle(staging_db, ELEM_5)
    assert hf5 == hf_noegle(staging_db, ELEM_6)
    p5 = raekker(
        staging_db,
        "SELECT f_id, loebenummer, startdato, start_markoer_kode, start_markoer_uuid, slutdato, "
        "slut_markoer_kode, foerste_forloebselement_uuid, indgaar_i_indikator, antal_markoerer, "
        f"clean_code_Udredningsforloeb FROM {PRE_FORLOEB_TABLE} "
        "WHERE helbredsforloeb_noegle = ? ORDER BY loebenummer",
        hf5,
    )
    assert p5 == [
        (
            "5005",
            1,
            "2026-06-01",
            "AFV01B",
            OBJ_HF5_START,
            "2026-06-10",
            "AFX01C",
            ELEM_5,
            False,
            2,
            f"{FORLOEB_PREFIX}{hf5}_20260601_1",
        ),
        (
            "5006",
            2,
            "2026-06-01",
            "AFV01A1",
            OBJ_HF5_HENVIST,
            None,
            None,
            ELEM_6,
            False,
            1,
            f"{FORLOEB_PREFIX}{hf5}_20260601_2",
        ),
    ]
    assert raekker(
        staging_db,
        "SELECT dw_ek_Dato_start, dw_ek_Dato_slut, udredningstid_dage, indgaar_i_indikator "
        f"FROM {FCT_FORLOEB_TABLE} WHERE clean_code_Helbredsforloeb = ? ORDER BY dw_ek_Dato_slut",
        f"{HF_PREFIX}{hf5}",
    ) == [(20260601, 20260610, None, False), (20260601, DATO_BLANK_KEY, None, False)]


# ---------------------------------------------------------------------------
# 4. Markoerfact: én raekke pr. arc-raekke, sentinel-stien, 0 foraeldreloese
# ---------------------------------------------------------------------------


def test_4_markoerfact_sentinel_og_foraeldreloese(staging_db):
    hf1 = hf_noegle(staging_db, ELEM_1)
    # markoeren UDEN F_ID: forloebs-clean_code NULL, men egen clean_code og hf/borger sat
    uden_fid = raekker(
        staging_db,
        "SELECT clean_code_Udredningsforloeb, clean_code_Helbredsforloeb, dw_ek_Borger, "
        "clean_code_Forloebsmarkoer, markoer_kode, f_id, dw_ek_Dato_markoer "
        f"FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid = ?",
        OBJ_UDEN_FID,
    )
    assert uden_fid == [
        (
            None,
            f"{HF_PREFIX}{hf1}",
            EK_BORGER_X,
            f"{FORLOEBSMARKOER_PREFIX}{KODE_UDEN_SKS}",
            KODE_UDEN_SKS,
            None,
            20260215,
        )
    ]
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} WHERE clean_code_Udredningsforloeb IS NULL",
        )
        == 1
    )
    # alle markoerer MED F_ID rammer en eksisterende pre_Udredningsforloeb-raekke
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} m "
            f"LEFT JOIN {PRE_FORLOEB_TABLE} u "
            "ON u.clean_code_Udredningsforloeb = m.clean_code_Udredningsforloeb "
            "WHERE m.f_id IS NOT NULL AND u.clean_code_Udredningsforloeb IS NULL",
        )
        == 0
    )
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} WHERE clean_code_Udredningsforloeb = ?",
            TILSTAND["hf1_clean_code_a"],
        )
        == 5
    )
    # arc-vaerdier baeres igennem: dubletreduktionens antal_kilderaekker, MRK_H, dato-noegle
    assert raekker(
        staging_db,
        "SELECT antal_kilderaekker, indgaar_i_indikator_H, dw_ek_Dato_markoer, markoer_dato, "
        f"CONVERT(NVARCHAR(10), dw_haendelsestidspunkt, 23) FROM {FCT_MARKOER_TABLE} "
        "WHERE markoer_kode = 'AFV01B' AND clean_code_Helbredsforloeb = ?",
        f"{HF_PREFIX}{hf1}",
    ) == [(2, False, 20260110, "2026-01-10", "2026-01-10")]
    assert (
        scalar(
            staging_db,
            f"SELECT indgaar_i_indikator_H FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid = ?",
            OBJ["a"],
        )
        is True
    )
    # SOR pr. rolle: SOR_1 -> seedet noegle, SOR_2 -> -1 med raa kode bevaret
    assert raekker(
        staging_db,
        "SELECT forloeb_enhed_sor, dw_ek_Organisation_forloeb, dw_ek_Borger, borger_opslag "
        f"FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid = ?",
        OBJ["g"],
    ) == [(SOR_2, -1, EK_BORGER_Y, "tidligere_cpr")]
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} "
            "WHERE forloeb_enhed_sor = ? AND dw_ek_Organisation_forloeb <> ?",
            SOR_1,
            EK_ORG_1,
        )
        == 0
    )
    # klargoer test_7: den markoer, leverance B fjerner (HF3's afbrydelse)
    TILSTAND["clean_code_markoer_i"] = scalar(
        staging_db,
        f"SELECT clean_code_UdredningsretMarkoer FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid = ?",
        OBJ["i"],
    )
    assert TILSTAND["clean_code_markoer_i"].startswith(MARKOER_PREFIX)


# ---------------------------------------------------------------------------
# 5. dim_Forloebsmarkoer: grupper, SKS-tekst, ukendt kode
# ---------------------------------------------------------------------------


def test_5_dim_forloebsmarkoer_grupper_og_tekster(staging_db):
    dim = {
        r[0]: r[1:]
        for r in raekker(
            staging_db,
            "SELECT kode, gruppe, gruppe_tekst, tekst, tekst_kilde, clean_code_Forloebsmarkoer "
            f"FROM {DIM_FORLOEBSMARKOER_TABLE}",
        )
    }
    assert set(dim) == set(SKS_TEKSTER) - {"AFX01D1"} | {KODE_UDEN_SKS}
    assert dim["AFV01A1"][:2] == ("henvist_udredning", "Henvist til udredning")
    assert dim["AFV01B"][:2] == ("udredning_start", "Udredning start")
    assert dim["AFX01C"][:2] == ("endeligt_udredt", "Endeligt udredt")
    assert dim["AFX01D4"][:2] == ("udredningsplan", "Udredningsplan")
    assert dim["AFV01X2"][:2] == ("afbrudt", "Afbrudt")
    assert dim[KODE_UDEN_SKS][:2] == ("behandling_start", "Behandling start")
    for kode, tekst in SKS_TEKSTER.items():
        if kode in dim:
            assert dim[kode][2:4] == (tekst, "SKS"), kode
    assert dim[KODE_UDEN_SKS][2:4] == ("Ukendt", "ukendt")
    assert dim["AFV01A1"][4] == f"{FORLOEBSMARKOER_PREFIX}AFV01A1"
    # markoerfactens clean_code_Forloebsmarkoer rammer altid dimensionen
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} m LEFT JOIN {DIM_FORLOEBSMARKOER_TABLE} d "
            "ON d.clean_code_Forloebsmarkoer = m.clean_code_Forloebsmarkoer "
            "WHERE d.kode IS NULL",
        )
        == 0
    )


# ---------------------------------------------------------------------------
# 6. GDPR: hverken kolonnenavne eller CPR-vaerdier i nogen stg-tabel
# ---------------------------------------------------------------------------


def test_6_ingen_cpr_i_nogen_stg_tabel(staging_db):
    tabelnavne = ", ".join(f"'{t.split('.')[1].strip('[]')}'" for t in STG_TABELLER)
    assert (
        scalar(
            staging_db,
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'stg' "
            f"AND TABLE_NAME IN ({tabelnavne}) AND COLUMN_NAME LIKE '%cpr%'",
        )
        == 0
    )
    # vaerdibaseret: et CPR gemt under et andet kolonnenavn ville slippe forbi
    # navnescannet — scan alle tekstkolonner for de syntetiske CPR-vaerdier
    for tabel in STG_TABELLER:
        for navn, data_type, _ in kolonner(staging_db, tabel):
            if data_type not in ("nvarchar", "varchar", "char", "nchar"):
                continue
            for cpr in (CPR_X, CPR_Y, CPR_Z, CPR_W):
                fund = scalar(
                    staging_db,
                    f"SELECT COUNT(*) FROM {tabel} WHERE [{navn}] LIKE '%' + ? + '%'",
                    cpr,
                )
                assert fund == 0, f"CPR-vaerdi fundet i {tabel}.{navn}"


# ---------------------------------------------------------------------------
# 7. Delta efter leverance B: scope, noeglestabilitet, soft-delete
# ---------------------------------------------------------------------------


def test_7_delta_efter_leverance_b(staging_db, delivery_dir, monkeypatch):  # noqa: F811
    cp = TILSTAND["cp_efter_a"]
    vent_paa_nyt_db_sekund(staging_db, cp)
    build_delivery(delivery_dir, B_FILE, "20260906", "2026-09-06", rows_b_staging())
    assert run_extract(staging_db, delivery_dir, monkeypatch).metadata["kandidater"] == 1
    arkiv = run_archive(staging_db)
    assert arkiv.metadata["rows_soft_deleted"] == 1
    assert arkiv.metadata["helbredsforloeb_genfindelse"]["andel"] == 1.0
    run_gate(staging_db)
    hf1, hf2, hf3, hf4 = (hf_noegle(staging_db, e) for e in (ELEM_1, ELEM_2, ELEM_3, ELEM_4))
    TILSTAND["cp_efter_b"] = scalar(
        staging_db,
        "SELECT CONVERT(NVARCHAR(50), MAX(dw_timestamp_update), 121) FROM arc.udredningsret_mont",
    )

    # alle fem downstream-ejere staar mellem de to indlaesninger -> delta-vindue
    saet_downstream_checkpoints(staging_db, cp)
    resultater = koer_kaeden(staging_db, mode="delta")
    assert resultater["pre_stg_Helbredsforloeb"].metadata["checkpoint"] == cp

    # scope: HF1 (omnummereret + genudstedt + ny plan), HF2 (DW_EK 200 -> 250),
    # HF3 (afbrydelsen soft-slettet) — men IKKE de uaendrede HF4 og HF5
    i_scope = {
        r[0] for r in raekker(staging_db, f"SELECT helbredsforloeb_noegle FROM {PRE_HF_TABLE}")
    }
    assert i_scope == {hf1, hf2, hf3}
    assert hf4 not in i_scope and hf_noegle(staging_db, ELEM_5) not in i_scope
    assert resultater["pre_stg_Udredningsforloeb"].metadata["loebenummer_over_1"] == 0
    assert (
        scalar(
            staging_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont WHERE helbredsforloeb_noegle = ? "
            "AND dw_timestamp_update > ?",
            hf4,
            cp,
        )
        == 0
    )
    assert (
        scalar(
            staging_db,
            f"SELECT helbredsforloeb_id FROM {PRE_HF_TABLE} WHERE helbredsforloeb_noegle = ?",
            hf2,
        )
        == "250"
    )

    # noeglestabilitet: HF1's F_ID er nu 9001, men clean_code_Udredningsforloeb er
    # IDENTISK med leverance A's (loebenummer pr. startdato, aldrig F_ID)
    p1 = raekker(
        staging_db,
        "SELECT clean_code_Udredningsforloeb, f_id, loebenummer, startdato, slutdato, "
        "slut_markoer_uuid, alder_raa, antal_markoerer, antal_udredningsplaner "
        f"FROM {PRE_FORLOEB_TABLE} WHERE helbredsforloeb_noegle = ?",
        hf1,
    )
    assert len(p1) == 1
    assert p1[0][0] == TILSTAND["hf1_clean_code_a"], "clean_code aendrede sig ved omnummerering"
    assert p1[0][1:6] == ("9001", 1, "2026-01-05", "2026-02-01", OBJ["j"])
    # B's genudstedte slutmarkoer (j) baerer ingen ALDER; ny udredningsplan taelles med
    assert p1[0][6:] == (None, 6, 3)
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} WHERE clean_code_Udredningsforloeb = ?",
            TILSTAND["hf1_clean_code_a"],
        )
        == 1
    )
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_FORLOEB_TABLE}") == 3
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_FORLOEB_TABLE} WHERE f_id = '5001'") == 0
    # HF3 har mistet afbrydelsen: taelleren er 0 igen og seneste markoer er henvisningen
    assert raekker(
        staging_db,
        f"SELECT antal_markoerer, antal_afbrydelser, seneste_markoer_dato FROM {PRE_FORLOEB_TABLE} "
        "WHERE helbredsforloeb_noegle = ?",
        hf3,
    ) == [(1, 0, "2026-04-01")]

    # soft-delete: HF3's afbrydelse stages med dw_is_deleted = 1 og beholder sin clean_code
    assert raekker(
        staging_db,
        "SELECT dw_is_deleted, clean_code_UdredningsretMarkoer, clean_code_Udredningsforloeb "
        f"FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid = ?",
        OBJ["i"],
    ) == [(True, TILSTAND["clean_code_markoer_i"], f"{FORLOEB_PREFIX}{hf3}_20260401_1")]
    assert resultater["stg_fct_UdredningsretMarkoer"].metadata["soft_slettede"] == 1
    # den soft-slettede afbrydelse taeller i den samlede kilderaekke-sum, ikke i den levende
    assert (
        resultater["stg_fct_UdredningsretMarkoer"].metadata["sum_antal_kilderaekker"]
        - resultater["stg_fct_UdredningsretMarkoer"].metadata["sum_antal_kilderaekker_levende"]
        == 1
    )
    # raekketal = arc for de scopede helbredsforloeb (levende OG soft-slettede)
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}") == scalar(
        staging_db,
        "SELECT COUNT(*) FROM arc.udredningsret_mont a "
        f"WHERE EXISTS (SELECT 1 FROM {PRE_HF_TABLE} h "
        "WHERE h.helbredsforloeb_noegle = a.helbredsforloeb_noegle)",
    )
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}") == 10
    # den genudstedte slutmarkoer (d -> j) er den SAMME fact-raekke (arc's noegle)
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} WHERE markoer_uuid IN (?, ?)",
            OBJ["d"],
            OBJ["j"],
        )
        == 1
    )
    assert (
        scalar(
            staging_db,
            f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} WHERE clean_code_Udredningsforloeb = ?",
            TILSTAND["hf1_clean_code_a"],
        )
        == 6
    )
    # dimensionen er ikke delta-scoped: den nye plankode er med, den soft-slettede
    # afbrydelse (ingen levende raekker) er ude
    dim_koder = {r[0] for r in raekker(staging_db, f"SELECT kode FROM {DIM_FORLOEBSMARKOER_TABLE}")}
    assert "AFX01D1" in dim_koder and "AFV01X2" not in dim_koder
    assert (
        scalar(
            staging_db,
            f"SELECT gruppe FROM {DIM_FORLOEBSMARKOER_TABLE} WHERE kode = 'AFX01D1'",
        )
        == "udredningsplan"
    )


# ---------------------------------------------------------------------------
# 8. Checkpoints i fremtiden: delta-scopet filtrerer virkelig
# ---------------------------------------------------------------------------


def test_8_checkpoint_i_fremtiden_stager_intet(staging_db):
    saet_downstream_checkpoints(staging_db, "2099-01-01 00:00:00")
    resultater = koer_kaeden(staging_db, mode="delta")
    for tabel in (
        PRE_HF_TABLE,
        PRE_FORLOEB_TABLE,
        FCT_MARKOER_TABLE,
        FCT_FORLOEB_TABLE,
        DIM_EGENSKABER_TABLE,
    ):
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {tabel}") == 0, tabel
    assert resultater["pre_stg_Helbredsforloeb"].metadata["row_count"] == 0
    assert resultater["stg_fct_UdredningsretMarkoer"].metadata["row_count"] == 0
    # kodelisten er bevidst ikke delta-scoped
    assert scalar(staging_db, f"SELECT COUNT(*) FROM {DIM_FORLOEBSMARKOER_TABLE}") == 6


# ---------------------------------------------------------------------------
# 9. Retireringskanalen: et helbredsforloeb, hvis markoerer ALLE er soft-slettede
# ---------------------------------------------------------------------------


def test_9_helt_soft_slettet_helbredsforloeb_stages_som_retirering(staging_db):
    """Spec 3.1 (sidste punkt), 3.2, 3.5 og 6: HF4's eneste markoer soft-slettes i arc
    (som archive_merge goer det: dw_is_deleted = 1 + tidsstempel). Helbredsforloebet
    kommer i scope, stages med dw_is_deleted = 1, 0 forloeb og taellere 0 med borgeren
    taget over de slettede raekker; pre_Udredningsforloeb har ingen raekke for det, og
    markoerfacten baerer raekken med dw_is_deleted = 1 og forloebs-clean_code NULL,
    fordi F_ID-gruppen ikke findes laengere (den 'foraeldede F_ID'-sti)."""
    cp = TILSTAND["cp_efter_b"]
    hf4 = hf_noegle(staging_db, ELEM_4)
    vent_paa_nyt_db_sekund(staging_db, cp)
    run_sql(
        staging_db,
        "UPDATE arc.udredningsret_mont SET dw_is_deleted = 1, dw_timestamp_update = SYSDATETIME() "
        "WHERE helbredsforloeb_noegle = ?",
        hf4,
    )
    assert (
        scalar(
            staging_db,
            "SELECT COUNT(*) FROM arc.udredningsret_mont "
            "WHERE helbredsforloeb_noegle = ? AND dw_is_deleted = 0",
            hf4,
        )
        == 0
    )

    saet_downstream_checkpoints(staging_db, cp)
    resultater = koer_kaeden(staging_db, mode="delta")

    # scope = praecis HF4; staged som retirering med borgeren fra de slettede raekker
    assert raekker(
        staging_db,
        "SELECT helbredsforloeb_noegle, dw_is_deleted, antal_markoerer, antal_forloebselementer, "
        "antal_udredningsforloeb, dw_ek_Borger, borger_opslag, foerste_markoer_kode, "
        f"foerste_markoer_dato, helbredsforloeb_id, clean_code_Helbredsforloeb FROM {PRE_HF_TABLE}",
    ) == [(hf4, True, 0, 0, 0, -1, "ukendt", None, None, None, f"{HF_PREFIX}{hf4}")]
    md_hf = resultater["pre_stg_Helbredsforloeb"].metadata
    assert md_hf["row_count"] == 1
    assert md_hf["helbredsforloeb_uden_levende_markoerer"] == 1
    assert md_hf["borger_opslag"] == {"ukendt": 1}

    # kun levende markoerer danner forloeb: intet forloeb, ingen fact-/dim-raekke
    for tabel in (PRE_FORLOEB_TABLE, FCT_FORLOEB_TABLE, DIM_EGENSKABER_TABLE):
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {tabel}") == 0, tabel
    assert resultater["pre_stg_Udredningsforloeb"].metadata["row_count"] == 0

    # markoerfacten: HF4's raekke med dw_is_deleted = 1, forloebs-clean_code NULL
    # (gruppen findes ikke), hf/borger sat, F_ID baeres som egenskab
    assert raekker(
        staging_db,
        "SELECT dw_is_deleted, clean_code_Udredningsforloeb, clean_code_Helbredsforloeb, "
        "dw_ek_Borger, borger_opslag, f_id, markoer_kode, clean_code_Forloebsmarkoer, "
        f"antal_kilderaekker FROM {FCT_MARKOER_TABLE}",
    ) == [
        (
            True,
            None,
            f"{HF_PREFIX}{hf4}",
            -1,
            "ukendt",
            "5004",
            "AFV01A1",
            f"{FORLOEBSMARKOER_PREFIX}AFV01A1",
            1,
        )
    ]
    md_markoer = resultater["stg_fct_UdredningsretMarkoer"].metadata
    assert md_markoer["row_count"] == 1
    assert md_markoer["soft_slettede"] == 1
    assert md_markoer["markoerer_uden_forloeb"] == 1
    assert (md_markoer["sum_antal_kilderaekker"], md_markoer["sum_antal_kilderaekker_levende"]) == (
        1,
        0,
    )
    # koden lever videre i dimensionen paa de andre helbredsforloebs levende raekker
    assert (
        scalar(
            staging_db, f"SELECT COUNT(*) FROM {DIM_FORLOEBSMARKOER_TABLE} WHERE kode = 'AFV01A1'"
        )
        == 1
    )


# ---------------------------------------------------------------------------
# 10. Borger-genopretning i delta-scope (U9): facten traekker helbredsforloebet ind
# ---------------------------------------------------------------------------

EK_HF_GENOPRETNING = 9100004  # syntetisk map-/fact-noegle; ryddes i testens finally


def _opret_hvis_mangler(resource, tabel: str, kolonner_sql: str) -> bool:
    """CREATE TABLE hvis den ikke findes; True naar testen selv oprettede den."""
    if scalar(resource, f"SELECT OBJECT_ID('{tabel}', 'U')") is not None:
        return False
    run_sql(resource, f"CREATE TABLE {tabel} ({kolonner_sql})")
    return True


def test_10_borger_genopretning_traekker_helbredsforloeb_ind_via_facten(staging_db):
    """Delta-scopets gren (2), spec 3.1/U9: med checkpoints i fremtiden bidrager arc
    intet, men et helbredsforloeb, hvis levende fct.Udredningsforloeb-raekke staar
    med dw_ek_Borger = -1, kommer alligevel i scope — noeglen genskabes CHAR(64)-
    praecist fra map.Helbredsforloeb's clean_code. Loest borger eller retireret
    fact-raekke -> ude igen. Indtil US-4's DDL ligger i tables/udredningsret bygges
    map/fct minimalt og droppes igen, hvis testen selv oprettede dem."""
    hf4 = hf_noegle(staging_db, ELEM_4)
    map_tabel, fct_tabel = "[map].[Helbredsforloeb]", "[fct].[Udredningsforloeb]"
    oprettet_map = _opret_hvis_mangler(
        staging_db,
        map_tabel,
        "[dw_ek_Helbredsforloeb] INT NOT NULL, [clean_code_Helbredsforloeb] VARCHAR(80) NOT NULL",
    )
    oprettet_fct = _opret_hvis_mangler(
        staging_db,
        fct_tabel,
        "[dw_ek_Udredningsforloeb] INT NOT NULL, [dw_ek_Helbredsforloeb] INT NOT NULL, "
        "[dw_ek_Borger] INT NOT NULL, [dw_is_deleted] BIT NOT NULL",
    )
    try:
        # US-4's rigtige map har IDENTITY paa dw_ek — saa saettes IDENTITY_INSERT i batchen
        run_sql(
            staging_db,
            f"IF OBJECTPROPERTY(OBJECT_ID('{map_tabel}'), 'TableHasIdentity') = 1 "
            f"SET IDENTITY_INSERT {map_tabel} ON; "
            f"INSERT INTO {map_tabel} ([dw_ek_Helbredsforloeb], [clean_code_Helbredsforloeb]) "
            "VALUES (?, ?); "
            f"IF OBJECTPROPERTY(OBJECT_ID('{map_tabel}'), 'TableHasIdentity') = 1 "
            f"SET IDENTITY_INSERT {map_tabel} OFF;",
            EK_HF_GENOPRETNING,
            f"{HF_PREFIX}{hf4}",
        )
        run_sql(
            staging_db,
            f"INSERT INTO {fct_tabel} ([dw_ek_Udredningsforloeb], [dw_ek_Helbredsforloeb], "
            "[dw_ek_Borger], [dw_is_deleted]) VALUES (?, ?, -1, 0)",
            EK_HF_GENOPRETNING,
            EK_HF_GENOPRETNING,
        )
        saet_downstream_checkpoints(staging_db, "2099-01-01 00:00:00")

        resultater = koer_kaeden(staging_db, mode="delta")
        assert raekker(
            staging_db,
            f"SELECT helbredsforloeb_noegle, clean_code_Helbredsforloeb FROM {PRE_HF_TABLE}",
        ) == [(hf4, f"{HF_PREFIX}{hf4}")]
        assert resultater["pre_stg_Helbredsforloeb"].metadata["row_count"] == 1
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}") == 1

        # borgeren er loest siden sidst -> ikke laengere i scope
        run_sql(
            staging_db,
            f"UPDATE {fct_tabel} SET [dw_ek_Borger] = ? WHERE [dw_ek_Helbredsforloeb] = ?",
            EK_BORGER_X,
            EK_HF_GENOPRETNING,
        )
        koer_kaeden(staging_db, mode="delta")
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_HF_TABLE}") == 0

        # en retireret fact-raekke med ukendt borger traekker heller ikke ind
        run_sql(
            staging_db,
            f"UPDATE {fct_tabel} SET [dw_ek_Borger] = -1, [dw_is_deleted] = 1 "
            "WHERE [dw_ek_Helbredsforloeb] = ?",
            EK_HF_GENOPRETNING,
        )
        koer_kaeden(staging_db, mode="delta")
        assert scalar(staging_db, f"SELECT COUNT(*) FROM {PRE_HF_TABLE}") == 0
    finally:
        run_sql(
            staging_db,
            f"DELETE FROM {fct_tabel} WHERE [dw_ek_Helbredsforloeb] = ?",
            EK_HF_GENOPRETNING,
        )
        run_sql(
            staging_db,
            f"DELETE FROM {map_tabel} WHERE [dw_ek_Helbredsforloeb] = ?",
            EK_HF_GENOPRETNING,
        )
        if oprettet_fct:
            run_sql(staging_db, f"DROP TABLE {fct_tabel}")
        if oprettet_map:
            run_sql(staging_db, f"DROP TABLE {map_tabel}")
