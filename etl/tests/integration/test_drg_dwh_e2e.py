# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E-integrationstest for DRG map/dim/fct-laget mod devcontainer-MSSQL.

Bygger paa staging-modulets fixture (frisk arc + dim.Borger/Organisation +
DDL, som ogsaa opretter map/dim/fct-tabellerne og deres seeds) og
materialiserer hele kaeden arc -> pre_stg -> stg -> map -> dim -> fct.

Kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true.
"""

import dagster as dg
import pytest
from core.dfm_config import RunModeConfig
from DSA.SQL.dim.dim_DRGKontaktEgenskaber import dim_DRGKontaktEgenskaber
from DSA.SQL.dim.dim_DRGKontaktKlassifikation import dim_DRGKontaktKlassifikation
from DSA.SQL.fct.fct_DRGKontakt import fct_DRGKontakt
from DSA.SQL.map.map_DRGDiagnoseGruppe import map_DRGDiagnoseGruppe
from DSA.SQL.map.map_DRGKontakt import map_DRGKontakt
from DSA.SQL.map.map_DRGKontaktKlassifikation import map_DRGKontaktKlassifikation
from DSA.SQL.map.map_DRGProcedureGruppe import map_DRGProcedureGruppe
from DSA.SQL.staging.stg_bridge_DRGKontaktDiagnose import stg_bridge_DRGKontaktDiagnose
from DSA.SQL.staging.stg_bridge_DRGKontaktProcedure import stg_bridge_DRGKontaktProcedure
from DSA.SQL.staging.stg_DRGDiagnoseGruppe import stg_DRGDiagnoseGruppe
from DSA.SQL.staging.stg_DRGProcedureGruppe import stg_DRGProcedureGruppe

# Genbruger staging-modulets fixture og hjaelpere: staging_db saetter frisk
# arc-grundlag op og koerer DDL-mappen, som ogsaa opretter dwh-tabellerne.
from tests.integration.test_drg_staging_e2e import (  # noqa: F401
    CPR_KENDT,
    CPR_TIDLIGERE,
    PSY_A,
    UUID_A,
    UUID_B,
    _koer_kaeden,
    require_real_environment,
    run_sql,
    scalar,
    staging_db,
)

pytestmark = pytest.mark.integration

# tabel -> dens noegle (egenskabsdimensionen deler PK med facten)
_DWH_TABELLER = (
    ("[fct].[DRGKontakt]", None),
    ("[dim].[DRGKontaktEgenskaber]", "dw_ek_DRGKontakt"),
    ("[dim].[DRGKontaktKlassifikation]", "dw_ek_DRGKontaktKlassifikation"),
    ("[map].[DRGKontakt]", "dw_ek_DRGKontakt"),
    ("[map].[DRGKontaktKlassifikation]", "dw_ek_DRGKontaktKlassifikation"),
)


@pytest.fixture(scope="module", autouse=True)
def dwh_db(staging_db):  # noqa: F811
    """Toem dwh-lagets tabeller (sentinel-raekkerne bevares) foer modulet."""
    for tabel, noegle in _DWH_TABELLER:
        if noegle is None:
            run_sql(staging_db, f"DELETE FROM {tabel}")
        else:
            run_sql(staging_db, f"DELETE FROM {tabel} WHERE [{noegle}] <> -1")
    return staging_db


def _koer_dwh(res):
    """Materialiser map -> dim -> fct oven paa den stagede batch."""
    cfg = RunModeConfig()
    resultater = {}
    for navn, fn in (
        ("map_DRGKontakt", map_DRGKontakt),
        ("map_DRGKontaktKlassifikation", map_DRGKontaktKlassifikation),
        ("dim_DRGKontaktEgenskaber", dim_DRGKontaktEgenskaber),
        ("dim_DRGKontaktKlassifikation", dim_DRGKontaktKlassifikation),
        # fct.DRGKontakt now resolves its code-set group FKs from the group
        # staging + group maps; build them first (empty here -> group -1).
        ("stg_bridge_DRGKontaktDiagnose", stg_bridge_DRGKontaktDiagnose),
        ("stg_bridge_DRGKontaktProcedure", stg_bridge_DRGKontaktProcedure),
        ("stg_DRGDiagnoseGruppe", stg_DRGDiagnoseGruppe),
        ("stg_DRGProcedureGruppe", stg_DRGProcedureGruppe),
        ("map_DRGDiagnoseGruppe", map_DRGDiagnoseGruppe),
        ("map_DRGProcedureGruppe", map_DRGProcedureGruppe),
        ("fct_DRGKontakt", fct_DRGKontakt),
    ):
        resultater[navn] = fn(dg.build_asset_context(), res, cfg)
    return resultater


def _koer_hele_kaeden(res):
    _koer_kaeden(res)
    return _koer_dwh(res)


def test_1_hele_kaeden_materialiserer_med_korrekt_grain(dwh_db):
    # takstkataloget skal have et medlem for kontakternes DRG-kode
    run_sql(
        dwh_db,
        "INSERT INTO [arc].[drg_takstbeskrivelse] ([takstsystem],[DRG],[BESKRIVELSE],"
        "[leverance_dato],[dw_timestamp_insert],[dw_timestamp_update],[dw_is_deleted]) "
        "VALUES ('T26','01MA01','Testgruppe','2026-08-23',SYSDATETIME(),SYSDATETIME(),0)",
    )
    _koer_hele_kaeden(dwh_db)
    assert scalar(dwh_db, "SELECT COUNT(*) FROM [fct].[DRGKontakt]") == 3
    assert (
        scalar(dwh_db, "SELECT COUNT(DISTINCT [clean_code_DRGKontakt]) FROM [fct].[DRGKontakt]")
        == 3
    )
    # delt PK: hver fact-raekke har sin dim-raekke
    assert (
        scalar(
            dwh_db,
            "SELECT COUNT(*) FROM [fct].[DRGKontakt] f "
            "JOIN [dim].[DRGKontaktEgenskaber] d ON d.[dw_ek_DRGKontakt] = f.[dw_ek_DRGKontakt]",
        )
        == 3
    )


def test_2_noeglestabilitet_paa_tvaers_af_leverancer(dwh_db):
    """En ny leverance aendrer maal, men aldrig dw_ek for en kendt kontakt."""
    foer = {
        r[0]: r[1]
        for r in _rows(
            dwh_db,
            "SELECT d.[clean_code_DRGKontakt], f.[dw_ek_DRGKontakt] "
            "FROM [fct].[DRGKontakt] f "
            "JOIN [dim].[DRGKontaktEgenskaber] d ON d.[dw_ek_DRGKontakt] = f.[dw_ek_DRGKontakt]",
        )
    }
    # simuler en genlevering: prisen revideres og arkivraekken roeres
    run_sql(
        dwh_db,
        "UPDATE [arc].[drg_kontakt] SET [TOTALPRIS_DRG] = '175', "
        "[dw_timestamp_update] = SYSDATETIME() WHERE [KONTAKT_VAERDI] = ?",
        UUID_A,
    )
    _koer_hele_kaeden(dwh_db)

    efter = {
        r[0]: r[1]
        for r in _rows(
            dwh_db,
            "SELECT d.[clean_code_DRGKontakt], f.[dw_ek_DRGKontakt] "
            "FROM [fct].[DRGKontakt] f "
            "JOIN [dim].[DRGKontaktEgenskaber] d ON d.[dw_ek_DRGKontakt] = f.[dw_ek_DRGKontakt]",
        )
    }
    assert efter == foer, "dw_ek_DRGKontakt skal vaere stabil paa tvaers af leverancer"
    assert scalar(dwh_db, "SELECT COUNT(*) FROM [fct].[DRGKontakt]") == 3
    ny_pris = scalar(
        dwh_db,
        "SELECT f.[totalpris_drg] FROM [fct].[DRGKontakt] f "
        "JOIN [dim].[DRGKontaktEgenskaber] d ON d.[dw_ek_DRGKontakt] = f.[dw_ek_DRGKontakt] "
        "WHERE d.[clean_code_DRGKontakt] LIKE '%' + ?",
        UUID_A,
    )
    assert float(ny_pris) == 175.0, "prisrevisionen skal opdatere in place"


def test_3_klassifikation_baerer_drg_kode_og_takstfelter(dwh_db):
    # DRG-koden og de denormaliserede takstfelter baeres af
    # klassifikationsdimensionen — der er ingen selvstaendig dim.DRG
    klass_ek = scalar(
        dwh_db,
        "SELECT f.[dw_ek_DRGKontaktKlassifikation] FROM [fct].[DRGKontakt] f "
        "WHERE f.[leverancetype] = 'DRG' AND f.[tungeste_drgkontakt] = '1'",
    )
    assert klass_ek > 0
    drg_kode, beskrivelse, takst = _rows(
        dwh_db,
        "SELECT [drg_kode], [drg_beskrivelse], [standardtakst] "
        "FROM [dim].[DRGKontaktKlassifikation] "
        "WHERE [dw_ek_DRGKontaktKlassifikation] = ?",
        klass_ek,
    )[0]
    assert drg_kode == "01MA01"
    assert beskrivelse == "Testgruppe"
    assert takst is not None

    # psyk er ikke DRG-grupperet: klassifikationsmedlemmet har tom DRG-kode
    psyk_klass = scalar(
        dwh_db,
        "SELECT k.[drg_kode] FROM [fct].[DRGKontakt] f "
        "JOIN [dim].[DRGKontaktKlassifikation] k "
        "  ON k.[dw_ek_DRGKontaktKlassifikation] = f.[dw_ek_DRGKontaktKlassifikation] "
        "WHERE f.[leverancetype] = 'DRGPSYK'",
    )
    assert psyk_klass in (None, "")

    # aktivitetsaar er typet som aarstal paa somatik-facten; psyk-leverancen
    # baerer intet aktivitetsaar og staar som NULL
    assert (
        scalar(
            dwh_db,
            "SELECT COUNT(*) FROM [fct].[DRGKontakt] "
            "WHERE [leverancetype] = 'DRG' AND [aktivitetsaar] IS NULL",
        )
        == 0
    )


def test_4_kontroltotal_brud_opdages(dwh_db):
    """Arkivet aendres efter fact-bygningen -> skive-afvigelse fejler koerslen."""
    run_sql(
        dwh_db,
        "UPDATE [arc].[drg_kontakt] SET [TOTALPRIS_DRG] = '99999' WHERE [KONTAKT_VAERDI] = ?",
        UUID_A,
    )
    with pytest.raises(dg.Failure, match="kontroltotalen holder ikke"):
        fct_DRGKontakt(dg.build_asset_context(), dwh_db, RunModeConfig())
    # genskab konsistens (uden at roere dw_timestamp_update: staging er
    # allerede i sync med den gamle vaerdi)
    run_sql(
        dwh_db,
        "UPDATE [arc].[drg_kontakt] SET [TOTALPRIS_DRG] = '175' WHERE [KONTAKT_VAERDI] = ?",
        UUID_A,
    )


def test_5_soft_delete_retirerer_fact_raekken(dwh_db):
    """En tilbagetrukket kontakt soft-slettes i arc, genstages og retireres i
    facten — og kontroltotallen er groen igen bagefter (levende == levende)."""
    run_sql(
        dwh_db,
        "UPDATE [arc].[drg_kontakt] SET [dw_is_deleted] = 1, "
        "[dw_timestamp_update] = SYSDATETIME() WHERE [KONTAKT_VAERDI] = ?",
        UUID_B,
    )
    _koer_hele_kaeden(dwh_db)
    retireret = scalar(
        dwh_db,
        "SELECT f.[dw_is_deleted] FROM [fct].[DRGKontakt] f "
        "JOIN [dim].[DRGKontaktEgenskaber] d ON d.[dw_ek_DRGKontakt] = f.[dw_ek_DRGKontakt] "
        "WHERE d.[clean_code_DRGKontakt] LIKE '%' + ?",
        UUID_B,
    )
    assert retireret == 1
    assert scalar(dwh_db, "SELECT COUNT(*) FROM [fct].[DRGKontakt]") == 3


def test_6_grain_brud_opdages(dwh_db):
    """To stg-raekker med samme clean_code skal faelde fact-koerslen.

    Stagingens eget unikke index forhindrer dubletten ad normal vej, saa
    testen maa fjerne vaernet for at bevise at fact-laget har sit eget."""
    run_sql(dwh_db, "DROP INDEX [IX_fct_DRGKontakt_PK] ON [stg].[fct_DRGKontakt]")
    run_sql(
        dwh_db,
        "INSERT INTO [stg].[fct_DRGKontakt] SELECT * FROM [stg].[fct_DRGKontakt] "
        "WHERE [kontakt_uuid] = ? AND [leverancetype] = 'DRG'",
        UUID_A,
    )
    try:
        with pytest.raises(ValueError, match="grain"):
            fct_DRGKontakt(dg.build_asset_context(), dwh_db, RunModeConfig())
    finally:
        run_sql(
            dwh_db,
            "WITH d AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY [clean_code_DRGKontakt] "
            "ORDER BY (SELECT NULL)) AS rn FROM [stg].[fct_DRGKontakt] WHERE [kontakt_uuid] = ?) "
            "DELETE FROM d WHERE rn > 1",
            UUID_A,
        )


def test_7_ingen_cpr_vaerdier_i_dwh_laget(dwh_db):
    """GDPR: hverken kolonnenavne eller de seedede CPR-vaerdier maa findes i
    map/dim/fct-laget."""
    n = scalar(
        dwh_db,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA IN ('fct','dim','map') "
        "AND TABLE_NAME IN ('DRGKontakt','DRGKontaktEgenskaber',"
        "'DRGKontaktKlassifikation','DRG') AND COLUMN_NAME LIKE '%cpr%'",
    )
    assert n == 0
    for tabel in ("[dim].[DRGKontaktEgenskaber]", "[dim].[DRGKontaktKlassifikation]"):
        kolonner = _rows(
            dwh_db,
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' = ? "
            "AND DATA_TYPE IN ('nvarchar','varchar','char','nchar')",
            tabel,
        )
        for (kolonne,) in kolonner:
            for cpr in (CPR_KENDT, CPR_TIDLIGERE):
                fund = scalar(
                    dwh_db,
                    f"SELECT COUNT(*) FROM {tabel} WHERE [{kolonne}] LIKE '%' + ? + '%'",
                    cpr,
                )
                assert fund == 0, f"CPR-vaerdi fundet i {tabel}.{kolonne}"


def _rows(resource, sql, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            return cursor.fetchall()
        finally:
            cursor.close()
