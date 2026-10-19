# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""E2E-integrationstest for DRG-staging-kaeden mod devcontainer-MSSQL.

Koerer arc -> pre_stg -> stg_fct/stg_dim mod syntetiske arkivdata og
verificerer acceptkriterierne:

1. GRAIN: én raekke pr. clean_code i alle tre stg-tabeller (fysisk unikt indeks)
2. GDPR: ingen kolonne med CPR i navnet i nogen stg-tabel; dw_ek_Borger
   resolvet med tidligere_cpr-fallback og -1/-2-sentinels
3. SOR pr. rolle: -1 ved ukendt, raa kode bevaret, tom/udfyldt-uden-match skilt
4. Forloebsmaal ligger KUN paa den baerende kontakt (somatik: tungeste;
   psyk: forloebets foerste kontakt) og kontroltotalen mod arc holder —
   inkl. at et konstrueret brud FEJLER koerslen
5. Dato-noegler via dato_key() med sentinel for tom dato

Kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true (devcontainer).
"""

import os

import dagster as dg
import pytest
from DSA.SQL.staging.drg_staging_sql import (
    build_delta_scope_sql,
    build_forloeb_kontroltotal_sql,
    build_pre_stg_sql,
    build_stg_dim_sql,
    build_stg_fct_sql,
    build_unresolved_borger_scope_sql,
    create_borger_cpr_lookup_tables_sql,
    create_organisation_sor_lookup_table_sql,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module", autouse=True)
def require_real_environment():
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not (use_real and writes):
        pytest.skip("kraever USE_REAL_DB_FOR_TESTS=true og ENABLE_TEST_WRITES=true")


UUID_A = "AAAAAAAA-1111-0000-0000-000000000001"  # tungeste, kendt CPR
UUID_B = "BBBBBBBB-1111-0000-0000-000000000002"  # ikke-tungeste, tidligere_cpr
PSY_A = "DDDDDDDD-1111-0000-0000-000000000001"  # psyk, tom CPR

CPR_KENDT = "0101011111"
CPR_TIDLIGERE = "0202022222"  # findes KUN som tidligere_cpr i dim.Borger
SOR_KENDT = "123456789012345"
SOR_UKENDT = "999999999999999"

EK_BORGER_A = 9000001
EK_BORGER_B = 9000003
EK_ORG = 9000002


def run_sql(resource, sql, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            conn.commit()
        finally:
            cursor.close()


def scalar(resource, sql, *params):
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, *params)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


@pytest.fixture(scope="module")
def staging_db(real_mssql_target_staging_resource):
    """Syntetisk arc-grundlag + dim-opslagsraekker; stg-tabellerne droppes."""
    from DSI.csv_files.drg_datasets import DATASETS
    from infrastructure.utils.ddl_loader import (
        execute_sql_file,
        execute_sql_files_from_directory,
    )

    res = real_mssql_target_staging_resource
    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for t in ("pre_DRGKontakt", "fct_DRGKontakt", "dim_DRGKontaktEgenskaber"):
                cursor.execute(f"DROP TABLE IF EXISTS [stg].[{t}]")
            for d in DATASETS:
                cursor.execute(f"DROP VIEW IF EXISTS [ext].[v_{d.ext_table}]")
                cursor.execute(f"DROP TABLE IF EXISTS [ext].[{d.ext_table}]")
                cursor.execute(f"DROP TABLE IF EXISTS [arc].[{d.ext_table}]")
            cursor.execute("DROP TABLE IF EXISTS [arc].[drg_pris_revision]")
            cursor.execute("DROP TABLE IF EXISTS [meta].[DRGLeverance]")
            # testens dim-raekker (hoeje noegler; ryddes i teardown)
            cursor.execute(
                "DELETE FROM [dim].[Borger] WHERE [dw_ek_Borger] IN (?, ?)",
                EK_BORGER_A,
                EK_BORGER_B,
            )
            cursor.execute(
                "DELETE FROM [dim].[Organisation] WHERE [dw_ek_Organisation] = ?", EK_ORG
            )
            cursor.execute(
                "INSERT INTO [dim].[Borger] ([dw_sk_Borger],[dw_ek_Borger],[cpr_nummer],"
                "[dw_valid_from],[dw_valid_to]) VALUES (?, ?, ?, '1900-01-01', '9999-12-31')",
                EK_BORGER_A,
                EK_BORGER_A,
                CPR_KENDT,
            )
            cursor.execute(
                "INSERT INTO [dim].[Borger] "
                "([dw_sk_Borger],[dw_ek_Borger],[cpr_nummer],[tidligere_cpr],"
                "[dw_valid_from],[dw_valid_to]) VALUES (?, ?, ?, ?, '1900-01-01', '9999-12-31')",
                EK_BORGER_B,
                EK_BORGER_B,
                "9999999999",  # nuvaerende CPR — leverancen baerer den TIDLIGERE
                CPR_TIDLIGERE,
            )
            cursor.execute(
                "INSERT INTO [dim].[Organisation] ([dw_ek_Organisation],[kode],[navn]) "
                "VALUES (?, ?, ?)",
                EK_ORG,
                SOR_KENDT,
                "Teststedet",
            )
            conn.commit()
        finally:
            cursor.close()

    execute_sql_file(res, "migrations/20260708_rename_00_meta.sql")
    execute_sql_files_from_directory(res, "tables/drg")
    # US-5 group-key FKs on an already-created fct.DRGKontakt (CREATE-IF-NOT-EXISTS
    # above won't add columns to an existing table).
    execute_sql_file(res, "migrations/20260830_drg_fct_gruppe_keys.sql")

    # Syntetisk arc-grundlag: 2 somatik-kontakter i ét forloeb + 1 psyk.
    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for uuid, cpr, sygehus, tungeste, pris in (
                (UUID_A, CPR_KENDT, SOR_KENDT, "1", "100"),
                (UUID_B, CPR_TIDLIGERE, SOR_UKENDT, "0", "200"),
            ):
                cursor.execute(
                    "INSERT INTO [arc].[drg_kontakt] ([takstsystem],[KONTAKT_VAERDI],"
                    "[sygehusforloeb_noegle],[CPR],[SYGEHUS],[HENVISENDEINSTANS],"
                    "[TUNGESTE_DRGKONTAKT],[UDFALDSGIVENDE_KONTAKT],[AKTIVITETSAAR],"
                    "[TOTALPRIS_DRG],[LIGGEDAGE],[INDTIDSPUNKT_DRGKONTAKT],[DRG],"
                    "[PRIS_DRG],"
                    "[leverance_dato],[dw_timestamp_insert],[dw_timestamp_update]) VALUES "
                    "('T26', ?, ?, ?, ?, '', ?, ?, '2025', ?, '3', "
                    "'2025-03-01 10:00:00', '01MA01', '100', "
                    "'2026-08-24', SYSDATETIME(), SYSDATETIME())",
                    uuid,
                    UUID_A,  # forloebsnoeglen = ind-kontaktens UUID (= A)
                    cpr,
                    sygehus,
                    tungeste,
                    tungeste,
                    pris,
                )
            cursor.execute(
                "INSERT INTO [arc].[drg_sghforlob] ([takstsystem],[ind_kontakt_uuid],"
                "[AKTIVITETSAAR],[TOTALPRIS_DRGSGHF],[PRIS_DRG_DRGSGHF],[ANTAL_I_FORLOB],"
                "[leverance_dato],[dw_timestamp_insert],[dw_timestamp_update]) VALUES ('T26', ?, '2025', '300', '300', '2', '2026-08-24', SYSDATETIME(), SYSDATETIME())",
                UUID_A,
            )
            cursor.execute(
                "INSERT INTO [arc].[drgpsyk_kontakt] ([takstsystem],[KONTAKT_VAERDI],"
                "[forloeb_noegle],[CPRnummer],[SYGEHUS],[KONTAKT_ENHED],"
                "[INDTIDSPUNKT_KONTAKT],[leverance_dato],[dw_timestamp_insert],[dw_timestamp_update]) VALUES "
                "('T26', ?, ?, '', ?, ?, '', '2026-08-12', SYSDATETIME(), SYSDATETIME())",
                PSY_A,
                PSY_A,
                SOR_KENDT,
                SOR_KENDT,
            )
            cursor.execute(
                "INSERT INTO [arc].[drgpsyk_forloeb] ([takstsystem],[foerste_kontakt_uuid],"
                "[PRIS_PsykiatriForloeb],[LIGGEDAGE_Forloeb],[leverance_dato],"
                "[dw_timestamp_insert],[dw_timestamp_update]) "
                "VALUES ('T26', ?, '1000', '5', '2026-08-12', SYSDATETIME(), SYSDATETIME())",
                PSY_A,
            )
            conn.commit()
        finally:
            cursor.close()

    yield res

    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM [dim].[Borger] WHERE [dw_ek_Borger] IN (?, ?)",
                EK_BORGER_A,
                EK_BORGER_B,
            )
            cursor.execute(
                "DELETE FROM [dim].[Organisation] WHERE [dw_ek_Organisation] = ?", EK_ORG
            )
            conn.commit()
        finally:
            cursor.close()


def _koer_kaeden(res):
    with res.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(create_borger_cpr_lookup_tables_sql())
            cursor.execute(build_pre_stg_sql(None))
            cursor.execute(create_organisation_sor_lookup_table_sql())
            cursor.execute(build_stg_fct_sql())
            cursor.execute(build_stg_dim_sql())
            conn.commit()
        finally:
            cursor.close()


def test_1_kaeden_koerer_og_grainen_holder(staging_db):
    _koer_kaeden(staging_db)
    for t in ("pre_DRGKontakt", "fct_DRGKontakt", "dim_DRGKontaktEgenskaber"):
        total = scalar(staging_db, f"SELECT COUNT(*) FROM [stg].[{t}]")
        distinkte = scalar(
            staging_db, f"SELECT COUNT(DISTINCT [clean_code_DRGKontakt]) FROM [stg].[{t}]"
        )
        assert total == 3, f"{t}: {total} raekker (2 somatik + 1 psyk ventet)"
        assert total == distinkte, f"{t}: grain brudt"


def test_2_ingen_raa_cpr_i_nogen_stg_tabel(staging_db):
    n = scalar(
        staging_db,
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'stg' "
        "AND TABLE_NAME IN ('pre_DRGKontakt','fct_DRGKontakt','dim_DRGKontaktEgenskaber') "
        "AND COLUMN_NAME LIKE '%cpr%'",
    )
    assert n == 0
    # og VAERDI-baseret: et CPR gemt under et andet kolonnenavn ville slippe
    # forbi navnescannet — scan alle tekstkolonner for de seedede CPR-vaerdier
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'stg' "
                "AND TABLE_NAME IN ('pre_DRGKontakt','fct_DRGKontakt','dim_DRGKontaktEgenskaber') "
                "AND DATA_TYPE IN ('nvarchar','varchar','char','nchar')"
            )
            kolonner = cursor.fetchall()
        finally:
            cursor.close()
    for tabel, kolonne in kolonner:
        for cpr in (CPR_KENDT, CPR_TIDLIGERE):
            fund = scalar(
                staging_db,
                f"SELECT COUNT(*) FROM [stg].[{tabel}] " f"WHERE [{kolonne}] LIKE '%' + ? + '%'",
                cpr,
            )
            assert fund == 0, f"CPR-vaerdi fundet i stg.{tabel}.{kolonne}"


def test_3_borger_opslag_med_fallback_og_sentinels(staging_db):
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Borger FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == EK_BORGER_A
    )
    # B's leverede CPR findes kun som tidligere_cpr -> fallback
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Borger FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_B,
        )
        == EK_BORGER_B
    )
    assert (
        scalar(
            staging_db,
            "SELECT borger_opslag FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_B,
        )
        == "tidligere_cpr"
    )
    # psyk-kontaktens CPR er tom -> N/A (-2)
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Borger FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            PSY_A,
        )
        == -2
    )


def test_4_sor_pr_rolle_med_raa_kode_og_sentinel(staging_db):
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Organisation_sygehus FROM [stg].[fct_DRGKontakt] "
            "WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == EK_ORG
    )
    # ukendt SOR-kode -> -1, men raa kode bevaret
    row_key = scalar(
        staging_db,
        "SELECT dw_ek_Organisation_sygehus FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
        UUID_B,
    )
    row_raw = scalar(
        staging_db,
        "SELECT sygehus_sor FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
        UUID_B,
    )
    assert row_key == -1 and row_raw == SOR_UKENDT
    # tom henvisende instans -> -1 (tom kode er den FORVENTEDE aarsag)
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Organisation_henvisende FROM [stg].[fct_DRGKontakt] "
            "WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == -1
    )


def test_5_forloebsmaal_kun_paa_baerende_kontakt(staging_db):
    # somatik: tungeste (A) baerer, B goer ikke
    assert (
        scalar(
            staging_db,
            "SELECT forloeb_totalpris FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == 300
    )
    assert (
        scalar(
            staging_db,
            "SELECT forloeb_totalpris FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_B,
        )
        is None
    )
    # psyk: foerste kontakt baerer
    assert (
        scalar(
            staging_db,
            "SELECT forloeb_pris_psykiatriforloeb FROM [stg].[fct_DRGKontakt] "
            "WHERE kontakt_uuid = ?",
            PSY_A,
        )
        == 1000
    )
    # kontroltotalen holder
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_forloeb_kontroltotal_sql())
            for _, flere, afvigende in cursor.fetchall():
                assert flere == 0 and afvigende == 0
        finally:
            cursor.close()


def test_6_dato_noegler_og_typede_maal(staging_db):
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Dato_ind FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == 20250301
    )
    # psyk-kontaktens ind-tidspunkt er tom streng -> BLANK-sentinel direkte
    # fra den delte dato_key (T-SQL konverterer '' til 1900-01-01, saa uden
    # blank-gren FOER TRY_CONVERT ville den keye til BEFORE/-2)
    assert (
        scalar(
            staging_db,
            "SELECT dw_ek_Dato_ind FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            PSY_A,
        )
        == 99999998
    )
    assert (
        scalar(
            staging_db,
            "SELECT totalpris_drg FROM [stg].[fct_DRGKontakt] WHERE kontakt_uuid = ?",
            UUID_A,
        )
        == 100
    )


def test_7_konstrueret_kontroltotal_brud_opdages(staging_db):
    """Saet BEGGE kontakter som tungeste -> to baerende pr. forloeb."""
    run_sql(
        staging_db,
        "UPDATE [arc].[drg_kontakt] SET [TUNGESTE_DRGKONTAKT] = '1' WHERE [KONTAKT_VAERDI] = ?",
        UUID_B,
    )
    _koer_kaeden(staging_db)
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_forloeb_kontroltotal_sql())
            resultat = {lt: (flere, afvig) for lt, flere, afvig in cursor.fetchall()}
        finally:
            cursor.close()
    assert resultat["DRG"][0] == 1, "to baerende kontakter i samme forloeb skal opdages"
    # og asset-varianten fejler haardt paa det (stg_fct_DRGKontakt raiser Failure)
    from core.dfm_config import RunModeConfig
    from DSA.SQL.staging.stg_fct_DRGKontakt import stg_fct_DRGKontakt

    with pytest.raises(dg.Failure, match="kontroltotalen holder ikke"):
        stg_fct_DRGKontakt(dg.build_asset_context(), staging_db, RunModeConfig())


def test_8_afvigende_maal_opdages(staging_db):
    """Kontroltotallens anden halvdel skal kunne fyre: den sammenligner det
    STAGEDE (materialiserede) maal mod arc paa kontroltidspunktet, saa en
    arc-aendring efter staging — eller en join-fejl i en af de to uafhaengige
    kopier af joinet — giver en afvigelse."""
    run_sql(
        staging_db,
        "UPDATE [arc].[drg_kontakt] SET [TUNGESTE_DRGKONTAKT] = '0' WHERE [KONTAKT_VAERDI] = ?",
        UUID_B,
    )
    _koer_kaeden(staging_db)
    run_sql(
        staging_db,
        "UPDATE [arc].[drg_sghforlob] SET [TOTALPRIS_DRGSGHF] = '999999' "
        "WHERE [ind_kontakt_uuid] = ?",
        UUID_A,
    )
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_forloeb_kontroltotal_sql())
            resultat = {lt: (flere, afvig) for lt, flere, afvig in cursor.fetchall()}
        finally:
            cursor.close()
    assert resultat["DRG"][1] == 1, "staget maal afviger fra arc og skal opdages"
    assert resultat["DRG"][0] == 0
    # genskab konsistens for efterfoelgende tests
    run_sql(
        staging_db,
        "UPDATE [arc].[drg_sghforlob] SET [TOTALPRIS_DRGSGHF] = '300' WHERE [ind_kontakt_uuid] = ?",
        UUID_A,
    )


def test_9_delta_scope_stager_kun_beroerte_kontakter(staging_db):
    """Delta-udpegningen mod rigtig SQL Server: en aendring i EN boernetabel
    (diagnose) skal traekke netop DENS kontakt med i deltaet — og intet andet.
    Checkpoints i fremtiden goer testen deterministisk uden sleeps."""
    cp = "2099-01-01 00:00:00"
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(create_borger_cpr_lookup_tables_sql())
            cursor.execute(build_delta_scope_sql(cp))
            cursor.execute(build_unresolved_borger_scope_sql())
            cursor.execute(build_pre_stg_sql(cp))
            conn.commit()
        finally:
            cursor.close()
    assert scalar(staging_db, "SELECT COUNT(*) FROM [stg].[pre_DRGKontakt]") == 0

    run_sql(
        staging_db,
        "INSERT INTO [arc].[drg_diagnose] ([takstsystem],[kontakt_uuid],[DIAGNOSE],"
        "[ART],[SIDEANGIVELSE],[Aktivitetsaar],[leverance_dato],"
        "[dw_timestamp_insert],[dw_timestamp_update],[dw_is_deleted]) "
        "VALUES ('T26', ?, 'DZ032', 'A', '', '2025', '2026-08-23',"
        " '2099-06-01', '2099-06-01', 0)",
        UUID_B,
    )
    with staging_db.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(create_borger_cpr_lookup_tables_sql())
            cursor.execute(build_delta_scope_sql(cp))
            cursor.execute(build_unresolved_borger_scope_sql())
            cursor.execute(build_pre_stg_sql(cp))
            conn.commit()
        finally:
            cursor.close()
    assert scalar(staging_db, "SELECT COUNT(*) FROM [stg].[pre_DRGKontakt]") == 1
    assert (
        scalar(staging_db, "SELECT [kontakt_uuid] FROM [stg].[pre_DRGKontakt]") == UUID_B
    ), "diagnose-aendringen skal traekke praecis dens kontakt med i deltaet"
