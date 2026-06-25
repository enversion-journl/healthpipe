# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration regression tests for duplicate-key handling in fct_vareudlevering.

Tests cover:
- Duplicate CleanCode_Vareudlevering in staging -> fact rejects (grain guard)
- Duplicate map.Vare rows for same bk_LeverandoerVarenummer -> fact succeeds
  because Resolved_Vare_CleanCode (pre-resolved in staging) uses map.Vare PK
- Staging Vare CleanCode resolution via primary (CVR+LEVVARE) and vnr fallback
- End-to-end: staging + fact succeed with correct DW_EK_Vare
- Pre-stg dedup of duplicate CleanCode_Vareudlevering rows from stg.udlev_ils
- Rerun idempotency: same partition twice produces no grain violations
- Rerun MERGE update path: changed measures are updated, row count stays 1
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_asset

pytestmark = pytest.mark.skip(
    reason="Udlevering dedup integration depends on unavailable upstream extract chains"
)

DUPLICATE_STAGE_CLEANCODE = "UDLEV_TEST_STAGE_DUP"
DUPLICATE_VARE_CLEANCODE = "UDLEV_TEST_VARE_DUP"
DUPLICATE_LNR = "LNR_TEST_DUP"
DUPLICATE_CVR = "CVR_TEST_DUP"
DUPLICATE_LVNR = "LVNR_TEST_DUP"
DUPLICATE_VARE_1 = "VARE_TEST_DUP_1"
DUPLICATE_VARE_2 = "VARE_TEST_DUP_2"

VNR_FALLBACK_CLEANCODE = "UDLEV_TEST_VNR_FALLBACK"
VNR_FALLBACK_LNR = "LNR_TEST_VNR_FB"
VNR_FALLBACK_CVR = "CVR_TEST_VNR_FB"
VNR_FALLBACK_VNR = "VNR_TESTPROD"
VNR_FALLBACK_VARE_CC = "CVR_TEST_VNR_FB_LEVVARE_FALLBACKLVNR"

E2E_CLEANCODE = "UDLEV_TEST_E2E"
E2E_LNR = "LNR_TEST_E2E"
E2E_CVR = "CVR_TEST_E2E"
E2E_LVNR = "E2ELVNR"
E2E_VARE_CC = "CVR_TEST_E2E_LEVVARE_E2ELVNR"

PRESTG_DEDUP_CLEANCODE = "UDLEV_TEST_PRESTG_DEDUP"
RERUN_CLEANCODE = "UDLEV_TEST_RERUN"
RERUN_UPDATE_CLEANCODE = "UDLEV_TEST_RERUN_UPD"

ALL_TEST_UDLEV_CLEANCODES = (
    DUPLICATE_STAGE_CLEANCODE,
    DUPLICATE_VARE_CLEANCODE,
    VNR_FALLBACK_CLEANCODE,
    E2E_CLEANCODE,
    PRESTG_DEDUP_CLEANCODE,
    RERUN_CLEANCODE,
    RERUN_UPDATE_CLEANCODE,
)

ALL_TEST_VARE_CLEANCODES = (
    DUPLICATE_VARE_1,
    DUPLICATE_VARE_2,
    VNR_FALLBACK_VARE_CC,
    E2E_VARE_CC,
)

ALL_TEST_LNRS = (DUPLICATE_LNR, VNR_FALLBACK_LNR, E2E_LNR)


def _ensure_vareudlevering_test_tables(real_mssql_target_resource) -> None:
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[fct_vareudlevering_ils]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[fct_vareudlevering_ils] (
                        [CleanCode_Vareudlevering] NVARCHAR(500) NULL,
                        [bk_prrowid] NVARCHAR(100) NULL,
                        [Lager_CleanCode] NVARCHAR(500) NULL,
                        [Rekvirent_CleanCode] NVARCHAR(500) NULL,
                        [Vare_CleanCode] NVARCHAR(500) NULL,
                        [Resolved_Vare_CleanCode] NVARCHAR(1100) NULL,
                        [Vareudlevering_Antal] DECIMAL(18,4) NULL,
                        [Vareudlevering_Beloeb] DECIMAL(19,4) NULL,
                        [Indkoeb_Beloeb] DECIMAL(19,4) NULL,
                        [Rabat_Beloeb] DECIMAL(19,4) NULL,
                        [Lager_Antal] DECIMAL(18,4) NULL,
                        [DW_EK_Dato_Vareudlevering] INT NULL,
                        [DW_EK_Dato_Udloeb] INT NULL,
                        [DW_EK_Dato_Postering] INT NULL,
                        [DW_EK_Dato_Aflevering] INT NULL,
                        [Datotid_Vareudlevering] DATETIME2 NULL,
                        [Datotid_Udloeb] DATETIME2 NULL,
                        [Datotid_Postering] DATETIME2 NULL,
                        [Datotid_Aflevering] DATETIME2 NULL,
                        [DW_haendelsestidspunkt] DATETIME2 NULL,
                        [IlsVare_CleanCode] NVARCHAR(100) NULL
                    );
                END
                ELSE IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'stg'
                      AND TABLE_NAME = 'fct_vareudlevering_ils'
                      AND COLUMN_NAME = 'Resolved_Vare_CleanCode'
                )
                BEGIN
                    ALTER TABLE [stg].[fct_vareudlevering_ils]
                    ADD [Resolved_Vare_CleanCode] NVARCHAR(1100) NULL;
                END
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'stg'
                      AND TABLE_NAME = 'fct_vareudlevering_ils'
                      AND COLUMN_NAME = 'IlsVare_CleanCode'
                )
                BEGIN
                    ALTER TABLE [stg].[fct_vareudlevering_ils]
                    ADD [IlsVare_CleanCode] NVARCHAR(100) NULL;
                END
            """
            )
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[lev_ils]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[lev_ils] (
                        [lnr] NVARCHAR(255) NULL,
                        [CVR_CleanCode] NVARCHAR(255) NULL,
                        [pro2modified] DATETIME2 NULL,
                        [pro2created] DATETIME2 NULL,
                        [DW_LoadTimestamp] DATETIME2 NULL
                    );
                END
            """
            )
            conn.commit()
        finally:
            cursor.close()


def _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource) -> None:
    cc_placeholders = ", ".join(["%s"] * len(ALL_TEST_UDLEV_CLEANCODES))
    lnr_placeholders = ", ".join(["%s"] * len(ALL_TEST_LNRS))
    vare_placeholders = ", ".join(["%s"] * len(ALL_TEST_VARE_CLEANCODES))
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                IF OBJECT_ID('[stg].[fct_vareudlevering_ils]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [stg].[fct_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering IN ({cc_placeholders});
                END
                """,
                ALL_TEST_UDLEV_CLEANCODES,
            )
            cursor.execute(
                f"""
                IF OBJECT_ID('[stg].[lev_ils]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [stg].[lev_ils]
                    WHERE lnr IN ({lnr_placeholders});
                END
                """,
                ALL_TEST_LNRS,
            )
            cursor.execute(
                f"""
                IF OBJECT_ID('[map].[Vare]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [map].[Vare]
                    WHERE CleanCode_Vare IN ({vare_placeholders});
                END
                """,
                ALL_TEST_VARE_CLEANCODES,
            )
            cursor.execute(
                f"""
                IF OBJECT_ID('[fct].[Vareudlevering]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering IN ({cc_placeholders});
                END
                """,
                ALL_TEST_UDLEV_CLEANCODES,
            )
            # Also clean stg.udlev_ils and stg.pre_vareudlevering_ils test rows
            cursor.execute(
                f"""
                IF OBJECT_ID('[stg].[udlev_ils]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [stg].[udlev_ils]
                    WHERE CleanCode_Vareudlevering IN ({cc_placeholders});
                END
                """,
                ALL_TEST_UDLEV_CLEANCODES,
            )
            cursor.execute(
                f"""
                IF OBJECT_ID('[stg].[pre_vareudlevering_ils]', 'U') IS NOT NULL
                BEGIN
                    DELETE FROM [stg].[pre_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering IN ({cc_placeholders});
                END
                """,
                ALL_TEST_UDLEV_CLEANCODES,
            )
            conn.commit()
        finally:
            cursor.close()


def _insert_staging_row(cursor, cleancode, prrowid, resolved_vare_cc=None):
    """Insert a single test row into stg.fct_vareudlevering_ils."""
    cursor.execute(
        """
        INSERT INTO [stg].[fct_vareudlevering_ils] (
            CleanCode_Vareudlevering, bk_prrowid, Lager_CleanCode, Rekvirent_CleanCode,
            Vare_CleanCode, Resolved_Vare_CleanCode,
            Vareudlevering_Antal, Vareudlevering_Beloeb, Indkoeb_Beloeb, Rabat_Beloeb, Lager_Antal,
            DW_EK_Dato_Vareudlevering, DW_EK_Dato_Udloeb, DW_EK_Dato_Postering, DW_EK_Dato_Aflevering,
            Datotid_Vareudlevering, Datotid_Udloeb, Datotid_Postering, Datotid_Aflevering,
            DW_haendelsestidspunkt
        )
        VALUES (
            %s, %s, NULL, NULL, NULL, %s,
            1.0000, 10.0000, 5.0000, 0.0000, 1.0000,
            20260301, 20260320, 20260301, 20260301,
            '2026-03-01T08:00:00', '2026-03-20T00:00:00',
            '2026-03-01T00:00:00', '2026-03-01T08:00:00',
            '2026-03-01T08:00:00'
        );
        """,
        (cleancode, prrowid, resolved_vare_cc),
    )


# ============================================================================
# Test 1: Duplicate CleanCode_Vareudlevering in staging -> fact rejects
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_rejects_duplicate_stage_rows(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """Genuine staging duplicates (same CleanCode_Vareudlevering) must still be rejected."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _insert_staging_row(cursor, DUPLICATE_STAGE_CLEANCODE, "PRROWID_STAGE_1")
                _insert_staging_row(cursor, DUPLICATE_STAGE_CLEANCODE, "PRROWID_STAGE_2")
                conn.commit()
            finally:
                cursor.close()
        with pytest.raises(Exception, match="Duplicate grain violation in fct_vareudlevering"):
            materialize_asset(
                instance=dagster_instance,
                asset_key="fct_vareudlevering",
                resources={
                    "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                        real_mssql_target_resource
                    )
                },
            )

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (DUPLICATE_STAGE_CLEANCODE,),
                )
                assert cursor.fetchone()[0] == 0
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)


# ============================================================================
# Test 2: Duplicate map.Vare rows no longer cause fan-out (Resolved_Vare_CleanCode)
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_succeeds_with_duplicate_vare_bk(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """Duplicate map.Vare rows for same bk_LeverandoerVarenummer must not cause
    fan-out because the fact asset now joins via Resolved_Vare_CleanCode (PK)."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    INSERT INTO [map].[Vare] (
                        bk_Kreditor, bk_LeverandoerVarenummer, CleanCode_Vare, DW_Timestamp_Insert
                    )
                    VALUES
                        (%s, %s, %s, CAST(SYSDATETIME() AS datetime2(0))),
                        (%s, %s, %s, CAST(SYSDATETIME() AS datetime2(0)));
                    """,
                    (
                        DUPLICATE_CVR,
                        f"LEVVARE_{DUPLICATE_LVNR}",
                        DUPLICATE_VARE_1,
                        DUPLICATE_CVR,
                        f"LEVVARE_{DUPLICATE_LVNR}",
                        DUPLICATE_VARE_2,
                    ),
                )

                _insert_staging_row(
                    cursor,
                    DUPLICATE_VARE_CLEANCODE,
                    "PRROWID_VARE_1",
                    resolved_vare_cc=DUPLICATE_VARE_1,
                )
                conn.commit()
            finally:
                cursor.close()
        result = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources={
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                )
            },
        )
        assert result is not None

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*), MIN(DW_EK_Vare)
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (DUPLICATE_VARE_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row[0] == 1, "Expected exactly one fact row"
                assert row[1] != -1, "DW_EK_Vare should be resolved (not -1)"

                cursor.execute(
                    """
                    SELECT DW_EK_Vare FROM [map].[Vare]
                    WHERE CleanCode_Vare = %s
                    """,
                    (DUPLICATE_VARE_1,),
                )
                expected_ek = cursor.fetchone()[0]
                assert (
                    row[1] == expected_ek
                ), f"DW_EK_Vare should match map.Vare for {DUPLICATE_VARE_1}"
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)


# ============================================================================
# Test 3: Staging resolves Vare via vnr fallback
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_stg_fct_vareudlevering_vare_fallback_via_vnr(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """When primary lvnr_1 lookup fails, Resolved_Vare_CleanCode should
    resolve via vnr fallback in stg_fct_vareudlevering_ils."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:

                cursor.execute(
                    """
                    INSERT INTO [stg].[lev_ils] (
                        lnr, CVR_CleanCode, pro2modified, pro2created, DW_LoadTimestamp
                    )
                    VALUES (%s, %s, '2026-03-02T00:00:00', '2026-03-01T00:00:00',
                            '2026-03-02T00:00:00');
                    """,
                    (VNR_FALLBACK_LNR, VNR_FALLBACK_CVR),
                )

                cursor.execute(
                    """
                    INSERT INTO [map].[Vare] (
                        bk_Kreditor, bk_LeverandoerVarenummer, CleanCode_Vare, vnr,
                        DW_Timestamp_Insert
                    )
                    VALUES (%s, %s, %s, %s, CAST(SYSDATETIME() AS datetime2(0)));
                    """,
                    (
                        VNR_FALLBACK_CVR,
                        "LEVVARE_FALLBACKLVNR",
                        VNR_FALLBACK_VARE_CC,
                        VNR_FALLBACK_VNR,
                    ),
                )

                cursor.execute(
                    """
                    IF OBJECT_ID('[stg].[pre_vareudlevering_ils]', 'U') IS NULL
                    BEGIN
                        CREATE TABLE [stg].[pre_vareudlevering_ils] (
                            [prrowid] NVARCHAR(100) NULL,
                            [sgh] NVARCHAR(50) NULL,
                            [lgr] NVARCHAR(50) NULL,
                            [vnr] NVARCHAR(255) NULL,
                            [lvnr_1] NVARCHAR(255) NULL,
                            [rnr] NVARCHAR(50) NULL,
                            [lnr] NVARCHAR(255) NULL,
                            [udlant] DECIMAL(18,4) NULL,
                            [udlbel] DECIMAL(19,4) NULL,
                            [indkbel] DECIMAL(19,4) NULL,
                            [rabatbel] DECIMAL(19,4) NULL,
                            [lagerantal] DECIMAL(18,4) NULL,
                            [udldto] DATETIME2 NULL,
                            [udlklk] INT NULL,
                            [udlobdto] DATETIME2 NULL,
                            [postdto] DATETIME2 NULL,
                            [aflevdto] DATETIME2 NULL,
                            [aflevklk] INT NULL,
                            [rvis] NVARCHAR(50) NULL,
                            [id] NVARCHAR(100) NULL,
                            [brik] NVARCHAR(50) NULL,
                            [sw_direkte] NVARCHAR(10) NULL,
                            [sw_paksdl] NVARCHAR(10) NULL,
                            [sw_flgsdl] NVARCHAR(10) NULL,
                            [sw_fly] NVARCHAR(10) NULL,
                            [sw_haster] NVARCHAR(10) NULL,
                            [sw_erstat] NVARCHAR(10) NULL,
                            [projnr] NVARCHAR(50) NULL,
                            [bruger] NVARCHAR(50) NULL,
                            [refnr] NVARCHAR(50) NULL,
                            [reflinie] NVARCHAR(50) NULL,
                            [salgsordre] NVARCHAR(50) NULL,
                            [salgsordreint] NVARCHAR(50) NULL,
                            [salgsfaknr] NVARCHAR(50) NULL,
                            [rvisvareid] NVARCHAR(50) NULL,
                            [plukvareid] NVARCHAR(50) NULL,
                            [aflevoper] NVARCHAR(50) NULL,
                            [aflevtxt] NVARCHAR(255) NULL,
                            [aflevstreg] NVARCHAR(100) NULL,
                            [vognnummer] NVARCHAR(50) NULL,
                            [flgsednr] NVARCHAR(50) NULL,
                            [ordre] NVARCHAR(50) NULL,
                            [ordlinie] NVARCHAR(50) NULL,
                            [kto] NVARCHAR(50) NULL,
                            [udlevenh] NVARCHAR(50) NULL,
                            [CleanCode_Vareudlevering] NVARCHAR(500) NULL,
                            [Lager_CleanCode] NVARCHAR(500) NULL,
                            [Rekvirent_CleanCode] NVARCHAR(500) NULL,
                            [Vare_CleanCode] NVARCHAR(500) NULL,
                            [DW_LoadTimestamp] DATETIME2 NULL
                        );
                    END
                """
                )

                cursor.execute(
                    """
                    DELETE FROM [stg].[pre_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering = %s;
                    """,
                    (VNR_FALLBACK_CLEANCODE,),
                )
                cursor.execute(
                    """
                    INSERT INTO [stg].[pre_vareudlevering_ils] (
                        prrowid, lnr, lvnr_1, vnr,
                        udlant, udlbel, indkbel, rabatbel, lagerantal,
                        udldto, udlklk, udlobdto, postdto, aflevdto, aflevklk,
                        CleanCode_Vareudlevering, Lager_CleanCode, Rekvirent_CleanCode,
                        Vare_CleanCode, DW_LoadTimestamp
                    )
                    VALUES (
                        'PRROWID_VNR_FB', %s, NULL, %s,
                        1.0, 10.0, 5.0, 0.0, 1.0,
                        '2026-03-01', 28800, '2026-03-20', '2026-03-01', '2026-03-01', 28800,
                        %s, NULL, NULL, NULL, SYSDATETIME()
                    );
                    """,
                    (VNR_FALLBACK_LNR, VNR_FALLBACK_VNR, VNR_FALLBACK_CLEANCODE),
                )
                conn.commit()
            finally:
                cursor.close()
        result = materialize_asset(
            instance=dagster_instance,
            asset_key="erp/stg_fct_vareudlevering_ils",
            resources={
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                )
            },
        )
        assert result is not None

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT Resolved_Vare_CleanCode
                    FROM [stg].[fct_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (VNR_FALLBACK_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row is not None, "Expected staging row for vnr fallback test"
                assert row[0] == VNR_FALLBACK_VARE_CC, (
                    f"Expected Resolved_Vare_CleanCode={VNR_FALLBACK_VARE_CC}, " f"got {row[0]}"
                )
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    IF OBJECT_ID('[stg].[pre_vareudlevering_ils]', 'U') IS NOT NULL
                        DELETE FROM [stg].[pre_vareudlevering_ils]
                        WHERE CleanCode_Vareudlevering = %s;
                    """,
                    (VNR_FALLBACK_CLEANCODE,),
                )
                conn.commit()
            finally:
                cursor.close()


# ============================================================================
# Test 4: End-to-end staging + fact with Resolved_Vare_CleanCode
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_e2e_resolved_vare(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """End-to-end: seed staging with Resolved_Vare_CleanCode, materialize fct_vareudlevering,
    verify no grain violation and DW_EK_Vare is correctly resolved."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:

                cursor.execute(
                    """
                    INSERT INTO [map].[Vare] (
                        bk_Kreditor, bk_LeverandoerVarenummer, CleanCode_Vare,
                        DW_Timestamp_Insert
                    )
                    VALUES (%s, %s, %s, CAST(SYSDATETIME() AS datetime2(0)));
                    """,
                    (E2E_CVR, f"LEVVARE_{E2E_LVNR}", E2E_VARE_CC),
                )

                _insert_staging_row(
                    cursor,
                    E2E_CLEANCODE,
                    "PRROWID_E2E_1",
                    resolved_vare_cc=E2E_VARE_CC,
                )
                conn.commit()
            finally:
                cursor.close()
        result = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources={
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                )
            },
        )
        assert result is not None

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT f.DW_EK_Vare, v.CleanCode_Vare
                    FROM [fct].[Vareudlevering] f
                    LEFT JOIN [map].[Vare] v ON v.DW_EK_Vare = f.DW_EK_Vare
                    WHERE f.CleanCode_Vareudlevering = %s
                    """,
                    (E2E_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row is not None, "Expected fact row for e2e test"
                assert row[0] != -1, "DW_EK_Vare should be resolved (not -1)"
                assert row[1] == E2E_VARE_CC, (
                    f"DW_EK_Vare should resolve to map.Vare with "
                    f"CleanCode_Vare={E2E_VARE_CC}, got {row[1]}"
                )

                cursor.execute(
                    """
                    SELECT COUNT(*) AS total,
                           COUNT(DISTINCT CleanCode_Vareudlevering) AS distinct_cc
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (E2E_CLEANCODE,),
                )
                grain_row = cursor.fetchone()
                assert grain_row[0] == grain_row[1], "Grain must be preserved (no duplicates)"
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)


# ============================================================================
# Helpers for rerun / pre-stg dedup tests
# ============================================================================


def _ensure_udlev_ils_table(real_mssql_target_resource) -> None:
    """Ensure stg.udlev_ils exists with the minimal columns needed for pre_stg dedup tests."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[udlev_ils]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[udlev_ils] (
                        [prrowid] NVARCHAR(100) NULL,
                        [sgh] NVARCHAR(50) NULL,
                        [lgr] NVARCHAR(50) NULL,
                        [vnr] NVARCHAR(255) NULL,
                        [lvnr##1] NVARCHAR(255) NULL,
                        [rnr] NVARCHAR(50) NULL,
                        [lnr] NVARCHAR(255) NULL,
                        [udlant] DECIMAL(18,4) NULL,
                        [udlbel] DECIMAL(19,4) NULL,
                        [indkbel] DECIMAL(19,4) NULL,
                        [rabatbel] DECIMAL(19,4) NULL,
                        [lagerantal] DECIMAL(18,4) NULL,
                        [udldto] DATETIME2 NULL,
                        [udlklk] INT NULL,
                        [udlobdto] DATETIME2 NULL,
                        [postdto] DATETIME2 NULL,
                        [aflevdto] DATETIME2 NULL,
                        [aflevklk] INT NULL,
                        [rvis] NVARCHAR(50) NULL,
                        [id] NVARCHAR(100) NULL,
                        [brik] NVARCHAR(50) NULL,
                        [sw_direkte] NVARCHAR(10) NULL,
                        [sw_paksdl] NVARCHAR(10) NULL,
                        [sw_flgsdl] NVARCHAR(10) NULL,
                        [sw_fly] NVARCHAR(10) NULL,
                        [sw_haster] NVARCHAR(10) NULL,
                        [sw_erstat] NVARCHAR(10) NULL,
                        [projnr] NVARCHAR(50) NULL,
                        [bruger] NVARCHAR(50) NULL,
                        [refnr] NVARCHAR(50) NULL,
                        [reflinie] NVARCHAR(50) NULL,
                        [salgsordre] NVARCHAR(50) NULL,
                        [salgsordreint] NVARCHAR(50) NULL,
                        [salgsfaknr] NVARCHAR(50) NULL,
                        [rvisvareid] NVARCHAR(50) NULL,
                        [plukvareid] NVARCHAR(50) NULL,
                        [aflevoper] NVARCHAR(50) NULL,
                        [aflevtxt] NVARCHAR(255) NULL,
                        [aflevstreg] NVARCHAR(100) NULL,
                        [vognnummer] NVARCHAR(50) NULL,
                        [flgsednr] NVARCHAR(50) NULL,
                        [ordre] NVARCHAR(50) NULL,
                        [ordlinie] NVARCHAR(50) NULL,
                        [kto] NVARCHAR(50) NULL,
                        [udlevenh] NVARCHAR(50) NULL,
                        [CleanCode_Vareudlevering] NVARCHAR(500) NULL,
                        [Lager_CleanCode] NVARCHAR(500) NULL,
                        [Rekvirent_CleanCode] NVARCHAR(500) NULL,
                        [Vare_CleanCode] NVARCHAR(500) NULL,
                        [SourceSystem] NVARCHAR(50) NULL,
                        [DW_LoadTimestamp] DATETIME2 NULL,
                        [DW_PartitionDate] DATE NULL
                    );
                END
            """
            )
            conn.commit()
        finally:
            cursor.close()


def _insert_udlev_ils_row(cursor, cleancode, prrowid, load_ts, udlbel=10.0):
    """Insert a minimal row into stg.udlev_ils for pre_stg dedup testing."""
    cursor.execute(
        """
        INSERT INTO [stg].[udlev_ils] (
            prrowid, sgh, lgr, vnr, [lvnr##1], rnr, lnr,
            udlant, udlbel, indkbel, rabatbel, lagerantal,
            udldto, udlklk, udlobdto, postdto, aflevdto, aflevklk,
            id, CleanCode_Vareudlevering, Lager_CleanCode,
            Rekvirent_CleanCode, Vare_CleanCode,
            SourceSystem, DW_LoadTimestamp
        )
        VALUES (
            %s, 'TST', 'L01', NULL, NULL, NULL, NULL,
            1.0, %s, 5.0, 0.0, 1.0,
            '2026-03-01', 28800, '2026-03-20', '2026-03-01', '2026-03-01', 28800,
            %s, %s, NULL, NULL, NULL,
            'ILS_TEST', %s
        );
        """,
        (prrowid, udlbel, prrowid, cleancode, load_ts),
    )


# ============================================================================
# Test 5: pre_stg dedup removes duplicate CleanCode_Vareudlevering rows
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_pre_stg_vareudlevering_dedups_same_cleancode(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """Duplicate CleanCode_Vareudlevering in stg.udlev_ils (from partition overlap)
    must be reduced to one row in stg.pre_vareudlevering_ils by ROW_NUMBER dedup.
    The row with the latest DW_LoadTimestamp wins."""
    try:
        _ensure_udlev_ils_table(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _insert_udlev_ils_row(
                    cursor,
                    PRESTG_DEDUP_CLEANCODE,
                    "PRROWID_DEDUP_1",
                    "2026-03-01T10:00:00",
                    udlbel=10.0,
                )
                _insert_udlev_ils_row(
                    cursor,
                    PRESTG_DEDUP_CLEANCODE,
                    "PRROWID_DEDUP_1",
                    "2026-03-02T10:00:00",
                    udlbel=20.0,
                )
                conn.commit()
            finally:
                cursor.close()
        result = materialize_asset(
            instance=dagster_instance,
            asset_key="erp/pre_stg_vareudlevering_ils",
            resources={
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                )
            },
        )
        assert result is not None

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*), MIN(udlbel)
                    FROM [stg].[pre_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (PRESTG_DEDUP_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row[0] == 1, f"Expected exactly 1 row after dedup, got {row[0]}"
                assert (
                    float(row[1]) == 20.0
                ), f"Latest DW_LoadTimestamp row (udlbel=20.0) should win, got {row[1]}"
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)


# ============================================================================
# Test 6: Rerun same partition -> no grain violation in fct_vareudlevering
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_rerun_no_duplicates(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """Materializing fct_vareudlevering twice on the same data must not raise a
    grain violation. The second run should succeed via the MERGE update path."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _insert_staging_row(cursor, RERUN_CLEANCODE, "PRROWID_RERUN_1")
                conn.commit()
            finally:
                cursor.close()
        resources = {
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            )
        }

        result1 = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources=resources,
        )
        assert result1 is not None, "First materialization must succeed"

        result2 = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources=resources,
        )
        assert result2 is not None, "Second materialization (rerun) must succeed"

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (RERUN_CLEANCODE,),
                )
                assert cursor.fetchone()[0] == 1, "Exactly one fact row after two runs"
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)


# ============================================================================
# Test 7: Rerun with changed measure -> MERGE updates existing row
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_rerun_updates_existing_row(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """Changing a measure between run 1 and run 2 for the same CleanCode_Vareudlevering
    must update the existing fact row (MERGE MATCHED path), not insert a duplicate."""
    try:
        _ensure_vareudlevering_test_tables(real_mssql_target_resource)
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)

        resources = {
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            )
        }

        # Run 1: insert with Vareudlevering_Antal = 1.0, Vareudlevering_Beloeb = 10.0
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _insert_staging_row(cursor, RERUN_UPDATE_CLEANCODE, "PRROWID_RERUN_UPD")
                conn.commit()
            finally:
                cursor.close()
        result1 = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources=resources,
        )
        assert result1 is not None

        # Verify initial values
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT Vareudlevering_Antal, Vareudlevering_Beloeb
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (RERUN_UPDATE_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row is not None, "Fact row must exist after run 1"
                assert float(row[0]) == 1.0
                assert float(row[1]) == 10.0
            finally:
                cursor.close()
        # Run 2: update staging row with different measures
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    UPDATE [stg].[fct_vareudlevering_ils]
                    SET Vareudlevering_Antal = 5.0, Vareudlevering_Beloeb = 99.0
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (RERUN_UPDATE_CLEANCODE,),
                )
                conn.commit()
            finally:
                cursor.close()
        result2 = materialize_asset(
            instance=dagster_instance,
            asset_key="fct_vareudlevering",
            resources=resources,
        )
        assert result2 is not None

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*), MIN(Vareudlevering_Antal), MIN(Vareudlevering_Beloeb)
                    FROM [fct].[Vareudlevering]
                    WHERE CleanCode_Vareudlevering = %s
                    """,
                    (RERUN_UPDATE_CLEANCODE,),
                )
                row = cursor.fetchone()
                assert row[0] == 1, f"Still exactly one fact row, got {row[0]}"
                assert (
                    float(row[1]) == 5.0
                ), f"Vareudlevering_Antal should be updated to 5.0, got {row[1]}"
                assert (
                    float(row[2]) == 99.0
                ), f"Vareudlevering_Beloeb should be updated to 99.0, got {row[2]}"
            finally:
                cursor.close()
    finally:
        _cleanup_seeded_vareudlevering_rows(real_mssql_target_resource)
