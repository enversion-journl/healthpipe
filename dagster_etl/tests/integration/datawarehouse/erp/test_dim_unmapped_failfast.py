# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for dimension update fail-fast behavior on unmapped primary map keys.

Each dim updater (rekvirent, lager, vareudlevering_egenskaber) should raise a ValueError when
staging rows have no matching entry in the primary map table.
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_asset

pytestmark = pytest.mark.skip(
    reason="Legacy fail-fast behavior no longer matches current dim update strategy"
)

UNMAPPED_CLEANCODE_REKVIRENT = "REKV_TEST_UNMAPPED_FAILFAST"
UNMAPPED_CLEANCODE_LAGER = "LAGER_TEST_UNMAPPED_FAILFAST"
UNMAPPED_CLEANCODE_VAREUDLEVERING = "UDLEV_TEST_UNMAPPED_FAILFAST"


def _seed_unmapped_dim_rekvirent(real_mssql_target_resource) -> None:
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[dim_rekvirent]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[dim_rekvirent] (
                        [CleanCode_Rekvirent] NVARCHAR(500) NULL,
                        [Code] NVARCHAR(255) NULL,
                        [Display] NVARCHAR(500) NULL,
                        [ParentCode] NVARCHAR(255) NULL,
                        [Level] INT NULL,
                        [EntityType] NVARCHAR(255) NULL,
                        [SGH] NVARCHAR(255) NULL,
                        [RGRP] NVARCHAR(255) NULL,
                        [RTYP] NVARCHAR(255) NULL,
                        [RNR] NVARCHAR(255) NULL,
                        [CodeSOR] NVARCHAR(255) NULL,
                        [SORRelationship] NVARCHAR(255) NULL
                    );
                END
                """
            )
            cursor.execute(
                """
                DELETE FROM [stg].[dim_rekvirent]
                WHERE CleanCode_Rekvirent = %s
                """,
                (UNMAPPED_CLEANCODE_REKVIRENT,),
            )
            cursor.execute(
                """
                INSERT INTO [stg].[dim_rekvirent] (
                    CleanCode_Rekvirent, Code, Display, ParentCode, [Level],
                    EntityType, SGH, RGRP, RTYP, RNR, CodeSOR, SORRelationship
                )
                VALUES (%s, 'TEST_CODE', 'Test Rekvirent', NULL, 1,
                        'Test', NULL, NULL, NULL, NULL, NULL, NULL);
                """,
                (UNMAPPED_CLEANCODE_REKVIRENT,),
            )
            conn.commit()
        finally:
            cursor.close()


def _seed_unmapped_dim_lager(real_mssql_target_resource) -> None:
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[dim_lager]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[dim_lager] (
                        [CleanCode_Lager] NVARCHAR(500) NULL,
                        [Code] NVARCHAR(255) NULL,
                        [Display] NVARCHAR(500) NULL,
                        [ParentCode] NVARCHAR(255) NULL,
                        [Level] INT NULL,
                        [EntityType] NVARCHAR(255) NULL,
                        [SGH] NVARCHAR(255) NULL,
                        [LGR] NVARCHAR(255) NULL,
                        [Adr] NVARCHAR(500) NULL,
                        [Land] NVARCHAR(100) NULL,
                        [Postdi] NVARCHAR(100) NULL,
                        [Tlf] NVARCHAR(100) NULL,
                        [CodeSOR] NVARCHAR(255) NULL
                    );
                END
                """
            )
            cursor.execute(
                """
                DELETE FROM [stg].[dim_lager]
                WHERE CleanCode_Lager = %s
                """,
                (UNMAPPED_CLEANCODE_LAGER,),
            )
            cursor.execute(
                """
                INSERT INTO [stg].[dim_lager] (
                    CleanCode_Lager, Code, Display, ParentCode, [Level],
                    EntityType, SGH, LGR, Adr, Land, Postdi, Tlf, CodeSOR
                )
                VALUES (%s, 'TEST_CODE', 'Test Lager', NULL, 1,
                        'Test', NULL, NULL, NULL, NULL, NULL, NULL, NULL);
                """,
                (UNMAPPED_CLEANCODE_LAGER,),
            )
            conn.commit()
        finally:
            cursor.close()


def _seed_unmapped_dim_vareudlevering(real_mssql_target_resource) -> None:
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF OBJECT_ID('[stg].[dim_vareudlevering_ils]', 'U') IS NULL
                BEGIN
                    CREATE TABLE [stg].[dim_vareudlevering_ils] (
                        [CleanCode_Vareudlevering] NVARCHAR(500) NULL,
                        [Revisionsnummer] NVARCHAR(255) NULL,
                        [Foelgeseddelnummer] NVARCHAR(255) NULL,
                        [Brik] NVARCHAR(255) NULL,
                        [Leverandoernummer] NVARCHAR(255) NULL,
                        [Ordrenummer] NVARCHAR(255) NULL,
                        [Ordrelinje] NVARCHAR(255) NULL,
                        [sw_direkte_udlevering] NVARCHAR(10) NULL,
                        [sw_pakkeseddel] NVARCHAR(10) NULL,
                        [sw_foelgeseddel] NVARCHAR(10) NULL,
                        [sw_fly] NVARCHAR(10) NULL,
                        [sw_haster] NVARCHAR(10) NULL,
                        [sw_erstat] NVARCHAR(10) NULL,
                        [Projektnummer] NVARCHAR(255) NULL,
                        [Kontonummer_kode] NVARCHAR(255) NULL,
                        [Bruger] NVARCHAR(255) NULL,
                        [bk_prrowid] NVARCHAR(255) NULL,
                        [Referencenummer] NVARCHAR(255) NULL,
                        [Referencelinje] NVARCHAR(255) NULL,
                        [Salgsordre] NVARCHAR(255) NULL,
                        [SalgsordreId] NVARCHAR(255) NULL,
                        [Salgsfakturanummer] NVARCHAR(255) NULL,
                        [RevisionsvarerId] NVARCHAR(255) NULL,
                        [PlukvarerId] NVARCHAR(255) NULL,
                        [Afleveringsoperatoer] NVARCHAR(255) NULL,
                        [Afleveringstekst] NVARCHAR(500) NULL,
                        [Afleveringsstregkode] NVARCHAR(255) NULL,
                        [Vognnummer] NVARCHAR(255) NULL
                    );
                END
                """
            )
            cursor.execute(
                """
                DELETE FROM [stg].[dim_vareudlevering_ils]
                WHERE CleanCode_Vareudlevering = %s
                """,
                (UNMAPPED_CLEANCODE_VAREUDLEVERING,),
            )
            cursor.execute(
                """
                INSERT INTO [stg].[dim_vareudlevering_ils] (
                    CleanCode_Vareudlevering, Revisionsnummer, Foelgeseddelnummer,
                    Brik, Leverandoernummer, Ordrenummer, Ordrelinje,
                    sw_direkte_udlevering, sw_pakkeseddel, sw_foelgeseddel,
                    sw_fly, sw_haster, sw_erstat,
                    Projektnummer, Kontonummer_kode, Bruger, bk_prrowid,
                    Referencenummer, Referencelinje,
                    Salgsordre, SalgsordreId, Salgsfakturanummer,
                    RevisionsvarerId, PlukvarerId,
                    Afleveringsoperatoer, Afleveringstekst,
                    Afleveringsstregkode, Vognnummer
                )
                VALUES (%s, 'REV1', 'FS1', 'BRIK1', 'LNR1', 'ORD1', 'OL1',
                        '0', '0', '0', '0', '0', '0',
                        NULL, NULL, 'TEST', 'BK1',
                        NULL, NULL, NULL, NULL, NULL,
                        NULL, NULL, NULL, NULL, NULL, NULL);
                """,
                (UNMAPPED_CLEANCODE_VAREUDLEVERING,),
            )
            conn.commit()
        finally:
            cursor.close()


def _cleanup_unmapped_test_rows(real_mssql_target_resource) -> None:
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for table, col, val in [
                ("[stg].[dim_rekvirent]", "CleanCode_Rekvirent", UNMAPPED_CLEANCODE_REKVIRENT),
                ("[stg].[dim_lager]", "CleanCode_Lager", UNMAPPED_CLEANCODE_LAGER),
                (
                    "[stg].[dim_vareudlevering_ils]",
                    "CleanCode_Vareudlevering",
                    UNMAPPED_CLEANCODE_VAREUDLEVERING,
                ),
            ]:
                cursor.execute(
                    f"""
                    IF OBJECT_ID('{table}', 'U') IS NOT NULL
                    BEGIN
                        DELETE FROM {table} WHERE {col} = %s;
                    END
                    """,
                    (val,),
                )
            conn.commit()
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_dim_rekvirent_fails_on_unmapped_primary_key(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """dim_rekvirent must fail fast when staging rows have no match in map.Rekvirent."""
    try:
        _seed_unmapped_dim_rekvirent(real_mssql_target_resource)

        with pytest.raises(Exception, match="dim_rekvirent.*no match in map.Rekvirent"):
            materialize_asset(
                instance=dagster_instance,
                asset_key="dim_rekvirent",
                resources={
                    "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                        real_mssql_target_resource
                    )
                },
            )
    finally:
        _cleanup_unmapped_test_rows(real_mssql_target_resource)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_dim_lager_fails_on_unmapped_primary_key(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """dim_lager must fail fast when staging rows have no match in map.Lager."""
    try:
        _seed_unmapped_dim_lager(real_mssql_target_resource)

        with pytest.raises(Exception, match="dim_lager.*no match in map.Lager"):
            materialize_asset(
                instance=dagster_instance,
                asset_key="dim_lager",
                resources={
                    "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                        real_mssql_target_resource
                    )
                },
            )
    finally:
        _cleanup_unmapped_test_rows(real_mssql_target_resource)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_dim_vareudlevering_egenskaber_fails_on_unmapped_primary_key(
    dagster_instance,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_all_tables,
):
    """dim_vareudlevering_egenskaber must fail fast when staging rows have no match in map.Vareudlevering."""
    try:
        _seed_unmapped_dim_vareudlevering(real_mssql_target_resource)

        with pytest.raises(
            Exception, match="dim_vareudlevering_egenskaber.*no match in map.Vareudlevering"
        ):
            materialize_asset(
                instance=dagster_instance,
                asset_key="dim_vareudlevering_egenskaber",
                resources={
                    "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                        real_mssql_target_resource
                    )
                },
            )
    finally:
        _cleanup_unmapped_test_rows(real_mssql_target_resource)
