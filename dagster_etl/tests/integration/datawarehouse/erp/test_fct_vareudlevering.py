# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for fct_vareudlevering using real databases.

Tests validate:
- Lager FK is resolved using map.Lager.CleanCode_Lager (regression guard)
- IlsVare FK is resolved via map.IlsVare using IlsVare_CleanCode
- Vare FK values always point to map.Vare when resolved
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)

pytestmark = pytest.mark.skip(
    reason="Udlevering integration depends on missing ext_os2mo_* extract assets for ConceptMaps"
)


def _materialize_vareudlevering_chain(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
) -> None:
    """Materialize assets required for Vareudlevering fact validation."""
    with real_ils_mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    OBJECT_ID('dbo.sgh') AS sgh_exists,
                    OBJECT_ID('dbo.lgr') AS lgr_exists
            """
            )
            sgh_exists, lgr_exists = cursor.fetchone()
            if sgh_exists is None or lgr_exists is None:
                pytest.skip(
                    "ILS source missing dbo.sgh/dbo.lgr required for Lager codesystem extract"
                )
        finally:
            cursor.close()
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    OBJECT_ID('stg.udlev_ils') AS stg_udlev_exists,
                    OBJECT_ID('stg.lev_ils') AS stg_lev_exists
            """
            )
            stg_udlev_exists, stg_lev_exists = cursor.fetchone()
            if stg_udlev_exists is None or stg_lev_exists is None:
                pytest.skip(
                    "Staging missing stg.udlev_ils/stg.lev_ils required for Vareudlevering fact tests"
                )
        finally:
            cursor.close()
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=[
            "cs_lager",
            "cm_lager_to_sor",
            "cs_rekvirent",
            "cm_rekvirent_to_sor",
            "stg_dim_lager",
            "stg_dim_rekvirent",
            "map_lager",
            "map_rekvirent",
            "dim_lager",
            "dim_rekvirent",
            "erp/pre_stg_vareudlevering_ils",
            "erp/stg_dim_vareudlevering_ils",
            "map_vareudlevering",
            "ext_vare_enriched_ils_daily",
            "erp/stg_map_ilsvare_ils",
            "map_ilsvare",
            "erp/stg_fct_vareudlevering_ils",
            "fct_vareudlevering",
        ],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )
    assert result is not None


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_stg_dim_rekvirent_clean_code_unique_case_insensitive(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """stg.dim_rekvirent must not contain duplicate CleanCode_Rekvirent values."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_rekvirent]")
            row_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT TOP 10
                    UPPER(CleanCode_Rekvirent) AS normalized_clean_code,
                    COUNT(*) AS dup_count
                FROM [stg].[dim_rekvirent]
                GROUP BY UPPER(CleanCode_Rekvirent)
                HAVING COUNT(*) > 1
                ORDER BY dup_count DESC, normalized_clean_code
            """
            )
            duplicate_rows = cursor.fetchall()
        finally:
            cursor.close()
    assert row_count > 0, "stg.dim_rekvirent should contain rows after materialization"
    assert (
        len(duplicate_rows) == 0
    ), "Found duplicate Rekvirent CleanCodes (case-insensitive): " + ", ".join(
        f"{row[0]} ({row[1]})" for row in duplicate_rows
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_lager_lookup_regression(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """DW_EK_Lager in fact must match lookup on map.Lager.CleanCode_Lager."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS mismatch_count
                FROM [fct].[Vareudlevering] f
                INNER JOIN [stg].[fct_vareudlevering_ils] src
                    ON src.CleanCode_Vareudlevering = f.CleanCode_Vareudlevering
                LEFT JOIN [map].[Lager] m
                    ON m.CleanCode_Lager = src.Lager_CleanCode
                WHERE COALESCE(m.DW_EK_Lager, -1) <> f.DW_EK_Lager
            """
            )
            mismatch_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert mismatch_count == 0, (
        f"Found {mismatch_count} rows where fact DW_EK_Lager does not match "
        "map.Lager lookup by CleanCode_Lager"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_ilsvare_lookup(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """DW_EK_IlsVare in fact must match lookup on map.IlsVare via IlsVare_CleanCode."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS mismatch_count
                FROM [fct].[Vareudlevering] f
                INNER JOIN [stg].[fct_vareudlevering_ils] src
                    ON src.CleanCode_Vareudlevering = f.CleanCode_Vareudlevering
                LEFT JOIN [map].[IlsVare] m
                    ON m.CleanCode_IlsVare = src.IlsVare_CleanCode
                WHERE COALESCE(m.DW_EK_IlsVare, -1) <> f.DW_EK_IlsVare
            """
            )
            mismatch_count = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT SUM(CASE WHEN DW_EK_IlsVare IS NULL THEN 1 ELSE 0 END)
                FROM [fct].[Vareudlevering]
            """
            )
            null_ilsvare_fk = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert mismatch_count == 0, (
        f"Found {mismatch_count} rows where fact DW_EK_IlsVare does not match "
        "map.IlsVare lookup by IlsVare_CleanCode"
    )
    assert null_ilsvare_fk == 0, "DW_EK_IlsVare should never be NULL in fct.Vareudlevering"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_vare_fk_references_map_vare(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """Resolved DW_EK_Vare values in fact must exist in map.Vare."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    metadata = get_materialization_result(dagster_instance, "fct_vareudlevering")
    assert metadata is not None
    assert "missing_ilsvare_fk" in metadata
    assert "missing_vare_fk" in metadata

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS dangling_vare_fk
                FROM [fct].[Vareudlevering] f
                LEFT JOIN [map].[Vare] v
                    ON v.DW_EK_Vare = f.DW_EK_Vare
                WHERE f.DW_EK_Vare <> -1
                  AND v.DW_EK_Vare IS NULL
            """
            )
            dangling_vare_fk = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT SUM(CASE WHEN DW_EK_Vare IS NULL THEN 1 ELSE 0 END)
                FROM [fct].[Vareudlevering]
            """
            )
            null_vare_fk = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert (
        dangling_vare_fk == 0
    ), f"Found {dangling_vare_fk} resolved DW_EK_Vare values that do not exist in map.Vare"
    assert null_vare_fk == 0, "DW_EK_Vare should never be NULL in fct.Vareudlevering"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_fct_vareudlevering_rekvirent_lookup_regression(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """DW_EK_Rekvirent in fact must match lookup on map.Rekvirent.CleanCode_Rekvirent."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS mismatch_count
                FROM [fct].[Vareudlevering] f
                INNER JOIN [stg].[fct_vareudlevering_ils] src
                    ON src.CleanCode_Vareudlevering = f.CleanCode_Vareudlevering
                LEFT JOIN [map].[Rekvirent] m
                    ON m.CleanCode_Rekvirent = src.Rekvirent_CleanCode
                WHERE COALESCE(m.DW_EK_Rekvirent, -1) <> f.DW_EK_Rekvirent
            """
            )
            mismatch_count = cursor.fetchone()[0]

            # If there are -1 values, investigate why
            if mismatch_count > 0:
                cursor.execute(
                    """
                    SELECT TOP 10
                        f.CleanCode_Vareudlevering,
                        src.Rekvirent_CleanCode,
                        f.DW_EK_Rekvirent,
                        m.DW_EK_Rekvirent AS expected_dw_ek
                    FROM [fct].[Vareudlevering] f
                    INNER JOIN [stg].[fct_vareudlevering_ils] src
                        ON src.CleanCode_Vareudlevering = f.CleanCode_Vareudlevering
                    LEFT JOIN [map].[Rekvirent] m
                        ON m.CleanCode_Rekvirent = src.Rekvirent_CleanCode
                    WHERE COALESCE(m.DW_EK_Rekvirent, -1) <> f.DW_EK_Rekvirent
                    ORDER BY f.CleanCode_Vareudlevering
                """
                )
                sample_mismatches = cursor.fetchall()
                context_msg = "\n".join(
                    [
                        f"  CleanCode={row[0]}, Rekvirent_CleanCode={row[1]}, "
                        f"Fact DW_EK={row[2]}, Expected DW_EK={row[3]}"
                        for row in sample_mismatches
                    ]
                )
        finally:
            cursor.close()
    assert mismatch_count == 0, (
        f"Found {mismatch_count} rows where fact DW_EK_Rekvirent does not match "
        f"map.Rekvirent lookup by CleanCode_Rekvirent.\n"
        f"Sample mismatches:\n{context_msg}"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(5)
def test_dim_rekvirent_organization_fk_coverage(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
):
    """Test DW_EK_Organisation FK in dim.Rekvirent - verify coverage and investigate missing references."""
    _materialize_vareudlevering_chain(
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_rekvirent,
                    SUM(CASE WHEN DW_EK_Organisation = -1 THEN 1 ELSE 0 END) AS missing_org_fk,
                    SUM(CASE WHEN DW_EK_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org_fk
                FROM [dim].[Rekvirent]
                WHERE DW_EK_Rekvirent <> -1
            """
            )
            stats = cursor.fetchone()
            total = stats[0]
            missing_org = stats[1]

            # If many missing references, investigate
            if missing_org > 0 and (missing_org / total) > 0.1:  # More than 10% missing
                cursor.execute(
                    """
                    SELECT TOP 20
                        r.DW_EK_Rekvirent,
                        r.Code,
                        r.CodeSOR,
                        r.SORRelationship,
                        r.Display
                    FROM [dim].[Rekvirent] r
                    WHERE r.DW_EK_Rekvirent <> -1
                      AND r.DW_EK_Organisation = -1
                    ORDER BY r.Code
                """
                )
                sample_missing = cursor.fetchall()
                context_msg = "\n".join(
                    [
                        f"  DW_EK={row[0]}, Code={row[1]}, CodeSOR={row[2]}, "
                        f"SORRelationship={row[3]}, Display={row[4]}"
                        for row in sample_missing
                    ]
                )
                pytest.fail(
                    f"Found {missing_org}/{total} ({missing_org/total*100:.1f}%) Rekvirent rows "
                    f"without Organisation FK. This may indicate missing SOR mappings.\n"
                    f"Sample missing references:\n{context_msg}"
                )

            # Verify resolved FKs point to valid Organization records.
            # dim.Rekvirent resolves DW_EK_Organisation from ref.Organisation
            # (replaced map.Organisation in [internal ticket]).
            cursor.execute(
                """
                SELECT COUNT(*) AS dangling_org_fk
                FROM [dim].[Rekvirent] r
                LEFT JOIN [ref].[Organisation] o
                    ON o.DW_EK_Organisation = r.DW_EK_Organisation
                WHERE r.DW_EK_Rekvirent <> -1
                  AND r.DW_EK_Organisation <> -1
                  AND o.DW_EK_Organisation IS NULL
            """
            )
            dangling_org_fk = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT SUM(CASE WHEN DW_EK_Organisation IS NULL THEN 1 ELSE 0 END)
                FROM [dim].[Rekvirent]
                WHERE DW_EK_Rekvirent <> -1
            """
            )
            null_org_fk = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert null_org_fk == 0, "DW_EK_Organisation should never be NULL in dim.Rekvirent"
    assert dangling_org_fk == 0, (
        f"Found {dangling_org_fk} resolved DW_EK_Organisation values in dim.Rekvirent "
        "that do not exist in ref.Organisation"
    )
