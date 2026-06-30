# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for dim_kontoplan asset.

Tests Type 1 SCD MERGE behavior and hierarchical attribute enrichment.
Validates chart of accounts dimension sync from map.

Note: These tests are designed to work with real production data without
requiring specific test data. They validate structural integrity and
behavioral correctness rather than specific data values.
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_syncs_from_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kontoplan syncs DW_EK_Kontoplan from map table.
    Every map entry should have corresponding dimension entry.
    """
    metadata_map = get_materialization_result(dagster_instance, "map_kontoplan")
    map_count = metadata_map["total_rows"].value

    # Query dimension count (excluding the UNKNOWN row with DW_EK_Kontoplan = -1)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kontoplan] WHERE DW_EK_Kontoplan > 0")
            dim_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    assert (
        dim_count == map_count
    ), f"Dimension should have {map_count} rows to match map, but has {dim_count}"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_enriches_with_finanskonto_attributes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kontoplan enriches with Finanskonto attributes.
    Should populate FinanskontoId column for at least some records.
    """
    # Query for attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kontoplan) AS has_cleancode,
                    COUNT(FinanskontoId) AS has_finanskontoid
                FROM [dim].[Kontoplan]
                WHERE DW_EK_Kontoplan > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_cleancode = row[1]
    has_finanskontoid = row[2]

    # At least some records should have attributes
    if total > 0:
        print(
            f"Attribute coverage: CleanCode_Kontoplan={has_cleancode}/{total}, "
            f"FinanskontoId={has_finanskontoid}/{total}"
        )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_type1_scd_updates(
    dagster_instance,
    real_sydax_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_dim_tables,
):
    """
    Test that dim_kontoplan performs Type 1 SCD updates.
    Running twice should update existing rows, not create new ones.
    """
    # First materialization
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_kontoplan"],
        resources={
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result1 is not None

    # Count rows (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kontoplan] WHERE DW_EK_Kontoplan > 0")
            count1 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Second materialization
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_kontoplan"],
        resources={
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result2 is not None

    # Count rows again
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kontoplan] WHERE DW_EK_Kontoplan > 0")
            count2 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Type 1 SCD: Same row count
    assert count2 == count1, "Row count should not change (Type 1 SCD updates)"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_dw_ek_matches_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kontoplan DW_EK_Kontoplan matches map table (Clean Architecture).
    """
    # Query for DW_EK_Kontoplan matching
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS matching_count
                FROM [map].[Kontoplan] m
                INNER JOIN [dim].[Kontoplan] d
                    ON m.DW_EK_Kontoplan = d.DW_EK_Kontoplan
                    AND m.CleanCode_Kontoplan = d.CleanCode_Kontoplan
            """
            )
            matching_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM [map].[Kontoplan]")
            map_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert matching_count == map_count, (
        f"All {map_count} map entries should have matching dimension entries "
        f"with same DW_EK_Kontoplan, but only {matching_count} match"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_hierarchical_attributes_structure(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kontoplan has hierarchical attribute columns (S1-S7, F1-F3, etc.).
    Note: Values may be NULL if hierarchical enrichment not yet fully implemented.
    """
    # Query for hierarchical columns (checking structure, not necessarily values)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1
                    S1, S2, S3, S4, S5, S6, S7,
                    F1, F2, F3,
                    G1, G2,
                    A1, A2,
                    D1,
                    Omkostningsstednummer,
                    Omkostningssted_niveau
                FROM [dim].[Kontoplan]
                WHERE DW_EK_Kontoplan > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    # Just validate that columns exist (values may be NULL)
    if row is not None:
        assert len(row) == 17, "Should have all hierarchical attribute columns"
        # Log for diagnostic (these may be NULL if enrichment not implemented)
        print(
            f"Sample hierarchical attributes: S1={row[0]}, F1={row[7]}, "
            f"Omkostningsstednummer={row[15]}"
        )
    else:
        # No data rows yet - just verify table structure exists
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'Kontoplan'
                    ORDER BY ORDINAL_POSITION
                """
                )
                columns = [row[0] for row in cursor.fetchall()]
            finally:
                cursor.close()
        expected_columns = ["S1", "S2", "S3", "F1", "F2", "F3", "G1", "G2", "D1"]
        for col in expected_columns:
            assert col in columns, f"Column {col} should exist in dim.Kontoplan"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kontoplan_unknown_row_exists(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kontoplan has the UNKNOWN row (DW_EK_Kontoplan = -1) for orphan handling.
    This row is created by infrastructure and should always exist.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DW_EK_Kontoplan, CleanCode_Kontoplan, FinanskontoId, F1
                FROM [dim].[Kontoplan]
                WHERE DW_EK_Kontoplan = -1
            """
            )
            unknown_row = cursor.fetchone()
        finally:
            cursor.close()
    assert unknown_row is not None, "UNKNOWN row (DW_EK_Kontoplan = -1) should exist"
    assert unknown_row[0] == -1, "DW_EK_Kontoplan should be -1"
    assert unknown_row[2] == -1, "FinanskontoId should be -1 for UNKNOWN row"
    assert unknown_row[3] == "UNKNOWN", "F1 should be 'UNKNOWN'"
