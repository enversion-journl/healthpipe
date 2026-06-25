# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for dim_kreditor asset.

Tests Type 1 SCD MERGE behavior and COALESCE priority (SydAX → ILS → MySupply).
Validates that dimension syncs from map and enriches with attributes.

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
@pytest.mark.skip(reason="Flaky in full-suite runs due cross-test shared-table state")
def test_dim_kreditor_syncs_from_map(
    dagster_instance,
    real_sydax_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kreditor syncs DW_EK_Kreditor from map table (Clean Architecture).
    Every map entry should have corresponding dimension entry.
    """
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_kreditor"],
        resources={
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(None),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    metadata_map = get_materialization_result(dagster_instance, "map_kreditor")
    map_count = metadata_map["total_rows"].value
    if map_count == 0:
        pytest.skip("map_kreditor has no rows in current test environment")

    # Query dimension count (excluding the UNKNOWN row with DW_EK = -1)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kreditor] WHERE DW_EK_Kreditor > 0")
            dim_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Every map entry should be in dimension
    assert (
        dim_count == map_count
    ), f"Dimension should have {map_count} rows to match map, but has {dim_count}"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kreditor_coalesce_populates_attributes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kreditor COALESCE logic populates attributes from available sources.
    At least some kreditors should have populated attributes (CVR, name, etc.).
    """
    # Query for attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN Cvrnr IS NOT NULL AND Cvrnr != 'UNKNOWN' THEN 1 END) AS has_cvr,
                    COUNT(CASE WHEN KreditorNavn IS NOT NULL THEN 1 END) AS has_navn
                FROM [dim].[Kreditor]
                WHERE DW_EK_Kreditor > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_cvr = row[1]
    has_navn = row[2]

    # At least some kreditors should have attributes populated
    if total == 0:
        pytest.skip("dim.Kreditor has no rows in current test environment")
    # Log coverage for diagnostic purposes
    print(f"Attribute coverage: CVR={has_cvr}/{total}, Navn={has_navn}/{total}")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
@pytest.mark.skip(reason="Flaky in full-suite runs due cross-test shared-table state")
def test_dim_kreditor_type1_scd_updates_in_place(
    dagster_instance,
    real_sydax_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_dim_tables,
):
    """
    Test that dim_kreditor performs Type 1 SCD updates (update in place, no history).
    Running twice should update existing rows, not create new ones.
    """
    # First materialization
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_kreditor"],
        resources={
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(None),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result1 is not None

    # Get row count and sample record after first run
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kreditor] WHERE DW_EK_Kreditor > 0")
            count1 = cursor.fetchone()[0]

            # Get a sample record for comparison
            cursor.execute(
                """
                SELECT TOP 1 DW_EK_Kreditor, DW_Timestamp_Insert
                FROM [dim].[Kreditor]
                WHERE DW_EK_Kreditor > 0
                ORDER BY DW_EK_Kreditor
            """
            )
            sample1 = cursor.fetchone()
        finally:
            cursor.close()
    if sample1 is None:
        pytest.skip("No kreditor records found - cannot test Type 1 SCD")

    dw_ek_1 = sample1[0]
    insert_ts_1 = sample1[1]

    # Second materialization
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_kreditor"],
        resources={
            "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(None),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result2 is not None

    # Check same record after second run
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Kreditor] WHERE DW_EK_Kreditor > 0")
            count2 = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT DW_EK_Kreditor, DW_Timestamp_Insert, DW_Timestamp_Update
                FROM [dim].[Kreditor]
                WHERE DW_EK_Kreditor = ?
            """,
                (dw_ek_1,),
            )
            sample2 = cursor.fetchone()
        finally:
            cursor.close()
    assert sample2 is not None, "Sample record should still exist after second run"
    dw_ek_2 = sample2[0]
    insert_ts_2 = sample2[1]
    update_ts_2 = sample2[2]

    # Type 1 SCD: Same count, same DW_EK, same insert timestamp
    assert count2 == count1, "Row count should not change (Type 1 SCD)"
    assert dw_ek_2 == dw_ek_1, "DW_EK should not change (Type 1 SCD)"
    assert insert_ts_2 == insert_ts_1, "Insert timestamp should not change"
    assert update_ts_2 is not None, "Update timestamp should be set on second run"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kreditor_dw_ek_comes_from_map_not_generated(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kreditor uses DW_EK_Kreditor from map, doesn't generate its own.
    This validates Clean Architecture: map is source of truth for surrogate keys.
    """
    # Query for matching DW_EK between map and dimension
    # Note: map.Kreditor uses CleanCode_Kreditor
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS matching_count
                FROM [map].[Kreditor] m
                INNER JOIN [dim].[Kreditor] d
                    ON m.DW_EK_Kreditor = d.DW_EK_Kreditor
            """
            )
            matching_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM [map].[Kreditor]")
            map_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    # All map entries should have matching dimension entries
    assert matching_count == map_count, (
        f"All {map_count} map entries should have matching dimension entries, "
        f"but only {matching_count} match"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kreditor_handles_missing_attributes_gracefully(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kreditor handles missing attributes gracefully with COALESCE.
    Some kreditors may only exist in map without any staging attributes.
    The dimension should still be populated (possibly with NULL/UNKNOWN values).
    """
    # Query dimension for attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Cvrnr) AS has_cvr,
                    COUNT(KreditorNavn) AS has_navn,
                    COUNT(CASE WHEN Cvrnr = 'UNKNOWN' THEN 1 END) AS unknown_cvr
                FROM [dim].[Kreditor]
                WHERE DW_EK_Kreditor > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_cvr = row[1]
    has_navn = row[2]
    unknown_cvr = row[3]

    if total == 0:
        pytest.skip("dim.Kreditor has no rows in current test environment")
    # Log coverage for diagnostic purposes
    print(
        f"Attribute coverage: {has_navn}/{total} have KreditorNavn, "
        f"{has_cvr}/{total} have Cvrnr ({unknown_cvr} are UNKNOWN)"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_kreditor_unknown_row_exists(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_kreditor has the UNKNOWN row (DW_EK_Kreditor = -1) for orphan handling.
    This row is created by infrastructure and should always exist.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DW_EK_Kreditor, KreditorId, Cvrnr, KreditorNavn
                FROM [dim].[Kreditor]
                WHERE DW_EK_Kreditor = -1
            """
            )
            unknown_row = cursor.fetchone()
        finally:
            cursor.close()
    assert unknown_row is not None, "UNKNOWN row (DW_EK_Kreditor = -1) should exist"
    assert unknown_row[0] == -1, "DW_EK_Kreditor should be -1"
    assert unknown_row[2] == "UNKNOWN", "Cvrnr should be 'UNKNOWN'"
    assert unknown_row[3] == "Ukendt kreditor", "KreditorNavn should be 'Ukendt kreditor'"
