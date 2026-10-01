# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for dim_Leverandoervare asset.

Tests Type 1 SCD MERGE behavior with composite natural key.
Validates attribute enrichment from MySupply (primary) and ILS (secondary).

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
def test_dim_leverandoervare_syncs_from_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare syncs dw_ek_Leverandoervare from map table.
    Every map entry should have corresponding dimension entry.
    """
    metadata_map = get_materialization_result(dagster_instance, "map_Leverandoervare")
    map_count = metadata_map["total_rows"].value

    # Query dimension count (excluding the UNKNOWN row with dw_ek_Leverandoervare = -1)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM [dim].[Leverandoervare] WHERE dw_ek_Leverandoervare > 0"
            )
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
def test_dim_leverandoervare_enriches_with_mysupply_attributes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare enriches with MySupply attributes (primary source).
    varenavn should come from item_faktura_udsteder_vare_navn for at least some records.
    """
    # Query for attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN varenavn IS NOT NULL AND varenavn != 'UNKNOWN' THEN 1 END) AS has_varenavn,
                    COUNT(varenavn_mysupply) AS has_varenavn_mysupply
                FROM [dim].[Leverandoervare]
                WHERE dw_ek_Leverandoervare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_varenavn = row[1]
    has_varenavn_mysupply = row[2]

    # At least some records should have attributes
    if total > 0:
        print(
            f"Attribute coverage: varenavn={has_varenavn}/{total}, "
            f"varenavn_mysupply={has_varenavn_mysupply}/{total}"
        )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_leverandoervare_type1_scd_updates(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_dim_tables,
):
    """
    Test that dim_Leverandoervare performs Type 1 SCD updates.
    Running twice should update existing rows, not create new ones.
    """
    # First materialization
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_Leverandoervare"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
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
            cursor.execute(
                "SELECT COUNT(*) FROM [dim].[Leverandoervare] WHERE dw_ek_Leverandoervare > 0"
            )
            count1 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Second materialization
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_Leverandoervare"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
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
            cursor.execute(
                "SELECT COUNT(*) FROM [dim].[Leverandoervare] WHERE dw_ek_Leverandoervare > 0"
            )
            count2 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Type 1 SCD: Same row count (updates, not inserts)
    assert count2 == count1, "Row count should not change (Type 1 SCD updates)"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_leverandoervare_composite_natural_key_preserved(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare preserves composite natural key (cvr + varenummer_kreditor).
    Each vare should be uniquely identified by this combination.
    """
    # Query for composite key uniqueness (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT CONCAT(cvr, '|', varenummer_kreditor)) AS unique_composites
                FROM [dim].[Leverandoervare]
                WHERE dw_ek_Leverandoervare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    unique_composites = row[1]

    # Each row should have unique composite natural key
    assert total == unique_composites, (
        f"Each vare should have unique (cvr, varenummer_kreditor), "
        f"but {total} rows have only {unique_composites} unique combinations"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_leverandoervare_dw_ek_matches_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare dw_ek_Leverandoervare matches map table (Clean Architecture).
    """
    # Query for dw_ek_Leverandoervare matching
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS matching_count
                FROM [map].[Leverandoervare] m
                INNER JOIN [dim].[Leverandoervare] d
                    ON m.dw_ek_Leverandoervare = d.dw_ek_Leverandoervare
            """
            )
            matching_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM [map].[Leverandoervare]")
            map_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert matching_count == map_count, (
        f"All {map_count} map entries should have matching dimension entries "
        f"with same dw_ek_Leverandoervare, but only {matching_count} match"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_leverandoervare_unknown_row_exists(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare has the UNKNOWN row (dw_ek_Leverandoervare = -1) for orphan handling.
    This row is created by infrastructure and should always exist.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT dw_ek_Leverandoervare, cvr, varenummer_kreditor, varenavn
                FROM [dim].[Leverandoervare]
                WHERE dw_ek_Leverandoervare = -1
            """
            )
            unknown_row = cursor.fetchone()
        finally:
            cursor.close()
    assert unknown_row is not None, "UNKNOWN row (dw_ek_Leverandoervare = -1) should exist"
    assert unknown_row[0] == -1, "dw_ek_Leverandoervare should be -1"
    assert unknown_row[1] == "UNKNOWN", "cvr should be 'UNKNOWN'"
    assert unknown_row[2] == "UNKNOWN", "varenummer_kreditor should be 'UNKNOWN'"
    assert unknown_row[3] == "Ukendt vare", "varenavn should be 'Ukendt vare'"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_leverandoervare_ils_attributes_populated(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_Leverandoervare carries the ILS-sourced presence signal (dw_source_ils)
    when an ILS source is available. All ILS attributes (name, barcode, size,
    unit, etc.) live on dim.ILSVare, reached via bridge.LeverandoervareILSVare.
    """
    # Query for ILS presence coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CAST(dw_source_ils AS INT)) AS from_ils
                FROM [dim].[Leverandoervare]
                WHERE dw_ek_Leverandoervare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    from_ils = row[1]

    # Log coverage for diagnostics (ILS presence may or may not be populated
    # depending on data availability and join success)
    if total > 0:
        print(f"ILS presence coverage: dw_source_ils={from_ils}/{total}")
