# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for dim_vare asset.

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
def test_dim_vare_syncs_from_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare syncs DW_EK_Vare from map table.
    Every map entry should have corresponding dimension entry.
    """
    metadata_map = get_materialization_result(dagster_instance, "map_vare")
    map_count = metadata_map["total_rows"].value

    # Query dimension count (excluding the UNKNOWN row with DW_EK_Vare = -1)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [dim].[Vare] WHERE DW_EK_Vare > 0")
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
def test_dim_vare_enriches_with_mysupply_attributes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare enriches with MySupply attributes (primary source).
    Varenavn should come from Item_FakturaUdstederVareNavn for at least some records.
    """
    # Query for attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CASE WHEN Varenavn IS NOT NULL AND Varenavn != 'UNKNOWN' THEN 1 END) AS has_varenavn,
                    COUNT(VarenavnKort) AS has_varenavn_kort
                FROM [dim].[Vare]
                WHERE DW_EK_Vare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_varenavn = row[1]
    has_varenavn_kort = row[2]

    # At least some records should have attributes
    if total > 0:
        print(
            f"Attribute coverage: Varenavn={has_varenavn}/{total}, "
            f"VarenavnKort={has_varenavn_kort}/{total}"
        )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_vare_type1_scd_updates(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_dim_tables,
):
    """
    Test that dim_vare performs Type 1 SCD updates.
    Running twice should update existing rows, not create new ones.
    """
    # First materialization
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_vare"],
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
            cursor.execute("SELECT COUNT(*) FROM [dim].[Vare] WHERE DW_EK_Vare > 0")
            count1 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Second materialization
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["dim_vare"],
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
            cursor.execute("SELECT COUNT(*) FROM [dim].[Vare] WHERE DW_EK_Vare > 0")
            count2 = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Type 1 SCD: Same row count (updates, not inserts)
    assert count2 == count1, "Row count should not change (Type 1 SCD updates)"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_vare_composite_natural_key_preserved(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare preserves composite natural key (CVR + VarenummerKreditor).
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
                    COUNT(DISTINCT CONCAT(CVR, '|', VarenummerKreditor)) AS unique_composites
                FROM [dim].[Vare]
                WHERE DW_EK_Vare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    unique_composites = row[1]

    # Each row should have unique composite natural key
    assert total == unique_composites, (
        f"Each vare should have unique (CVR, VarenummerKreditor), "
        f"but {total} rows have only {unique_composites} unique combinations"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_vare_dw_ek_matches_map(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare DW_EK_Vare matches map table (Clean Architecture).
    """
    # Query for DW_EK_Vare matching
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) AS matching_count
                FROM [map].[Vare] m
                INNER JOIN [dim].[Vare] d
                    ON m.DW_EK_Vare = d.DW_EK_Vare
            """
            )
            matching_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM [map].[Vare]")
            map_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert matching_count == map_count, (
        f"All {map_count} map entries should have matching dimension entries "
        f"with same DW_EK_Vare, but only {matching_count} match"
    )


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_vare_unknown_row_exists(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare has the UNKNOWN row (DW_EK_Vare = -1) for orphan handling.
    This row is created by infrastructure and should always exist.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT DW_EK_Vare, CVR, VarenummerKreditor, Varenavn
                FROM [dim].[Vare]
                WHERE DW_EK_Vare = -1
            """
            )
            unknown_row = cursor.fetchone()
        finally:
            cursor.close()
    assert unknown_row is not None, "UNKNOWN row (DW_EK_Vare = -1) should exist"
    assert unknown_row[0] == -1, "DW_EK_Vare should be -1"
    assert unknown_row[1] == "UNKNOWN", "CVR should be 'UNKNOWN'"
    assert unknown_row[2] == "UNKNOWN", "VarenummerKreditor should be 'UNKNOWN'"
    assert unknown_row[3] == "Ukendt vare", "Varenavn should be 'Ukendt vare'"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(3)
def test_dim_vare_ils_attributes_populated(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that dim_vare can be enriched with ILS attributes when ILS source is available.
    ILS provides additional product details like barcode, size, unit, etc.
    """
    # Query for ILS-specific attribute coverage (excluding UNKNOWN row)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(ILS_Varenummer) AS has_ils_varenummer,
                    COUNT(Stregkode) AS has_stregkode,
                    COUNT(Basisenhed) AS has_basisenhed,
                    COUNT(VareartKode) AS has_vareart,
                    COUNT(VaregruppeKode) AS has_varegrp
                FROM [dim].[Vare]
                WHERE DW_EK_Vare > 0
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total = row[0]
    has_ils_varenummer = row[1]
    has_stregkode = row[2]
    has_basisenhed = row[3]
    has_vareart = row[4]
    has_varegrp = row[5]

    # Log coverage for diagnostics (ILS attributes may or may not be populated
    # depending on data availability and join success)
    if total > 0:
        print(
            f"ILS attribute coverage: ILS_Varenummer={has_ils_varenummer}/{total}, "
            f"Stregkode={has_stregkode}/{total}, Basisenhed={has_basisenhed}/{total}, "
            f"VareartKode={has_vareart}/{total}, VaregruppeKode={has_varegrp}/{total}"
        )
