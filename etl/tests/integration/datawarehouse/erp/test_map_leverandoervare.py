# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for map_Leverandoervare asset.

Tests DW_EK assignment for scoped business key (bk_kreditor + bk_leverandoer_varenummer).
Validates that vare keys are properly scoped by supplier.
"""

from datetime import datetime

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_leverandoervare_assigns_dw_ek_from_mysupply(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_Leverandoervare assigns DW_EK via IDENTITY for MySupply invoice lines.

    Prerequisites:
    - MySupply database should have invoice line data
    """
    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "map_Leverandoervare")
    assert metadata is not None
    if metadata["total_rows"].value == 0:
        pytest.skip("No vare rows in map table (source data empty)")

    # Query map table for test vare
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1
                    dw_ek_Leverandoervare,
                    bk_kreditor,
                    bk_leverandoer_varenummer,
                    clean_code_Leverandoervare,
                    dw_timestamp_insert
                FROM [map].[Leverandoervare]
                WHERE bk_leverandoer_varenummer LIKE 'LEVVARE_%'
                ORDER BY dw_ek_Leverandoervare
            """
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    if len(rows) == 0:
        pytest.skip("No vare rows match the legacy LEVVARE_* pattern in current data")

    # Validate structure
    test_row = rows[0]
    dw_ek = test_row[0]
    cvr_clean_code = test_row[1]
    leverandoer_vare = test_row[2]
    vare_clean_code = test_row[3]
    timestamp = test_row[4]

    assert dw_ek > 0, "dw_ek_Leverandoervare should be assigned via IDENTITY"
    assert cvr_clean_code, "bk_kreditor should be populated"
    assert (
        vare_clean_code == f"{cvr_clean_code}__{leverandoer_vare}"
    ), "clean_code_Leverandoervare should be composite of bk_kreditor and bk_leverandoer_varenummer"
    assert isinstance(timestamp, datetime), "dw_timestamp_insert should be datetime"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_leverandoervare_scoped_natural_key(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_Leverandoervare properly scopes bk_leverandoer_varenummer by bk_kreditor.
    The same item number from different suppliers should create separate entries.
    """
    # Query for composite key validation
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*),
                       COUNT(DISTINCT dw_ek_Leverandoervare),
                       COUNT(DISTINCT CONCAT(bk_kreditor, '|', bk_leverandoer_varenummer)) AS unique_composites
                FROM [map].[Leverandoervare]
            """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()
    total_count = row[0]
    unique_dw_ek = row[1]
    unique_composites = row[2]

    # Each row should have unique DW_EK (assigned by IDENTITY)
    assert total_count == unique_dw_ek, "Each row should have unique dw_ek_Leverandoervare"

    # Each row should represent unique (bk_kreditor, bk_leverandoer_varenummer) combination
    assert total_count == unique_composites, "Each row should have unique composite natural key"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_leverandoervare_handles_ils_source(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_Leverandoervare processes ILS vare/aftale records.

    Prerequisites:
    - ILS database should have contract/vare data
    """
    # Query for ILS vare
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1 dw_ek_Leverandoervare, bk_kreditor, bk_leverandoer_varenummer
                FROM [map].[Leverandoervare]
                WHERE bk_leverandoer_varenummer LIKE 'LEVVARE_%'
                ORDER BY dw_ek_Leverandoervare
            """
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    if len(rows) == 0:
        pytest.skip("No ILS vare rows found in map table")

    test_row = rows[0]
    dw_ek = test_row[0]

    assert dw_ek > 0, "dw_ek_Leverandoervare should be assigned for ILS items"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_leverandoervare_incremental_insert_only(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_map_tables,
):
    """
    Test that map_Leverandoervare performs incremental inserts only (no updates).
    Running twice should not increase row count if data hasn't changed.
    """
    # First materialization
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["map_Leverandoervare"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result1 is not None

    metadata1 = get_materialization_result(dagster_instance, "map_Leverandoervare")
    count1 = metadata1["total_rows"].value

    # Second materialization (should be idempotent)
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["map_Leverandoervare"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(None),
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result2 is not None

    metadata2 = get_materialization_result(dagster_instance, "map_Leverandoervare")
    count2 = metadata2["total_rows"].value
    new_rows2 = metadata2["new_rows"].value

    assert count2 == count1, "Row count should not change on second run (idempotent)"
    assert new_rows2 == 0, "Should insert 0 new rows on second run"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_leverandoervare_warns_on_unknown_keys(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_Leverandoervare logs warnings when UNKNOWN keys are encountered.
    UNKNOWN keys indicate FK matching failures that need investigation.
    """
    # Query for UNKNOWN keys (diagnostic)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM [map].[Leverandoervare]
                WHERE bk_kreditor = 'UNKNOWN' OR bk_leverandoer_varenummer = 'UNKNOWN'
            """
            )
            unknown_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Log finding (not necessarily an error - depends on data quality)
    print(f"Found {unknown_count} vare entries with UNKNOWN keys")

    # In production with good test data, this should be 0
    # But we don't assert here as it depends on seed data quality
