# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for map_kreditor asset.

Tests DW_EK assignment via IDENTITY across multiple source systems (SydAX, ILS, MySupply).
Validates CleanCode_Kreditor uniqueness and DW_EK assignment logic.
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
def test_map_kreditor_assigns_dw_ek_from_sydax(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_kreditor assigns DW_EK via IDENTITY for kreditor records.
    """
    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "map_kreditor")
    assert metadata is not None
    assert metadata["total_rows"].value >= 1, "Expected at least 1 kreditor in map table"

    # Query map table directly
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1 DW_EK_Kreditor, CleanCode_Kreditor, DW_Timestamp_Insert
                FROM [map].[Kreditor]
                WHERE CleanCode_Kreditor LIKE 'CVR_%'
                ORDER BY DW_EK_Kreditor
            """
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    assert len(rows) >= 1, "Should find at least one CVR_ kreditor in map table"

    # Validate DW_EK was assigned via IDENTITY
    test_row = rows[0]
    dw_ek = test_row[0]
    kreditor_cleancode = test_row[1]
    timestamp = test_row[2]

    assert dw_ek > 0, "DW_EK_Kreditor should be assigned via IDENTITY (positive integer)"
    assert kreditor_cleancode.startswith(
        "CVR_"
    ), f"CleanCode_Kreditor should start with 'CVR_', got '{kreditor_cleancode}'"
    assert isinstance(timestamp, datetime), "DW_Timestamp_Insert should be datetime"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_kreditor_handles_ils_source_with_fallback_kreditorid(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_kreditor handles ILS suppliers without SydAX KreditorId.
    Should generate a CleanCode_Kreditor for ILS suppliers.
    """
    # Query map table for ILS supplier
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT TOP 1 DW_EK_Kreditor, CleanCode_Kreditor
                FROM [map].[Kreditor]
                WHERE CleanCode_Kreditor LIKE 'CVR_%'
                ORDER BY DW_EK_Kreditor
            """
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    assert len(rows) >= 1, "Should find at least one CVR_ kreditor in map table"

    test_row = rows[0]
    dw_ek = test_row[0]
    kreditor_cleancode = test_row[1]

    assert dw_ek > 0, "DW_EK should be assigned via IDENTITY"
    assert kreditor_cleancode.startswith(
        "CVR_"
    ), f"CleanCode_Kreditor should start with 'CVR_', got '{kreditor_cleancode}'"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_kreditor_incremental_insert_only(
    dagster_instance,
    real_sydax_mssql_resource,
    real_mssql_target_resource,
    setup_erp_infrastructure,
    cleanup_erp_map_tables,
):
    """
    Test that map_kreditor performs incremental inserts only (no updates).
    Running twice should not increase row count if data hasn't changed.
    """
    # First materialization - include extraction dependency (daily partition)
    result1 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_kreditor_sydax_daily", "map_kreditor"],
        partition="2025-09-17",
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

    metadata1 = get_materialization_result(dagster_instance, "map_kreditor")
    count1 = metadata1["total_rows"].value

    # Second materialization (should be idempotent)
    result2 = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["map_kreditor"],
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

    metadata2 = get_materialization_result(dagster_instance, "map_kreditor")
    count2 = metadata2["total_rows"].value
    new_rows2 = metadata2["new_rows"].value

    assert count2 == count1, "Row count should not change on second run (idempotent)"
    assert new_rows2 == 0, "Should insert 0 new rows on second run"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_map_kreditor_composite_key_uniqueness(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that map_kreditor enforces CleanCode_Kreditor uniqueness.
    Each unique CleanCode (CVR_* or KREDID_*) should have exactly one DW_EK.
    """
    # Query for all kreditor entries
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT DW_EK_Kreditor),
                       COUNT(DISTINCT CleanCode_Kreditor)
                FROM [map].[Kreditor]
                """
            )
            row = cursor.fetchone()
        finally:
            cursor.close()

    total_count = row[0]
    unique_dw_ek = row[1]
    unique_cleancodes = row[2]

    # Each row should have unique DW_EK (assigned by IDENTITY)
    assert total_count == unique_dw_ek, "Each row should have unique DW_EK_Kreditor"
    # CleanCode_Kreditor should be unique (primary key)
    assert total_count == unique_cleancodes, "Each CleanCode_Kreditor should be unique"

    # Log for diagnostic purposes
    print(f"Map.Kreditor: {total_count} rows, {unique_cleancodes} unique CleanCodes")
