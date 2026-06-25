# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for MySupply staging assets (daily partitioned).

Tests validate:
- Assets materialize successfully against real database
- Data is loaded into staging tables
- Clean codes are generated with correct patterns
- SourceSystem and DW_LoadTimestamp are populated
- DW_PartitionDate tracks partition membership
- String date partitioning (FakturaDato) works correctly
"""

from datetime import date, datetime

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)
from tests.utils.staging_helpers import query_staging_table

# Default test partition - a date known to have data in test DB
TEST_PARTITION_KEY = "2007-01-12"

pytestmark = pytest.mark.skip(
    reason="Legacy stg_* MySupply assets are no longer part of the active extraction pipeline"
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_stg_statistic_invoice_header_mysupply_daily_materializes_successfully(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that stg_statistic_invoice_header_mysupply_daily materializes successfully
    and loads data from the real MySupply database.

    This is a ROOT_PARTITION table - data is filtered by FakturaDato = partition_key.
    Note: FakturaDato is a string column in YYYY-MM-DD format.
    """
    # Materialize the staging asset
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["stg_statistic_invoice_header_mysupply_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(
        dagster_instance, "stg_statistic_invoice_header_mysupply_daily"
    )
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should have row count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_stg_statistic_invoice_line_mysupply_daily_materializes_successfully(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that stg_statistic_invoice_line_mysupply_daily materializes successfully
    and loads data from the real MySupply database.

    This is a DEPENDENT table - data is filtered via FK to statistic_invoice_header.
    """
    # Materialize the staging asset
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["stg_statistic_invoice_line_mysupply_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(
        dagster_instance, "stg_statistic_invoice_line_mysupply_daily"
    )
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should have row count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_stg_statistic_invoice_line_mysupply_daily_generates_clean_codes(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_staging_tables,
):
    """
    Test that stg_statistic_invoice_line_mysupply_daily generates clean codes correctly.

    Validates:
    - Leverandoer_Varenummer_CleanCode follows expected pattern
    - SourceSystem is set to 'MySupply'
    - DW_LoadTimestamp is a datetime
    - DW_PartitionDate matches the partition key
    """
    # Materialize the staging asset
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["stg_statistic_invoice_line_mysupply_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    # Query staging table
    rows = query_staging_table(real_mssql_target_resource, "statistic_invoice_line_mysupply")
    if len(rows) == 0:
        pytest.skip("No data found for test partition - skipping validation")

    # Get column names to find indices dynamically
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[statistic_invoice_line_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Find column indices
    vare_clean_code_idx = column_names.index("Leverandoer_Varenummer_CleanCode")
    source_system_idx = column_names.index("SourceSystem")
    dw_timestamp_idx = column_names.index("DW_LoadTimestamp")
    dw_partition_date_idx = column_names.index("DW_PartitionDate")

    # Validate each row
    rows_with_clean_code = 0
    for row in rows:
        vare_clean_code = row[vare_clean_code_idx]
        source_system = row[source_system_idx]
        dw_timestamp = row[dw_timestamp_idx]
        dw_partition_date = row[dw_partition_date_idx]

        # SourceSystem should always be MySupply
        assert (
            source_system == "MySupply"
        ), f"SourceSystem should be 'MySupply', got '{source_system}'"

        # DW_LoadTimestamp should be a datetime
        assert isinstance(dw_timestamp, datetime), "DW_LoadTimestamp should be datetime"

        # DW_PartitionDate should match the partition key
        expected_date = date.fromisoformat(TEST_PARTITION_KEY)
        assert (
            dw_partition_date == expected_date
        ), f"DW_PartitionDate should be {expected_date}, got {dw_partition_date}"

        # Vare_CleanCode should follow pattern VARE_* when populated
        if vare_clean_code is not None:
            assert vare_clean_code.startswith("LEVVARE_"), (
                "Leverandoer_Varenummer_CleanCode should start with 'LEVVARE_', "
                f"got '{vare_clean_code}'"
            )
            rows_with_clean_code += 1

    # At least some rows should have clean codes
    if rows_with_clean_code > 0:
        print(f"Validated {rows_with_clean_code}/{len(rows)} rows have Vare_CleanCode")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_stg_statistic_invoice_line_mysupply_daily_has_expected_columns(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that stg_statistic_invoice_line_mysupply_daily creates table with expected columns.

    Validates the staging table schema includes:
    - Source columns from MySupply
    - Generated clean code columns
    - ETL metadata columns (SourceSystem, DW_LoadTimestamp, DW_PartitionDate)
    """
    # Materialize the staging asset
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["stg_statistic_invoice_line_mysupply_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    # Get column names
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[statistic_invoice_line_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Expected ETL metadata columns
    assert (
        "Leverandoer_Varenummer_CleanCode" in column_names
    ), "Should have Leverandoer_Varenummer_CleanCode column"
    assert "FakturaLines_CleanCode" in column_names, "Should have FakturaLines_CleanCode column"
    assert "SourceSystem" in column_names, "Should have SourceSystem column"
    assert "DW_LoadTimestamp" in column_names, "Should have DW_LoadTimestamp column"
    assert "DW_PartitionDate" in column_names, "Should have DW_PartitionDate column"

    # Expected source columns (key business columns)
    expected_source_columns = [
        "id",
        "statistic_invoice_header_id",
    ]
    for col in expected_source_columns:
        assert col in column_names, f"Should have source column '{col}'"

    print(f"Table has {len(column_names)} columns including all expected columns")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_stg_metadata_mysupply_daily_materializes_successfully(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_staging_tables,
):
    """
    Test that stg_metadata_mysupply_daily materializes successfully
    and loads data from the real MySupply database.

    This is a DEPENDENT table - data is filtered via FK to statistic_invoice_header.
    """
    # Materialize the staging asset
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["stg_metadata_mysupply_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "stg_metadata_mysupply_daily")
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should have row count metadata"
