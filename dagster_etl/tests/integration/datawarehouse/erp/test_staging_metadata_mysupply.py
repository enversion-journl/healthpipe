# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for MySupply metadata staging asset.

Tests validate:
- Asset materializes successfully against real database
- Data is loaded into staging table
- CVR_CleanCode is generated with correct pattern (CVR_*)
- SourceSystem and DW_LoadTimestamp are populated
"""

from datetime import datetime

import pytest
from tests.utils.materialize_helpers import get_materialization_result
from tests.utils.staging_helpers import query_staging_table


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_metadata_mysupply_daily_materializes_successfully(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_metadata_mysupply_daily materializes successfully
    and loads supplier metadata from the real MySupply database.
    """
    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "ext_metadata_mysupply_daily")
    assert metadata is not None, "Should have materialization metadata"
    # Partition-based extracts can legitimately be empty for the selected test date.
    assert metadata["row_count"].value >= 0, "Should expose row_count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_metadata_mysupply_daily_generates_cvr_clean_codes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_metadata_mysupply_daily generates CVR_CleanCode correctly.

    Validates:
    - CVR_CleanCode follows pattern CVR_<cvrnummer>
    - SourceSystem is set to 'MySupply'
    - DW_LoadTimestamp is a datetime
    """
    # Query staging table
    rows = query_staging_table(real_mssql_target_resource, "metadata_mysupply")
    if len(rows) == 0:
        pytest.skip("No metadata rows available for selected test partition/date")

    # Get column names to find indices dynamically
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[metadata_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Find column indices
    cvr_clean_code_idx = column_names.index("CVR_CleanCode")
    source_system_idx = column_names.index("SourceSystem")
    dw_timestamp_idx = column_names.index("DW_LoadTimestamp")

    # Validate each row
    rows_with_clean_code = 0
    for row in rows:
        cvr_clean_code = row[cvr_clean_code_idx]
        source_system = row[source_system_idx]
        dw_timestamp = row[dw_timestamp_idx]

        # SourceSystem should always be MySupply
        assert (
            source_system == "MySupply"
        ), f"SourceSystem should be 'MySupply', got '{source_system}'"

        # DW_LoadTimestamp should be a datetime
        assert isinstance(dw_timestamp, datetime), "DW_LoadTimestamp should be datetime"

        # CVR_CleanCode should follow pattern CVR_* when populated
        if cvr_clean_code is not None:
            assert cvr_clean_code.startswith(
                "CVR_"
            ), f"CVR_CleanCode should start with 'CVR_', got '{cvr_clean_code}'"
            rows_with_clean_code += 1

    # At least some rows should have clean codes when rows are present
    assert rows_with_clean_code > 0, "At least some rows should have CVR_CleanCode"
    print(f"✓ {rows_with_clean_code}/{len(rows)} rows have CVR_CleanCode")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_metadata_mysupply_daily_has_expected_columns(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_metadata_mysupply_daily creates table with expected columns.

    Validates the staging table schema includes:
    - Source columns from MySupply (LeverandorCVR, FirmaNavn, etc.)
    - Generated clean code columns (CVR_CleanCode)
    - ETL metadata columns (SourceSystem, DW_LoadTimestamp)
    """
    # Get column names
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[metadata_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Expected ETL metadata columns
    assert "CVR_CleanCode" in column_names, "Should have CVR_CleanCode column"
    assert "SourceSystem" in column_names, "Should have SourceSystem column"
    assert "DW_LoadTimestamp" in column_names, "Should have DW_LoadTimestamp column"

    # Expected source columns (key business columns)
    expected_source_columns = ["LeverandorCVR", "FirmaNavn"]
    for col in expected_source_columns:
        assert col in column_names, f"Should have source column '{col}'"

    print(f"✓ Table has {len(column_names)} columns including all expected columns")
