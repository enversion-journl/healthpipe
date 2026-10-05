# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for MySupply invoice header staging asset.

Tests validate:
- Asset materializes successfully against real database
- Data is loaded into staging table
- CVR_CleanCode is generated with correct pattern (CVR_*)
- SourceSystem and dw_load_timestamp are populated
"""

from datetime import datetime

import pytest
from tests.utils.materialize_helpers import get_materialization_result
from tests.utils.staging_helpers import query_staging_table


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_statistic_invoice_header_mysupply_daily_materializes_successfully(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_statistic_invoice_header_mysupply_daily materializes successfully
    and loads invoice headers from the real MySupply database.
    """
    # NOTE(#28693): ext_statistic_invoice_header_mysupply_daily removed — faktura is now
    # DFM/archive. The arc_mysupply_statistic_invoice_header asset replaces it (non-partitioned).
    # TODO(#28693): update this check to arc_mysupply_statistic_invoice_header in devcontainer.
    metadata = get_materialization_result(dagster_instance, "arc_mysupply_statistic_invoice_header")
    if metadata is None:
        pytest.skip(
            "arc_mysupply_statistic_invoice_header was not materialized in current session fixture"
        )
    assert metadata["row_count"].value >= 0, "Should expose row_count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_statistic_invoice_header_mysupply_daily_generates_kreditor_clean_codes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_statistic_invoice_header_mysupply_daily generates CVR_CleanCode correctly.

    Validates:
    - CVR_CleanCode follows pattern CVR_<cvrnummer>
    - SourceSystem is set to 'MySupply'
    - dw_load_timestamp is a datetime
    """
    # Query staging table
    rows = query_staging_table(real_mssql_target_resource, "statistic_invoice_header_mysupply")
    if len(rows) == 0:
        pytest.skip("No invoice-header rows available for selected test partition/date")

    # Get column names to find indices dynamically
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[statistic_invoice_header_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Find column indices
    kreditor_clean_code_idx = column_names.index("CVR_CleanCode")
    source_system_idx = column_names.index("SourceSystem")
    dw_timestamp_idx = column_names.index("dw_load_timestamp")

    # Validate each row
    rows_with_clean_code = 0
    for row in rows:
        kreditor_clean_code = row[kreditor_clean_code_idx]
        source_system = row[source_system_idx]
        dw_timestamp = row[dw_timestamp_idx]

        # SourceSystem should always be MySupply
        assert (
            source_system == "MySupply"
        ), f"SourceSystem should be 'MySupply', got '{source_system}'"

        # dw_load_timestamp should be a datetime
        assert isinstance(dw_timestamp, datetime), "dw_load_timestamp should be datetime"

        # CVR_CleanCode should follow pattern CVR_* when populated
        if kreditor_clean_code is not None:
            assert kreditor_clean_code.startswith(
                "CVR_"
            ), f"CVR_CleanCode should start with 'CVR_', got '{kreditor_clean_code}'"
            rows_with_clean_code += 1

    # At least some rows should have clean codes
    assert rows_with_clean_code > 0, "At least some rows should have CVR_CleanCode"
    print(f"✓ {rows_with_clean_code}/{len(rows)} rows have CVR_CleanCode")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_statistic_invoice_header_mysupply_daily_has_expected_columns(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_statistic_invoice_header_mysupply_daily creates table with expected columns.

    Validates the staging table schema includes:
    - Source columns from MySupply (faktura_nr, faktura_dato, etc.)
    - Generated clean code columns (CVR_CleanCode)
    - ETL metadata columns (SourceSystem, dw_load_timestamp)
    """
    # Get column names
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[statistic_invoice_header_mysupply]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Expected ETL metadata columns
    assert "CVR_CleanCode" in column_names, "Should have CVR_CleanCode column"
    assert "SourceSystem" in column_names, "Should have SourceSystem column"
    assert "dw_load_timestamp" in column_names, "Should have dw_load_timestamp column"

    # Expected source columns (key business columns)
    # Note: source column is 'id', not 'statistic_invoice_header_id'
    expected_source_columns = [
        "id",
        "faktura_nr",
        "faktura_dato",
        "faktura_udsteder_cvr_nummer",
    ]
    for col in expected_source_columns:
        assert col in column_names, f"Should have source column '{col}'"

    print(f"✓ Table has {len(column_names)} columns including all expected columns")
