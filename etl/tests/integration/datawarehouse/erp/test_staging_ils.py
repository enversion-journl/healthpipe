# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for ILS staging assets.

Tests validate:
- Assets materialize successfully against real database
- Data is loaded into staging tables
- Clean codes are generated with correct patterns
- SourceSystem and dw_load_timestamp are populated
- dw_partition_date tracks partition membership (for partitioned tables)

Note: ILS tables are now split:
- Full-load tables: stg_finance_ils (vareart, varegrp, lafth, vare, lev)
- Large tables (>1000 rows): Daily partitioned via stg_finance_ils_daily (laftdpris, laftdstam, lgrvare, pmdm, indkfak, laftd)

Note: The test database may not have pro2created/pro2modified columns in all tables.
Tests for daily partitioned ILS assets will be skipped if these columns are missing.
"""

from datetime import datetime

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)
from tests.utils.staging_helpers import query_staging_table

# Default test partition - a date known to have data in test DB
TEST_PARTITION_KEY = "2025-09-17"


def _check_partition_columns_exist(ils_resource, table_name: str) -> bool:
    """Check if pro2created and pro2modified columns exist in the given ILS table."""
    with ils_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = %s
                AND COLUMN_NAME IN ('pro2created', 'pro2modified')
                """,
                (table_name,),
            )
            count = cursor.fetchone()[0]
            return count == 2  # Both columns must exist
        finally:
            cursor.close()


def _check_all_ils_daily_tables_have_partition_columns(ils_resource) -> bool:
    """Check if ALL ILS daily partitioned tables have pro2created/pro2modified columns.

    The stg_finance_ils_daily multi-asset includes:
    laftdpris, laftdstam, lgrvare, pmdm, indkfak, laftd.
    All must have partition columns for the multi-asset to work.
    """
    daily_tables = ["laftdpris", "laftdstam", "lgrvare", "pmdm", "indkfak", "laftd"]
    for table in daily_tables:
        if not _check_partition_columns_exist(ils_resource, table):
            return False
    return True


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_lev_ils_materializes_successfully(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_lev_ils materializes successfully as full load.
    """
    # Materialize the extraction asset (no partition key for full load)
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_lev_ils"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "ext_lev_ils")
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should have row count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_lev_ils_generates_cvr_clean_codes(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_lev_ils generates CVR_CleanCode correctly.

    Validates (when rows exist):
    - CVR_CleanCode follows pattern CVR_<cvrnummer> or KREDITOR_<id>
    - SourceSystem is set to 'ILS'
    - dw_load_timestamp is a datetime
    - dw_partition_date is NULL for full load
    """
    # Materialize the extraction asset
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_lev_ils"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    # Query staging table
    rows = query_staging_table(real_mssql_target_resource, "ils_lev")

    # Full load may still produce 0 rows if source table is empty in test env
    if len(rows) == 0:
        print("No suppliers found in source - skipping clean code validation")
        return

    # Get column names to find indices dynamically
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT TOP 1 * FROM [stg].[ils_lev]")
            column_names = [desc[0] for desc in cursor.description]
        finally:
            cursor.close()
    # Find column indices
    cvr_clean_code_idx = column_names.index("CVR_CleanCode")
    source_system_idx = column_names.index("SourceSystem")
    dw_timestamp_idx = column_names.index("dw_load_timestamp")
    dw_partition_date_idx = column_names.index("dw_partition_date")

    # Validate each row
    rows_with_clean_code = 0
    for row in rows:
        cvr_clean_code = row[cvr_clean_code_idx]
        source_system = row[source_system_idx]
        dw_timestamp = row[dw_timestamp_idx]
        dw_partition_date = row[dw_partition_date_idx]

        # SourceSystem should always be ILS
        assert source_system == "ILS", f"SourceSystem should be 'ILS', got '{source_system}'"

        # dw_load_timestamp should be a datetime
        assert isinstance(dw_timestamp, datetime), "dw_load_timestamp should be datetime"

        # Full load writes NULL dw_partition_date
        assert (
            dw_partition_date is None
        ), f"dw_partition_date should be NULL for full load, got {dw_partition_date}"

        # CVR_CleanCode should follow pattern CVR_* or KREDITOR_* when populated
        # CVR_* is used when cvr number is available
        # KREDITOR_* is used as fallback when only kreditor ID is available
        if cvr_clean_code is not None:
            valid_prefixes = ("CVR_", "KREDITOR_")
            assert cvr_clean_code.startswith(
                valid_prefixes
            ), f"CVR_CleanCode should start with 'CVR_' or 'KREDITOR_', got '{cvr_clean_code}'"
            rows_with_clean_code += 1

    # At least some rows should have clean codes (if rows exist)
    if len(rows) > 0:
        assert rows_with_clean_code > 0, "At least some rows should have CVR_CleanCode"
    print(f"✓ {rows_with_clean_code}/{len(rows)} rows have CVR_CleanCode")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
@pytest.mark.skip(
    reason="ext_laftd_ils_daily migrated to arc/DFM path (#28690); partitioned extract removed"
)
def test_ext_laftd_ils_daily_materializes_successfully(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_laftd_ils_daily materializes successfully
    and loads contract line data from the real ILS database.

    Note: laftd is partitioned by pro2created OR pro2modified - row count may be 0
    if no records were created/modified on the partition date.

    SKIPPED: laftd migrated to arc/DFM path (#28690); partitioned extract removed.
    Data now flows through arc_ils_laftd -> pre_stg_BridgeVare_ils / pre_stg_Vareaftaler_ils.
    """
    # Skip if ANY ILS daily table is missing partition columns (multi-asset processes all)
    if not _check_all_ils_daily_tables_have_partition_columns(real_ils_mssql_resource):
        pytest.skip("Test DB missing pro2created/pro2modified columns in one or more ILS tables")

    # Materialize the extraction asset with a partition key
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_laftd_ils_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "ext_laftd_ils_daily")
    assert metadata is not None, "Should have materialization metadata"
    # Row count may be 0 if no records were created/modified on this partition date
    assert metadata["row_count"].value >= 0, "Should have row count metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
@pytest.mark.skip(
    reason="ext_laftd_ils_daily migrated to arc/DFM path (#28690); partitioned extract removed"
)
def test_ext_laftd_ils_daily_has_expected_columns(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Test that ext_laftd_ils_daily creates table with expected columns.

    Validates the staging table schema includes:
    - Source columns from ILS
    - ETL metadata columns (SourceSystem, dw_load_timestamp, dw_partition_date)

    SKIPPED: laftd migrated to arc/DFM path (#28690); partitioned extract removed.
    Data now flows through arc_ils_laftd -> pre_stg_BridgeVare_ils / pre_stg_Vareaftaler_ils.
    """
    # Skip if ANY ILS daily table is missing partition columns (multi-asset processes all)
    if not _check_all_ils_daily_tables_have_partition_columns(real_ils_mssql_resource):
        pytest.skip("Test DB missing pro2created/pro2modified columns in one or more ILS tables")

    # Materialize the extraction asset
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_laftd_ils_daily"],
        partition=TEST_PARTITION_KEY,
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    # Get column names - the table should exist even if empty
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = 'laftd_ils'"
            )
            column_names = [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
    # Expected ETL metadata columns
    assert "SourceSystem" in column_names, "Should have SourceSystem column"
    assert "dw_load_timestamp" in column_names, "Should have dw_load_timestamp column"
    assert "dw_partition_date" in column_names, "Should have dw_partition_date column"

    print(f"✓ Table has {len(column_names)} columns including all expected columns")


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(1)
def test_ext_vareart_ils_materializes_successfully(
    dagster_instance,
    real_ils_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_staging_tables,
):
    """
    Test that ext_vareart_ils materializes successfully (full load for small table).

    Note: vareart is a small table (191 rows) and remains a full load table.
    The consistency check may return row_count=0 if data is unchanged from a previous run.
    """
    # Materialize the extraction asset (no partition key - full load)
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["ext_vareart_ils"],
        resources={
            "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    assert result is not None, "Materialization should complete"
    assert result.success, "Materialization should succeed"

    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "ext_vareart_ils")
    assert metadata is not None, "Should have materialization metadata"
    # row_count may be 0 if consistency check determined data is unchanged
    assert metadata["row_count"].value >= 0, "Should have row count metadata"

    # Also verify that the table exists and has data
    rows = query_staging_table(real_mssql_target_resource, "ils_vareart")
    assert len(rows) >= 1, "Table should have at least 1 row"
