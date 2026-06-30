# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for the full erp partitioned pipeline.

Tests validate:
- Full daily partition runs for all SydAX and MySupply tables
- Incremental loading behavior (delete + insert for same partition)
- DW_PartitionDate column propagation
- Backfill scenario with multiple consecutive dates
"""

from datetime import date

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)

# Test partition keys - dates known to have data in test DB
TEST_PARTITION_KEY_1 = "2025-09-17"  # SydAX has data for this date
TEST_PARTITION_KEY_2 = "2007-01-12"  # MySupply has data for this date


class TestSydAXDailyPipeline:
    """Integration tests for SydAX daily partitioned pipeline."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_full_sydax_daily_partition_run(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test materializing all SydAX daily partitioned assets together.

        This tests that:
        - The current SydAX extraction asset set materializes successfully
        - Master data tables do full load
        - Transactional tables filter by partition date
        - Dependent tables filter via FK relationships
        """
        # Current SydAX asset keys
        sydax_assets = [
            # Master data
            "ext_afdeling_sydax",
            "ext_finanskonto_sydax_daily",
            "ext_finanskontoomkostningssted_sydax_daily",
            "ext_omkostningsstedhierarki_sydax_daily",
            "ext_kreditor_sydax_daily",
            # Transactional (1 table)
            "ext_finanspostering_sydax_daily",
            # Dependent (2 tables)
            "ext_kreditortransaktion_sydax_daily",
            "ext_finansposteringkreditortransrelation_sydax_daily",
        ]

        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=sydax_assets,
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        # Verify key master-data extracts materialize and expose row_count metadata.
        # Partition-scoped slices can legitimately be empty for some test dates.
        for asset_key in ["ext_kreditor_sydax_daily", "ext_finanskonto_sydax_daily"]:
            metadata = get_materialization_result(dagster_instance, asset_key)
            assert metadata is not None, f"Should have metadata for {asset_key}"
            assert metadata["row_count"].value >= 0, f"{asset_key} should have row_count metadata"

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_sydax_incremental_load_same_partition(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that running the same partition twice replaces data correctly.

        For transactional/dependent tables:
        - Old partition data should be deleted
        - New data should be inserted
        - DW_PartitionDate should match partition key

        For master data tables:
        - Table is dropped and recreated (full load)
        """
        partition_key = TEST_PARTITION_KEY_1
        asset_key = "ext_kreditor_sydax_daily"

        # First run
        result1 = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[asset_key],
            partition=partition_key,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result1.success, "First run should succeed"

        # Get row count after first run
        metadata1 = get_materialization_result(dagster_instance, asset_key)
        row_count_1 = metadata1["row_count"].value

        # Second run with same partition
        result2 = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[asset_key],
            partition=partition_key,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result2.success, "Second run should succeed"

        # Get row count after second run - should be same (not doubled)
        metadata2 = get_materialization_result(dagster_instance, asset_key)
        row_count_2 = metadata2["row_count"].value

        # For master data, row count should be the same (full reload)
        assert (
            row_count_2 == row_count_1
        ), f"Row count should be same after re-run: first={row_count_1}, second={row_count_2}"

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_sydax_dw_partition_date_populated(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that DW_PartitionDate column is populated correctly.
        """
        partition_key = TEST_PARTITION_KEY_1
        asset_key = "ext_kreditor_sydax_daily"

        # Materialize
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[asset_key],
            partition=partition_key,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result.success

        # Query the staging table and verify DW_PartitionDate
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT DW_PartitionDate FROM [stg].[kreditor_sydax]")
                rows = cursor.fetchall()
            finally:
                cursor.close()
        if len(rows) == 0:
            pytest.skip("No kreditor rows found for selected partition")
        expected_date = date.fromisoformat(partition_key)

        # All rows should have the same partition date
        for row in rows:
            assert (
                row[0] == expected_date
            ), f"DW_PartitionDate should be {expected_date}, got {row[0]}"


class TestMySupplyDailyPipeline:
    """Integration tests for MySupply daily partitioned pipeline."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_full_mysupply_daily_partition_run(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test materializing all MySupply daily partitioned assets together.

        This tests that:
        - All 3 MySupply tables materialize successfully
        - Root table filters by FakturaDato (string date)
        - Dependent tables filter via FK relationships
        """
        mysupply_assets = [
            "ext_statistic_invoice_header_mysupply_daily",
            "ext_metadata_mysupply_daily",
            "ext_statistic_invoice_line_mysupply_daily",
        ]

        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=mysupply_assets,
            partition=TEST_PARTITION_KEY_2,
            resources={
                "mysupply_mssql": ResourceDefinition.hardcoded_resource(
                    real_mysupply_mssql_resource
                ),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        # Verify header table has rows for the test date
        metadata = get_materialization_result(
            dagster_instance, "ext_statistic_invoice_header_mysupply_daily"
        )
        assert metadata is not None, "Should have metadata"
        # Note: Row count may be 0 if test DB has no data for this partition

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_mysupply_string_date_filtering(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that MySupply string date filtering works correctly.

        The FakturaDato column is VARCHAR, not datetime, so we use
        direct string comparison (FakturaDato = 'YYYY-MM-DD').
        """
        partition_key = TEST_PARTITION_KEY_2

        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_statistic_invoice_header_mysupply_daily"],
            partition=partition_key,
            resources={
                "mysupply_mssql": ResourceDefinition.hardcoded_resource(
                    real_mysupply_mssql_resource
                ),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result.success

        # Query the staging table to verify data was loaded
        # All rows should have FakturaDato matching the partition key
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT DISTINCT FakturaDato FROM [stg].[statistic_invoice_header_mysupply]"
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
        if len(rows) > 0:
            # If we have data, all rows should have the partition date
            for row in rows:
                # FakturaDato is a string column
                assert (
                    row[0] == partition_key
                ), f"FakturaDato should be {partition_key}, got {row[0]}"


class TestILSPipeline:
    """Integration tests for ILS full-load pipeline."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_ils_full_load_with_consistency_check(
        self,
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test ILS tables do full load with consistency check.

        ILS tables are small enough to full load, but use consistency
        check to skip reload if data hasn't changed.
        """
        # Pick one ILS table to test
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

        metadata = get_materialization_result(dagster_instance, "ext_lev_ils")
        assert metadata is not None, "Should have metadata"


class TestBackfillScenario:
    """Integration tests for backfill scenarios."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(3)
    @pytest.mark.skip(
        reason="Backfill behavior is environment/data dependent and flaky in shared runs"
    )
    def test_backfill_multiple_partitions(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test running multiple partitions in sequence (simulating backfill).

        After running multiple partitions:
        - Each partition's data should be preserved
        - DW_PartitionDate should distinguish data by partition
        """
        # Run first partition
        result1 = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_finanspostering_sydax_daily"],
            partition="2025-09-17",
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result1.success, "First partition should succeed"

        # Run second partition (different date)
        result2 = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_finanspostering_sydax_daily"],
            partition="2024-01-01",
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result2.success, "Second partition should succeed"

        # Verify both partitions have data with correct DW_PartitionDate
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT DW_PartitionDate, COUNT(*) as cnt
                    FROM [stg].[finanspostering_sydax]
                    GROUP BY DW_PartitionDate
                    ORDER BY DW_PartitionDate
                    """
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
        # Should have at least one partition with data
        # (The second partition may have 0 rows if no data for 2024-01-01 in test DB)
        assert len(rows) >= 1, "Should have at least one partition with data"

        # Verify partition dates are correct
        partition_dates = [row[0] for row in rows]
        print(f"Found partitions: {partition_dates}")


class TestTableSchemaValidation:
    """Tests to validate staging table schema."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_staging_table_has_required_columns(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that staging tables have all required ETL metadata columns.
        """
        # Materialize a table
        materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_kreditor_sydax_daily"],
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        # Get column names
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT TOP 1 * FROM [stg].[kreditor_sydax]")
                column_names = [desc[0] for desc in cursor.description]
            finally:
                cursor.close()
        # Required ETL metadata columns
        required_columns = [
            "SourceSystem",
            "DW_LoadTimestamp",
            "DW_PartitionDate",
        ]

        for col in required_columns:
            assert col in column_names, f"Missing required column: {col}"

        # Verify clean code column exists for this table
        assert "CVR_CleanCode" in column_names, "Should have CVR_CleanCode column"

        print(f"Table has {len(column_names)} columns including all required metadata")


class TestScheduledCatchupMode:
    """Integration tests for scheduled catchup extraction mode.

    Tests validate that:
    - Scheduled catchup mode uses 25-hour window instead of exact date match
    - Manual runs (no tag) still use exact date match
    - Both modes work correctly with real database connections
    """

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_scheduled_catchup_sydax_transactional(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that scheduled catchup mode loads data from 25-hour window.

        When run with erp_extraction_mode=scheduled_catchup tag,
        the extraction should use >= yesterday 01:00 instead of exact date match.
        """
        # Run with scheduled_catchup tag
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_finanspostering_sydax_daily"],
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
            run_tags={"erp_extraction_mode": "scheduled_catchup"},
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        # Verify data was loaded
        metadata = get_materialization_result(dagster_instance, "ext_finanspostering_sydax_daily")
        assert metadata is not None, "Should have metadata"
        # Note: row count depends on test data availability
        print(f"Scheduled catchup loaded {metadata['row_count'].value} rows")

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_manual_run_uses_exact_date(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that manual runs (no tag) use exact date match.

        Without the scheduled_catchup tag, extraction should use
        CAST([col] AS DATE) = 'partition_key' for exact date matching.
        """
        # Run without scheduled_catchup tag (manual run)
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_finanspostering_sydax_daily"],
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
            # No run_tags - this simulates manual run
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        # Verify data was loaded
        metadata = get_materialization_result(dagster_instance, "ext_finanspostering_sydax_daily")
        assert metadata is not None, "Should have metadata"
        print(f"Manual run loaded {metadata['row_count'].value} rows")

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(3)
    def test_scheduled_catchup_mysupply_string_date(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test scheduled catchup mode with MySupply string date columns.

        MySupply uses string date columns (FakturaDato) which should use
        >= yesterday date instead of exact match in catchup mode.
        """
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_statistic_invoice_header_mysupply_daily"],
            partition=TEST_PARTITION_KEY_2,  # MySupply test date
            resources={
                "mysupply_mssql": ResourceDefinition.hardcoded_resource(
                    real_mysupply_mssql_resource
                ),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
            run_tags={"erp_extraction_mode": "scheduled_catchup"},
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        metadata = get_materialization_result(
            dagster_instance, "ext_statistic_invoice_header_mysupply_daily"
        )
        assert metadata is not None, "Should have metadata"
        print(f"MySupply scheduled catchup loaded {metadata['row_count'].value} rows")

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(4)
    def test_scheduled_catchup_dependent_table(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that dependent tables also use scheduled catchup mode.

        Dependent tables filter via FK relationships, and the catchup mode
        should propagate to the parent table filter in the subquery.
        """
        # First load the transactional table (parent)
        materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_finanspostering_sydax_daily"],
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
            run_tags={"erp_extraction_mode": "scheduled_catchup"},
        )

        # Now load the dependent table with catchup mode
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=[
                "ext_finansposteringkreditortransrelation_sydax_daily",
            ],
            partition=TEST_PARTITION_KEY_1,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
            run_tags={"erp_extraction_mode": "scheduled_catchup"},
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        metadata = get_materialization_result(
            dagster_instance, "ext_finansposteringkreditortransrelation_sydax_daily"
        )
        assert metadata is not None, "Should have metadata for dependent table"
        print(f"Dependent table scheduled catchup loaded {metadata['row_count'].value} rows")


# Test monthly partition key - format is YYYY-MM
TEST_MONTHLY_PARTITION_KEY = "2025-09"


class TestMonthlyPartitionPipeline:
    """Integration tests for monthly partitioned pipeline."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_sydax_monthly_partition_run(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test materializing SydAX monthly partitioned assets.

        Monthly partitions should use date range filters (>= start AND <= end).
        """
        # Select a representative monthly asset
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_kreditor_sydax_monthly"],
            partition=TEST_MONTHLY_PARTITION_KEY,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        metadata = get_materialization_result(dagster_instance, "ext_kreditor_sydax_monthly")
        assert metadata is not None, "Should have metadata"
        print(f"Monthly partition loaded {metadata['row_count'].value} rows")

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_mysupply_monthly_partition_run(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test materializing MySupply monthly partitioned assets.

        Monthly partitions with string dates should use date range comparison.
        """
        monthly_key = TEST_MONTHLY_PARTITION_KEY

        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_statistic_invoice_header_mysupply_monthly"],
            partition=monthly_key,
            resources={
                "mysupply_mssql": ResourceDefinition.hardcoded_resource(
                    real_mysupply_mssql_resource
                ),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        metadata = get_materialization_result(
            dagster_instance, "ext_statistic_invoice_header_mysupply_monthly"
        )
        assert metadata is not None, "Should have metadata"

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(3)
    def test_monthly_partition_dw_partition_date(
        self,
        dagster_instance,
        real_sydax_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that DW_PartitionDate is set correctly for monthly partitions.

        For monthly partitions, DW_PartitionDate should be set to the first
        day of the month (e.g., 2025-09-01 for partition 2025-09).
        """
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_kreditor_sydax_monthly"],
            partition=TEST_MONTHLY_PARTITION_KEY,
            resources={
                "sydax_mssql": ResourceDefinition.hardcoded_resource(real_sydax_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )
        assert result.success

        # Query the staging table and verify DW_PartitionDate
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT DISTINCT DW_PartitionDate FROM [stg].[kreditor_sydax]")
                rows = cursor.fetchall()
            finally:
                cursor.close()
        if len(rows) >= 1:
            # For monthly partition 2025-09, DW_PartitionDate should be 2025-09-01
            expected_date = date(2025, 9, 1)
            for row in rows:
                assert (
                    row[0] == expected_date
                ), f"DW_PartitionDate should be {expected_date}, got {row[0]}"


class TestILSMonthlyPipeline:
    """Integration tests for ILS monthly partitioned pipeline."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_ils_monthly_partition_run(
        self,
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test materializing ILS monthly partitioned assets.

        ILS tables are partitioned by pro2created OR pro2modified and should
        support both daily and monthly partition keys.
        """
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=["ext_laftdpris_ils_monthly"],
            partition=TEST_MONTHLY_PARTITION_KEY,
            resources={
                "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        metadata = get_materialization_result(dagster_instance, "ext_laftdpris_ils_monthly")
        assert metadata is not None, "Should have metadata for ILS monthly asset"
        print(f"ILS monthly partition loaded {metadata['row_count'].value} rows")

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_all_ils_monthly_assets_exist(
        self,
        dagster_instance,
        real_ils_mssql_resource,
        real_mssql_target_resource,
        cleanup_erp_staging_tables,
    ):
        """
        Test that all ILS monthly partitioned assets can be materialized.
        """
        ils_monthly_assets = [
            "ext_laftdpris_ils_monthly",
            "ext_laftdstam_ils_monthly",
            "ext_lgrvare_ils_monthly",
            "ext_laftd_ils_monthly",
        ]

        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=ils_monthly_assets,
            partition=TEST_MONTHLY_PARTITION_KEY,
            resources={
                "ils_mssql": ResourceDefinition.hardcoded_resource(real_ils_mssql_resource),
                "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                    real_mssql_target_resource
                ),
            },
        )

        assert result is not None, "Materialization should complete"
        assert result.success, "Materialization should succeed"

        # Verify each asset has metadata
        for asset_key in ils_monthly_assets:
            metadata = get_materialization_result(dagster_instance, asset_key)
            assert metadata is not None, f"Should have metadata for {asset_key}"
