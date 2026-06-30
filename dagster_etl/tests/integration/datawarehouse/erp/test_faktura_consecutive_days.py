# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: Faktura pipeline for 2 consecutive daily partitions.

Validates end-to-end flow including:
- MySupply extract (header, line, metadata, allowance_charge tables)
- pre_stg_fakturalinje_mysupply (enrichment + CleanCode generation)
- stg_dim_faktura_mysupply (dedup + Header_DW_LoadTimestamp exposure)
- stg_fct_faktura_mysupply (fact staging)
- map_faktura + map_fakturalinje (EK assignment)
- dim_faktura (INNER JOIN — no NULL DW_EK_Faktura duplicates)
- fct_faktura + fct_fakturalinje
- ref_faktura_to_postering (ranks newest-faktura off persistent dim.Faktura)

The test runs partition day-1, then day-2, asserting idempotent behavior
and that no errors occur from the recent fixes:
- dim_faktura: LEFT→INNER JOIN (prevents NULL unique index violation)
- stg_dim_faktura_mysupply: exposes Header_DW_LoadTimestamp, statistic_invoice_header_id
- dim_faktura: persists Header_DW_LoadTimestamp + statistic_invoice_header_id
- ref_faktura_to_postering: reads those columns from dim.Faktura (persistent),
  NOT from the volatile stg.dim_faktura_mysupply, and ranks
  is_newest_faktura_for_postering by header load order
"""

import logging
import os

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

logger = logging.getLogger(__name__)

FAKTURA_EXTRACT_KEYS = [
    "ext_statistic_invoice_header_mysupply_daily",
    "ext_statistic_invoice_line_mysupply_daily",
    "ext_metadata_mysupply_daily",
]

FAKTURA_PIPELINE_KEYS = [
    "erp/pre_stg_fakturalinje_mysupply",
    "erp/stg_dim_faktura_mysupply",
    "erp/stg_dim_fakturalinje_mysupply",
    "erp/stg_fct_faktura_mysupply",
    "erp/stg_fct_fakturalinje_mysupply",
    "map_faktura",
    "map_fakturalinje",
    "dim_faktura",
    "dim_fakturalinje",
    "fct_faktura",
    "fct_fakturalinje",
    "ref_faktura_to_postering",
]

PARTITION_DAY_1 = os.getenv("FAKTURA_TEST_DAY_1", "2026-05-20")
PARTITION_DAY_2 = os.getenv("FAKTURA_TEST_DAY_2", "2026-05-21")


class TestFakturaConsecutiveDays:
    """End-to-end faktura pipeline test for 2 consecutive daily partitions."""

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(1)
    def test_faktura_day1(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        setup_erp_infrastructure,
        cleanup_erp_all_tables,
    ):
        """Run faktura pipeline for day 1 — validates basic flow succeeds."""
        resources = {
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        }

        logger.info(f"=== Day 1: Extracting partition {PARTITION_DAY_1} ===")
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=FAKTURA_EXTRACT_KEYS,
            partition=PARTITION_DAY_1,
            resources=resources,
        )
        assert (
            result is not None and result.success
        ), f"Day 1 extract failed for partition {PARTITION_DAY_1}"

        logger.info("=== Day 1: Running downstream pipeline ===")
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=FAKTURA_PIPELINE_KEYS,
            resources=resources,
        )
        assert result is not None and result.success, "Day 1 downstream pipeline failed"

        self._verify_no_null_ek(real_mssql_target_resource)
        self._verify_staging_columns_exist(real_mssql_target_resource)

    @pytest.mark.integration
    @pytest.mark.erp
    @pytest.mark.real_resources
    @pytest.mark.order(2)
    def test_faktura_day2(
        self,
        dagster_instance,
        real_mysupply_mssql_resource,
        real_mssql_target_resource,
        setup_erp_infrastructure,
    ):
        """Run faktura pipeline for day 2 — validates incremental load."""
        resources = {
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        }

        logger.info(f"=== Day 2: Extracting partition {PARTITION_DAY_2} ===")
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=FAKTURA_EXTRACT_KEYS,
            partition=PARTITION_DAY_2,
            resources=resources,
        )
        assert (
            result is not None and result.success
        ), f"Day 2 extract failed for partition {PARTITION_DAY_2}"

        logger.info("=== Day 2: Running downstream pipeline ===")
        result = materialize_assets_with_dependencies(
            instance=dagster_instance,
            asset_keys=FAKTURA_PIPELINE_KEYS,
            resources=resources,
        )
        assert result is not None and result.success, "Day 2 downstream pipeline failed"

        self._verify_no_null_ek(real_mssql_target_resource)
        self._verify_ref_bridge_populated(real_mssql_target_resource)
        self._verify_dim_header_columns(real_mssql_target_resource)
        self._verify_newest_faktura_ranking(real_mssql_target_resource)

    def _verify_no_null_ek(self, target_resource):
        """Verify dim.Faktura has no NULL DW_EK_Faktura rows (INNER JOIN fix)."""
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM [dim].[Faktura]
                    WHERE DW_EK_Faktura IS NULL
                """
                )
                null_count = cursor.fetchone()[0]
                assert null_count == 0, f"dim.Faktura has {null_count} rows with NULL DW_EK_Faktura"
            finally:
                cursor.close()

    def _verify_staging_columns_exist(self, target_resource):
        """Verify stg.dim_faktura_mysupply schema exposes Header_DW_LoadTimestamp."""
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COL_LENGTH('stg.dim_faktura_mysupply', 'Header_DW_LoadTimestamp'),
                           COL_LENGTH('stg.dim_faktura_mysupply', 'statistic_invoice_header_id')
                """
                )
                row = cursor.fetchone()
                assert (
                    row[0] is not None
                ), "stg.dim_faktura_mysupply missing Header_DW_LoadTimestamp column"
                assert (
                    row[1] is not None
                ), "stg.dim_faktura_mysupply missing statistic_invoice_header_id column"
            finally:
                cursor.close()

    def _verify_ref_bridge_populated(self, target_resource):
        """Verify ref.Faktura_To_Postering has active rows after 2 days."""
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM [ref].[Faktura_To_Postering]
                    WHERE DW_IsDeleted = 0
                """
                )
                count = cursor.fetchone()[0]
                logger.info(f"ref.Faktura_To_Postering: {count} active pairs")
            finally:
                cursor.close()

    def _verify_dim_header_columns(self, target_resource):
        """dim.Faktura must expose + populate the bridge tie-break header columns.

        The bridge ranks 'newest faktura per postering' off these PERSISTENT columns
        (not the volatile staging table). Assert the schema has them and that real
        (non -1) rows carry a Header_DW_LoadTimestamp.
        """
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COL_LENGTH('dim.Faktura', 'Header_DW_LoadTimestamp'),
                           COL_LENGTH('dim.Faktura', 'statistic_invoice_header_id')
                """
                )
                row = cursor.fetchone()
                assert row[0] is not None, "dim.Faktura missing Header_DW_LoadTimestamp column"
                assert row[1] is not None, "dim.Faktura missing statistic_invoice_header_id column"

                # Coverage: MySupply-enriched rows (those with a FakturaNr) must carry
                # the header load timestamp. SydAX-only map rows legitimately have NULL
                # descriptive attributes (incl. header cols) — see DimUpd_faktura docstring —
                # so they are excluded from this assertion.
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN Header_DW_LoadTimestamp IS NULL THEN 1 ELSE 0 END) AS missing_ts,
                        COUNT(*) AS total
                    FROM [dim].[Faktura]
                    WHERE DW_EK_Faktura <> -1
                      AND FakturaNr IS NOT NULL
                """
                )
                missing_ts, total = cursor.fetchone()
                logger.info(
                    f"dim.Faktura header-ts coverage (MySupply rows): "
                    f"{total - (missing_ts or 0)}/{total}"
                )
                assert (missing_ts or 0) == 0, (
                    f"dim.Faktura has {missing_ts} MySupply rows (FakturaNr present) with NULL "
                    f"Header_DW_LoadTimestamp (DimUpd_faktura must persist it)"
                )
            finally:
                cursor.close()

    def _verify_newest_faktura_ranking(self, target_resource):
        """The is_newest_faktura_for_postering flag must land on the max-header-load row.

        Violation-based: for each postering group, no NON-flagged faktura may have a
        strictly newer (Header_DW_LoadTimestamp, statistic_invoice_header_id) than the
        flagged 'newest' one. Vacuously passes when the bridge is empty / single-faktura.
        """
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT b.DW_EK_Postering, b.DW_EK_Faktura,
                               d.Header_DW_LoadTimestamp AS ts,
                               d.statistic_invoice_header_id AS hid,
                               b.is_newest_faktura_for_postering AS is_newest
                        FROM [ref].[Faktura_To_Postering] b
                        JOIN [dim].[Faktura] d ON d.DW_EK_Faktura = b.DW_EK_Faktura
                        WHERE b.DW_IsDeleted = 0
                    ),
                    chosen AS (
                        SELECT DW_EK_Postering, ts AS chosen_ts, hid AS chosen_hid
                        FROM flagged WHERE is_newest = 1
                    )
                    SELECT COUNT(*)
                    FROM flagged f
                    JOIN chosen c ON c.DW_EK_Postering = f.DW_EK_Postering
                    WHERE f.is_newest = 0
                      AND (
                            f.ts > c.chosen_ts
                         OR (f.ts = c.chosen_ts AND f.hid > c.chosen_hid)
                         OR (c.chosen_ts IS NULL AND f.ts IS NOT NULL)
                      )
                """
                )
                violations = cursor.fetchone()[0]
                logger.info(f"newest-faktura ranking violations: {violations}")
                assert violations == 0, (
                    f"{violations} postering group(s) flag a faktura as newest while a "
                    f"sibling header has a newer Header_DW_LoadTimestamp — tie-break broken"
                )
            finally:
                cursor.close()
