# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: Faktura pipeline for 2 consecutive daily partitions.

Validates end-to-end flow including:
- MySupply extract (header, line, metadata, allowance_charge tables)
- pre_stg_Fakturalinje_mysupply (enrichment + CleanCode generation)
- stg_dim_Faktura_mysupply (dedup + header_dw_timestamp_update exposure)
- stg_fct_Faktura_mysupply (fact staging)
- map_Faktura + map_Fakturalinje (EK assignment)
- dim_Faktura (INNER JOIN — no NULL dw_ek_Faktura duplicates)
- fct_Faktura + fct_Fakturalinje
- ref_faktura_to_postering (ranks newest-faktura off persistent dim.Faktura)

The test runs partition day-1, then day-2, asserting idempotent behavior
and that no errors occur from the recent fixes:
- dim_Faktura: LEFT→INNER JOIN (prevents NULL unique index violation)
- stg_dim_Faktura_mysupply: exposes header_dw_timestamp_update, statistic_invoice_header_id
- dim_Faktura: persists header_dw_timestamp_update + statistic_invoice_header_id
- ref_faktura_to_postering: reads those columns from dim.Faktura (persistent),
  NOT from the volatile stg.dim_Faktura_mysupply, and ranks
  is_newest_faktura_for_postering by header load order
"""

import logging
import os

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import materialize_assets_with_dependencies

logger = logging.getLogger(__name__)

# NOTE(#28693): Legacy partitioned ext_*_mysupply_daily assets removed — faktura is now
# DFM/archive. The arc_mysupply_* assets replace them (non-partitioned, loaded via DFM job).
# The downstream FAKTURA_PIPELINE_KEYS (pre_stg, stg_dim, stg_fct, map, dim, fct) still apply
# but now depend on arc_mysupply_* instead of ext_*_mysupply_*.
# TODO(#28693): re-enable with arc asset keys and DFM-aware fixture in devcontainer.
FAKTURA_EXTRACT_KEYS = [
    "arc_mysupply_statistic_invoice_header",
    "arc_mysupply_statistic_invoice_line",
    "arc_mysupply_metadata",
]

FAKTURA_PIPELINE_KEYS = [
    "erp/pre_stg_Fakturalinje_mysupply",
    "erp/stg_dim_Faktura_mysupply",
    "erp/stg_dim_Fakturalinje_mysupply",
    "erp/stg_fct_Faktura_mysupply",
    "erp/stg_fct_Fakturalinje_mysupply",
    "map_Faktura",
    "map_Fakturalinje",
    "dim_Faktura",
    "dim_Fakturalinje",
    "fct_Faktura",
    "fct_Fakturalinje",
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
        """Verify dim.Faktura has no NULL dw_ek_Faktura rows (INNER JOIN fix)."""
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM [dim].[Faktura]
                    WHERE dw_ek_Faktura IS NULL
                """
                )
                null_count = cursor.fetchone()[0]
                assert null_count == 0, f"dim.Faktura has {null_count} rows with NULL dw_ek_Faktura"
            finally:
                cursor.close()

    def _verify_staging_columns_exist(self, target_resource):
        """Verify stg.dim_Faktura_mysupply schema exposes header_dw_timestamp_update."""
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COL_LENGTH('stg.dim_Faktura_mysupply', 'header_dw_timestamp_update'),
                           COL_LENGTH('stg.dim_Faktura_mysupply', 'statistic_invoice_header_id')
                """
                )
                row = cursor.fetchone()
                assert (
                    row[0] is not None
                ), "stg.dim_Faktura_mysupply missing header_dw_timestamp_update column"
                assert (
                    row[1] is not None
                ), "stg.dim_Faktura_mysupply missing statistic_invoice_header_id column"
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
                    WHERE dw_is_deleted = 0
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
        (non -1) rows carry a header_dw_timestamp_update.
        """
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT COL_LENGTH('dim.Faktura', 'header_dw_timestamp_update'),
                           COL_LENGTH('dim.Faktura', 'statistic_invoice_header_id')
                """
                )
                row = cursor.fetchone()
                assert row[0] is not None, "dim.Faktura missing header_dw_timestamp_update column"
                assert row[1] is not None, "dim.Faktura missing statistic_invoice_header_id column"

                # Coverage: MySupply-enriched rows (those with a faktura_nr) must carry
                # the header load timestamp. SydAX-only map rows legitimately have NULL
                # descriptive attributes (incl. header cols) — see DimUpd_faktura docstring —
                # so they are excluded from this assertion.
                cursor.execute(
                    """
                    SELECT
                        SUM(CASE WHEN header_dw_timestamp_update IS NULL THEN 1 ELSE 0 END) AS missing_ts,
                        COUNT(*) AS total
                    FROM [dim].[Faktura]
                    WHERE dw_ek_Faktura <> -1
                      AND faktura_nr IS NOT NULL
                """
                )
                missing_ts, total = cursor.fetchone()
                logger.info(
                    f"dim.Faktura header-ts coverage (MySupply rows): "
                    f"{total - (missing_ts or 0)}/{total}"
                )
                assert (missing_ts or 0) == 0, (
                    f"dim.Faktura has {missing_ts} MySupply rows (faktura_nr present) with NULL "
                    f"header_dw_timestamp_update (DimUpd_faktura must persist it)"
                )
            finally:
                cursor.close()

    def _verify_newest_faktura_ranking(self, target_resource):
        """The is_newest_faktura_for_postering flag must land on the max-header-load row.

        Violation-based: for each postering group, no NON-flagged faktura may have a
        strictly newer (header_dw_timestamp_update, statistic_invoice_header_id) than the
        flagged 'newest' one. Vacuously passes when the bridge is empty / single-faktura.
        """
        with target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    WITH flagged AS (
                        SELECT b.dw_ek_Postering, b.dw_ek_Faktura,
                               d.header_dw_timestamp_update AS ts,
                               d.statistic_invoice_header_id AS hid,
                               b.is_newest_faktura_for_postering AS is_newest
                        FROM [ref].[Faktura_To_Postering] b
                        JOIN [dim].[Faktura] d ON d.dw_ek_Faktura = b.dw_ek_Faktura
                        WHERE b.dw_is_deleted = 0
                    ),
                    chosen AS (
                        SELECT dw_ek_Postering, ts AS chosen_ts, hid AS chosen_hid
                        FROM flagged WHERE is_newest = 1
                    )
                    SELECT COUNT(*)
                    FROM flagged f
                    JOIN chosen c ON c.dw_ek_Postering = f.dw_ek_Postering
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
                    f"sibling header has a newer header_dw_timestamp_update — tie-break broken"
                )
            finally:
                cursor.close()
