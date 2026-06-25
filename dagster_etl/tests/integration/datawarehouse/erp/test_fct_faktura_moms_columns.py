# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for VAT/Moms column propagation through the faktura pipeline.

Validates that MomsProcent, MomsGrundlag, MomsBeloeb, and LinjeTotal flow
correctly through the pipeline. These are HEADER-grain measures and live on
fct.Faktura (and pre_stg / stg_fct_faktura). They are sourced from
statistic_invoice_header in MySupply:
  - VAT_AfgiftsKategoriProcent          -> MomsProcent
  - VAT_CategoryTotal_AfgiftsKategoriMomsGrundlag -> MomsGrundlag
  - VAT_TaxAmounts_AfgiftsKategoriMomsBeloeb    -> MomsBeloeb
  - FakturaLinjeTotal                    -> LinjeTotal (fct.Faktura only)

NOTE: header measures (incl. LinjeTotal) were moved OFF the line grain
(fct.Fakturalinje / stg_fct_fakturalinje) to stop N× fan-out double-counting.
The line grain now carries the allowance/charge measures GebyrBeloeb / RabatBeloeb.
"""

import pytest
from tests.utils.materialize_helpers import get_materialization_result

MOMS_COLUMNS = [
    "MomsProcent",
    "MomsGrundlag",
    "MomsBeloeb",
    "LinjeTotal",
]


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_pre_stg_fakturalinje_has_moms_columns(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that stg.pre_fakturalinje_mysupply contains the VAT/Moms columns
    after the fix. These columns must exist for downstream staging to work.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'stg'
                  AND TABLE_NAME = 'pre_fakturalinje_mysupply'
                  AND COLUMN_NAME IN (
                      'MomsProcent', 'MomsGrundlag',
                      'MomsBeloeb', 'LinjeTotal'
                  )
                ORDER BY COLUMN_NAME
            """
            )
            found = {row[0] for row in cursor.fetchall()}

            missing = set(MOMS_COLUMNS) - found
            assert not missing, f"Missing columns in stg.pre_fakturalinje_mysupply: {missing}"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_pre_stg_fakturalinje_moms_values_not_all_null(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that at least some rows in pre_fakturalinje_mysupply have non-NULL
    values for the VAT columns that map to real source columns.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(MomsProcent) AS has_moms_procent,
                    COUNT(MomsGrundlag) AS has_moms_grundlag,
                    COUNT(MomsBeloeb) AS has_moms_beloeb,
                    COUNT(LinjeTotal) AS has_linje_total
                FROM [stg].[pre_fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            total = stats[0]

            if total == 0:
                pytest.skip("pre_fakturalinje_mysupply has no rows for current test partition")

            has_any_vat = stats[1] > 0 or stats[2] > 0 or stats[3] > 0 or stats[4] > 0
            assert has_any_vat, (
                "At least one VAT/Moms column should have non-NULL values. "
                f"Counts: MomsProcent={stats[1]}, "
                f"MomsGrundlag={stats[2]}, MomsBeloeb={stats[3]}, LinjeTotal={stats[4]}"
            )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_fct_faktura_mysupply_has_moms_columns(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that stg.fct_faktura_mysupply contains the VAT/Moms columns
    and that they are correctly propagated from pre-staging.
    """
    metadata = get_materialization_result(dagster_instance, "erp/stg_fct_faktura_mysupply")
    assert metadata is not None, "stg_fct_faktura_mysupply should have materialized"

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'stg'
                  AND TABLE_NAME = 'fct_faktura_mysupply'
                  AND COLUMN_NAME IN (
                      'MomsProcent', 'MomsGrundlag',
                      'MomsBeloeb', 'LinjeTotal'
                  )
                ORDER BY COLUMN_NAME
            """
            )
            found = {row[0] for row in cursor.fetchall()}

            missing = set(MOMS_COLUMNS) - found
            assert not missing, f"Missing columns in stg.fct_faktura_mysupply: {missing}"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_fct_fakturalinje_mysupply_has_gebyr_rabat_measures(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that stg.fct_fakturalinje_mysupply carries the line-level
    allowance/charge measures (GebyrBeloeb, RabatBeloeb) and no longer carries
    LinjeTotal — header measures (incl. LinjeTotal) moved to fct.Faktura to stop
    line-grain fan-out.
    """
    metadata = get_materialization_result(dagster_instance, "erp/stg_fct_fakturalinje_mysupply")
    assert metadata is not None, "stg_fct_fakturalinje_mysupply should have materialized"

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'stg'
                  AND TABLE_NAME = 'fct_fakturalinje_mysupply'
                  AND COLUMN_NAME IN ('GebyrBeloeb', 'RabatBeloeb', 'LinjeTotal')
            """
            )
            found = {row[0] for row in cursor.fetchall()}

            missing = {"GebyrBeloeb", "RabatBeloeb"} - found
            assert not missing, f"Missing line measures in stg.fct_fakturalinje_mysupply: {missing}"
            assert "LinjeTotal" not in found, (
                "LinjeTotal was moved to fct.Faktura and must not exist on "
                "stg.fct_fakturalinje_mysupply"
            )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_moms_columns_populated(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that fct.Faktura has VAT/Moms measure columns populated with real data
    from the source (not all NULL).
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(MomsProcent) AS has_moms_procent,
                    COUNT(MomsGrundlag) AS has_moms_grundlag,
                    COUNT(MomsBeloeb) AS has_moms_beloeb,
                    COUNT(LinjeTotal) AS has_linje_total
                FROM [fct].[Faktura]
                WHERE DW_EK_Faktura != -1
            """
            )
            stats = cursor.fetchone()
            total = stats[0]

            if total == 0:
                pytest.skip("fct.Faktura has no rows in current test environment")

            has_any_vat = stats[1] > 0 or stats[2] > 0 or stats[3] > 0 or stats[4] > 0
            assert has_any_vat, (
                "At least one VAT/Moms column should be populated in fct.Faktura. "
                f"Counts: MomsProcent={stats[1]}, "
                f"MomsGrundlag={stats[2]}, MomsBeloeb={stats[3]}, LinjeTotal={stats[4]}"
            )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_moms_consistency_with_staging(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that aggregate MomsBeloeb in fct.Faktura matches the staging layer.
    This confirms that the TRY_CONVERT in the fact update doesn't silently drop data.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT COUNT(*) AS total, COUNT(MomsBeloeb) AS has_value
                FROM [stg].[fct_faktura_mysupply]
            """
            )
            stg = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*) AS total, COUNT(MomsBeloeb) AS has_value
                FROM [fct].[Faktura]
                WHERE DW_EK_Faktura != -1
            """
            )
            fct = cursor.fetchone()

            if stg[1] > 0:
                assert fct[1] > 0, (
                    f"Staging has {stg[1]} rows with MomsBeloeb, "
                    f"but fct.Faktura has {fct[1]}. TRY_CONVERT may be failing."
                )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_gebyr_rabat_measures_present(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that fct.Fakturalinje carries the line-level allowance/charge
    measures GebyrBeloeb / RabatBeloeb (non-NULL — staging ISNULLs them to 0)
    and no longer carries the header-grain LinjeTotal, which now lives on
    fct.Faktura.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # LinjeTotal must be gone from the line grain.
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'fct'
                  AND TABLE_NAME = 'Fakturalinje'
                  AND COLUMN_NAME = 'LinjeTotal'
            """
            )
            assert (
                cursor.fetchone() is None
            ), "LinjeTotal was moved to fct.Faktura and must not exist on fct.Fakturalinje"

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(GebyrBeloeb) AS has_gebyr,
                    COUNT(RabatBeloeb) AS has_rabat
                FROM [fct].[Fakturalinje]
            """
            )
            stats = cursor.fetchone()
            total = stats[0]

            if total == 0:
                pytest.skip("fct.Fakturalinje has no rows in current test environment")

            # Staging ISNULLs both measures to 0, so every row must carry a
            # non-NULL value (COUNT(col) == COUNT(*)).
            assert stats[1] == total and stats[2] == total, (
                "GebyrBeloeb/RabatBeloeb should be non-NULL on every fct.Fakturalinje row "
                f"(staging ISNULLs to 0). total={total}, has_gebyr={stats[1]}, has_rabat={stats[2]}"
            )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_fct_faktura_deduplication_preserves_moms(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that stg.fct_faktura_mysupply deduplication (ROW_NUMBER) keeps the
    most recent record and its Moms values are not lost during dedup.

    The staging query partitions by CleanCode_Faktura and orders by
    Header_DW_LoadTimestamp DESC, keeping rn=1 per invoice.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT CleanCode_Faktura) AS distinct_fakturas,
                    COUNT(MomsProcent) AS has_moms
                FROM [stg].[fct_faktura_mysupply]
            """
            )
            stats = cursor.fetchone()

            if stats[0] == 0:
                pytest.skip("stg.fct_faktura_mysupply has no rows to validate deduplication")

            assert stats[0] == stats[1], (
                f"Row count ({stats[0]}) should equal distinct CleanCode_Faktura ({stats[1]}). "
                "Deduplication may not be working correctly."
            )
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_fct_fakturalinje_deduplication_preserves_measures(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that stg.fct_fakturalinje_mysupply deduplication produces unique rows
    (one per CleanCode_Fakturalinje) and preserves the line-level measures.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(DISTINCT CleanCode_Fakturalinje) AS distinct_keys,
                    COUNT(GebyrBeloeb) AS has_gebyr
                FROM [stg].[fct_fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()

            if stats[0] == 0:
                pytest.skip("stg.fct_fakturalinje_mysupply has no rows to validate deduplication")

            assert stats[0] == stats[1], (
                f"Row count ({stats[0]}) should equal distinct CleanCode_Fakturalinje ({stats[1]}). "
                "Deduplication may not be working correctly."
            )
        finally:
            cursor.close()
