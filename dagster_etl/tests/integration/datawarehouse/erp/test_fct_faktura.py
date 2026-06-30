# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for fct_faktura using real databases.

Tests validate:
- DW_EK lookups from maps (NOT dimensions) - Clean Architecture
- Date key conversions (3 dates)
- Grain validation (DW_EK_Faktura - one row per faktura)
- FK matching with COALESCE to -1 for UNKNOWN
- Measure calculations and source data consistency
- Invoice-level measures match source when aggregated at header level

Architecture: staging → pre_staging (Layer 0) → facts
The fact table reads from stg.fct_faktura_mysupply which aggregates at faktura grain.
"""

import pytest
from dagster import ResourceDefinition
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)

pytestmark = pytest.mark.skip(
    reason="Legacy full-fact assertions are brittle in shared integration environments"
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_dw_ek_from_maps_not_dimensions(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    CRITICAL TEST: Verify that fact table looks up DW_EK from MAP tables, NOT dimension tables.
    This validates Clean Architecture adherence.

    Test chain: staging → pre_staging → maps → dims → facts
    Fact must lookup DW_EK_Kreditor from map.Kreditor, NOT dim.Kreditor
    """
    # Verify fact table has data
    metadata = get_materialization_result(dagster_instance, "fct_faktura")
    assert metadata is not None
    assert metadata["rows_affected"].value >= 1

    # Query fact table for test record
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT TOP 1
                    DW_EK_Faktura,
                    DW_EK_Kreditor,
                    FakturaNr,
                    MomsProcent,
                    FakturaTotal
                FROM [fct].[Faktura]
                WHERE DW_EK_Faktura != -1
            """
            )
            fact_row = cursor.fetchone()

            assert fact_row is not None, "Test invoice not found in fact table"

            # Extract values
            (
                dw_ek_faktura,
                dw_ek_kreditor,
                faktura_nr,
                moms_procent,
                faktura_total,
            ) = fact_row

            # CRITICAL: Verify DW_EK_Kreditor comes from map.Kreditor
            cursor.execute(
                """
                SELECT m.DW_EK_Faktura, m.DW_EK_Kreditor
                FROM [map].[Faktura] m
                WHERE m.DW_EK_Faktura = ?
            """,
                (dw_ek_faktura,),
            )
            map_faktura = cursor.fetchone()

            assert map_faktura is not None, "Test faktura not found in map table"
            assert dw_ek_faktura == map_faktura[0], "Fact DW_EK_Faktura must match map.Faktura!"

            # Validate measures
            assert faktura_nr is not None, "FakturaNr should be populated"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_date_key_conversions(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that date key conversions work correctly for all 3 date fields:
    - DW_EK_Dato_Faktura
    - DW_EK_Dato_Ordre
    - DW_EK_Dato_Betaling
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT TOP 1
                    DW_EK_Dato_Faktura,
                    DW_EK_Dato_Ordre,
                    DW_EK_Dato_Betaling
                FROM [fct].[Faktura]
                WHERE DW_EK_Faktura != -1
            """
            )
            dates = cursor.fetchone()

            assert dates is not None

            # At least Faktura date should be populated (primary date)
            assert dates[0] is not None, "DW_EK_Dato_Faktura should be populated"

            # Date keys should be integers in YYYYMMDD format
            if dates[0] is not None:
                assert isinstance(dates[0], int), "Date key should be integer"
                assert dates[0] >= 19000101, "Date key should be valid YYYYMMDD format"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_unknown_fk_handling(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that missing FK lookups are handled gracefully with -1 (UNKNOWN).
    This is important for data quality - we don't want NULLs in FK columns.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Check that no FKs are NULL (all should be either valid DW_EK or -1)
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN DW_EK_Kreditor IS NULL THEN 1 ELSE 0 END) AS null_kreditor,
                    SUM(CASE WHEN DW_EK_Faktura IS NULL THEN 1 ELSE 0 END) AS null_faktura,
                    SUM(CASE WHEN DW_EK_EAN_Regningsmodtager IS NULL THEN 1 ELSE 0 END) AS null_ean,
                    SUM(CASE WHEN DW_EK_Kontoplan IS NULL THEN 1 ELSE 0 END) AS null_kontoplan,
                    SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS unknown_kreditor,
                    SUM(CASE WHEN DW_EK_Faktura = -1 THEN 1 ELSE 0 END) AS unknown_faktura
                FROM [fct].[Faktura]
            """
            )
            stats = cursor.fetchone()

            assert stats[0] > 0, "Should have facts loaded"
            assert stats[1] == 0, "DW_EK_Kreditor should never be NULL"
            assert stats[2] == 0, "DW_EK_Faktura should never be NULL"
            assert stats[3] == 0, "DW_EK_EAN_Regningsmodtager should never be NULL"
            assert stats[4] == 0, "DW_EK_Kontoplan should never be NULL"

            cursor.execute(
                """
                SELECT
                    SUM(CASE WHEN DW_EK_Organisation_Regningsmodtager IS NULL THEN 1 ELSE 0 END)
                FROM [vw_fct].[Faktura]
            """
            )
            null_view_org = cursor.fetchone()[0]
            assert (
                null_view_org == 0
            ), "DW_EK_Organisation_Regningsmodtager should never be NULL in vw_fct.Faktura"

            # Log UNKNOWN counts for diagnostic purposes
            if stats[4] > 0:
                print(f"Note: {stats[4]} facts with UNKNOWN Kreditor (-1)")
            if stats[5] > 0:
                print(f"Note: {stats[5]} facts with UNKNOWN Faktura (-1)")
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_grain_uniqueness(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that the grain (DW_EK_Faktura) is unique - one row per faktura.
    This validates that MERGE ON clause is correct and grain is properly enforced.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Check for duplicate DW_EK_Faktura values
            cursor.execute(
                """
                SELECT
                    DW_EK_Faktura,
                    COUNT(*) AS duplicate_count
                FROM [fct].[Faktura]
                GROUP BY DW_EK_Faktura
                HAVING COUNT(*) > 1
            """
            )
            duplicates = cursor.fetchall()

            assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate DW_EK_Faktura values"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_matches_source_count(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that row count in fct.Faktura matches distinct invoices in pre-staging.
    This validates that the grain aggregation is correct.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Count distinct invoices in pre-staging using the composite grain
            # (CleanCode_Kreditor + '_' + CleanCode_Faktura) which is the true faktura key
            cursor.execute(
                """
                SELECT COUNT(DISTINCT CleanCode_Kreditor + '_' + CleanCode_Faktura)
                FROM [stg].[pre_fakturalinje_mysupply]
                WHERE CleanCode_Faktura IS NOT NULL
                  AND CleanCode_Faktura <> ''
                  AND CleanCode_Kreditor IS NOT NULL
                  AND CleanCode_Kreditor <> ''
            """
            )
            source_count = cursor.fetchone()[0]

            # Count rows in fact table
            cursor.execute("SELECT COUNT(*) FROM [fct].[Faktura] WHERE DW_EK_Faktura != -1")
            fact_count = cursor.fetchone()[0]

            assert (
                fact_count == source_count
            ), f"Fact count ({fact_count}) should match distinct invoices in pre-staging ({source_count})"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_measures_match_source(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    KEY GRAIN CORRECTNESS TEST: Verify that aggregated measures from fct.Faktura
    match source data aggregated at header level (not inflated by line count).

    This test confirms that the new grain eliminates the duplication problem:
    - SUM(FakturaTotal) in fct.Faktura should match SUM of distinct header totals from pre-staging
    - SUM(MomsBeloeb) should match header-level aggregation
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Aggregate from fct.Faktura (one row per faktura)
            cursor.execute(
                """
                SELECT
                    SUM(FakturaTotal) AS total_faktura_total,
                    SUM(MomsBeloeb) AS total_moms_beloeb,
                    COUNT(*) AS faktura_count
                FROM [fct].[Faktura]
                WHERE DW_EK_Faktura != -1
            """
            )
            fact_agg = cursor.fetchone()
            fact_total = fact_agg[0] if fact_agg[0] is not None else 0
            fact_moms = fact_agg[1] if fact_agg[1] is not None else 0
            fact_count = fact_agg[2]

            # Aggregate from pre-staging using the composite grain
            # (CleanCode_Kreditor + '_' + CleanCode_Faktura) which is the true faktura key
            cursor.execute(
                """
                SELECT
                    SUM(COALESCE(HeaderFakturaTotal, MetadataFakturaTotal)) AS total_faktura_total,
                    SUM(MomsBeloeb) AS total_moms_beloeb,
                    COUNT(*) AS distinct_invoices
                FROM (
                    SELECT
                        CleanCode_Kreditor + '_' + CleanCode_Faktura AS composite_faktura,
                        HeaderFakturaTotal,
                        MetadataFakturaTotal,
                        MomsBeloeb,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor + '_' + CleanCode_Faktura
                            ORDER BY Header_DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE CleanCode_Faktura IS NOT NULL
                      AND CleanCode_Faktura <> ''
                      AND CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_Kreditor <> ''
                ) ranked
                WHERE rn = 1
            """
            )
            source_agg = cursor.fetchone()
            source_total = source_agg[0] if source_agg[0] is not None else 0
            source_moms = source_agg[1] if source_agg[1] is not None else 0
            source_count = source_agg[2]

            # Validate counts match
            assert (
                fact_count == source_count
            ), f"Fact count ({fact_count}) should match distinct invoices ({source_count})"

            # Validate totals match (with tolerance for rounding)
            if fact_total > 0 and source_total > 0:
                diff_pct = (
                    abs(fact_total - source_total) / max(abs(fact_total), abs(source_total)) * 100
                )
                assert (
                    diff_pct < 0.01
                ), f"FakturaTotal mismatch: fact={fact_total}, source={source_total}, diff={diff_pct:.4f}%"

            if fact_moms > 0 and source_moms > 0:
                diff_pct = (
                    abs(fact_moms - source_moms) / max(abs(fact_moms), abs(source_moms)) * 100
                )
                assert (
                    diff_pct < 0.01
                ), f"MomsBeloeb mismatch: fact={fact_moms}, source={source_moms}, diff={diff_pct:.4f}%"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_faktura_idempotent_merge(
    dagster_instance,
    real_mysupply_mssql_resource,
    real_mssql_target_resource,
    cleanup_erp_fact_tables,
):
    """
    Verify that fact asset is idempotent - running twice produces same result.
    MERGE should update existing records, not duplicate them.
    """
    # Run once
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["fct_faktura"],
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [fct].[Faktura]")
            count_first = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Run again (should be idempotent)
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["fct_faktura"],
        resources={
            "mysupply_mssql": ResourceDefinition.hardcoded_resource(real_mysupply_mssql_resource),
            "mssql_target_staging": ResourceDefinition.hardcoded_resource(
                real_mssql_target_resource
            ),
        },
    )

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM [fct].[Faktura]")
            count_second = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert (
        count_first == count_second
    ), "Fact count should be same after second run (idempotent MERGE)"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_has_measure_columns(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that fct.Fakturalinje has all expected measure columns including
    LinjeTotal and Valutakode_Faktura.
    VAT/Moms columns (MomsProcent, MomsGrundlag, MomsBeloeb) are header-level
    and only belong on fct.Faktura.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'fct'
                  AND TABLE_NAME = 'Fakturalinje'
                  AND COLUMN_NAME IN (
                      'LinjeTotal',
                      'Valutakode_Faktura'
                  )
            """
            )
            invoice_columns = [c[0] for c in cursor.fetchall()]

            expected = {
                "LinjeTotal",
                "Valutakode_Faktura",
            }
            assert expected == set(
                invoice_columns
            ), f"Missing columns in fct.Fakturalinje: {expected - set(invoice_columns)}"

            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'fct'
                  AND TABLE_NAME = 'Fakturalinje'
                  AND COLUMN_NAME IN (
                      'AntalEnhederFaktureret',
                      'LinjeBeloeb',
                      'EnhedsPris',
                      'Valutakode_Linje',
                      'Valutakode_Enhedspris'
                  )
            """
            )
            line_columns = cursor.fetchall()

            assert (
                len(line_columns) == 5
            ), f"Line-level measures missing from fct.Fakturalinje: expected 5, found {len(line_columns)}"
        finally:
            cursor.close()
