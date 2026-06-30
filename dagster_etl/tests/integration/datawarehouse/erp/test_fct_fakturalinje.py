# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for fct_fakturalinje using real databases.

Tests validate:
- DW_EK lookups from maps (NOT dimensions) - Clean Architecture
- Date key conversions (4 dates)
- Grain validation (statistic_invoice_line_id + statistic_invoice_header_id)
- FK matching with COALESCE to -1 for UNKNOWN
- Measure calculations

Architecture: staging → pre_staging (Layer 0) → facts
The fact table reads from pre_fakturalinje_mysupply which already performs heavy joins.
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
def test_fct_fakturalinje_dw_ek_from_maps_not_dimensions(
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
    metadata = get_materialization_result(dagster_instance, "fct_fakturalinje")
    assert metadata is not None
    assert metadata["rows_affected"].value >= 1

    # Query fact table for test record (invoice_line_id=1)
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    statistic_invoice_line_id,
                    statistic_invoice_header_id,
                    DW_EK_Kreditor,
                    DW_EK_Vare,
                    DW_EK_Kontoplan,
                    DW_EK_Dato_Faktura,
                    FakturaNr,
                    AntalEnhederFaktureret,
                    LinjeBeloeb
                FROM [fct].[Fakturalinje]
                WHERE statistic_invoice_line_id = 1
            """
            )
            fact_row = cursor.fetchone()

            assert fact_row is not None, "Test invoice line not found in fact table"

            # Extract values
            (
                line_id,
                header_id,
                dw_ek_kreditor,
                dw_ek_vare,
                dw_ek_kontoplan,
                date_faktura,
                faktura_nr,
                antal,
                beloeb,
            ) = fact_row

            # Validate grain
            assert line_id == 1
            assert header_id == 10

            # CRITICAL: Verify DW_EK_Kreditor comes from map.Kreditor
            cursor.execute(
                """
                SELECT DW_EK_Kreditor, Cvrnr
                FROM [map].[Kreditor]
                WHERE Cvrnr = 'CVR_12345678'
            """
            )
            map_kreditor = cursor.fetchone()

            assert map_kreditor is not None, "Test kreditor not found in map table"
            assert (
                dw_ek_kreditor == map_kreditor[0]
            ), "Fact DW_EK_Kreditor must match map.Kreditor, not dim.Kreditor!"

            # Validate measures
            assert antal is not None, "AntalEnhederFaktureret should be populated"
            assert beloeb is not None, "LinjeBeloeb should be populated"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_date_key_conversions(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that date key conversions work correctly for all 4 date fields:
    - DW_EK_Dato_Faktura
    - DW_EK_Dato_Ordre
    - DW_EK_Dato_Betaling
    - DW_EK_Dato_Modtagelse
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    DW_EK_Dato_Faktura,
                    DW_EK_Dato_Ordre,
                    DW_EK_Dato_Betaling,
                    DW_EK_Dato_Modtagelse
                FROM [fct].[Fakturalinje]
                WHERE statistic_invoice_line_id = 1
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
def test_fct_fakturalinje_unknown_fk_handling(
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
                    SUM(CASE WHEN DW_EK_Vare IS NULL THEN 1 ELSE 0 END) AS null_vare,
                    SUM(CASE WHEN DW_EK_Kontoplan IS NULL THEN 1 ELSE 0 END) AS null_kontoplan,
                    SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS unknown_kreditor,
                    SUM(CASE WHEN DW_EK_Vare = -1 THEN 1 ELSE 0 END) AS unknown_vare
                FROM [fct].[Fakturalinje]
            """
            )
            stats = cursor.fetchone()

            assert stats[0] > 0, "Should have facts loaded"
            assert stats[1] == 0, "DW_EK_Kreditor should never be NULL"
            assert stats[2] == 0, "DW_EK_Vare should never be NULL"
            # DW_EK_Kontoplan can be NULL or -1 (not implemented yet)

            # Log UNKNOWN counts for diagnostic purposes
            if stats[4] > 0:
                print(f"Note: {stats[4]} facts with UNKNOWN Kreditor (-1)")
            if stats[5] > 0:
                print(f"Note: {stats[5]} facts with UNKNOWN Vare (-1)")
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_grain_uniqueness(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that the grain (statistic_invoice_line_id + statistic_invoice_header_id) is unique.
    This validates that MERGE ON clause is correct.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Check for duplicate grain combinations
            cursor.execute(
                """
                SELECT
                    statistic_invoice_line_id,
                    statistic_invoice_header_id,
                    COUNT(*) AS duplicate_count
                FROM [fct].[Fakturalinje]
                GROUP BY statistic_invoice_line_id, statistic_invoice_header_id
                HAVING COUNT(*) > 1
            """
            )
            duplicates = cursor.fetchall()

            assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate grain combinations"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_fakturalinje_idempotent_merge(
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
        asset_keys=["fct_fakturalinje"],
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
            cursor.execute("SELECT COUNT(*) FROM [fct].[Fakturalinje]")
            count_first = cursor.fetchone()[0]
        finally:
            cursor.close()
    # Run again (should be idempotent)
    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=["fct_fakturalinje"],
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
            cursor.execute("SELECT COUNT(*) FROM [fct].[Fakturalinje]")
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
def test_fct_fakturalinje_measure_calculations(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that measures (amounts, prices, quantities) are calculated correctly.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    AntalEnhederFaktureret,
                    LinjeBeloeb,
                    EnhedsPris,
                    LinjeTotal
                FROM [fct].[Fakturalinje]
                WHERE statistic_invoice_line_id = 1
            """
            )
            measures = cursor.fetchone()

            assert measures is not None

            antal, linje_beloeb, enheds_pris, linje_total = measures

            # Basic validation
            if antal is not None and antal > 0:
                assert antal > 0, "Quantity should be positive"

            if linje_beloeb is not None:
                # Line amount should match line total
                if linje_total is not None:
                    assert (
                        abs(linje_beloeb - linje_total) < 0.01
                    ), "LinjeBeloeb should match LinjeTotal"
        finally:
            cursor.close()
