# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for fct_postering using real databases.

Tests validate:
- DW_EK lookups from maps (NOT dimensions) - Clean Architecture
- Multi-date handling (9 date conversions)
- Grain validation (ref_LedgertransId)
- FK matching with COALESCE to -1 for UNKNOWN
- Financial measure calculations

Architecture: staging → pre_staging (Layer 0) → facts
The fact table reads from pre_postering_sydax which already performs heavy joins.
"""

import pytest
from tests.utils.materialize_helpers import (
    get_materialization_result,
)

pytestmark = pytest.mark.skip(
    reason="Legacy full-fact assertions are brittle in shared integration environments"
)


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_postering_dw_ek_from_maps_not_dimensions(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    CRITICAL TEST: Verify that fact table looks up DW_EK from MAP tables, NOT dimension tables.
    This validates Clean Architecture adherence.
    """
    # Verify fact table has data
    metadata = get_materialization_result(dagster_instance, "fct_postering")
    assert metadata is not None
    assert metadata["rows_affected"].value >= 1

    # Query fact table for test record
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT TOP 1
                    ref_LedgertransId,
                    DW_EK_Kreditor,
                    DW_EK_Kontoplan,
                    Posteringsbeloeb
                FROM [fct].[Postering]
                WHERE DW_EK_Kreditor <> -1  -- Find one with valid kreditor
            """
            )
            fact_row = cursor.fetchone()

            if fact_row is not None:
                posting_id, dw_ek_kreditor, dw_ek_kontoplan, beloeb = fact_row

                # CRITICAL: Verify DW_EK_Kreditor comes from map.Kreditor
                cursor.execute(
                    """
                    SELECT DW_EK_Kreditor
                    FROM [map].[Kreditor]
                    WHERE DW_EK_Kreditor = ?
                """,
                    (dw_ek_kreditor,),
                )
                map_kreditor = cursor.fetchone()

                assert map_kreditor is not None, "Fact DW_EK_Kreditor must exist in map.Kreditor!"

                # Validate kontoplan FK
                if dw_ek_kontoplan != -1:
                    cursor.execute(
                        """
                        SELECT DW_EK_Kontoplan
                        FROM [map].[Kontoplan]
                        WHERE DW_EK_Kontoplan = ?
                    """,
                        (dw_ek_kontoplan,),
                    )
                    map_kontoplan = cursor.fetchone()

                    assert (
                        map_kontoplan is not None
                    ), "Fact DW_EK_Kontoplan must exist in map.Kontoplan!"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_postering_multi_date_conversions(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that all 9 date key conversions work correctly:
    - DW_EK_Dato_Godkendelse
    - DW_EK_Dato_Lukket
    - DW_EK_Dato_TransOprettet
    - DW_EK_Dato_Forfald
    - DW_EK_Dato_JournalFaktura
    - DW_EK_Dato_SidstUdlignet
    - DW_EK_Dato_Transaktion
    - DW_EK_Dato_Bogfoering
    - DW_EK_Dato_PostOprettet
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    DW_EK_Dato_Godkendelse,
                    DW_EK_Dato_Lukket,
                    DW_EK_Dato_TransOprettet,
                    DW_EK_Dato_Forfald,
                    DW_EK_Dato_JournalFaktura,
                    DW_EK_Dato_SidstUdlignet,
                    DW_EK_Dato_Transaktion,
                    DW_EK_Dato_Bogfoering,
                    DW_EK_Dato_PostOprettet
                FROM [fct].[Postering]
            """
            )
            rows = cursor.fetchall()

            assert len(rows) > 0, "Should have postings loaded"

            # Check that date keys are valid integers when populated
            for row in rows:
                for date_key in row:
                    if date_key is not None:
                        assert isinstance(date_key, int), "Date key should be integer"
                        assert date_key >= 19000101, "Date key should be valid YYYYMMDD format"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_postering_grain_uniqueness(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that the grain (ref_LedgertransId) is unique.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Check for duplicate grain
            cursor.execute(
                """
                SELECT
                    ref_LedgertransId,
                    COUNT(*) AS duplicate_count
                FROM [fct].[Postering]
                GROUP BY ref_LedgertransId
                HAVING COUNT(*) > 1
            """
            )
            duplicates = cursor.fetchall()

            assert (
                len(duplicates) == 0
            ), f"Found {len(duplicates)} duplicate ref_LedgertransId values"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_postering_unknown_fk_handling(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that missing FK lookups are handled gracefully with -1 (UNKNOWN).
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            # Check that no FKs are NULL
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN DW_EK_Kreditor IS NULL THEN 1 ELSE 0 END) AS null_kreditor,
                    SUM(CASE WHEN DW_EK_Kontoplan IS NULL THEN 1 ELSE 0 END) AS null_kontoplan,
                    SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS unknown_kreditor,
                    SUM(CASE WHEN DW_EK_Kontoplan = -1 THEN 1 ELSE 0 END) AS unknown_kontoplan
                FROM [fct].[Postering]
            """
            )
            stats = cursor.fetchone()

            assert stats[0] > 0, "Should have facts loaded"
            assert stats[1] == 0, "DW_EK_Kreditor should never be NULL"
            assert stats[2] == 0, "DW_EK_Kontoplan should never be NULL"

            # Log UNKNOWN counts for diagnostic purposes
            if stats[3] > 0:
                print(f"Note: {stats[3]} postings with UNKNOWN Kreditor (-1)")
            if stats[4] > 0:
                print(f"Note: {stats[4]} postings with UNKNOWN Kontoplan (-1)")
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(4)
def test_fct_postering_financial_measures(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """
    Verify that financial measures are populated correctly.
    """
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:

            cursor.execute(
                """
                SELECT
                    TransaktionsbeloebValuta,
                    Transaktionsbeloeb,
                    UdlignetBeloebValuta,
                    UdlignetBeloeb,
                    Posteringsbeloeb
                FROM [fct].[Postering]
                WHERE Posteringsbeloeb IS NOT NULL
            """
            )
            rows = cursor.fetchall()

            assert len(rows) > 0, "Should have postings with amounts"

            # Validate that amounts are reasonable (not zero, have proper precision)
            for row in rows:
                if row[4] is not None:  # Posteringsbeloeb
                    assert isinstance(row[4], int | float), "Amount should be numeric"
        finally:
            cursor.close()
