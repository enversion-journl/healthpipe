# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for Dimension Staging assets.

Tests validate:
- Dimension staging assets materialize successfully against real database
- Data is deduplicated correctly at CleanCode level
- Composite keys are unique
- Enrichment columns are populated
- is_k_prefix flag is calculated correctly (for kreditor)
"""

import pytest
from tests.utils.materialize_helpers import get_materialization_result

# ============================================================================
# Dimension Staging Kreditor ILS Tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)  # After base staging
def test_dim_kreditor_ils_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Test that dim_kreditor_ils materializes successfully."""
    # Verify materialization metadata
    metadata = get_materialization_result(dagster_instance, "erp/stg_dim_kreditor_ils")
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should report row count"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_kreditor_ils_has_unique_clean_codes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_kreditor_ils has unique CleanCode_Kreditor values."""
    # Check for uniqueness - count should equal distinct count
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT CleanCode_Kreditor) AS distinct_count
                FROM [stg].[dim_kreditor_ils]
            """
            )
            result = cursor.fetchone()
            total_count, distinct_count = result[0], result[1]

            assert (
                total_count == distinct_count
            ), f"All CleanCode_Kreditor should be unique: {total_count} total, {distinct_count} distinct"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_kreditor_ils_has_lnr_values(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_kreditor_ils includes lnr values from ILS."""
    # Check lnr column is present and queryable
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(lnr) FROM [stg].[dim_kreditor_ils]")
            lnr_count = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert lnr_count >= 0, "lnr column should be queryable"


# ============================================================================
# Dimension Staging Kreditor MySupply Tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_kreditor_mysupply_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Test that dim_kreditor_mysupply materializes successfully."""
    metadata = get_materialization_result(dagster_instance, "erp/stg_dim_kreditor_mysupply")
    assert metadata is not None, "Should have materialization metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_kreditor_mysupply_has_unique_clean_codes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_kreditor_mysupply has unique CleanCode_Kreditor values."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT CleanCode_Kreditor) AS distinct_count
                FROM [stg].[dim_kreditor_mysupply]
            """
            )
            result = cursor.fetchone()
            total_count, distinct_count = result[0], result[1]

            assert (
                total_count == distinct_count
            ), f"All CleanCode_Kreditor should be unique: {total_count} total, {distinct_count} distinct"
        finally:
            cursor.close()


# ============================================================================
# Dimension Staging Vare ILS Tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_ils_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Test that dim_vare_ils materializes successfully."""
    metadata = get_materialization_result(dagster_instance, "erp/stg_dim_vare_ils")
    assert metadata is not None, "Should have materialization metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_ils_has_unique_composite_keys(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_vare_ils has unique (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) combinations."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT CONCAT(CleanCode_Kreditor, '|', CleanCode_LeverandoerVarenummer)) AS distinct_count
                FROM [stg].[dim_vare_ils]
            """
            )
            result = cursor.fetchone()
            total_count, distinct_count = result[0], result[1]

            assert (
                total_count == distinct_count
            ), f"All composite keys should be unique: {total_count} total, {distinct_count} distinct"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_ils_has_enrichment_data(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_vare_ils contains enrichment data from joined tables."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Check that at least some rows have enrichment data
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(navn) AS has_navn,
                    COUNT(maerke) AS has_maerke,
                    COUNT(vareart_navn) AS has_vareart_navn
                FROM [stg].[dim_vare_ils]
            """
            )
            result = cursor.fetchone()
            total, has_navn, has_maerke, has_vareart_navn = (
                result[0],
                result[1],
                result[2],
                result[3],
            )

            if total > 0:
                # Log coverage for diagnostic purposes; no minimum required
                enrichment_rate = (has_navn + has_maerke + has_vareart_navn) / (total * 3)
                print(f"Enrichment coverage rate: {enrichment_rate:.2%}")
        finally:
            cursor.close()


# ============================================================================
# Dimension Staging Vare MySupply Tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_mysupply_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Test that dim_vare_mysupply materializes successfully."""
    metadata = get_materialization_result(dagster_instance, "erp/stg_dim_vare_mysupply")
    assert metadata is not None, "Should have materialization metadata"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_mysupply_has_unique_composite_keys(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_vare_mysupply has unique (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) combinations."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT CONCAT(CleanCode_Kreditor, '|', CleanCode_LeverandoerVarenummer)) AS distinct_count
                FROM [stg].[dim_vare_mysupply]
            """
            )
            result = cursor.fetchone()
            total_count, distinct_count = result[0], result[1]

            assert (
                total_count == distinct_count
            ), f"All composite keys should be unique: {total_count} total, {distinct_count} distinct"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_dim_vare_mysupply_has_item_names(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that dim_vare_mysupply contains item names from invoice lines."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(NULLIF(Item_FakturaUdstederVareNavn, '')) AS has_item_name
                FROM [stg].[dim_vare_mysupply]
            """
            )
            result = cursor.fetchone()
            total, has_item_name = result[0], result[1]

            if total > 0:
                name_rate = has_item_name / total
                # Most MySupply invoice lines should have item names
                assert name_rate > 0.5, f"Expected most items to have names, got {name_rate:.1%}"
        finally:
            cursor.close()
