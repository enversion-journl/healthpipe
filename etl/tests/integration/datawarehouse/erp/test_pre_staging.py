# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for Dimension Staging assets.

Tests validate:
- Dimension staging assets materialize successfully against real database
- Data is deduplicated correctly at clean_code level
- Composite keys are unique
- Enrichment columns are populated
"""

import pytest
from tests.utils.materialize_helpers import get_materialization_result

# ============================================================================
# Dimension Staging Kreditor (unified) Tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)  # After base staging
def test_stg_dim_kreditor_materializes_successfully(
    dagster_instance,
    erp_materialized_session,
):
    """Test that the unified stg_dim_Kreditor materializes successfully."""
    metadata = get_materialization_result(dagster_instance, "erp/stg_dim_Kreditor")
    assert metadata is not None, "Should have materialization metadata"
    assert metadata["row_count"].value >= 0, "Should report row count"


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_dim_kreditor_has_unique_clean_codes(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Test that stg_dim_Kreditor has unique clean_code_Kreditor values (CVR grain)."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    COUNT(DISTINCT clean_code_Kreditor) AS distinct_count
                FROM [stg].[dim_Kreditor]
            """
            )
            result = cursor.fetchone()
            total_count, distinct_count = result[0], result[1]

            assert (
                total_count == distinct_count
            ), f"All clean_code_Kreditor should be unique: {total_count} total, {distinct_count} distinct"
        finally:
            cursor.close()


@pytest.mark.integration
@pytest.mark.erp
@pytest.mark.real_resources
@pytest.mark.order(2)
def test_stg_dim_kreditor_every_row_has_a_source_flag(
    dagster_instance,
    real_mssql_target_resource,
    erp_materialized_session,
):
    """Every staged CVR must be flagged in at least one source (rows come from the source unions)."""
    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM [stg].[dim_Kreditor]
                WHERE dw_source_sydax = 0
                  AND dw_source_ils = 0
                  AND dw_source_mysupply = 0
            """
            )
            flagless = cursor.fetchone()[0]
        finally:
            cursor.close()
    assert flagless == 0, f"{flagless} rows have no source flag set"
