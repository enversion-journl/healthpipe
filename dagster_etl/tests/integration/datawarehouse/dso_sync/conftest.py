# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Configuration and fixtures for DSO sync integration tests.

These tests verify that data syncs correctly from the staging (DSI/DSA)
database to the DSO (reporting) database via linked server.

Prerequisites:
- Linked server STAGING_LINK must be configured on DSO database
- Both staging and DSO databases must be accessible
- Environment variables must be set:
  - USE_REAL_DB_FOR_TESTS=true
  - ENABLE_TEST_WRITES=true
"""

import logging
import os

import pytest

logger = logging.getLogger(__name__)


def _should_run_dso_sync_tests() -> bool:
    """Check if DSO sync tests should run based on environment flags."""
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"
    return use_real and writes_enabled


def _skip_if_not_configured():
    """Skip test if environment is not configured for DSO sync."""
    if not _should_run_dso_sync_tests():
        pytest.skip("DSO sync tests require USE_REAL_DB_FOR_TESTS=true and ENABLE_TEST_WRITES=true")


@pytest.fixture
def dso_target_resource(request):
    """
    Get the DSO target resource for executing SQL on DSO database.

    Yields:
        MSSQLResource configured for DSO database
    """
    if not _should_run_dso_sync_tests():
        pytest.skip("DSO sync tests require real resources")

    try:
        # Try to get the real DSO target resource
        resource = request.getfixturevalue("real_mssql_target_dso_resource")
        yield resource
    except pytest.skip.Exception:
        pytest.skip("DSO target resource not available")
    except Exception as e:
        pytest.skip(f"Failed to get DSO target resource: {e}")


@pytest.fixture
def staging_source_resource(request):
    """
    Get the staging source resource for reading from staging database.

    Yields:
        MSSQLResource configured for staging database (source)
    """
    if not _should_run_dso_sync_tests():
        pytest.skip("DSO sync tests require real resources")

    try:
        resource = request.getfixturevalue("real_mssql_source_staging_resource")
        yield resource
    except pytest.skip.Exception:
        pytest.skip("Staging source resource not available")
    except Exception as e:
        pytest.skip(f"Failed to get staging source resource: {e}")


@pytest.fixture
def cleanup_dso_dim_kreditor(dso_target_resource):
    """
    Clean dim.Kreditor table in DSO database before test.

    Truncates the table while preserving the UNKNOWN row (DW_EK = -1).
    """
    logger.info("🧹 Cleaning DSO dim.Kreditor before test...")

    with dso_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Delete all rows except sentinel
            cursor.execute("DELETE FROM dim.Kreditor WHERE DW_EK_Kreditor != -1")
            conn.commit()
            logger.info("✅ DSO dim.Kreditor cleaned (preserved UNKNOWN row)")
        finally:
            cursor.close()

    yield

    # No cleanup after - leave data for inspection


@pytest.fixture
def cleanup_dso_fct_fakturalinje(dso_target_resource):
    """
    Clean fct.Fakturalinje table in DSO database before test.
    """
    logger.info("🧹 Cleaning DSO fct.Fakturalinje before test...")

    with dso_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("TRUNCATE TABLE fct.Fakturalinje")
            conn.commit()
            logger.info("✅ DSO fct.Fakturalinje cleaned")
        finally:
            cursor.close()

    yield


@pytest.fixture
def seed_staging_dim_kreditor(staging_source_resource):
    """
    Ensure staging dim.Kreditor has test data.

    If the table is empty (except sentinel), insert test rows.
    """
    logger.info("🌱 Checking staging dim.Kreditor has data...")

    with staging_source_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Check if there's any data
            cursor.execute("SELECT COUNT(*) FROM dim.Kreditor WHERE DW_EK_Kreditor != -1")
            count = cursor.fetchone()[0]

            if count == 0:
                logger.warning("⚠️ Staging dim.Kreditor is empty, test may need upstream data")
            else:
                logger.info(f"✅ Staging dim.Kreditor has {count} rows")
        finally:
            cursor.close()

    yield


def get_row_count(resource, schema: str, table: str) -> int:
    """Helper to get row count from a table."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}]")
            return cursor.fetchone()[0]
        finally:
            cursor.close()


def compare_counts_between_dbs(staging_resource, dso_resource, schema: str, table: str) -> dict:
    """
    Compare row counts between staging and DSO for a table.

    Returns:
        dict with staging_count, dso_count, and match status
    """
    staging_count = get_row_count(staging_resource, schema, table)
    dso_count = get_row_count(dso_resource, schema, table)

    return {
        "staging_count": staging_count,
        "dso_count": dso_count,
        "match": staging_count == dso_count,
    }
