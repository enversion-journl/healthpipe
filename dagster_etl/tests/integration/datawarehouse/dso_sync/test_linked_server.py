# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Linked Server Connectivity Integration Tests

Tests that verify the linked server connection from DSO to staging database
is working correctly.

Prerequisites:
- Linked server STAGING_LINK must be created on DSO database
- USE_REAL_DB_FOR_TESTS=true
- ENABLE_TEST_WRITES=true
"""

import pytest
from DSO.load.sync_utils import (
    LINKED_SERVER_NAME,
    STAGING_DATABASE,
)

# Linked-server checks are environment-specific and not portable across test environments.
pytestmark = pytest.mark.skip(reason="Requires dedicated DSO linked-server environment")


@pytest.fixture(autouse=True)
def require_linked_server(dso_target_resource):
    with dso_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM sys.servers WHERE name = %s",
                (LINKED_SERVER_NAME,),
            )
            count = cursor.fetchone()[0]
        finally:
            cursor.close()
    if count == 0:
        pytest.skip(f"Linked server '{LINKED_SERVER_NAME}' not configured")


class TestLinkedServerConnectivity:
    """Tests for linked server connectivity."""

    def test_linked_server_exists(self, dso_target_resource):
        """Test that the linked server exists on DSO database."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT(*) FROM sys.servers WHERE name = %s",
                    (LINKED_SERVER_NAME,),
                )
                count = cursor.fetchone()[0]
            finally:
                cursor.close()

            assert count == 1, f"Linked server '{LINKED_SERVER_NAME}' not found on DSO database"

    def test_linked_server_connectivity(self, dso_target_resource):
        """Test that we can connect to staging via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Test connectivity using sp_testlinkedserver
                try:
                    cursor.execute(
                        f"EXEC sp_testlinkedserver @servername = N'{LINKED_SERVER_NAME}'"
                    )
                    # If no exception, connectivity is OK
                except Exception as e:
                    pytest.fail(f"Linked server connectivity test failed: {e}")
            finally:
                cursor.close()

    def test_linked_server_can_read_information_schema(self, dso_target_resource):
        """Test that we can query INFORMATION_SCHEMA via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Query staging database schema information
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].INFORMATION_SCHEMA.TABLES
                    """
                )
                table_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            # Staging should have tables
            assert table_count > 0, "No tables found in staging database via linked server"

    def test_linked_server_can_read_dim_schema(self, dso_target_resource):
        """Test that we can read from dim schema in staging via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # Check if dim schema has tables
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'dim'
                    """
                )
                dim_table_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            assert dim_table_count > 0, "No dim.* tables found in staging via linked server"

    def test_linked_server_can_read_map_schema(self, dso_target_resource):
        """Test that we can read from map schema in staging via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'map'
                    """
                )
                map_table_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            assert map_table_count > 0, "No map.* tables found in staging via linked server"

    def test_linked_server_can_read_fct_schema(self, dso_target_resource):
        """Test that we can read from fct schema in staging via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = 'fct'
                    """
                )
                fct_table_count = cursor.fetchone()[0]
            finally:
                cursor.close()

            assert fct_table_count > 0, "No fct.* tables found in staging via linked server"


class TestLinkedServerDataAccess:
    """Tests for reading actual data via linked server."""

    def test_can_count_dim_kreditor_rows(self, dso_target_resource):
        """Test that we can count rows in dim.Kreditor via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].[dim].[Kreditor]
                        """
                    )
                    row_count = cursor.fetchone()[0]
                    # Just verify we can execute the query - count may be 0 or more
                    assert row_count >= 0
                except Exception as e:
                    # Table might not exist yet
                    if "Invalid object name" in str(e):
                        pytest.skip(
                            "dim.Kreditor table not found in staging - run staging ETL first"
                        )
                    raise
            finally:
                cursor.close()

    def test_can_sample_dim_kreditor_data(self, dso_target_resource):
        """Test that we can read sample data from dim.Kreditor via linked server."""
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(
                        f"""
                        SELECT TOP 1 DW_EK_Kreditor, CleanCode_Kreditor
                        FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].[dim].[Kreditor]
                        WHERE DW_EK_Kreditor != -1
                        """
                    )
                    row = cursor.fetchone()

                    if row is None:
                        pytest.skip("No data in staging dim.Kreditor - run staging ETL first")

                    # Verify we got valid data
                    assert row[0] is not None  # DW_EK_Kreditor
                    assert row[1] is not None  # CleanCode_Kreditor

                except Exception as e:
                    if "Invalid object name" in str(e):
                        pytest.skip(
                            "dim.Kreditor table not found in staging - run staging ETL first"
                        )
                    raise
            finally:
                cursor.close()

    def test_can_count_ref_patient_borger_rows(self, dso_target_resource):
        """Test that we can count rows in ref.Patient_Borger via linked server.

        Replaces the legacy test_can_count_map_borger_rows: map.Borger was
        removed when the Borger pipeline switched to the FHIR + ref-table
        architecture. ref.Patient_Borger is the new entity-mapping table
        (DW_EK_Borger INT IDENTITY ↔ patient_id).
        """
        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].[ref].[Patient_Borger]
                        """
                    )
                    row_count = cursor.fetchone()[0]
                    assert row_count >= 0
                except Exception as e:
                    if "Invalid object name" in str(e):
                        pytest.skip("ref.Patient_Borger table not found in staging")
                    raise
            finally:
                cursor.close()


class TestLinkedServerPerformance:
    """Basic performance tests for linked server queries."""

    def test_count_query_completes_quickly(self, dso_target_resource):
        """Test that a simple count query completes in reasonable time."""
        import time

        with dso_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                start_time = time.time()

                try:
                    cursor.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM [{LINKED_SERVER_NAME}].[{STAGING_DATABASE}].INFORMATION_SCHEMA.TABLES
                        """
                    )
                    cursor.fetchone()
                except Exception:
                    pytest.skip("Could not execute count query via linked server")

                elapsed = time.time() - start_time
            finally:
                cursor.close()

            # Should complete in less than 10 seconds
            assert (
                elapsed < 10
            ), f"Count query took {elapsed:.2f}s - linked server may have connectivity issues"
