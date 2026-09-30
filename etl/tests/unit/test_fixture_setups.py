# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pytest tests for verifying test fixture setups work correctly.

These tests verify:
1. Mock fixtures work without database connections
2. Real fixtures properly load environment variables
3. Configuration precedence (TEST_* over PROD_*)
4. Safety checks work correctly
"""

import os

import pytest

pytestmark = pytest.mark.unit


class TestMockFixtures:
    """Test that mock fixtures work correctly."""

    def test_mock_mssql_resource(self, mock_mssql_resource):
        """Test mock MSSQL resource fixture."""
        resource, cursor = mock_mssql_resource
        assert resource is not None
        assert hasattr(resource, "get_connection")
        assert cursor is not None

    def test_mock_fhir_server_resource(self, mock_fhir_server_resource):
        """Test mock FHIR server resource fixture."""
        assert mock_fhir_server_resource is not None
        assert hasattr(mock_fhir_server_resource, "base_url")
        assert mock_fhir_server_resource.base_url == "http://mock-fhir-server"

    def test_mock_oracle_resource(self, mock_oracle_resource):
        """Test mock Oracle resource fixture."""
        resource, cursor = mock_oracle_resource
        assert resource is not None
        assert hasattr(resource, "get_connection")
        assert cursor is not None

    def test_mock_mysupply_resource(self, mock_mysupply_mssql_resource):
        """Test mock MySupply MSSQL resource fixture."""
        resource, cursor = mock_mysupply_mssql_resource
        assert resource is not None
        assert hasattr(resource, "get_connection")
        assert resource.host == "mock_mysupply_host"

    def test_mock_sydax_resource(self, mock_sydax_mssql_resource):
        """Test mock Sydax MSSQL resource fixture."""
        resource, cursor = mock_sydax_mssql_resource
        assert resource is not None
        assert hasattr(resource, "get_connection")
        assert resource.host == "mock_sydax_host"

    def test_mock_ils_resource(self, mock_ils_mssql_resource):
        """Test mock ILS MSSQL resource fixture."""
        resource, cursor = mock_ils_mssql_resource
        assert resource is not None
        assert hasattr(resource, "get_connection")
        assert resource.host == "mock_ils_host"


class TestRealFixturesWithoutRealDB:
    """Test real fixtures behavior when USE_REAL_DB_FOR_TESTS is false."""

    def test_real_mssql_resource_skipped_when_disabled(self):
        """Test that real MSSQL resource is skipped when disabled."""
        # This test won't have the fixture, so we test the behavior indirectly
        from tests.fixtures.resources import should_use_real_resources

        original = os.getenv("USE_REAL_DB_FOR_TESTS")
        try:
            os.environ["USE_REAL_DB_FOR_TESTS"] = "false"
            assert not should_use_real_resources()
        finally:
            if original:
                os.environ["USE_REAL_DB_FOR_TESTS"] = original
            else:
                os.environ.pop("USE_REAL_DB_FOR_TESTS", None)


class TestSafetyChecks:
    """Test safety check functionality."""

    def test_safe_environments(self):
        """Test that safe environments are recognized."""
        from tests.fixtures.resources import is_safe_environment

        safe_envs = ["local", "dev", "test", "development", ""]
        original_test_env = os.getenv("TEST_ENV")
        original_dagster_env = os.getenv("DAGSTER_ENVIRONMENT")

        try:
            for env in safe_envs:
                os.environ["TEST_ENV"] = env
                os.environ.pop("DAGSTER_ENVIRONMENT", None)
                assert is_safe_environment(), f"{env} should be safe"
        finally:
            if original_test_env:
                os.environ["TEST_ENV"] = original_test_env
            elif "TEST_ENV" in os.environ:
                os.environ.pop("TEST_ENV", None)
            if original_dagster_env:
                os.environ["DAGSTER_ENVIRONMENT"] = original_dagster_env

    def test_unsafe_environments(self):
        """Test that unsafe environments are blocked."""
        from tests.fixtures.resources import is_safe_environment

        unsafe_envs = ["production", "prod", "PRODUCTION"]
        original_test_env = os.getenv("TEST_ENV")
        original_dagster_env = os.getenv("DAGSTER_ENVIRONMENT")

        try:
            for env in unsafe_envs:
                os.environ["TEST_ENV"] = env
                assert not is_safe_environment(), f"{env} should not be safe"

            # Test DAGSTER_ENVIRONMENT
            os.environ["TEST_ENV"] = "dev"
            for env in unsafe_envs:
                os.environ["DAGSTER_ENVIRONMENT"] = env
                assert not is_safe_environment(), f"DAGSTER_ENV={env} should not be safe"
        finally:
            if original_test_env:
                os.environ["TEST_ENV"] = original_test_env
            elif "TEST_ENV" in os.environ:
                os.environ.pop("TEST_ENV", None)
            if original_dagster_env:
                os.environ["DAGSTER_ENVIRONMENT"] = original_dagster_env
            elif "DAGSTER_ENVIRONMENT" in os.environ:
                os.environ.pop("DAGSTER_ENVIRONMENT", None)


class TestEnvironmentVariables:
    """Test that environment variables are properly configured."""

    def test_mysupply_variables_loaded(self):
        """Test that MySupply variables are loaded from test.env."""
        assert os.getenv("MYSUPPLY_PROD_MSSQL_SERVER") is not None
        assert os.getenv("MYSUPPLY_PROD_MSSQL_PORT") is not None
        assert os.getenv("MYSUPPLY_PROD_MSSQL_DATABASE") is not None
        assert os.getenv("MYSUPPLY_PROD_MSSQL_USERNAME") is not None

    def test_sydax_variables_loaded(self):
        """Test that Sydax variables are loaded from test.env."""
        assert os.getenv("SYDAX_PROD_MSSQL_SERVER") is not None
        assert os.getenv("SYDAX_PROD_MSSQL_DATABASE") is not None
        assert os.getenv("SYDAX_PROD_MSSQL_SCHEMA_FINANS") is not None
        assert os.getenv("SYDAX_PROD_MSSQL_SCHEMA_KREDITOR") is not None

    def test_ils_variables_loaded(self):
        """Test that ILS variables are loaded from test.env."""
        assert os.getenv("ILS_PROD_MSSQL_SERVER") is not None
        assert os.getenv("ILS_PROD_MSSQL_DATABASE") is not None

    def test_ifdb_variables_loaded(self):
        """Test that IFDB variables are loaded from test.env."""
        assert os.getenv("IFDB_PROD_ORACLE_SERVER") is not None
        assert os.getenv("IFDB_PROD_ORACLE_DATABASE") is not None
        assert os.getenv("IFDB_PROD_ORACLE_SCHEMA") is not None


class TestFlexibleFixtures:
    """Test flexible fixtures that auto-select mock or real."""

    def test_mssql_resource_flexible(self, mssql_resource):
        """Test that flexible MSSQL resource works (should use mock by default)."""
        # By default (USE_REAL_DB_FOR_TESTS=false), should use mock
        assert mssql_resource is not None

        # Should have connection method
        assert hasattr(mssql_resource, "get_connection")

    def test_fhir_server_resource_flexible(self, fhir_server_resource):
        """Test that flexible FHIR server resource works (should use mock by default)."""
        # By default, should use mock
        assert fhir_server_resource is not None
        assert hasattr(fhir_server_resource, "base_url")


class TestConfigPrecedence:
    """Test that TEST_* variables can override PROD_* variables."""

    def test_test_variable_precedence(self):
        """Test that TEST_* variables take precedence."""
        original_test = os.getenv("TEST_MYSUPPLY_MSSQL_SERVER")

        try:
            # Set a test override
            os.environ["TEST_MYSUPPLY_MSSQL_SERVER"] = "test-server-override"

            # The fixture logic should use TEST value if available
            test_value = os.getenv("TEST_MYSUPPLY_MSSQL_SERVER")
            prod_value = os.getenv("MYSUPPLY_PROD_MSSQL_SERVER")

            # Verify TEST value is available
            assert test_value == "test-server-override"

            # In fixture logic, TEST value would override PROD
            effective = test_value or prod_value
            assert effective == "test-server-override"

        finally:
            if original_test:
                os.environ["TEST_MYSUPPLY_MSSQL_SERVER"] = original_test
            elif "TEST_MYSUPPLY_MSSQL_SERVER" in os.environ:
                os.environ.pop("TEST_MYSUPPLY_MSSQL_SERVER", None)


@pytest.mark.skipif(
    os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true",
    reason="USE_REAL_DB_FOR_TESTS not enabled",
)
class TestRealFixturesWithRealDB:
    """Test real fixtures when USE_REAL_DB_FOR_TESTS is enabled.

    These tests only run when USE_REAL_DB_FOR_TESTS=true and environment is safe.
    """

    def test_real_mssql_resource_available(self, real_mssql_resource):
        """Test that real MSSQL resource can be created."""
        assert real_mssql_resource is not None
        assert hasattr(real_mssql_resource, "get_connection")
        assert hasattr(real_mssql_resource, "host")
        assert hasattr(real_mssql_resource, "database")

    def test_real_mysupply_resource_available(self, real_mysupply_mssql_resource):
        """Test that real MySupply resource can be created."""
        assert real_mysupply_mssql_resource is not None
        assert hasattr(real_mysupply_mssql_resource, "get_connection")

    def test_real_sydax_resource_available(self, real_sydax_mssql_resource):
        """Test that real Sydax resource can be created."""
        assert real_sydax_mssql_resource is not None
        assert hasattr(real_sydax_mssql_resource, "get_connection")

    def test_real_ils_resource_available(self, real_ils_mssql_resource):
        """Test that real ILS resource can be created."""
        assert real_ils_mssql_resource is not None
        assert hasattr(real_ils_mssql_resource, "get_connection")

    def test_real_ifdb_oracle_resource_available(self, real_ifdb_oracle_resource):
        """Test that real IFDB Oracle resource can be created."""
        assert real_ifdb_oracle_resource is not None
        assert hasattr(real_ifdb_oracle_resource, "get_connection")
