# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Test script to verify different test setups and configurations.

This script tests:
1. Environment variable loading from test.env
2. Mock resource fixtures
3. Real resource fixtures (when enabled)
4. Safety checks
5. Configuration precedence
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from tests.fixtures.resources import (
    is_safe_environment,
    should_use_real_resources,
)


def test_env_file_loading():
    """Test that test.env file is loaded correctly."""
    print("\n=== Testing Environment Variable Loading ===")

    # Load test.env manually to test
    test_env_file = project_root / "tests" / "config" / "test.env"
    if test_env_file.exists():
        load_dotenv(test_env_file, override=True)
        print(f"✓ test.env file exists at: {test_env_file}")

        # Check critical variables
        critical_vars = [
            "USE_REAL_DB_FOR_TESTS",
            "TEST_ENV",
            "MYSUPPLY_PROD_MSSQL_SERVER",
            "SYDAX_PROD_MSSQL_SERVER",
            "ILS_PROD_MSSQL_SERVER",
            "IFDB_PROD_ORACLE_SERVER",
        ]

        print("\nChecking critical environment variables:")
        for var in critical_vars:
            value = os.getenv(var)
            if value:
                # Mask sensitive values
                if "PASSWORD" in var:
                    display_value = "***" if value else "NOT SET"
                else:
                    display_value = value
                print(f"  ✓ {var} = {display_value}")
            else:
                print(f"  ✗ {var} = NOT SET")
    else:
        print(f"✗ test.env file not found at: {test_env_file}")


def test_configuration_precedence():
    """Test that TEST_* variables take precedence over PROD_* variables."""
    print("\n=== Testing Configuration Precedence ===")

    # Test that TEST variables can override PROD variables
    original_prod = os.getenv("MYSUPPLY_PROD_MSSQL_SERVER")
    original_test = os.getenv("TEST_MYSUPPLY_MSSQL_SERVER")

    # Set test variable
    os.environ["TEST_MYSUPPLY_MSSQL_SERVER"] = "test-override-server"

    # Simulate what the fixture does
    test_value = os.getenv("TEST_MYSUPPLY_MSSQL_SERVER")
    prod_value = os.getenv("MYSUPPLY_PROD_MSSQL_SERVER")

    if test_value:
        effective_value = test_value
        print(f"✓ TEST variable found: {test_value}")
        print(f"  Effective value: {effective_value}")
    else:
        effective_value = prod_value or "default"
        print(f"✓ Using PROD variable: {effective_value}")

    # Cleanup
    if original_test:
        os.environ["TEST_MYSUPPLY_MSSQL_SERVER"] = original_test
    elif "TEST_MYSUPPLY_MSSQL_SERVER" in os.environ:
        del os.environ["TEST_MYSUPPLY_MSSQL_SERVER"]


def test_safety_checks():
    """Test safety checks for real resource usage."""
    print("\n=== Testing Safety Checks ===")

    # Test 1: Normal safe environment (local)
    original_test_env = os.getenv("TEST_ENV")
    original_dagster_env = os.getenv("DAGSTER_ENVIRONMENT")

    # Set local environment
    os.environ["TEST_ENV"] = "local"
    if "DAGSTER_ENVIRONMENT" in os.environ:
        os.environ.pop("DAGSTER_ENVIRONMENT")

    is_safe = is_safe_environment()
    print(f"✓ TEST_ENV=local (no DAGSTER_ENV) -> safe={is_safe} (expected: True)")
    assert is_safe, "local environment should be safe"

    # Test 1b: With DAGSTER_ENVIRONMENT=development
    os.environ["DAGSTER_ENVIRONMENT"] = "development"
    is_safe = is_safe_environment()
    print(f"✓ TEST_ENV=local, DAGSTER_ENV=development -> safe={is_safe} (expected: True)")
    assert is_safe, "local + development should be safe"

    # Test 2: Production environment should fail
    os.environ["TEST_ENV"] = "production"
    is_safe = is_safe_environment()
    print(f"✓ TEST_ENV=production -> safe={is_safe} (expected: False)")
    assert not is_safe, "production environment should not be safe"

    # Test 3: DAGSTER_ENVIRONMENT production should fail
    os.environ["TEST_ENV"] = "dev"
    os.environ["DAGSTER_ENVIRONMENT"] = "production"
    is_safe = is_safe_environment()
    print(f"✓ DAGSTER_ENVIRONMENT=production -> safe={is_safe} (expected: False)")
    assert not is_safe, "production DAGSTER_ENVIRONMENT should not be safe"

    # Restore
    if original_test_env:
        os.environ["TEST_ENV"] = original_test_env
    else:
        os.environ.pop("TEST_ENV", None)
    if original_dagster_env:
        os.environ["DAGSTER_ENVIRONMENT"] = original_dagster_env
    elif "DAGSTER_ENVIRONMENT" in os.environ:
        del os.environ["DAGSTER_ENVIRONMENT"]


def test_real_resources_flag():
    """Test USE_REAL_DB_FOR_TESTS flag behavior."""
    print("\n=== Testing USE_REAL_DB_FOR_TESTS Flag ===")

    original_flag = os.getenv("USE_REAL_DB_FOR_TESTS")

    # Test false
    os.environ["USE_REAL_DB_FOR_TESTS"] = "false"
    should_use = should_use_real_resources()
    print(f"✓ USE_REAL_DB_FOR_TESTS=false -> should_use={should_use} (expected: False)")
    assert not should_use, "false should not enable real resources"

    # Test true
    os.environ["USE_REAL_DB_FOR_TESTS"] = "true"
    should_use = should_use_real_resources()
    print(f"✓ USE_REAL_DB_FOR_TESTS=true -> should_use={should_use} (expected: True)")
    assert should_use, "true should enable real resources"

    # Test True (capitalized)
    os.environ["USE_REAL_DB_FOR_TESTS"] = "True"
    should_use = should_use_real_resources()
    print(f"✓ USE_REAL_DB_FOR_TESTS=True -> should_use={should_use} (expected: True)")
    assert should_use, "True should enable real resources"

    # Restore
    if original_flag:
        os.environ["USE_REAL_DB_FOR_TESTS"] = original_flag
    else:
        os.environ.pop("USE_REAL_DB_FOR_TESTS", None)


def test_database_configurations():
    """Test that all database configurations are available."""
    print("\n=== Testing Database Configurations ===")

    databases = {
        "MYSUPPLY": [
            "MYSUPPLY_PROD_MSSQL_SERVER",
            "MYSUPPLY_PROD_MSSQL_PORT",
            "MYSUPPLY_PROD_MSSQL_DATABASE",
            "MYSUPPLY_PROD_MSSQL_USERNAME",
            "MYSUPPLY_PROD_MSSQL_SCHEMA",
        ],
        "SYDAX": [
            "SYDAX_PROD_MSSQL_SERVER",
            "SYDAX_PROD_MSSQL_PORT",
            "SYDAX_PROD_MSSQL_DATABASE",
            "SYDAX_PROD_MSSQL_USERNAME",
            "SYDAX_PROD_MSSQL_SCHEMA_FINANS",
            "SYDAX_PROD_MSSQL_SCHEMA_KREDITOR",
        ],
        "ILS": [
            "ILS_PROD_MSSQL_SERVER",
            "ILS_PROD_MSSQL_PORT",
            "ILS_PROD_MSSQL_DATABASE",
            "ILS_PROD_MSSQL_USERNAME",
            "ILS_PROD_MSSQL_SCHEMA",
        ],
        "IFDB": [
            "IFDB_PROD_ORACLE_SERVER",
            "IFDB_PROD_ORACLE_PORT",
            "IFDB_PROD_ORACLE_DATABASE",
            "IFDB_PROD_ORACLE_USERNAME",
            "IFDB_PROD_ORACLE_SCHEMA",
        ],
    }

    for db_name, vars_list in databases.items():
        print(f"\nChecking {db_name} configuration:")
        all_present = True
        for var in vars_list:
            value = os.getenv(var)
            if value:
                # Mask passwords
                if "PASSWORD" in var or "PASSWORD" in var:
                    display = "***"
                else:
                    display = value
                print(f"  ✓ {var} = {display}")
            else:
                print(f"  ✗ {var} = NOT SET")
                all_present = False

        if all_present:
            print(f"  ✓ All {db_name} variables present")
        else:
            print(f"  ✗ Some {db_name} variables missing")


def test_mock_fixtures():
    """Test that mock fixtures can be created."""
    print("\n=== Testing Mock Fixtures ===")

    try:
        from tests.fixtures.resources import (
            mock_fhir_server_resource,
            mock_ils_mssql_resource,
            mock_mssql_resource,
            mock_mysupply_mssql_resource,
            mock_oracle_resource,
            mock_sydax_mssql_resource,
        )

        print("✓ All mock fixture functions are importable:")
        print("  - mock_mssql_resource")
        print("  - mock_fhir_server_resource")
        print("  - mock_oracle_resource")
        print("  - mock_mysupply_mssql_resource")
        print("  - mock_sydax_mssql_resource")
        print("  - mock_ils_mssql_resource")

    except ImportError as e:
        print(f"✗ Failed to import mock fixtures: {e}")


def main():
    """Run all test setup verification tests."""
    print("=" * 60)
    print("TEST SETUP VERIFICATION")
    print("=" * 60)

    # Load test.env
    test_env_file = project_root / "tests" / "config" / "test.env"
    if test_env_file.exists():
        load_dotenv(test_env_file, override=True)

    try:
        test_env_file_loading()
        test_configuration_precedence()
        test_safety_checks()
        test_real_resources_flag()
        test_database_configurations()
        test_mock_fixtures()

        print("\n" + "=" * 60)
        print("✓ All verification tests completed")
        print("=" * 60)

    except Exception as e:
        print(f"\n✗ Error during verification: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
