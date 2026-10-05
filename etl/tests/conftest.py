# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pytest configuration and shared fixtures.
"""

import logging
import os
import sys
from pathlib import Path

# Add project root to Python path (packages are directly in etl/)
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
from dotenv import load_dotenv


def load_test_environment():
    """
    Load test environment variables from test.env file.
    This function can be called to reload environment variables.
    """
    test_env_file = project_root / "tests" / "config" / "test.env"
    if test_env_file.exists():
        # Use override=True to force reload of environment variables
        load_dotenv(test_env_file, override=True)
        # Handle variable expansion (dotenv doesn't do this by default)
        # Expand ${VAR} references in loaded variables
        import re

        env_vars = dict(os.environ)
        for key, value in list(os.environ.items()):
            if isinstance(value, str) and "${" in value:
                # Expand variables like ${VAR} using os.environ
                expanded = re.sub(r"\$\{([^}]+)\}", lambda m: env_vars.get(m.group(1), ""), value)
                os.environ[key] = expanded
    else:
        # Fallback to root .env if test.env doesn't exist
        env_file = project_root / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=True)


# Load test environment variables on import
load_test_environment()

# Import fixtures (pytest will find them automatically)
# Note: Fixtures are auto-discovered, but we import here for IDE support
from tests.fixtures.dagster_instance import (  # noqa: F401
    dagster_instance,
    test_dagster_home,
)
from tests.fixtures.resources import *  # noqa: F401, F403
from tests.fixtures.resources import is_safe_environment

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8s] %(name)s: %(message)s",
)


@pytest.fixture
def test_partition():
    """Test partition key for partitioned assets."""
    return "2025-10-28"


@pytest.fixture
def test_config():
    """Test configuration loaded from environment."""
    return {
        "use_real_resources": os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true",
        "test_env": os.getenv("TEST_ENV", "local"),
        "enable_writes": os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true",
    }


@pytest.fixture(autouse=True)
def setup_test_environment(test_config):
    """
    Auto-setup test environment before each test.
    Ensures safety checks for real resource usage.
    Reloads environment variables to pick up changes.
    """
    # Reload environment variables before each test (helps with VS Code caching)
    load_test_environment()

    if test_config["use_real_resources"] and not is_safe_environment():
        pytest.fail(
            "Cannot use real resources: TEST_ENV/DAGSTER_ENVIRONMENT indicates an unsafe "
            "(prod) environment!"
        )

    yield

    # Cleanup if needed
    pass


@pytest.fixture(autouse=True)
def cleanup_fhir_resources(request):
    """
    Function-scoped fixture that cleans up FHIR resources before integration tests run.
    Only runs when real FHIR resources are enabled and for FHIR IFDB integration tests.
    For episode_of_care_test marked tests, only cleans up EpisodeOfCare resources.
    For other tests, cleans up all resources.
    Skips cleanup for performance tests to preserve test data.
    """
    # Check if this test is in the FHIR IFDB directory
    test_file_path = str(request.node.fspath)
    if "fhir_ifdb" not in test_file_path:
        yield
        return

    # Skip cleanup for performance test directory
    if "performance" in test_file_path:
        logger = logging.getLogger(__name__)
        logger.info("🧹 FHIR cleanup skipped: performance test directory")
        yield
        return

    # Check if real resources are enabled
    use_real_resources = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"

    if not use_real_resources:
        logger = logging.getLogger(__name__)
        logger.info("🧹 FHIR cleanup skipped: real resources not enabled")
        yield
        return

    # Import here to avoid circular imports
    from core.resources import FHIRServerResource

    # Create the real FHIR server resource directly
    try:
        # Get FHIR server configuration from environment
        base_url = os.getenv("FHIR_SERVER_BASE_URL", "")
        username = os.getenv("FHIR_SERVER_USERNAME")
        password = os.getenv("FHIR_SERVER_PASSWORD")
        use_fhir_api = os.getenv("USE_FHIR_API")

        if not base_url:
            logger = logging.getLogger(__name__)
            logger.warning("❌ FHIR cleanup skipped: FHIR_SERVER_BASE_URL not configured")
            yield
            return

        fhir_server = FHIRServerResource(
            base_url=base_url,
            client_username=username,
            client_password=password,
            use_fhir_api=use_fhir_api,
        )

        logger = logging.getLogger(__name__)

        # Skip cleanup for performance tests (to test with existing data)
        if hasattr(request, "node") and request.node.get_closest_marker("performance"):
            logger.info("🧹 FHIR cleanup skipped: performance test (testing with existing data)")
            yield
            return

        # Mapping from test markers to FHIR resource types for selective cleanup
        marker_to_resources = {
            "patient_test": ["Patient"],
            "organization_test": ["Organization"],
            "location_test": ["Location"],
            "episode_of_care_test": ["EpisodeOfCare"],
            "encounter_test": ["Encounter"],
            "observation_test": ["Observation"],
            "structure_definition_test": ["StructureDefinition"],
        }

        # Detect which resource types this test is for
        resource_types_to_clean = []
        if hasattr(request, "node"):
            for marker_name, resources in marker_to_resources.items():
                if request.node.get_closest_marker(marker_name):
                    resource_types_to_clean.extend(resources)
                    break  # Only clean resources for the first matching marker

        # If no specific marker found, clean all (fallback for non-integration tests)
        if not resource_types_to_clean:
            logger.info("🧹 Starting full FHIR server cleanup for non-integration tests...")
            resource_types_to_clean = [
                "Encounter",
                "EpisodeOfCare",
                "Patient",
                "Location",
                "Organization",
                "Observation",
                "StructureDefinition",
            ]
        else:
            resource_list = ", ".join(resource_types_to_clean)
            logger.info(f"🧹 Starting targeted FHIR server cleanup for {resource_list}...")

        resource_types = resource_types_to_clean

        total_deleted = 0

        for resource_type in resource_types:
            try:
                logger.info(f"  📋 Fetching all {resource_type} resources...")
                resources = fhir_server.get_all_resources(resource_type)

                if resources:
                    logger.info(f"  🗑️ Deleting {len(resources)} {resource_type} resources...")
                    # Collect resource IDs for bulk deletion
                    resource_ids = [
                        resource.get("id") for resource in resources if resource.get("id")
                    ]
                    if resource_ids:
                        try:
                            # Use bulk delete for better performance
                            responses = fhir_server.delete_resource_batches(
                                resource_ids=resource_ids,
                                resource_type=resource_type,
                                chunk_size=1000,  # Process in chunks of 1000
                            )
                            total_deleted += len(resource_ids)
                            logger.info(
                                f"  ✅ Successfully submitted {len(resource_ids)} "
                                f"{resource_type} resources for deletion in "
                                f"{len(responses)} chunks"
                            )
                        except Exception as e:
                            logger.warning(f"    Bulk delete failed for {resource_type}: {e}")
                            # Fallback to individual deletes if bulk fails
                            logger.info(
                                f"    Falling back to individual deletes for {resource_type}"
                            )
                            for resource_id in resource_ids:
                                try:
                                    fhir_server.delete_resource(resource_type, resource_id)
                                    total_deleted += 1
                                except Exception:
                                    msg = f"Delete failed: {resource_type}/{resource_id}"
                                    logger.warning(f"    {msg}")
                    logger.info(f"  ✅ Processed {len(resources)} {resource_type} resources")
                else:
                    logger.info(f"  ℹ️ No {resource_type} resources found")

            except Exception as e:
                logger.warning(f"  ❌ Failed to clean up {resource_type} resources: {e}")

        if total_deleted > 0:
            logger.info(f"🧹 FHIR cleanup complete: deleted {total_deleted} total resources")
        else:
            logger.info("🧹 FHIR cleanup complete: no resources to delete")

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.warning(f"❌ FHIR cleanup failed: {e}")

    yield


@pytest.fixture
def materialize_context(dagster_instance, test_partition):
    """
    Context for asset materialization with test partition.
    """
    return {
        "instance": dagster_instance,
        "partition": test_partition,
    }


# Re-export fixtures for convenience
__all__ = [
    "dagster_instance",
    "test_dagster_home",
    "test_partition",
    "test_config",
    "mock_mssql_resource",
    "mock_fhir_server_resource",
    "mock_oracle_resource",
    "mock_mysupply_mssql_resource",
    "mock_sydax_mssql_resource",
    "mock_ils_mssql_resource",
    "real_mssql_resource",
    "real_fhir_server_resource",
    "real_ifdb_oracle_resource",
    "real_mysupply_mssql_resource",
    "real_sydax_mssql_resource",
    "real_ils_mssql_resource",
    "mssql_resource",
    "fhir_server_resource",
    "materialize_context",
]
