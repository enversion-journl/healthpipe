# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pytest configuration for performance tests.

Performance tests require:
1. Real database connections (USE_REAL_DB_FOR_TESTS=true)
2. Performance test data seeded in Oracle
3. Sufficient time to run (use -m "not slow" to skip in CI)
"""

import logging
import os

import pytest

logger = logging.getLogger(__name__)


def pytest_collection_modifyitems(config, items):
    """
    Automatically skip performance tests if real resources are not enabled.
    """
    skip_perf = pytest.mark.skip(reason="Performance tests require USE_REAL_DB_FOR_TESTS=true")

    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"

    for item in items:
        # Check if this is a performance test
        if "performance" in str(item.fspath):
            if not use_real:
                item.add_marker(skip_perf)


@pytest.fixture(scope="session")
def perf_test_config():
    """
    Performance test configuration from environment.
    """
    return {
        "max_rows": int(os.getenv("PERF_TEST_MAX_ROWS", "5000")),
        "oracle_host": os.getenv("IFDB_PROD_ORACLE_SERVER", "oracle"),
        "oracle_port": int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
        "oracle_service": os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
        "fhir_base_url": os.getenv("FHIR_SERVER_BASE_URL", "http://fhir-server:8888"),
    }


@pytest.fixture(autouse=True)
def skip_fhir_cleanup():
    """
    Disable FHIR cleanup for all performance tests.
    Performance tests need data to persist between runs.
    """
    # This fixture runs for all tests in this directory
    # The parent conftest.py checks for performance marker/directory
    yield


@pytest.fixture
def test_data_sizes():
    """Different data sizes to test with for memory profiling."""
    return [100, 500, 1000]  # Start small for testing
