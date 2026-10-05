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
