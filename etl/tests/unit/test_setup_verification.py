# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the test-environment safety gate and real-resource opt-in flag."""

import os

import pytest
from tests.fixtures.resources import (
    is_safe_environment,
    should_use_real_resources,
)

pytestmark = pytest.mark.unit


def test_safety_checks():
    """is_safe_environment must reject prod in TEST_ENV or DAGSTER_ENVIRONMENT."""
    original_test_env = os.getenv("TEST_ENV")
    original_dagster_env = os.getenv("DAGSTER_ENVIRONMENT")

    try:
        os.environ["TEST_ENV"] = "local"
        os.environ.pop("DAGSTER_ENVIRONMENT", None)
        assert is_safe_environment(), "local environment should be safe"

        os.environ["DAGSTER_ENVIRONMENT"] = "development"
        assert is_safe_environment(), "local + development should be safe"

        os.environ["TEST_ENV"] = "production"
        assert not is_safe_environment(), "production environment should not be safe"

        os.environ["TEST_ENV"] = "dev"
        os.environ["DAGSTER_ENVIRONMENT"] = "production"
        assert not is_safe_environment(), "production DAGSTER_ENVIRONMENT should not be safe"
    finally:
        if original_test_env is not None:
            os.environ["TEST_ENV"] = original_test_env
        else:
            os.environ.pop("TEST_ENV", None)
        if original_dagster_env is not None:
            os.environ["DAGSTER_ENVIRONMENT"] = original_dagster_env
        else:
            os.environ.pop("DAGSTER_ENVIRONMENT", None)


def test_real_resources_flag():
    """should_use_real_resources must honor USE_REAL_DB_FOR_TESTS case-insensitively."""
    original_flag = os.getenv("USE_REAL_DB_FOR_TESTS")

    try:
        os.environ["USE_REAL_DB_FOR_TESTS"] = "false"
        assert not should_use_real_resources(), "false should not enable real resources"

        os.environ["USE_REAL_DB_FOR_TESTS"] = "true"
        assert should_use_real_resources(), "true should enable real resources"

        os.environ["USE_REAL_DB_FOR_TESTS"] = "True"
        assert should_use_real_resources(), "True should enable real resources"
    finally:
        if original_flag is not None:
            os.environ["USE_REAL_DB_FOR_TESTS"] = original_flag
        else:
            os.environ.pop("USE_REAL_DB_FOR_TESTS", None)
