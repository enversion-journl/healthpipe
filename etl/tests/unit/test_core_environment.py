# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for core/environment.py — the shared prod guard."""

from __future__ import annotations

import pytest
from core.environment import is_prod_environment, prod_environment_reason

pytestmark = pytest.mark.unit

_ALL_VARS = ("DAGSTER_ENVIRONMENT", "MSSQL_TARGET_STAGING_HOST", "MSSQL_TARGET_DSO_HOST")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _ALL_VARS:
        monkeypatch.delenv(var, raising=False)


def test_not_prod_when_nothing_set():
    assert is_prod_environment() is False
    assert prod_environment_reason() == "not prod"


@pytest.mark.parametrize("value", ["prod", "PROD", " production ", "sydbi-prod"])
def test_prod_detected_from_environment_name(monkeypatch, value):
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", value)
    assert is_prod_environment() is True


def test_prod_detected_from_staging_host(monkeypatch):
    """The host check must not be lost when merging the two copies."""
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "dev")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_HOST", "mssql-prod.rsyd.net")
    assert is_prod_environment() is True
    assert "MSSQL_TARGET_STAGING_HOST" in prod_environment_reason()


def test_prod_detected_from_dso_host(monkeypatch):
    monkeypatch.setenv("MSSQL_TARGET_DSO_HOST", "npdbaprod01.rsyd.net")
    assert is_prod_environment() is True


def test_dev_environment_with_dev_hosts_is_not_prod(monkeypatch):
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "dev")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_HOST", "mssql-staging-dev")
    monkeypatch.setenv("MSSQL_TARGET_DSO_HOST", "mssql-dso-dev")
    assert is_prod_environment() is False


def test_reason_prefers_environment_name_over_host(monkeypatch):
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "prod")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_HOST", "mssql-prod")
    assert "DAGSTER_ENVIRONMENT" in prod_environment_reason()
