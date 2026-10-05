# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for cadence-specific IFDB job concurrency defaults."""

import pytest
from jobs import fhir_ifdb_jobs


@pytest.mark.unit
def test_ifdb_monthly_default_is_lower_than_daily_when_env_is_unset(monkeypatch):
    monkeypatch.delenv("FHIR_IFDB_MAX_CONCURRENT", raising=False)
    monkeypatch.delenv("FHIR_IFDB_DAILY_MAX_CONCURRENT", raising=False)
    monkeypatch.delenv("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", raising=False)

    assert fhir_ifdb_jobs._max_concurrent("FHIR_IFDB_DAILY_MAX_CONCURRENT", 4) == 4
    assert fhir_ifdb_jobs._max_concurrent("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", 2) == 2


@pytest.mark.unit
def test_ifdb_concurrency_uses_global_fallback(monkeypatch):
    monkeypatch.setenv("FHIR_IFDB_MAX_CONCURRENT", "3")
    monkeypatch.delenv("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", raising=False)

    assert fhir_ifdb_jobs._max_concurrent("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", 2) == 3


@pytest.mark.unit
def test_ifdb_concurrency_prefers_cadence_specific_env(monkeypatch):
    monkeypatch.setenv("FHIR_IFDB_MAX_CONCURRENT", "3")
    monkeypatch.setenv("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", "1")

    assert fhir_ifdb_jobs._max_concurrent("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", 2) == 1
