# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for infrastructure/security/security_seed.py."""

from __future__ import annotations

import pytest
from infrastructure.security import security_seed
from infrastructure.security.security_seed import (
    SeedCollisionError,
    SeedInProdError,
    assert_no_collision,
    rolle_domaene_rows,
    seed_enabled,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _non_prod_environment(monkeypatch):
    monkeypatch.delenv(security_seed.SEED_ENABLED_ENV, raising=False)
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "dev")
    monkeypatch.setenv("MSSQL_TARGET_STAGING_HOST", "mssql")
    monkeypatch.setenv("MSSQL_TARGET_DSO_HOST", "mssql")


def test_seed_disabled_when_unset():
    assert seed_enabled() is False


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "ja"])
def test_seed_enabled_for_true_values(monkeypatch, value):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, value)
    assert seed_enabled() is True


def test_seed_rejects_unknown_value(monkeypatch):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "ture")
    with pytest.raises(ValueError, match="ugyldig"):
        seed_enabled()


@pytest.mark.parametrize(
    ("var", "value"),
    [("DAGSTER_ENVIRONMENT", "prod"), ("MSSQL_TARGET_STAGING_HOST", "sql-prod-01")],
)
def test_seed_refused_in_prod_like_environment(monkeypatch, var, value):
    monkeypatch.setenv(security_seed.SEED_ENABLED_ENV, "true")
    monkeypatch.setenv(var, value)
    with pytest.raises(SeedInProdError):
        seed_enabled()


def test_rolle_domaene_rows_add_ad_only_domain_without_duplicating_known_domain():
    rows = rolle_domaene_rows(["dpa_medicin", "DPA_Nyt"])

    population_domains = [d for b, d, _, _ in rows if b == "testbruger_population"]
    assert "DPA_Nyt" in population_domains
    assert [d.lower() for d in population_domains].count("dpa_medicin") == 1
    assert len(population_domains) == len(security_seed.SIKKERHEDSDOMAENER) + 1


def test_rolle_domaene_rows_population_user_has_p_and_slb_in_every_domain():
    rows = rolle_domaene_rows([])
    population = {(r, p) for b, _, r, p in rows if b == "testbruger_population"}
    assert population == {("P", "SLB")}


def test_assert_no_collision_is_case_insensitive():
    with pytest.raises(SeedCollisionError, match="testbruger_u"):
        assert_no_collision(["ab1", "TESTBRUGER_U"])


def test_assert_no_collision_passes_for_real_users():
    assert_no_collision(["ab1", "esti5m"])
