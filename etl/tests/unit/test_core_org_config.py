# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for core.org_config.

org_config reads os.getenv at import time, so each test reloads the module after
adjusting the environment. The autouse fixture restores the module to its
ambient-environment state around every test -- its teardown runs *after* monkeypatch
has already undone any env changes (monkeypatch is set up after the autouse fixture,
so it tears down first), guaranteeing reloaded overrides never leak across tests.
"""

import importlib

import core.org_config as org_config
import pytest

pytestmark = pytest.mark.unit

# Env vars this module externalizes.
_OVERRIDE_KEYS = ("DATA_DIR", "FHIR_PUBLISHER")


@pytest.fixture(autouse=True)
def _restore_org_config():
    importlib.reload(org_config)
    yield
    importlib.reload(org_config)


def test_defaults_are_locked(monkeypatch):
    for key in _OVERRIDE_KEYS:
        monkeypatch.delenv(key, raising=False)
    cfg = importlib.reload(org_config)

    assert cfg.DATA_DIR == "/workspaces/healthpipe/data"
    assert cfg.FHIR_PUBLISHER == "Your Organization"


def test_data_dir_env_override(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "/tmp/data")
    cfg = importlib.reload(org_config)

    assert cfg.DATA_DIR == "/tmp/data"


def test_fhir_publisher_env_override(monkeypatch):
    # The published CodeSystem publisher must follow the deployer's override so the
    # platform is not bound to one organization.
    monkeypatch.setenv("FHIR_PUBLISHER", "Acme Health")
    cfg = importlib.reload(org_config)

    assert cfg.FHIR_PUBLISHER == "Acme Health"


def test_fhir_contract_urls_not_externalized(monkeypatch):
    for key in _OVERRIDE_KEYS:
        monkeypatch.delenv(key, raising=False)
    cfg = importlib.reload(org_config)

    assert not hasattr(cfg, "FHIR_EXTENSION_BASE_URL")
    assert not hasattr(cfg, "CODESYSTEM_BASE_URL")
