# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the environment gate on dev-only assets.

DSA.SQL.dev registers destructive reset assets (truncate/delete of faktura
fct/map/dim tables). The gate must keep them out of any deployment whose
DAGSTER_ENVIRONMENT contains 'prod' while keeping them available in dev.
"""

import importlib

import DSA.SQL.dev as dev_module
import pytest
from dagster import AssetKey

RESET_ASSET_KEY = AssetKey(["erp_faktura_reset_tables"])


@pytest.fixture
def reload_dev_with_env(monkeypatch):
    """Reload DSA.SQL.dev under a given DAGSTER_ENVIRONMENT, then restore."""

    def _reload(env_value: str):
        monkeypatch.setenv("DAGSTER_ENVIRONMENT", env_value)
        return importlib.reload(dev_module)

    yield _reload
    monkeypatch.undo()
    importlib.reload(dev_module)


@pytest.mark.unit
@pytest.mark.parametrize("env_value", ["prod", "production", "PROD"])
def test_reset_asset_absent_in_prod(reload_dev_with_env, env_value):
    module = reload_dev_with_env(env_value)
    keys = {key for asset in module.all_assets for key in asset.keys}
    assert RESET_ASSET_KEY not in keys


@pytest.mark.unit
def test_reset_asset_present_in_dev(reload_dev_with_env):
    module = reload_dev_with_env("dev")
    keys = {key for asset in module.all_assets for key in asset.keys}
    assert RESET_ASSET_KEY in keys
