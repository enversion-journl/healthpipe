# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the sensor that starts the OS2mo delivery."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.branch_postfix import postfix_assets
from DSO.export.os2mo.codesystem_assets import os2mo_export_assets
from sensors.os2mo_delivery_sensor import delivery_run_config, evaluate

pytestmark = pytest.mark.unit


def _context(run_id="run-1"):
    context = MagicMock()
    context.dagster_run.run_id = run_id
    return context


def test_a_deployment_without_os2mo_skips(monkeypatch):
    monkeypatch.setenv("OS2MO_ENABLED", "false")
    result = evaluate(_context(), delivery_job_name="os2mo_codesystem_export_job")
    assert isinstance(result, dg.SkipReason)


def test_an_unset_flag_also_skips(monkeypatch):
    monkeypatch.delenv("OS2MO_ENABLED", raising=False)
    result = evaluate(_context(), delivery_job_name="os2mo_codesystem_export_job")
    assert isinstance(result, dg.SkipReason)


def test_an_enabled_deployment_requests_the_delivery(monkeypatch):
    monkeypatch.setenv("OS2MO_ENABLED", "true")
    result = evaluate(_context("run-42"), delivery_job_name="os2mo_codesystem_export_job")

    assert isinstance(result, dg.RunRequest)
    assert result.job_name == "os2mo_codesystem_export_job"
    # One delivery per codesystem rebuild.
    assert result.run_key == "os2mo_delivery:run-42"
    assert all(op["config"]["dry_run"] is False for op in result.run_config["ops"].values())


def test_the_run_config_names_the_ops_a_feature_branch_has(monkeypatch):
    """The branch postfix renames asset keys, not ops, so the bare names must stay."""
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-ab29666-os2mo")
    op_names = {asset.node_def.name for asset in postfix_assets(list(os2mo_export_assets))}

    assert set(delivery_run_config()["ops"]) <= op_names
