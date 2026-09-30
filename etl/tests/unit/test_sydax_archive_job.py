# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for sydax_archive job + asset registration."""

import sys
from unittest.mock import MagicMock

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()

import pytest
from DSI.mssql.archive import sydax_archive_assets
from jobs.sydax_archive_job import sydax_archive_job

pytestmark = pytest.mark.unit


def test_sydax_archive_assets_has_21():
    # 17 SydAX base tables + 4 budget tables (budgetpost, budgetmodel,
    # budgetpoststatus, budgetbeloebtype) migrated to DFM/archive in #28688.
    assert len(sydax_archive_assets) == 21


def test_all_assets_are_in_group():
    for asset in sydax_archive_assets:
        # Each asset is a Dagster AssetsDefinition; group_name comes from the
        # decorator. Inspect via the assets_def metadata.
        group_names = {spec.group_name for spec in asset.specs}
        assert group_names == {"sydax_archive"}, asset.key


def test_job_selects_sydax_archive_group():
    # Lightweight check that the job exists and is named correctly.
    assert sydax_archive_job.name == "sydax_archive_job"
