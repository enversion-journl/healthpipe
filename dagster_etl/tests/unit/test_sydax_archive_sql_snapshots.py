# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Snapshot tests for SydAX archive MERGE SQL.

For each ArchiveMergeConfig, generate the MERGE SQL via
build_archive_merge_sql() and compare to a checked-in expected fixture.
This catches accidental config changes that would alter production SQL.

To update fixtures after an intentional change:
    UPDATE_SYDAX_SNAPSHOTS=1 pytest tests/unit/test_sydax_archive_sql_snapshots.py

(Note: UPDATE_SYDAX_SNAPSHOTS=1 is an env-var handled by the test itself —
when set, each test writes its fixture and calls pytest.skip().)
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()

import pytest
from core.archive_merge import build_archive_merge_sql
from tests.unit.test_sydax_archive_configs import ALL_CONFIGS

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "sydax_archive_sql"
UPDATE_SNAPSHOTS = os.getenv("UPDATE_SYDAX_SNAPSHOTS") == "1"


@pytest.mark.parametrize("cfg", ALL_CONFIGS, ids=lambda c: c.target_table)
def test_merge_sql_matches_fixture(cfg):
    fixture_path = FIXTURE_DIR / f"{cfg.target_table}.sql"
    actual = build_archive_merge_sql(cfg)

    if UPDATE_SNAPSHOTS:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(actual, encoding="utf-8")
        pytest.skip(f"Updated snapshot for {cfg.target_table}")

    assert fixture_path.exists(), (
        f"Missing fixture {fixture_path}. Run with UPDATE_SYDAX_SNAPSHOTS=1 " f"to generate it."
    )
    expected = fixture_path.read_text(encoding="utf-8")
    assert actual == expected, (
        f"SQL for {cfg.target_table} drifted from fixture. "
        f"If intentional, regenerate with UPDATE_SYDAX_SNAPSHOTS=1."
    )
