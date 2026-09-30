# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for environment-aware schedule default status."""

import dagster as dg
import pytest
from schedules.schedule_default_status import (
    get_schedule_default_status,
    should_auto_enable_schedules,
)


def _set_or_clear_env(monkeypatch: pytest.MonkeyPatch, key: str, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv(key, raising=False)
        return
    monkeypatch.setenv(key, value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "location_name,branch_postfix,expected",
    [
        ("development", None, True),
        ("DeVelopMent", None, True),
        ("prod", None, True),
        ("production", None, True),
        ("development", "feature-jira-123", True),
        (None, "release-1-2-3", True),
        (None, "main-1-2-3", True),
        ("qa", "feature-jira-123", False),
        ("1-2-3", "", False),
        (None, None, False),
    ],
)
def test_should_auto_enable_schedules(
    monkeypatch: pytest.MonkeyPatch,
    location_name: str | None,
    branch_postfix: str | None,
    expected: bool,
) -> None:
    _set_or_clear_env(monkeypatch, "DAGSTER_CODE_LOCATION_NAME", location_name)
    _set_or_clear_env(monkeypatch, "DAGSTER_BRANCH_POSTFIX", branch_postfix)

    assert should_auto_enable_schedules() is expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "location_name,branch_postfix,expected",
    [
        ("development", None, dg.DefaultScheduleStatus.RUNNING),
        (None, "release-1-2-3", dg.DefaultScheduleStatus.RUNNING),
        (None, "feature-jira-123", dg.DefaultScheduleStatus.STOPPED),
    ],
)
def test_get_schedule_default_status(
    monkeypatch: pytest.MonkeyPatch,
    location_name: str | None,
    branch_postfix: str | None,
    expected: dg.DefaultScheduleStatus,
) -> None:
    _set_or_clear_env(monkeypatch, "DAGSTER_CODE_LOCATION_NAME", location_name)
    _set_or_clear_env(monkeypatch, "DAGSTER_BRANCH_POSTFIX", branch_postfix)

    assert get_schedule_default_status() == expected
