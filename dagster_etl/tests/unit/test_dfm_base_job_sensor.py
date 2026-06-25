# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the DFM base-job sensors' pure helpers.

Focus on the logic that does not require a live Dagster run environment:
  - _resolve_business_date: anchors the partition to the monitored run's start
    time (Europe/Copenhagen), not the sensor's wall-clock now().
  - _failure_trigger_enabled: env-flag parsing for the opt-in failure trigger.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from sensors.dfm_base_job_sensor import (
    _failure_trigger_enabled,
    _resolve_business_date,
)


def _ctx_with_start_time(epoch_utc: float | None):
    """Build a sensor-context mock whose run record has the given start_time."""
    ctx = MagicMock()
    record = MagicMock()
    record.start_time = epoch_utc
    ctx.instance.get_run_record_by_id.return_value = record
    ctx.log = MagicMock()
    return ctx


# ===========================================================================
# _resolve_business_date
# ===========================================================================


class TestResolveBusinessDate:
    def test_uses_run_start_time_not_now(self):
        # Base run started 2025-06-13 06:00 CET (= 04:00 UTC on 2025-06-13).
        start = datetime(2025, 6, 13, 4, 0, tzinfo=UTC).timestamp()
        ctx = _ctx_with_start_time(start)

        assert _resolve_business_date(ctx, "run-friday") == "2025-06-13"

    def test_backlog_runs_map_to_their_own_day(self):
        """Friday and Saturday successes processed later must keep their dates."""
        fri = datetime(2025, 6, 13, 4, 0, tzinfo=UTC).timestamp()
        sat = datetime(2025, 6, 14, 4, 0, tzinfo=UTC).timestamp()

        assert _resolve_business_date(_ctx_with_start_time(fri), "r1") == "2025-06-13"
        assert _resolve_business_date(_ctx_with_start_time(sat), "r2") == "2025-06-14"

    def test_copenhagen_timezone_applied(self):
        """A start time late UTC on one day is still the CET calendar day."""
        # 2025-06-13 23:30 UTC → 2025-06-14 01:30 CEST.
        start = datetime(2025, 6, 13, 23, 30, tzinfo=UTC).timestamp()
        ctx = _ctx_with_start_time(start)

        assert _resolve_business_date(ctx, "run-late") == "2025-06-14"

    def test_fallback_to_now_when_no_start_time(self):
        ctx = _ctx_with_start_time(None)
        result = _resolve_business_date(ctx, "run-no-start")

        # Falls back to wall-clock today in Europe/Copenhagen + warns.
        expected = datetime.now(tz=ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d")
        assert result == expected
        ctx.log.warning.assert_called_once()

    def test_fallback_when_record_missing(self):
        ctx = MagicMock()
        ctx.instance.get_run_record_by_id.return_value = None
        ctx.log = MagicMock()

        result = _resolve_business_date(ctx, "run-gone")
        expected = datetime.now(tz=ZoneInfo("Europe/Copenhagen")).strftime("%Y-%m-%d")
        assert result == expected
        ctx.log.warning.assert_called_once()


# ===========================================================================
# _failure_trigger_enabled
# ===========================================================================


class TestFailureTriggerEnabled:
    def test_default_off(self, monkeypatch):
        monkeypatch.delenv("DFM_BASE_FAILURE_TRIGGER_DAILY", raising=False)
        assert _failure_trigger_enabled() is False

    def test_truthy_values(self, monkeypatch):
        for val in ("1", "true", "TRUE", "yes", " Yes "):
            monkeypatch.setenv("DFM_BASE_FAILURE_TRIGGER_DAILY", val)
            assert _failure_trigger_enabled() is True

    def test_falsy_values(self, monkeypatch):
        for val in ("0", "false", "no", ""):
            monkeypatch.setenv("DFM_BASE_FAILURE_TRIGGER_DAILY", val)
            assert _failure_trigger_enabled() is False
