# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime, timedelta

import pytest
from utils.time_normalization import (
    datetime_to_fhir_date,
    datetime_to_fhir_instant,
    normalize_epoch_ts,
    should_normalize_epoch,
)

pytestmark = pytest.mark.unit


def test_normalize_epoch_seconds_and_millis():
    seconds_value = 1_600_000_000  # 2020-09-13 12:26:40 UTC
    millis_value = 1_600_000_000_000  # same instant but ms

    dt_seconds = normalize_epoch_ts(seconds_value)
    dt_millis = normalize_epoch_ts(millis_value)

    assert dt_seconds == datetime(2020, 9, 13, 12, 26, 40, tzinfo=UTC)
    assert dt_millis == datetime(2020, 9, 13, 12, 26, 40, tzinfo=UTC)


def test_normalize_epoch_invalid_and_out_of_range():
    assert normalize_epoch_ts("not-a-number") is None
    # Too small to be epoch seconds
    assert normalize_epoch_ts(123456) is None

    clamp_start = datetime(2020, 1, 1, tzinfo=UTC)
    clamp_end = datetime(2020, 1, 2, tzinfo=UTC)
    too_early = clamp_start - timedelta(days=1)
    too_late = clamp_end + timedelta(days=1)

    assert normalize_epoch_ts(too_early.timestamp(), clamp_start, clamp_end) is None
    assert normalize_epoch_ts(too_late.timestamp(), clamp_start, clamp_end) is None


def test_datetime_to_fhir_instant_and_date():
    naive_dt = datetime(2020, 5, 4, 12, 0, 0)
    aware_dt = datetime(2020, 5, 4, 12, 0, 0, tzinfo=UTC)

    assert datetime_to_fhir_instant(naive_dt).endswith("Z")
    assert datetime_to_fhir_instant(aware_dt).endswith("Z")
    assert datetime_to_fhir_instant(None) is None

    assert datetime_to_fhir_date(aware_dt) == "2020-05-04"
    assert datetime_to_fhir_date(None) is None


def test_should_normalize_epoch_env_and_tags(monkeypatch):
    monkeypatch.setenv("NORMALIZE_EPOCH_TS", "true")
    assert should_normalize_epoch() is True

    monkeypatch.setenv("NORMALIZE_EPOCH_TS", "false")
    assert should_normalize_epoch() is False

    assert should_normalize_epoch(run_tags={"normalize_epoch": "true"}) is True
    assert should_normalize_epoch(run_tags={"normalize_epoch": "false"}) is False
