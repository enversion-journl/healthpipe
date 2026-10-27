# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR dateTime -> DATETIME2(0) key normalisation.

Must be single-valued, total (never raises), and agree with the
``1900-01-01`` sentinel on ``ref.Patient_Address_To_Borger``.
"""

from datetime import date, datetime, timedelta, timezone

import pytest
from DSA.FHIR.utils.sql_on_fhir.period_keys import (
    PERIOD_START_SENTINEL,
    is_unparseable,
    normalize_fhir_datetime,
    parse_fhir_datetime,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # The exact pair that broke dim_Borger: '' is what the current loader
        # writes, '1900-01-01' is the legacy convention still in the table.
        ("", PERIOD_START_SENTINEL),
        ("1900-01-01", PERIOD_START_SENTINEL),
        (None, PERIOD_START_SENTINEL),
        ("   ", PERIOD_START_SENTINEL),
        # Format variants of the same instant.
        ("2020-03-01", datetime(2020, 3, 1)),
        ("2020-03-01T00:00:00", datetime(2020, 3, 1)),
        # Partial precision is legal FHIR dateTime.
        ("2020", datetime(2020, 1, 1)),
        ("2020-03", datetime(2020, 3, 1)),
        # Sub-second is truncated, not rounded — .999 must not roll to the next
        # second, or Python and a T-SQL CAST backfill would disagree.
        ("2020-03-01T08:00:00.999", datetime(2020, 3, 1, 8, 0, 0)),
        ("2020-03-01T08:00:00.1234567", datetime(2020, 3, 1, 8, 0, 0)),
    ],
)
def test_normalizes_to_expected_key(raw, expected):
    assert normalize_fhir_datetime(raw) == expected


def test_offsets_resolve_to_the_same_local_instant():
    """A UTC 'Z' value and its CET-offset equivalent must produce one key.

    Winter: Copenhagen is UTC+1, so 07:00Z and 08:00+01:00 are the same moment
    and must not become two distinct address periods.
    """
    assert normalize_fhir_datetime("2020-03-01T07:00:00Z") == datetime(2020, 3, 1, 8, 0, 0)
    assert normalize_fhir_datetime("2020-03-01T08:00:00+01:00") == datetime(2020, 3, 1, 8, 0, 0)
    assert normalize_fhir_datetime("2020-03-01T07:00:00Z") == normalize_fhir_datetime(
        "2020-03-01T08:00:00+01:00"
    )


def test_summer_offset_uses_dst_not_a_fixed_offset():
    """July is UTC+2 in Copenhagen — a fixed +1 conversion would be an hour off."""
    assert normalize_fhir_datetime("2020-07-01T06:00:00Z") == datetime(2020, 7, 1, 8, 0, 0)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2020-07-01T23:30:00+00:00", datetime(2020, 7, 1, 23, 30, 0)),
        ("2020-03-01T07:00:00Z", datetime(2020, 3, 1, 7, 0, 0)),
        ("2020-03-01T07:00:00.987+01:00", datetime(2020, 3, 1, 7, 0, 0)),
        ("2020-07-01T23:30:00", datetime(2020, 7, 1, 23, 30, 0)),
    ],
)
def test_keep_wall_clock_drops_the_offset_without_converting(raw, expected):
    """A source that labels local time +00:00 must not be moved an hour or two, past midnight."""
    assert normalize_fhir_datetime(raw, keep_wall_clock=True) == expected


def test_naive_values_are_taken_as_local_and_not_shifted():
    """A value without an offset is already Danish local time — shifting it
    would move midnight-anchored move-in dates onto the previous day."""
    assert normalize_fhir_datetime("2020-07-01T00:00:00") == datetime(2020, 7, 1, 0, 0, 0)


@pytest.mark.parametrize(
    "raw",
    ["not-a-date", "2020-13-01", "31-12-2020", "0000-00-00", "N/A"],
)
def test_unparseable_falls_back_to_sentinel_without_raising(raw):
    """Normalisation is total: a malformed source value must never fail the load."""
    assert normalize_fhir_datetime(raw) == PERIOD_START_SENTINEL
    assert parse_fhir_datetime(raw) is None
    assert is_unparseable(raw) is True


@pytest.mark.parametrize(
    "raw", [None, "", "  ", "2020-03-01", datetime(2020, 3, 1), date(2020, 3, 1)]
)
def test_absent_and_valid_values_are_not_flagged_unparseable(raw):
    """Only present-but-malformed counts — absence is expected and must not
    raise a data-quality alarm on every patient without a recorded period."""
    assert is_unparseable(raw) is False


def test_accepts_datetime_and_date_objects():
    """Defensive: the driver may hand back typed values rather than strings."""
    assert normalize_fhir_datetime(date(2020, 3, 1)) == datetime(2020, 3, 1)
    assert normalize_fhir_datetime(datetime(2020, 3, 1, 8, 0, 0, 500000)) == datetime(
        2020, 3, 1, 8, 0, 0
    )
    aware = datetime(2020, 3, 1, 7, 0, 0, tzinfo=timezone(timedelta(hours=0)))
    assert normalize_fhir_datetime(aware) == datetime(2020, 3, 1, 8, 0, 0)


def test_sentinel_matches_the_ref_table_default():
    """ref.Patient_Address_To_Borger.address_period_start DEFAULTs to
    '1900-01-01'. If these drift apart, sentinel rows stop joining and addresses
    silently become NULL in dim.Borger."""
    assert PERIOD_START_SENTINEL == datetime(1900, 1, 1)
