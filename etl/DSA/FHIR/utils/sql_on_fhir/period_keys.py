# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Canonical DATETIME2(0) values parsed from raw FHIR dateTime strings.

Used as the stored key for ``fhir.Patient_Address.address_period_start``.
Several FHIR spellings map to one instant; normalisation happens once at load.

    ''                          -> 1900-01-01  (no address on the resource)
    '1900-01-01'                -> 1900-01-01  (address, start unknown)
    '2020-03-01'                -> 2020-03-01 00:00:00
    '2020-03-01T00:00:00'       -> 2020-03-01 00:00:00
    '2020-03-01T08:00:00+01:00' -> 2020-03-01 08:00:00
    '2020-03-01T07:00:00Z'      -> 2020-03-01 08:00:00  (same instant, CET)
    '2020-03-01T08:00:00.987'   -> 2020-03-01 08:00:00  (truncated)

Offset-aware values convert to Europe/Copenhagen before the offset is dropped.
Sub-seconds are truncated, not rounded (T-SQL ``CAST`` to datetime2(0) rounds).
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

# The DWH's business timezone. Offset-aware source values are resolved to this
# zone before the offset is discarded.
DW_TIMEZONE = ZoneInfo("Europe/Copenhagen")

# Canonical "no period start recorded" value. Matches the DEFAULT on
# ref.Patient_Address_To_Borger.address_period_start, so the sentinel means the
# same thing in every layer.
PERIOD_START_SENTINEL = datetime(1900, 1, 1)

# FHIR dateTime permits partial precision: YYYY and YYYY-MM are valid values.
_YEAR_ONLY = re.compile(r"^(\d{4})$")
_YEAR_MONTH = re.compile(r"^(\d{4})-(\d{2})$")

# More than 6 fractional digits is valid FHIR instant but not valid for
# datetime.fromisoformat on every supported runtime; trim to microseconds.
_LONG_FRACTION = re.compile(r"(\.\d{6})\d+")


def parse_fhir_datetime(value: Any) -> datetime | None:
    """Parse a raw FHIR dateTime into a naive Europe/Copenhagen ``datetime``.

    Returns ``None`` when the value is absent OR unparseable — the caller decides
    whether that is benign (missing period.start) or a data-quality signal worth
    counting. Use :func:`normalize_fhir_datetime` when you just want the key.
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    else:
        text = str(value).strip()
        if not text:
            return None

        if match := _YEAR_ONLY.match(text):
            return datetime(int(match.group(1)), 1, 1)
        if match := _YEAR_MONTH.match(text):
            return datetime(int(match.group(1)), int(match.group(2)), 1)

        # 'Z' and >6 fractional digits are both legal FHIR but not universally
        # accepted by fromisoformat.
        text = _LONG_FRACTION.sub(r"\1", text)
        if text.endswith(("Z", "z")):
            text = f"{text[:-1]}+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(DW_TIMEZONE).replace(tzinfo=None)

    return parsed.replace(microsecond=0)


def normalize_fhir_datetime(value: Any) -> datetime:
    """Return the canonical DATETIME2(0) key value for a raw FHIR dateTime.

    Absent and unparseable values both collapse to :data:`PERIOD_START_SENTINEL`.
    """
    parsed = parse_fhir_datetime(value)
    return PERIOD_START_SENTINEL if parsed is None else parsed


def is_unparseable(value: Any) -> bool:
    """True when a value is present but could not be parsed.

    Distinguishes "this patient has no recorded period.start" (benign, expected)
    from "the source sent something we do not understand" (worth logging).
    """
    if value is None:
        return False
    if isinstance(value, datetime | date):
        return False
    if not str(value).strip():
        return False
    return parse_fhir_datetime(value) is None
