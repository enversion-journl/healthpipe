# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for normalize_decimals_for_bcp.

Background:
    The mssql-python bulk-copy driver transmits Decimal values over TDS using
    their raw coefficient + exponent. SQL Server rejects the row when the
    transmitted scale (-exponent) exceeds the target column's scale, even when
    the value is exactly representable at the target scale. Concretely this
    blocked the finanskladdelinje_enriched extraction for two value shapes:

      * Decimal('0E-12') — zero with negative exponent
      * Decimal('2876.000000000000000') — scale-15 source values into a
        DECIMAL(28, 12) target column

    normalize_decimals_for_bcp must produce a TDS-acceptable encoding without
    changing the value mathematically.
"""

from datetime import datetime
from decimal import Decimal

import pytest
from DSI.connectors.mssql.bulk_copy import normalize_decimals_for_bcp


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expected",
    [
        # Original bug: zero with negative exponent
        (Decimal("0E-12"), Decimal((0, (0,), 0))),
        (Decimal("-0E-12"), Decimal((1, (0,), 0))),
        (Decimal("0"), Decimal((0, (0,), 0))),
        # New bug: non-zero values with excess scale due to trailing zeros
        (Decimal("2876.000000000000000"), Decimal("2876")),
        (Decimal("13309.150000000000000"), Decimal("13309.15")),
        (Decimal("-5.00"), Decimal("-5")),
        # Values without trailing zeros: must be left untouched
        (Decimal("1.5"), Decimal("1.5")),
        (Decimal("100"), Decimal("100")),
        (Decimal("1.234567890123456"), Decimal("1.234567890123456")),
        (Decimal("0.000000000001"), Decimal("0.000000000001")),
    ],
)
def test_normalize_value_and_internal_representation(value, expected):
    data = [(value,)]
    normalize_decimals_for_bcp(data)
    actual = data[0][0]
    assert actual == expected
    # Internal coefficient/exponent must also match — TDS encoding depends on it.
    assert actual.as_tuple() == expected.as_tuple()


@pytest.mark.unit
def test_unchanged_rows_keep_tuple_identity():
    unchanged = (Decimal("1.5"),)
    changed = (Decimal("0E-12"),)
    data = [unchanged, changed]

    normalize_decimals_for_bcp(data)

    assert data[0] is unchanged
    assert data[1] is not changed


@pytest.mark.unit
def test_nan_and_infinity_are_skipped():
    data = [(Decimal("NaN"), Decimal("Infinity"), Decimal("0E-12"))]

    normalize_decimals_for_bcp(data)

    assert data[0][0].is_nan()
    assert data[0][1].is_infinite()
    assert data[0][2].as_tuple() == (0, (0,), 0)


@pytest.mark.unit
def test_non_decimal_cells_are_left_alone():
    row = (1, "hello", None, datetime(2024, 1, 1), Decimal("0E-12"))
    data = [row]

    normalize_decimals_for_bcp(data)

    assert data[0][:4] == row[:4]
    assert data[0][4].as_tuple() == (0, (0,), 0)


@pytest.mark.unit
def test_count_reflects_normalized_cells_only():
    data = [
        (Decimal("0E-12"), Decimal("1.5"), Decimal("2876.000000000000000")),
        (Decimal("3.14"), Decimal("1.5"), Decimal("0E-12")),
    ]

    count = normalize_decimals_for_bcp(data)

    assert count == 3
