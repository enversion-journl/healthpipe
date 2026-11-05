# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils.partition_utils import (
    get_month_date_range,
    parse_monthly_partition,
)

pytestmark = pytest.mark.unit


def test_parse_monthly_partition_valid():
    start, end = parse_monthly_partition("2025-02")
    assert start == "2025-02-01"
    assert end == "2025-02-28"  # 2025 not leap year


def test_parse_monthly_partition_invalid():
    with pytest.raises(ValueError):
        parse_monthly_partition("2025/02")
    with pytest.raises(ValueError):
        parse_monthly_partition("2025-13")


def test_get_month_date_range_formats():
    start, end = get_month_date_range("2025-01")
    assert (start, end) == ("2025-01-01", "2025-01-31")

    start2, end2 = get_month_date_range("2025-01-15")
    assert (start2, end2) == ("2025-01-01", "2025-01-31")

    with pytest.raises(ValueError):
        get_month_date_range("invalid")
