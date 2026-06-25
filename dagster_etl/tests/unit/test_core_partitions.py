# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from core.partitions import _validate_partition_key


@pytest.mark.parametrize(
    "key",
    [
        "full_load",
        "2025-01-01",
        "2025-01",
    ],
)
def test_validate_partition_key_valid(key):
    assert _validate_partition_key(key) is True


@pytest.mark.parametrize("key", ["20250101", "2025/01/01", "invalid", "2025-13-01"])
def test_validate_partition_key_invalid(key):
    assert _validate_partition_key(key) is False
