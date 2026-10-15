# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Partition-key helpers left over from the partitioned extraction era.

No asset or job in the deployment carries a ``partitions_def`` any more — every
source runs on DFM checkpoints, and a re-run of a window is a ``RunModeConfig``
(``period`` / ``full_load`` / ``ids``) rather than a backfill. The partition
*definitions* that used to live here went with the last partitioned producer.

What remains are two key-format helpers that the SQL extractors still call. They
are reachable because the extractors accept an optional ``partition_key`` and
build a bounded WHERE from it; on the DFM path that argument is always None, so
these are effectively dormant. They stay until the extractors' partition
branches are unwound, which is a separate change.
"""

from datetime import datetime

import dagster as dg

# The start dates the partition definitions used to begin at. Only the *values*
# matter now: is_first_partition() compares a key against them to decide when an
# extract should use an upper bound only, so that everything before the start
# date is swept in.
START_DATE_DAILY = "2021-01-01"
START_DATE_MONTHLY = "2021-01"

_PARTITION_START_KEYS: frozenset[str] = frozenset({START_DATE_DAILY, START_DATE_MONTHLY})


def is_first_partition(partition_key: str) -> bool:
    """Return True when *partition_key* is the earliest key in its range.

    For the earliest partition an extract query should use an upper bound only
    (no lower bound), so that all historical data before the start date is
    included rather than silently dropped.
    """
    return partition_key in _PARTITION_START_KEYS


def _validate_partition_key(partition_key: str) -> bool:
    """Validate if partition key is in a supported format."""
    # Check full_load partition key
    if partition_key == "full_load":
        return True

    # Check daily format (YYYY-MM-DD) - also used by weekly partitions
    if len(partition_key) == 10 and partition_key.count("-") == 2:
        try:
            datetime.strptime(partition_key, "%Y-%m-%d")
            return True
        except ValueError:
            pass
    # Check monthly format (YYYY-MM)
    if len(partition_key) == 7 and partition_key.count("-") == 1:
        try:
            datetime.strptime(partition_key, "%Y-%m")
            return True
        except ValueError:
            pass

    return False


# Dynamic partition set kept for the integration-test materialization helper,
# which registers a key into it before materializing. No asset uses it, so the
# registration is inert; it goes when that helper stops taking a partition.
unified_partitions = dg.DynamicPartitionsDefinition(name="unified_partitions")
