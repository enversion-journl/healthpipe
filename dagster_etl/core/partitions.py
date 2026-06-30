# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import datetime

import dagster as dg

# Centralized place for all asset partitions definitions
#
# Start-date constants are defined once and reused by both the partition
# definitions and the PARTITION_START_KEYS lookup.  To change a start date,
# edit ONLY the constant below.

START_DATE_DAILY = "2021-01-01"
START_DATE_MONTHLY = "2021-01"
START_DATE_DAILY_ERP = "2005-01-01"
START_DATE_MONTHLY_ERP = "2012-01"

# Daily partitions starting from 2021 to cover legacy data.
# The end_offset=1 tells Dagster to include the current day's partition,
# even though the day is not yet complete.
daily_partitions = dg.DailyPartitionsDefinition(start_date=START_DATE_DAILY, end_offset=1)

# Monthly partitions starting from the same early date
# Used for monthly ETL workflows and aggregation
# The end_offset=1 tells Dagster to include the current month's partition,
# even though the month is not yet complete.
monthly_partitions = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE_MONTHLY,
    end_offset=1,
    fmt="%Y-%m",
)

# Daily partitions starting from 2005 for ERP data (legacy data going further back).
# The end_offset=1 tells Dagster to include the current day's partition,
# even though the day is not yet complete.
daily_partitions_erp = dg.DailyPartitionsDefinition(start_date=START_DATE_DAILY_ERP, end_offset=1)

# Monthly partitions starting from 2012 for ERP data
# Used for monthly ETL workflows and aggregation
# The end_offset=1 tells Dagster to include the current month's partition,
# even though the month is not yet complete.
monthly_partitions_erp = dg.MonthlyPartitionsDefinition(
    start_date=START_DATE_MONTHLY_ERP,
    end_offset=1,  # Include current month (like daily_partitions)
    fmt="%Y-%m",
)

# Static partition for full load scenarios
# Used for one-time/initial data loads where all data is processed at once
full_load_partitions = dg.StaticPartitionsDefinition(["full_load"])

# Mapping from each partition definition to its configured start key.
# Used by is_first_partition() to decide when upper-bound-only filtering applies.
PARTITION_START_KEYS: dict[dg.PartitionsDefinition, str] = {
    daily_partitions: START_DATE_DAILY,
    monthly_partitions: START_DATE_MONTHLY,
    daily_partitions_erp: START_DATE_DAILY_ERP,
    monthly_partitions_erp: START_DATE_MONTHLY_ERP,
}


def is_first_partition(
    partition_key: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> bool:
    """Return True when *partition_key* equals the configured start key.

    When the partition is the earliest in the range, extract queries should
    use an upper bound only (no lower bound) so that all historical data
    before the start date is included.

    If *partitions_def* is provided, the start key is looked up from
    PARTITION_START_KEYS.  Otherwise the function falls back to checking
    against every known start key.
    """
    if partitions_def is not None:
        start_key = PARTITION_START_KEYS.get(partitions_def)
        if start_key is not None:
            return partition_key == start_key
        return False

    return partition_key in PARTITION_START_KEYS.values()


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


# Unified partition definition using DynamicPartitionsDefinition
# Partitions must be added dynamically before use
unified_partitions = dg.DynamicPartitionsDefinition(name="unified_partitions")
