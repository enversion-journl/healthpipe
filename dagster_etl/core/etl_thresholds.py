# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Soft-delete threshold guards for MERGE-based ETL assets.

Each asset that performs soft-deletes (NOT MATCHED BY SOURCE) must check
the count of soft-deleted rows against a hard threshold. If breached,
the asset fails loudly via dagster.Failure rather than silently removing
a large fraction of the table.
"""

import dagster as dg

SOFT_DELETE_THRESHOLDS: dict[str, int] = {
    "ref.Patient_Borger": 1000,
    "ref.Patient_Address_Borger": 1000,
    "dim.Borger": 1000,
    "ref.Organisation": 1000,
}

DEFAULT_THRESHOLD = 1000


def get_soft_delete_threshold(asset_name: str) -> int:
    """Return the configured soft-delete threshold for asset_name."""
    return SOFT_DELETE_THRESHOLDS.get(asset_name, DEFAULT_THRESHOLD)


def check_soft_delete_threshold(
    asset_name: str,
    soft_deleted_count: int,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """
    Raise dagster.Failure if soft_deleted_count exceeds the threshold for asset_name.

    Always logs the count and threshold via context (if provided) for dashboard visibility.
    """
    threshold = SOFT_DELETE_THRESHOLDS.get(asset_name, DEFAULT_THRESHOLD)

    if context is not None:
        context.log.info(
            f"{asset_name}: soft-deleted {soft_deleted_count} rows " f"(threshold: {threshold})"
        )

    if soft_deleted_count > threshold:
        raise dg.Failure(
            description=(
                f"{asset_name}: soft-deleted {soft_deleted_count} rows "
                f"exceeds threshold of {threshold}"
            ),
            metadata={
                "soft_deleted_this_run": soft_deleted_count,
                "threshold": threshold,
                "asset": asset_name,
            },
        )
