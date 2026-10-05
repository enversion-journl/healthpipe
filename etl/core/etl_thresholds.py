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
from pydantic import Field

SOFT_DELETE_THRESHOLDS: dict[str, int] = {
    "ref.PatientToBorger": 1000,
    "ref.Patient_Address_To_Borger": 1000,
    "dim.Borger": 1000,
    "ref.Organisation": 1000,
    "fhir.Organization": 1000,
}

# Marked / live ratio. Fires on a small table where the absolute cap is
# unreachable (50 live orgs, 20 vanished) and is skipped when a per-run
# override is set.
SOFT_DELETE_RATIOS: dict[str, float] = {
    "fhir.Organization": 0.10,
    "ref.Organisation": 0.10,
}

# Dagster op names for the per-run override path. The threshold dict is keyed
# by schema.table; the config lives on ops.<function_name>.
SOFT_DELETE_OPS: dict[str, str] = {
    "ref.PatientToBorger": "ref_patient_borger",
    "ref.Patient_Address_To_Borger": "ref_patient_address_borger",
    "dim.Borger": "dim_Borger",
    "ref.Organisation": "ref_organisation",
    "fhir.Organization": "sof_organization",
}

DEFAULT_THRESHOLD = 1000


class SoftDeleteThresholdConfig(dg.Config):
    """Per-run override of the standing soft-delete cap.

    The slim counterpart to RunModeConfig for assets that are not DFM-partitioned
    and therefore have no mode / date_from / ids knobs to expose.
    """

    soft_delete_threshold: int | None = Field(
        default=None,
        description="Raise the soft-delete guard for THIS RUN only, and skip the "
        "marked-vs-live ratio guard. A deliberate rebuild retires far more rows than "
        "a daily delta, but raising the standing threshold would blind the guard on "
        "every scheduled run afterwards. Confirm the count is explained before "
        "setting it. Example: 10000.",
    )


def get_soft_delete_threshold(asset_name: str, override: int | None = None) -> int:
    """Return the soft-delete threshold in force for asset_name.

    ``override`` comes from run config and applies to that run only -- see
    ``check_soft_delete_threshold``.
    """
    if override is not None:
        return override
    return SOFT_DELETE_THRESHOLDS.get(asset_name, DEFAULT_THRESHOLD)


def check_soft_delete_threshold(
    asset_name: str,
    soft_deleted_count: int,
    context: dg.AssetExecutionContext | None = None,
    override: int | None = None,
    live_row_count: int | None = None,
    ratio: float | None = None,
) -> None:
    """
    Raise dagster.Failure if soft_deleted_count exceeds the threshold for asset_name.

    Always logs the count and threshold via context (if provided) for dashboard visibility.

    ``override`` raises the bar for a single run via the asset's run config
    (``soft_delete_threshold``). Schedules do not set it, so a rebuild override
    cannot leak into daily runs. A set override also skips the marked-vs-live
    ratio (the rebuild is already explicit).

    ``live_row_count`` is the live population before this run's deletes. When
    the asset has a ``SOFT_DELETE_RATIOS`` entry, a marked/live fraction above
    that ratio fails even if the absolute count is under the cap.

    ``ratio`` overrides the ``SOFT_DELETE_RATIOS`` lookup for callers whose
    asset names are dynamic and cannot live in the registry (e.g. the DRG
    archive tables, keyed per dataset in the DSI layer, which core must not
    import from).
    """
    threshold = get_soft_delete_threshold(asset_name, override)
    if ratio is None:
        ratio = SOFT_DELETE_RATIOS.get(asset_name)
    marked_ratio = (
        soft_deleted_count / live_row_count
        if live_row_count is not None and live_row_count > 0
        else None
    )

    if context is not None:
        origin = " (overridden for this run)" if override is not None else ""
        ratio_note = (
            f", {marked_ratio:.1%} of {live_row_count} live" if marked_ratio is not None else ""
        )
        context.log.info(
            f"{asset_name}: soft-deleted {soft_deleted_count} rows "
            f"(threshold: {threshold}{origin}{ratio_note})"
        )

    op_name = SOFT_DELETE_OPS.get(asset_name, asset_name.split(".")[-1])
    if soft_deleted_count > threshold:
        raise dg.Failure(
            description=(
                f"{asset_name}: soft-deleted {soft_deleted_count} rows "
                f"exceeds threshold of {threshold}. If this is a deliberate rebuild, "
                f"confirm the count is explained and re-run with config "
                f"ops.{op_name}.config.soft_delete_threshold set above it."
            ),
            metadata={
                "soft_deleted_this_run": soft_deleted_count,
                "threshold": threshold,
                "threshold_overridden": override is not None,
                "asset": asset_name,
            },
        )

    if override is None and ratio is not None and marked_ratio is not None and marked_ratio > ratio:
        raise dg.Failure(
            description=(
                f"{asset_name}: soft-deleted {soft_deleted_count} of {live_row_count} "
                f"live rows ({marked_ratio:.1%}) exceeds the {ratio:.0%} marked-vs-live "
                f"guard. If this is a deliberate rebuild, confirm the count is explained "
                f"and re-run with config ops.{op_name}.config.soft_delete_threshold "
                f"set above {soft_deleted_count}."
            ),
            metadata={
                "soft_deleted_this_run": soft_deleted_count,
                "live_row_count": live_row_count,
                "marked_ratio": marked_ratio,
                "ratio_limit": ratio,
                "threshold_overridden": False,
                "asset": asset_name,
            },
        )
