# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Soft-delete threshold guard.

ref.PatientToBorger, ref.Patient_Address_To_Borger, dim.Borger and the three
arc.epji_practitioner* archives fail via dagster.Failure when one run soft-deletes
more rows than their cap. The archives count before their MERGE, so nothing is
written.
"""

import dagster as dg

SOFT_DELETE_THRESHOLDS: dict[str, int] = {
    "ref.PatientToBorger": 1000,
    # A citizen who becomes protected retires all her earlier address rows; a day's
    # CPR delivery gives about a thousand of them.
    "ref.Patient_Address_To_Borger": 10000,
    "dim.Borger": 10000,
    # Full EPJI snapshots. An EPJ account changes state rather than disappearing,
    # each account carries four identifier rows, and the role catalogue is static.
    "arc.epji_practitioner": 100,
    "arc.epji_practitioner_identifier": 400,
    "arc.epji_practitioner_role": 10,
}

# Dagster op names for the per-run override path. The threshold dict is keyed
# by schema.table; the config lives on ops.<function_name>.
SOFT_DELETE_OPS: dict[str, str] = {
    "ref.PatientToBorger": "ref_patient_borger",
    "ref.Patient_Address_To_Borger": "ref_patient_address_borger",
    "dim.Borger": "dim_Borger",
    "arc.epji_practitioner": "arc_epji_practitioner",
    "arc.epji_practitioner_identifier": "arc_epji_practitioner_identifier",
    "arc.epji_practitioner_role": "arc_epji_practitioner_role",
}

DEFAULT_THRESHOLD = 1000


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
) -> None:
    """
    Raise dagster.Failure if soft_deleted_count exceeds the threshold for asset_name.

    Always logs the count and threshold via context (if provided) for dashboard visibility.

    ``override`` raises the bar for a single run via the asset's run config
    (``soft_delete_threshold``). Schedules do not set it, so a rebuild override
    cannot leak into daily runs.
    """
    threshold = get_soft_delete_threshold(asset_name, override)

    if context is not None:
        origin = " (overridden for this run)" if override is not None else ""
        context.log.info(
            f"{asset_name}: soft-deleted {soft_deleted_count} rows "
            f"(threshold: {threshold}{origin})"
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
