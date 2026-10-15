# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
EPJI location archive layer — UPSERT ext.epji_location into arc.epji_location.

Unlike SOR's cumulative version file, ext_epji_location delivers a genuine
full snapshot every run (no watermark). detect_deletes=True is therefore
correct here: a location that disappears from EPJI should be soft-deleted
in the archive rather than silently vanishing with no trace.

EXTERNAL_ID has no enforced uniqueness upstream in EPJI.LOCATION (a
data-entry error could give two distinct locations the same code), so
dedup_source_on_keys=True wraps ext in a ROW_NUMBER dedup keyed on `code`
rather than assuming the extract already guarantees it. sor_code sorts
first in non_key_columns so a duplicate pair with different, non-empty SOR
codes deterministically keeps one and reports the collapse as
rows_dedup_dropped (with a warning log) — see ArchiveMergeConfig's
dedup_source_on_keys docstring for the general mechanism.

Named after the source (arc.epji_location), not the SHAK-a-like consumer —
see epji_extract.py for the full reasoning. The asset itself keeps the
`arc_epji_shakalike` name: it is still the specific step that feeds the
SHAK-a-like pipeline, even though the table it writes is a general-purpose
EPJI location archive.

Uses execute_archive_merge, the same DFM pattern as the ILS/SYDAX/SOR archives.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

_MERGE_KEYS = ("code",)

EPJI_LOCATION_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="epji_location",
    target_schema="arc",
    target_table="epji_location",
    merge_keys=_MERGE_KEYS,
    # sor_code first: the tie-break priority for a duplicate `code` — a row
    # carrying a SOR code beats one without, matching the archive's job of
    # keeping the mapping usable. location_pk/display/identifier_system are
    # only tie-breakers among rows that already agree on sor_code.
    non_key_columns=("sor_code", "location_pk", "display", "identifier_system"),
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    upstream_is_full_load=True,
    dedup_source_on_keys=True,
    enable_bulk_insert_on_first_load=True,
)


@dg.asset(
    name="arc_epji_shakalike",
    group_name="epji_archive",
    compute_kind="mssql",
    deps=["ext_epji_location", "staging_infra_epji_archive"],
    tags={"domain": "codesystem", "layer": "archive", "source": "epji"},
    description=(
        "UPSERT ext.epji_location → arc.epji_location (persistent EPJI "
        "location archive). Soft-deletes locations absent from the latest "
        "snapshot."
    ),
)
def arc_epji_shakalike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(
        context, mssql_target_staging, EPJI_LOCATION_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd into arc.epji_location — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}, soft_deleted={result.rows_soft_deleted}; "
        f"mode={config.mode}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.epji_location",
            target_table="arc.epji_location",
        )
    )


__all__ = ["EPJI_LOCATION_ARCHIVE_CONFIG", "arc_epji_shakalike"]
