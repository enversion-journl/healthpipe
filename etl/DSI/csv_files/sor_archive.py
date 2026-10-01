# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SOR DFM archive layer — UPSERT ext.sor_register into arc.sor_register.

The archive is version-grained (code + from_date) and never deletes: a
"current"-mode extract upserts the current/future versions, an
"all_historical" run backfills every version ever published, and closed
entities simply stop changing (closure is carried by to_date on the current
version). update_only_on_change keeps dw_timestamp_update meaningful as a
change signal.

Uses execute_archive_merge + meta.DFMState, the same DFM pattern as the
ILS/SYDAX/KMD archives.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from DSI.csv_files.sor_register import EXT_COLUMNS

_MERGE_KEYS = ("code", "from_date")

SOR_REGISTER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="sor_register",
    target_schema="arc",
    target_table="sor_register",
    merge_keys=_MERGE_KEYS,
    non_key_columns=tuple(c for c in EXT_COLUMNS if c not in _MERGE_KEYS),
    strategy="upsert",
    update_only_on_change=True,
    # Every extract is a complete snapshot of its mode's file, but the archive
    # must never soft-delete: historical versions are absent from the daily
    # file by design.
    detect_deletes=False,
    upstream_is_full_load=True,
    # ext rows are unique on (code, from_date) — the extract dedupes on the
    # version key — so the cold-start fast path is safe.
    enable_bulk_insert_on_first_load=True,
)


@dg.asset(
    name="arc_sor_register",
    group_name="sor_archive",
    compute_kind="mssql",
    deps=["extract_sor_register"],
    tags={"domain": "codesystem", "layer": "archive", "source": "sor"},
    description=(
        "UPSERT ext.sor_register → arc.sor_register (persistent SOR archive, "
        "one row per entity version). Never deletes — full history retained."
    ),
)
def arc_sor_register(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(
        context, mssql_target_staging, SOR_REGISTER_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd into arc.sor_register — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}; mode={config.mode}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.sor_register",
            target_table="arc.sor_register",
        )
    )


__all__ = ["SOR_REGISTER_ARCHIVE_CONFIG", "arc_sor_register"]
