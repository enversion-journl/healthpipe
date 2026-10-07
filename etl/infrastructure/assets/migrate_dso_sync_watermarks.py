# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""One-shot cutover asset: DSO SyncWatermarks → staging DFMState (scope='dso')."""

import dagster as dg
from core.resources import MSSQLResource


@dg.asset(
    name="migrate_dso_sync_watermarks_to_dfm",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_dfm", "dso_infra_meta"],
    tags={
        "layer": "cutover",
        "type": "migration",
        "run_once": "true",
        "domain": "dso",
    },
    description=(
        "One-shot cutover: copy DSO [meta].[SyncWatermarks] into staging "
        "[meta].[DFMState] with scope='dso' (skips sfi_datapoint). "
        "Fills NULL auto-seeded checkpoints; leaves non-NULL untouched. "
        "Prefer once per env before first watermark DSO sync on DFM code."
    ),
)
def migrate_dso_sync_watermarks_to_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Materialize to run the SyncWatermarks → DFMState migration."""
    # Lazy import avoids cycle: DSO.load → infrastructure → this asset → DSO.load
    from DSO.load.migrate_sync_watermarks_to_dfm import migrate_sync_watermarks_to_dfm as _migrate

    stats = _migrate(
        mssql_dso=mssql_target_dso,
        mssql_staging=mssql_target_staging,
        context=context,
    )
    return dg.MaterializeResult(
        metadata={
            "rows_inserted": dg.MetadataValue.int(stats.inserted),
            "rows_updated_null": dg.MetadataValue.int(stats.updated),
            "rows_skipped_existing": dg.MetadataValue.int(stats.skipped),
            "scope": "dso",
            "skips": "sfi_datapoint.*",
            "idempotent": True,
        }
    )
