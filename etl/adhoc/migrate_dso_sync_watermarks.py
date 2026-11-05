# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Cutover asset: DSO SyncWatermarks → staging DFMState (scope 'dso' and 'sfi_store')."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.migrate_sync_watermarks_to_dfm import migrate_sync_watermarks_to_dfm as _migrate


@dg.asset(
    name="migrate_dso_sync_watermarks_to_dfm",
    group_name="adhoc",
    compute_kind="mssql",
    deps=["staging_infra_dfm", "dso_infra_meta"],
    tags={
        "layer": "cutover",
        "type": "migration",
        "run_once": "true",
        "domain": "dso",
    },
    description=(
        "Cutover: copy DSO [meta].[SyncWatermarks] into staging [meta].[DFMState] with "
        "scope='dso', and the SFI datapoint store's refresh_daily watermark minus three "
        "hours as its checkpoint under scope='sfi_store'. Fills NULL auto-seeded "
        "checkpoints; leaves non-NULL untouched, so a rerun is safe."
    ),
)
def migrate_dso_sync_watermarks_to_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Materialize to run the SyncWatermarks → DFMState migration."""
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
            "scope": "dso, sfi_store",
            "idempotent": True,
        }
    )
