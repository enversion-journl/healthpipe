# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO sync for ERP ref tables: RekvirentUndtagelse and ILSAendringslog."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import SHARED_DIMS_REF_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_LINKED_SERVER,
    compare_row_counts,
    sync_table_via_linked_server,
)


def _sync_ref_table(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
    config_key: str,
) -> dg.MaterializeResult:
    config = SHARED_DIMS_REF_CONFIGS[config_key]
    result = sync_table_via_linked_server(
        context,
        mssql_target_dso,
        config,
        mssql_source_staging=mssql_source_staging,
        validate_schemas=True,
    )
    counts = compare_row_counts(
        context,
        mssql_target_dso,
        config.schema,
        config.table,
        mssql_source_staging=mssql_source_staging,
    )
    sync_engine = result.get("sync_engine", SYNC_ENGINE_LINKED_SERVER)
    metadata = {
        "rows_affected": result["rows_affected"],
        "sync_mode": result["mode"],
        "sync_engine": sync_engine,
        "sync_duration_seconds": result.get("sync_duration_seconds", 0.0),
        "rows_per_second": result.get("rows_per_second", 0.0),
        "staging_count": counts["staging_count"],
        "dso_count": counts["dso_count"],
        "counts_match": counts["match"],
    }
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="dso_sync_ref_RekvirentUndtagelse",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["ref_RekvirentUndtagelse"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "ref"},
    description="Sync ref.RekvirentUndtagelse from staging to DSO",
)
def dso_sync_ref_RekvirentUndtagelse(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _sync_ref_table(
        context, mssql_target_dso, mssql_source_staging, "ref_RekvirentUndtagelse"
    )


@dg.asset(
    name="dso_sync_ref_ILSAendringslog",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["ref_ILSAendringslog"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "ref"},
    description="Sync ref.ILSAendringslog (ILS changelog as-is) from staging to DSO",
)
def dso_sync_ref_ILSAendringslog(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _sync_ref_table(context, mssql_target_dso, mssql_source_staging, "ref_ILSAendringslog")


all_dso_sync_ref_erp_assets = [
    dso_sync_ref_RekvirentUndtagelse,
    dso_sync_ref_ILSAendringslog,
]
