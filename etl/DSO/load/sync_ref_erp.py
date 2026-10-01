# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO sync for ERP ref.RekvirentUndtagelse (exclusion list for reelt forbrug)."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import SHARED_DIMS_REF_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_PYTHON_FALLBACK,
    compare_row_counts,
    sync_table_via_linked_server,
)


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
    config = SHARED_DIMS_REF_CONFIGS["ref_RekvirentUndtagelse"]
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
    fallback_reason = result.get("fallback_reason")
    sync_engine = (
        SYNC_ENGINE_PYTHON_FALLBACK
        if fallback_reason
        else result.get("sync_engine", "linked_server")
    )
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
    if fallback_reason:
        metadata["fallback_reason"] = fallback_reason
    return dg.MaterializeResult(metadata=metadata)


all_dso_sync_ref_erp_assets = [
    dso_sync_ref_RekvirentUndtagelse,
]
