# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Fact Sync Assets

Syncs ERP fct tables from staging (DSI/DSA) to DSO (reporting) database.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import ERP_FCT_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_PYTHON_FALLBACK,
    compare_row_counts,
    sync_table_via_linked_server,
)


def _materialize_sync_result(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
    config_key: str,
    config_group: dict,
) -> dg.MaterializeResult:
    """Run one table sync and return standardized metadata."""
    config = config_group[config_key]
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


@dg.asset(
    name="dso_sync_fct_faktura",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Faktura",
        "fct_apply_faktura_postering_relations",
        "dso_sync_dim_kreditor",
        "dso_sync_dim_kontoplan",
        "dso_sync_dim_faktura",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Faktura from staging to DSO database",
)
def dso_sync_fct_faktura(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "fct_Faktura", ERP_FCT_CONFIGS
    )


@dg.asset(
    name="dso_sync_fct_fakturalinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Fakturalinje",
        "fct_apply_fakturalinje_unspsc",
        "dso_sync_dim_kreditor",
        "dso_sync_dim_leverandoervare",
        "dso_sync_dim_faktura",
        "dso_sync_dim_fakturalinje",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Fakturalinje from staging to DSO database",
)
def dso_sync_fct_fakturalinje(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_Fakturalinje",
        ERP_FCT_CONFIGS,
    )


@dg.asset(
    name="dso_sync_fct_postering",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Postering",
        # fct_Postering, the faktura bridge and apply-pass now all run in
        # erp_daily_dfm_job upstream of this sync — dw_ek_Faktura is fresh (#28687).
        "fct_apply_faktura_postering_relations",
        "dso_sync_dim_kreditor",
        "dso_sync_dim_kontoplan",
        "dso_sync_dim_postering",
        "dso_sync_dim_finanskladdelinje",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Postering from staging to DSO database",
)
def dso_sync_fct_postering(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "fct_Postering", ERP_FCT_CONFIGS
    )


@dg.asset(
    name="dso_sync_fct_finanskladdelinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Finanskladdelinje",
        "dso_sync_dim_finanskladdelinje",
        "dso_sync_dim_finanskladde",
        "dso_sync_dim_kontoplan",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Finanskladdelinje from staging to DSO database",
)
def dso_sync_fct_finanskladdelinje(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_Finanskladdelinje",
        ERP_FCT_CONFIGS,
    )


@dg.asset(
    name="dso_sync_fct_budget",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Budget",
        "dso_sync_dim_budget_egenskaber",
        "dso_sync_dim_kontoplan",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Budget from staging to DSO database",
)
def dso_sync_fct_budget(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "fct_Budget", ERP_FCT_CONFIGS
    )


@dg.asset(
    name="dso_sync_fct_vareudlevering",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_Vareudlevering",
        "dso_sync_dim_vareudlevering",
        "dso_sync_dim_lager",
        "dso_sync_dim_rekvirent",
        "dso_sync_dim_leverandoervare",
    ],
    tags={
        "domain": "erp",
        "layer": "dso_sync",
        "type": "fact",
    },
    description="Sync fct.Vareudlevering from staging to DSO database",
)
def dso_sync_fct_vareudlevering(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_Vareudlevering",
        ERP_FCT_CONFIGS,
    )


@dg.asset(
    name="dso_sync_fct_AftaleVarePris",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=[
        "fct_AftaleVarePris",
        "dso_sync_dim_aftale",
        "dso_sync_dim_leverandoervare",
        "dso_sync_dim_ilsvare",
        "dso_sync_dim_kreditor",
    ],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.AftaleVarePris from staging to DSO database",
)
def dso_sync_fct_AftaleVarePris(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_AftaleVarePris",
        ERP_FCT_CONFIGS,
    )


@dg.asset(
    name="dso_sync_fct_Lagerbeholdning",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["fct_Lagerbeholdning"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.Lagerbeholdning from staging to DSO database",
)
def dso_sync_fct_Lagerbeholdning(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_Lagerbeholdning",
        ERP_FCT_CONFIGS,
    )


@dg.asset(
    name="dso_sync_fct_AfdelingLagerbeholdning",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["fct_AfdelingLagerbeholdning"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "fact"},
    description="Sync fct.AfdelingLagerbeholdning from staging to DSO database",
)
def dso_sync_fct_AfdelingLagerbeholdning(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "fct_AfdelingLagerbeholdning",
        ERP_FCT_CONFIGS,
    )


all_dso_sync_fct_assets = [
    dso_sync_fct_faktura,
    dso_sync_fct_fakturalinje,
    dso_sync_fct_postering,
    dso_sync_fct_finanskladdelinje,
    dso_sync_fct_budget,
    dso_sync_fct_vareudlevering,
    dso_sync_fct_AftaleVarePris,
    dso_sync_fct_Lagerbeholdning,
    dso_sync_fct_AfdelingLagerbeholdning,
]
