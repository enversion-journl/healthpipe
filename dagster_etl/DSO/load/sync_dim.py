# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO dimension sync assets: ERP and shared dimensions from staging to DSO."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import ERP_DIM_CONFIGS, SHARED_DIMS_DIM_CONFIGS, SHARED_DIMS_MAP_CONFIGS
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
    name="dso_sync_dim_aftale",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_aftale"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Aftale from staging to DSO database",
)
def dso_sync_dim_aftale(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_aftale", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_kreditor",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_kreditor"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Kreditor from staging to DSO database",
)
def dso_sync_dim_kreditor(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_kreditor", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_vare",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_vare"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Vare from staging to DSO database",
)
def dso_sync_dim_vare(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_vare", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_kontoplan",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_kontoplan"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Kontoplan from staging to DSO database",
)
def dso_sync_dim_kontoplan(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_kontoplan", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_vareudlevering_egenskaber",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_vareudlevering_egenskaber"],
    tags={
        "domain": "erp",
        "layer": "dso_sync",
        "type": "dimension",
    },
    description="Sync dim.VareudleveringEgenskaber from staging to DSO database",
)
def dso_sync_dim_vareudlevering_egenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_vareudlevering_egenskaber",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_faktura",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_faktura"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Faktura from staging to DSO database",
)
def dso_sync_dim_faktura(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_faktura", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_fakturalinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_fakturalinje"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Fakturalinje from staging to DSO database",
)
def dso_sync_dim_fakturalinje(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_fakturalinje",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_postering",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_postering"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Postering from staging to DSO database",
)
def dso_sync_dim_postering(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_postering", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_finanskladde",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_finanskladde"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Finanskladde from staging to DSO database",
)
def dso_sync_dim_finanskladde(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_finanskladde", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_finanskladdelinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_finanskladdelinje"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Finanskladdelinje from staging to DSO database",
)
def dso_sync_dim_finanskladdelinje(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_finanskladdelinje",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_lager",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_lager"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Lager from staging to DSO database",
)
def dso_sync_dim_lager(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_lager", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_rekvirent",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_rekvirent"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Rekvirent from staging to DSO database",
)
def dso_sync_dim_rekvirent(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_rekvirent", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_ean",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_ean"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.EAN from staging to DSO database",
)
def dso_sync_dim_ean(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_ean", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_budget_egenskaber",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_budget_egenskaber"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.BudgetEgenskaber from staging to DSO database",
)
def dso_sync_dim_budget_egenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_budget_egenskaber",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_borger",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_borger"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Borger (Type 2 SCD) from staging to DSO database",
)
def dso_sync_dim_borger(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    result = _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_borger", SHARED_DIMS_DIM_CONFIGS
    )
    metadata = dict(result.metadata or {})
    metadata["note"] = "Type 2 SCD with truncate_insert mode"
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="dso_sync_map_organisation",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["map_organisation", "dso_infra_organisation"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "map"},
    description="Sync map.Organisation from staging to DSO database",
)
def dso_sync_map_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "map_organisation",
        SHARED_DIMS_MAP_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_organisation",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    # No longer gated on dso_sync_map_organisation: dim.Organisation is built
    # from ref.Organisation, not map.Organisation (US #28193).
    deps=["dim_organisation", "dso_infra_organisation"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Organisation from staging to DSO database",
)
def dso_sync_dim_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_organisation",
        SHARED_DIMS_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_LagerbeholdningEgenskaber",
    group_name="dso_sync_lagerbeholdning",
    compute_kind="mssql",
    deps=["dim_LagerbeholdningEgenskaber"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.LagerbeholdningEgenskaber from staging to DSO database",
)
def dso_sync_dim_LagerbeholdningEgenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_LagerbeholdningEgenskaber",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_AfdelingLagerbeholdningEgenskaber",
    group_name="dso_sync_lagerbeholdning",
    compute_kind="mssql",
    deps=["dim_AfdelingLagerbeholdningEgenskaber"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.AfdelingLagerbeholdningEgenskaber from staging to DSO database",
)
def dso_sync_dim_AfdelingLagerbeholdningEgenskaber(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_AfdelingLagerbeholdningEgenskaber",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_ilsvare",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_ilsvare"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.IlsVare from staging to DSO database",
)
def dso_sync_dim_ilsvare(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_ilsvare", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_UNSPSC",
    group_name="dso_sync_unspsc",
    compute_kind="mssql",
    deps=["dim_UNSPSC"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.UNSPSC from staging to DSO database",
)
def dso_sync_dim_UNSPSC(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_UNSPSC",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_okosted",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_okosted", "dso_infra_erp"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Okosted from staging to DSO database",
)
def dso_sync_dim_okosted(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_okosted",
        SHARED_DIMS_DIM_CONFIGS,
    )


all_dso_sync_dim_assets = [
    dso_sync_dim_aftale,
    dso_sync_dim_kreditor,
    dso_sync_dim_vare,
    dso_sync_dim_kontoplan,
    dso_sync_dim_vareudlevering_egenskaber,
    dso_sync_dim_faktura,
    dso_sync_dim_fakturalinje,
    dso_sync_dim_postering,
    dso_sync_dim_finanskladde,
    dso_sync_dim_finanskladdelinje,
    dso_sync_dim_lager,
    dso_sync_dim_rekvirent,
    dso_sync_dim_ean,
    dso_sync_dim_LagerbeholdningEgenskaber,
    dso_sync_dim_AfdelingLagerbeholdningEgenskaber,
    dso_sync_dim_ilsvare,
    dso_sync_dim_UNSPSC,
    dso_sync_dim_budget_egenskaber,
    dso_sync_dim_borger,
    dso_sync_map_organisation,
    dso_sync_dim_organisation,
    dso_sync_dim_okosted,
]
