# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO dimension sync assets: ERP and shared dimensions from staging to DSO."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import ERP_DIM_CONFIGS, SHARED_DIMS_DIM_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_LINKED_SERVER,
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
    name="dso_sync_dim_aftale",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Aftale"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Aftale from staging to DSO database",
)
def dso_sync_dim_aftale(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Aftale", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_ilsleverandoer",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_ILSLeverandoer"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.ILSLeverandoer from staging to DSO database",
)
def dso_sync_dim_ilsleverandoer(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_ILSLeverandoer", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_bridge_leverandoervareilsvare",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["bridge_LeverandoervareILSVare"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "bridge"},
    description="Sync bridge.LeverandoervareILSVare from staging to DSO database",
)
def dso_sync_bridge_leverandoervareilsvare(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "bridge_LeverandoervareILSVare",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_bridge_aftalelager",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["bridge_AftaleLager"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "bridge"},
    description="Sync bridge.AftaleLager from staging to DSO database",
)
def dso_sync_bridge_aftalelager(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "bridge_AftaleLager",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_kreditor",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Kreditor"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Kreditor from staging to DSO database",
)
def dso_sync_dim_kreditor(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Kreditor", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_leverandoervare",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Leverandoervare"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Leverandoervare from staging to DSO database",
)
def dso_sync_dim_leverandoervare(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Leverandoervare", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_kontoplan",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Kontoplan"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Kontoplan from staging to DSO database",
)
def dso_sync_dim_kontoplan(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Kontoplan", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_vareudlevering",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Vareudlevering"],
    tags={
        "domain": "erp",
        "layer": "dso_sync",
        "type": "dimension",
    },
    description="Sync dim.Vareudlevering from staging to DSO database",
)
def dso_sync_dim_vareudlevering(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_Vareudlevering",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_ordre",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Ordre", "dso_sync_dim_ilsbruger"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Ordre from staging to DSO database",
)
def dso_sync_dim_ordre(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Ordre", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_faktura",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Faktura"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Faktura from staging to DSO database",
)
def dso_sync_dim_faktura(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Faktura", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_fakturalinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Fakturalinje"],
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
        "dim_Fakturalinje",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_postering",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Postering"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Postering from staging to DSO database",
)
def dso_sync_dim_postering(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Postering", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_finanskladde",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Finanskladde"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Finanskladde from staging to DSO database",
)
def dso_sync_dim_finanskladde(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Finanskladde", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_finanskladdelinje",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Finanskladdelinje"],
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
        "dim_Finanskladdelinje",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_lager",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Lager"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Lager from staging to DSO database",
)
def dso_sync_dim_lager(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Lager", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_rekvirent",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_Rekvirent"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Rekvirent from staging to DSO database",
)
def dso_sync_dim_rekvirent(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Rekvirent", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_ean",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_EAN"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.ean from staging to DSO database",
)
def dso_sync_dim_ean(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_EAN", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_budget_egenskaber",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_BudgetEgenskaber"],
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
        "dim_BudgetEgenskaber",
        ERP_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_borger",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_Borger", "dso_sync_dim_borgerdemografi", "dso_infra_shared_dimensions"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.Borger (Type 2 SCD) from staging to DSO database",
)
def dso_sync_dim_borger(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    result = _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_Borger", SHARED_DIMS_DIM_CONFIGS
    )
    metadata = dict(result.metadata or {})
    metadata["note"] = "Type 2 SCD with merge mode"
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    name="dso_sync_dim_borgerdemografi",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_BorgerDemografi", "dso_infra_shared_dimensions"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync shared demographic combinations keyed by the hash on dim.Borger",
)
def dso_sync_dim_borgerdemografi(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_BorgerDemografi",
        SHARED_DIMS_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_organisation",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    # No longer gated on dso_sync_map_organisation: dim.Organisation is built
    # from ref.Organisation, not map.Organisation.
    deps=["dim_Organisation", "dso_infra_organisation"],
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
        "dim_Organisation",
        SHARED_DIMS_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_LagerbeholdningEgenskaber",
    group_name="erp_dso_sync",
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
    group_name="erp_dso_sync",
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
    deps=["dim_ILSVare"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.ILSVare from staging to DSO database",
)
def dso_sync_dim_ilsvare(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_ILSVare", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_ilsbruger",
    group_name="erp_dso_sync",
    compute_kind="mssql",
    deps=["dim_ILSBruger"],
    tags={"domain": "erp", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.ILSBruger from staging to DSO database",
)
def dso_sync_dim_ilsbruger(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context, mssql_target_dso, mssql_source_staging, "dim_ILSBruger", ERP_DIM_CONFIGS
    )


@dg.asset(
    name="dso_sync_dim_UNSPSC",
    group_name="erp_dso_sync",
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
    deps=["dim_Okosted", "dso_infra_erp"],
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
        "dim_Okosted",
        SHARED_DIMS_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_shak",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_SHAK", "dso_infra_erp"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.SHAK from staging to DSO database",
)
def dso_sync_dim_shak(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_SHAK",
        SHARED_DIMS_DIM_CONFIGS,
    )


@dg.asset(
    name="dso_sync_dim_shakalike",
    group_name="shared_dimensions_dso_sync",
    compute_kind="mssql",
    deps=["dim_SHAKAlike", "dso_infra_erp"],
    tags={"domain": "shared_dimensions", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.SHAKAlike from staging to DSO database",
)
def dso_sync_dim_shakalike(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _materialize_sync_result(
        context,
        mssql_target_dso,
        mssql_source_staging,
        "dim_SHAKAlike",
        SHARED_DIMS_DIM_CONFIGS,
    )


all_dso_sync_dim_assets = [
    dso_sync_dim_aftale,
    dso_sync_dim_ilsleverandoer,
    dso_sync_bridge_leverandoervareilsvare,
    dso_sync_bridge_aftalelager,
    dso_sync_dim_kreditor,
    dso_sync_dim_leverandoervare,
    dso_sync_dim_kontoplan,
    dso_sync_dim_ordre,
    dso_sync_dim_vareudlevering,
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
    dso_sync_dim_ilsbruger,
    dso_sync_dim_ilsvare,
    dso_sync_dim_UNSPSC,
    dso_sync_dim_budget_egenskaber,
    dso_sync_dim_borger,
    dso_sync_dim_borgerdemografi,
    dso_sync_dim_organisation,
    dso_sync_dim_okosted,
    dso_sync_dim_shak,
    dso_sync_dim_shakalike,
]
