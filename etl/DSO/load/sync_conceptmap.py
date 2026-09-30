# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSO ConceptMap sync assets: staging conceptmap.vw_* views to DSO conceptmap.* tables."""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import CONCEPTMAP_SYNC_CONFIGS
from DSO.load.sync_utils import (
    SYNC_ENGINE_PYTHON_FALLBACK,
    compare_row_counts,
    sync_table_via_linked_server,
)

# (sync_config_key, upstream_cm_asset, dso_table_name)
_CONCEPTMAP_SYNC_SPECS: list[tuple[str, str, str]] = [
    ("conceptmap_rekvirent_to_sor", "cm_rekvirent_to_sor", "rekvirent_to_sor"),
    ("conceptmap_lager_to_sor", "cm_lager_to_sor", "lager_to_sor"),
    ("conceptmap_ean_to_sor", "cm_ean_to_sor", "ean_to_sor"),
    ("conceptmap_okosted_to_sor", "cm_okosted_to_sor", "okosted_to_sor"),
    ("conceptmap_shak_to_sor", "cm_shak_to_sor", "shak_to_sor"),
]


def _materialize_sync_result(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
    config_key: str,
) -> dg.MaterializeResult:
    """Run one ConceptMap table sync and return standardized metadata."""
    config = CONCEPTMAP_SYNC_CONFIGS[config_key]
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
        staging_schema=config.staging_schema,
        staging_table=config.staging_table,
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


def _create_conceptmap_sync(
    config_key: str,
    upstream_dep: str,
    table_name: str,
) -> dg.AssetsDefinition:
    """Factory: one DSO sync asset per ConceptMap table."""

    @dg.asset(
        name=f"dso_sync_conceptmap_{table_name.lower()}",
        group_name="conceptmap_dso_sync",
        compute_kind="mssql",
        # staging_infra_views_conceptmap creates the conceptmap.vw_*_to_sor dedup
        # views this sync reads from (source_table); without it a fresh staging
        # environment would have no view to MERGE from.
        deps=[upstream_dep, "dso_infra_conceptmap", "staging_infra_views_conceptmap"],
        tags={"domain": "conceptmap", "layer": "dso_sync", "type": "conceptmap"},
        description=f"Sync conceptmap.{table_name} from staging to DSO",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _materialize_sync_result(context, mssql_target_dso, mssql_source_staging, config_key)

    return _asset


all_conceptmap_dso_sync_assets = [
    _create_conceptmap_sync(cfg_key, upstream, tbl)
    for cfg_key, upstream, tbl in _CONCEPTMAP_SYNC_SPECS
]

# Named exports for backward compatibility
dso_sync_conceptmap_rekvirent_to_sor = all_conceptmap_dso_sync_assets[0]
dso_sync_conceptmap_lager_to_sor = all_conceptmap_dso_sync_assets[1]
dso_sync_conceptmap_ean_to_sor = all_conceptmap_dso_sync_assets[2]
dso_sync_conceptmap_okosted_to_sor = all_conceptmap_dso_sync_assets[3]
dso_sync_conceptmap_shak_to_sor = all_conceptmap_dso_sync_assets[4]
