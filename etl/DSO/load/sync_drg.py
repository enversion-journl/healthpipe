# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO DRG Sync Assets

Syncs the DRG dim/fct/bridge tables from staging (DSA) to the DSO reporting
database via the linked server, using watermark deltas on dw_timestamp_update.

What deliberately does NOT sync:

* ``map.DRG*`` — staging-internal IDENTITY key minting (incl. the group keys
  ``map.DRG{Diagnose,Procedure}Gruppe``). ``dso_infra_drg`` creates only the
  dim/fct/bridge tables on DSO for the same reason. The group keys the fact
  references are exposed on DSO through the group->code bridges, not a group
  dimension.
* ``arc.drg_*`` / ``ext.drg*`` / ``meta.DRGLeverance`` — pipeline internals
  with raw CPR on the contact tables.

The dim/fct/bridge layer is CPR-free by construction: the fact carries
dw_ek_Borger (never raw CPR), the egenskaber-dimension the delivery's own
demografi, and the code dims / bridges only SKS codes and surrogate keys. No
CPR leaves staging. Access is restricted at the VIEW layer, not the sync: the
person-referable fct.DRGKontakt and the demographic dim.DRGKontaktEgenskaber
are read through the DPA_DRG-secured views, which are owned by the separate
security-views repo; the code dims and group bridges are CPR-free and exposed
here as plain pass-through views (views/drg).

Dependency shape: every sync waits for the DSA asset that fills its table, and
for the syncs of the tables its FKs point at (RI ordering on DSO). The code
dims sync first (the group bridges reference them), then the bridges and the
klassifikation/egenskaber dims, then the fact last — it references the
klassifikation, both egenskaber (shared PK) and group bridges, plus the shared
dim.Borger / dim.Organisation. dim.Dato is seeded on DSO by its shared-dimensions
DDL and needs no sync.

The safe read for consumers is the view layer (``dso_infra_views_drg`` +
views/security), which filters dw_is_deleted = 0; the sync moves all rows,
including soft-deleted ones, as the retraction audit trail.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import DRG_SYNC_CONFIGS
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
) -> dg.MaterializeResult:
    """Run one table sync and return standardized metadata."""
    config = DRG_SYNC_CONFIGS[config_key]
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


def _build_sync_asset(config_key: str, dim_syncs: tuple[str, ...] = ()):
    """Build one DSO-sync asset for the DRG table named ``config_key``.

    The DSA asset that fills the table shares that name, so ``config_key``
    doubles as the upstream dependency; ``dim_syncs`` are the extra DSO
    dim/bridge syncs the table's FKs point at (for RI ordering on DSO).
    """
    config = DRG_SYNC_CONFIGS[config_key]

    @dg.asset(
        name=f"dso_sync_{config_key.lower()}",
        group_name="drg_dso_sync",
        compute_kind="mssql",
        deps=[config_key, "dso_infra_drg", *dim_syncs],
        tags={"domain": "drg", "layer": "dso_sync", "source": "drg"},
        description=(
            f"Sync {config.schema}.{config.table} from staging to DSO "
            f"(watermark on dw_timestamp_update; CPR-free)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _materialize_sync_result(context, mssql_target_dso, mssql_source_staging, config_key)

    return _asset


# Built at the call site (no index indirection). Ordered so a table's FK
# targets are on DSO first: code dims -> group bridges + klassifikation/
# egenskaber -> fact. Consumers use the list; the individual assets are reached
# by their Dagster names (the dep strings), never as module attributes.
all_drg_dso_sync_assets = [
    _build_sync_asset("dim_DRGDiagnose"),
    _build_sync_asset("dim_DRGProcedure"),
    _build_sync_asset("dim_DRGKontaktKlassifikation"),
    _build_sync_asset("dim_DRGKontaktEgenskaber"),
    _build_sync_asset("bridge_DRGDiagnoseGruppe", ("dso_sync_dim_drgdiagnose",)),
    _build_sync_asset(
        "bridge_DRGProcedureGruppe",
        ("dso_sync_dim_drgprocedure", "dso_sync_dim_organisation"),
    ),
    _build_sync_asset(
        "fct_DRGKontakt",
        (
            "dso_sync_dim_drgkontaktklassifikation",
            "dso_sync_dim_drgkontaktegenskaber",
            "dso_sync_bridge_drgdiagnosegruppe",
            "dso_sync_bridge_drgproceduregruppe",
            "dso_sync_dim_borger",
            "dso_sync_dim_organisation",
        ),
    ),
]
