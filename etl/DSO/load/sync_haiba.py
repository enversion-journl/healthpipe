# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO HAIBA Sync Assets

Syncs the eight dm.HAIBA tables (four facts + four egenskaber transaction
dimensions) from staging to the DSO reporting database.

What deliberately does NOT sync:

* ``map.HAIBA_*`` — staging-internal key minting; the case tables' clean_code
  carries CPR, so the map must never leave staging. ``dso_infra_haiba``
  creates only the dm tables on DSO for the same reason.
* ``arc.haiba_*`` / ``ext.haiba_*`` / ``meta.HAIBALeverance`` — pipeline
  internals with raw CPR on the case tables.

The dm layer itself is CPR-free by construction: the facts carry
dw_ek_Borger and the egenskaber the raw SOR codes and dates, so no access
restriction is needed on the DSO side. A consumer answers "infections per
hospital per month" by joining the fact to dim.Organisation and dim.Dato
alone — dim.Dato is created and seeded on DSO by its shared-dimensions DDL,
so only dim.Organisation and dim.Borger need a sync ordering here.

Dependency shape: every sync waits for the dm asset that fills its table.
The case tables — BOTH fact and egenskaber — additionally wait for the
kontakt asset: the rebuild resets the relation keys on the fact to NULL and
match_kvalitet_* on egenskaber to 'ikke_koert', and syncing between rebuild
and stamp would publish a week of "relation not computed" to every consumer.

All rows sync, including soft-deleted ones (dw_is_deleted is the retraction
audit trail). The safe read is the default one: consumers use vw_dm.HAIBA_*
(``dso_infra_views_haiba``), which filters dw_is_deleted = 0 — SDS's window
rolls every January and soft-deletes roughly a fifth of the archive, so an
unfiltered SELECT over-counts more year over year. The audit trail is read
from the dm tables directly.

The publish itself is timed per run: every sync asset records
sync_duration_seconds and rows_per_second in its Dagster metadata, so the
cost of the ~6.7M-row truncate_insert is visible on the asset — input to the
watermark follow-up noted in sync_configs.py.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import HAIBA_DM_CONFIGS
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
    config = HAIBA_DM_CONFIGS[config_key]
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


# (config key, upstream asset that fills the table, extra dim syncs the keys
# point at). ALL four case syncs depend on the KONTAKT asset, not the bare
# rebuild: the fact carries the stamped relation keys and the egenskaber the
# stamped quality — see the module docstring.
_SYNC_SPECS = (
    ("dm_HAIBA_Agg_Infektion", "dm_haiba_agg_infektion", ("dso_sync_dim_organisation",)),
    ("dm_HAIBA_Agg_Infektion_Egenskaber", "dm_haiba_agg_infektion", ()),
    ("dm_HAIBA_Agg_Ortopaedi", "dm_haiba_agg_ortopaedi", ("dso_sync_dim_organisation",)),
    ("dm_HAIBA_Agg_Ortopaedi_Egenskaber", "dm_haiba_agg_ortopaedi", ()),
    (
        "dm_HAIBA_Cases_Infektion",
        "dm_haiba_cases_infektion_kontakt",
        ("dso_sync_dim_organisation", "dso_sync_dim_borger"),
    ),
    ("dm_HAIBA_Cases_Infektion_Egenskaber", "dm_haiba_cases_infektion_kontakt", ()),
    (
        "dm_HAIBA_Cases_SSI",
        "dm_haiba_cases_ssi_kontakt",
        ("dso_sync_dim_organisation", "dso_sync_dim_borger"),
    ),
    ("dm_HAIBA_Cases_SSI_Egenskaber", "dm_haiba_cases_ssi_kontakt", ()),
)


def _build_sync_asset(config_key: str, upstream: str, dim_syncs: tuple[str, ...]):
    config = HAIBA_DM_CONFIGS[config_key]

    @dg.asset(
        name=f"dso_sync_{config_key.lower()}",
        group_name="haiba_dso_sync",
        compute_kind="mssql",
        deps=[upstream, "dso_infra_haiba", *dim_syncs],
        tags={"domain": "haiba", "layer": "dso_sync", "source": "haiba"},
        description=(
            f"Sync dm.{config.table} from staging to DSO database "
            f"(truncate_insert; CPR-free by construction)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _materialize_sync_result(context, mssql_target_dso, mssql_source_staging, config_key)

    return _asset


dso_sync_dm_haiba_agg_infektion = _build_sync_asset(*_SYNC_SPECS[0])
dso_sync_dm_haiba_agg_infektion_egenskaber = _build_sync_asset(*_SYNC_SPECS[1])
dso_sync_dm_haiba_agg_ortopaedi = _build_sync_asset(*_SYNC_SPECS[2])
dso_sync_dm_haiba_agg_ortopaedi_egenskaber = _build_sync_asset(*_SYNC_SPECS[3])
dso_sync_dm_haiba_cases_infektion = _build_sync_asset(*_SYNC_SPECS[4])
dso_sync_dm_haiba_cases_infektion_egenskaber = _build_sync_asset(*_SYNC_SPECS[5])
dso_sync_dm_haiba_cases_ssi = _build_sync_asset(*_SYNC_SPECS[6])
dso_sync_dm_haiba_cases_ssi_egenskaber = _build_sync_asset(*_SYNC_SPECS[7])

all_haiba_dso_sync_assets = [
    dso_sync_dm_haiba_agg_infektion,
    dso_sync_dm_haiba_agg_infektion_egenskaber,
    dso_sync_dm_haiba_agg_ortopaedi,
    dso_sync_dm_haiba_agg_ortopaedi_egenskaber,
    dso_sync_dm_haiba_cases_infektion,
    dso_sync_dm_haiba_cases_infektion_egenskaber,
    dso_sync_dm_haiba_cases_ssi,
    dso_sync_dm_haiba_cases_ssi_egenskaber,
]

__all__ = [
    "all_haiba_dso_sync_assets",
    "dso_sync_dm_haiba_agg_infektion",
    "dso_sync_dm_haiba_agg_infektion_egenskaber",
    "dso_sync_dm_haiba_agg_ortopaedi",
    "dso_sync_dm_haiba_agg_ortopaedi_egenskaber",
    "dso_sync_dm_haiba_cases_infektion",
    "dso_sync_dm_haiba_cases_infektion_egenskaber",
    "dso_sync_dm_haiba_cases_ssi",
    "dso_sync_dm_haiba_cases_ssi_egenskaber",
]
