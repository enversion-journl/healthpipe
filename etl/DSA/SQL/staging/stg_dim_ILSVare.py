# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: ILSVare (Dimension Layer, delta from hub)

DELTA set of ILS item numbers, scoped to the changed ILSVare entities carried
by the pre-staging hub (`stg.pre_Leverandoervare_ILSVare_delta_entiteter`). The hub
already applied the checkpoint_ILSVare axis filter; this asset bounds the hub
read further with its OWN ("dim", "ILSVare") checkpoint (no checkpoint /
full_load => the full hub picture).

Source for map.ILSVare and dim.ILSVare (delta build, not a full rebuild from
arc.* every run).

Grain: vnr (one row per item number in the delta). The hub carries the
natural key (raw vnr) and the derived clean_code_ILSVare; enrichment LEFT
JOINs the item master on the natural key, so lager-only vnr (absent from
ils_vare) become shell rows with NULL attributes.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)


def _build_stg_sql(checkpoint: str | None) -> str:
    """Delta-vnr staging from the hub. checkpoint None => full hub picture.

    Drives from the hub's delta rows (natural key: raw vnr) and LEFT JOINs the
    item master — a delta-vnr without an ils_vare row (stock-table-only items)
    becomes a shell row with NULL attributes but keeps its vnr and cleancode
    from the hub.
    """

    cp_filter = ""
    if checkpoint is not None:
        cp_filter = f"\n          AND dw_timestamp_update > '{checkpoint}'"

    return f"""
    DROP TABLE IF EXISTS [stg].[dim_ILSVare];

    ;WITH delta_vnr AS (
        SELECT vnr, clean_code_ILSVare
        FROM [stg].[pre_Leverandoervare_ILSVare_delta_entiteter]
        WHERE vnr IS NOT NULL{cp_filter}
    )
    SELECT
        d.vnr, v.navn, v.navn2, v.varestr, v.basisenh,
        v.vareart, va.navn AS vareart_navn,
        v.varegrp, vg.navn AS varegrp_navn,
        v.varetype, vt.navn AS varetype_navn,
        v.momskd, mo.txt AS momskode_navn, mo.momspct,
        v.oprdto, v.oprtid,
        v.unspc,  u1.betegn AS unspsc_navn,
        v.unspsc2, u2.betegn AS unspsc_intern_navn,
        v.aktiv, v.faelvnr, v.ean_vnr, v.billed,
        v.oprbruger, zz.bd_navn AS oprbruger_navn,
        v.billedli, v.ejlukkes, v.steril, v.pro2modified,
        d.clean_code_ILSVare
    INTO [stg].[dim_ILSVare]
    FROM delta_vnr d
    LEFT JOIN [arc].[ils_vare]     v  ON v.vnr        = d.vnr
    LEFT JOIN [arc].[ils_vareart]  va ON va.vareart  = v.vareart
    LEFT JOIN [arc].[ils_varegrp]  vg ON vg.varegrp  = v.varegrp
    LEFT JOIN [arc].[ils_varetype] vt ON vt.varetype = v.varetype
    LEFT JOIN [arc].[ils_moms]     mo ON mo.momskd   = v.momskd
    LEFT JOIN [arc].[ils_unspsc]   u1 ON u1.unspsc   = v.unspc
    LEFT JOIN [arc].[ils_unspsc]   u2 ON u2.unspsc   = v.unspsc2
    LEFT JOIN [arc].[ils_zz_oper]  zz ON zz.bd_init  = v.oprbruger;
"""


@asset(
    key=AssetKey(["erp", "stg_dim_ILSVare"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["erp", "pre_stg_Leverandoervare_ILSVare_delta_entiteter"]),
        AssetKey(["arc_ils_vareart"]),
        AssetKey(["arc_ils_varegrp"]),
        AssetKey(["arc_ils_varetype"]),
        AssetKey(["arc_ils_moms"]),
        AssetKey(["arc_ils_unspsc"]),
        AssetKey(["arc_ils_zz_oper"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "ilsvare",
    },
    description=(
        "Dimension staging DELTA for the ILS item master, scoped to changed "
        "ILSVare entities carried by stg.pre_Leverandoervare_ILSVare_delta_entiteter (dim.ILSVare "
        "checkpoint). Builds stg.dim_ILSVare with enriched attributes and "
        "clean_code_ILSVare inline. Feeds map.ILSVare and dim.ILSVare."
    ),
)
def stg_dim_ILSVare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Dimension staging for ILS item master — delta build scoped to the hub.

    Drives from the hub's delta rows (natural key: raw vnr), LEFT JOINs the
    item master and its vareart/varegrp/varetype/moms/unspsc/zz_oper lookups;
    vnr and clean_code_ILSVare come from the hub, attributes from ils_vare
    (NULL for lager-only vnr without an item-master row).
    """

    dfm = DfmManager(mssql_target_staging, context)

    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_ILSVare",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0

    try:
        cp = dfm.get_checkpoint("dim", "ILSVare")
        if config.mode == "full_load":
            cp = None

        if cp is not None:
            context.log.info(f"Delta mode: staging hub entities changed since {cp}")
        else:
            context.log.info("No checkpoint — staging the full hub picture")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building stg.dim_ILSVare from the entity-delta hub...")

                cursor.execute(_build_stg_sql(cp))

                context.log.info("Computing row count for stg.dim_ILSVare...")
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ILSVare]")
                row_count = cursor.fetchone()[0]

                context.log.info(f"Created stg.dim_ILSVare with {row_count} rows")

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_stg_dim_ILSVare_cc
                    ON [stg].[dim_ILSVare](clean_code_ILSVare);
                """
                )
                context.log.info("Created nonclustered index on clean_code_ILSVare")

                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_ILSVare",
            "row_count": row_count,
            "source_tables": (
                "stg.pre_Leverandoervare_ILSVare_delta_entiteter (delta scope + natural key) + "
                "arc.ils_vare, arc.ils_vareart, arc.ils_varegrp, arc.ils_varetype, "
                "arc.ils_moms, arc.ils_unspsc, arc.ils_zz_oper"
            ),
            "grain": "vnr (one row per changed item number)",
            "checkpoint_filter": cp if cp else "none (full hub picture)",
            "mode": config.mode,
            "layer": "dim (Layer 1)",
        }
    )
