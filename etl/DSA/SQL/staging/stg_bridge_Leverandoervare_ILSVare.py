# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: bridge_Leverandoervare_ILSVare (par-saet + scope-liste)

Builds two tables from the entity-delta hub (stg.pre_Leverandoervare_ILSVare_delta_entiteter):
  1. stg.bridge_Leverandoervare_ILSVare — full (kreditor, leverandoervarenummer, ilsvare)
     pair history for delta-affected entities, WITHOUT pair dedup (a
     rn=1 winner-selection here would recreate the information loss the
     bridge exists to fix). bridge_kilder is a priority-ordered STRING_AGG
     over DISTINCT (source, priority) pairs.
  2. stg.bridge_Leverandoervare_ILSVare_delta_entiteter — the scope list: hub entities
     changed since CP_Bridge, read directly from the hub (never from pair-set
     counterparts, which would incorrectly widen the soft-delete scope).

The hub only READS checkpoints (checkpoint_after=None).
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
from utils.sql.leverandoervare_ilsvare_sql import LVNR_CC as _LVNR_CC
from utils.sql.leverandoervare_ilsvare_sql import VNR_CC as _VNR_CC
from utils.sql.leverandoervare_ilsvare_sql import sql_lev_current_cte


def _cp_filter(checkpoint: str | None, expr: str = "dw_timestamp_update") -> str:
    if checkpoint is None:
        return ""
    return f" AND {expr} > '{checkpoint}'"


def _build_scope_sql(cp: str | None) -> str:
    """Scope list: hub entities changed since CP_Bridge. Reads ONLY the hub."""
    cp_filter = _cp_filter(cp)
    return f"""
    DROP TABLE IF EXISTS [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter];

    SELECT clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare, vnr
    INTO [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]
    FROM [stg].[pre_Leverandoervare_ILSVare_delta_entiteter]
    WHERE 1 = 1{cp_filter};
"""


def _build_pairs_sql(cp: str | None) -> str:
    """Pair set: full (kreditor, lvnr, vnr) history for delta-affected entities.

    NO pair dedup — bridge_kilder aggregates every (source, priority) pair
    contributing to a key combination. `cp` is unused; scope comes from
    stg.bridge_Leverandoervare_ILSVare_delta_entiteter, not a re-derived hub filter.
    """
    return f"""
    DROP TABLE IF EXISTS [stg].[bridge_Leverandoervare_ILSVare];

    ;WITH{sql_lev_current_cte()},
    ils_pairs AS (
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="s.[lvnr##1]")} AS clean_code_LeverandoerVarenummer,
               {_VNR_CC.format(col="s.vnr")} AS clean_code_ILSVare,
               CAST(s.vnr AS NVARCHAR(100)) AS vnr,
               'laftdstam' AS bridge_source, 1 AS bridge_priority
        FROM [arc].[ils_laftdstam] s
        INNER JOIN lev_current lev ON lev.lnr = s.lnr
        WHERE s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''
          AND NULLIF(LTRIM(RTRIM(CAST(s.vnr AS NVARCHAR(100)))), '') IS NOT NULL
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="lg.lvnr")},
               {_VNR_CC.format(col="lg.vnr")},
               CAST(lg.vnr AS NVARCHAR(100)),
               'lgrvare', 2
        FROM [arc].[ils_lgrvare] lg
        INNER JOIN lev_current lev ON lev.lnr = lg.lnr
        WHERE lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''
          AND NULLIF(LTRIM(RTRIM(CAST(lg.vnr AS NVARCHAR(100)))), '') IS NOT NULL
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="ik.levvarenr")},
               {_VNR_CC.format(col="ik.vnr")},
               CAST(ik.vnr AS NVARCHAR(100)),
               'indkfak', 3
        FROM [arc].[ils_indkfak] ik
        INNER JOIN lev_current lev ON lev.lnr = ik.lnr
        WHERE ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
          AND ik.levvarenr <> '-'
          AND LEN(LTRIM(RTRIM(ik.levvarenr))) >= 3
          AND ik.levvarenr LIKE '%[a-zA-Z0-9]%'
          AND NULLIF(LTRIM(RTRIM(CAST(ik.vnr AS NVARCHAR(100)))), '') IS NOT NULL
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="ld.[lvnr##1]")},
               {_VNR_CC.format(col="ld.vnr")},
               CAST(ld.vnr AS NVARCHAR(100)),
               'laftd', 4
        FROM [arc].[ils_laftd] ld
        INNER JOIN lev_current lev ON lev.lnr = ld.lnr
        WHERE ld.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(ld.[lvnr##1])) <> ''
          AND NULLIF(LTRIM(RTRIM(CAST(ld.vnr AS NVARCHAR(100)))), '') IS NOT NULL
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="p.lvnr")},
               {_VNR_CC.format(col="p.vnr")},
               CAST(p.vnr AS NVARCHAR(100)),
               'pmdm', 5
        FROM [arc].[ils_pmdm] p
        INNER JOIN lev_current lev ON lev.lnr = p.lnr
        WHERE p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''
          AND NULLIF(LTRIM(RTRIM(CAST(p.vnr AS NVARCHAR(100)))), '') IS NOT NULL
    ),
    scope AS (
        SELECT clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare, vnr
        FROM [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]
    ),
    affected_pairs AS (
        SELECT p.clean_code_Kreditor, p.clean_code_LeverandoerVarenummer, p.clean_code_ILSVare, p.bridge_source, p.bridge_priority
        FROM ils_pairs p
        INNER JOIN scope s
            ON s.clean_code_Kreditor = p.clean_code_Kreditor
           AND s.clean_code_LeverandoerVarenummer = p.clean_code_LeverandoerVarenummer
        UNION
        SELECT p.clean_code_Kreditor, p.clean_code_LeverandoerVarenummer, p.clean_code_ILSVare, p.bridge_source, p.bridge_priority
        FROM ils_pairs p
        INNER JOIN scope s ON s.vnr = p.vnr
    )
    SELECT
        clean_code_Kreditor,
        clean_code_LeverandoerVarenummer,
        clean_code_ILSVare,
        STRING_AGG(bridge_source, ',') WITHIN GROUP (ORDER BY bridge_priority) AS bridge_kilder
    INTO [stg].[bridge_Leverandoervare_ILSVare]
    FROM (SELECT DISTINCT clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare, bridge_source, bridge_priority
          FROM affected_pairs) d
    GROUP BY clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare;
"""


@asset(
    key=AssetKey(["erp", "stg_bridge_Leverandoervare_ILSVare"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["erp", "pre_stg_Leverandoervare_ILSVare_delta_entiteter"])],
    tags={
        "domain": "erp",
        "layer": "bridge",
        "source": "combined",
        "entity": "leverandoervare_ilsvare",
    },
    description=(
        "Bridge staging: full pair-history (no dedup) for delta-affected "
        "(kreditor, leverandoervarenummer, ilsvare) entities in "
        "stg.bridge_Leverandoervare_ILSVare, plus the CP_Bridge scope list of changed "
        "hub entities in stg.bridge_Leverandoervare_ILSVare_delta_entiteter. Feeds bridge.LeverandoervareILSVare."
    ),
)
def stg_bridge_Leverandoervare_ILSVare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Delta-driven bridge staging: pair set (no dedup) + hub-derived scope list."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "bridge_Leverandoervare_ILSVare", dagster_run_id=context.run_id, mode=config.mode
    )
    pairs_row_count = 0
    scope_row_count = 0
    try:
        cp = dfm.get_checkpoint("bridge", "LeverandoervareILSVare")
        if config.mode == "full_load":
            cp = None

        if cp is not None:
            context.log.info(f"Delta mode: staging hub changes since {cp}")
        else:
            context.log.info("No checkpoint — staging the full hub picture")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Building stg.bridge_Leverandoervare_ILSVare_delta_entiteter (scope)..."
                )
                cursor.execute(_build_scope_sql(cp))
                cursor.execute(
                    "SELECT COUNT(*) FROM [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]"
                )
                scope_row_count = cursor.fetchone()[0]
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_stg_bridge_scope
                    ON [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter](
                        clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare
                    );
                """
                )

                context.log.info("Building stg.bridge_Leverandoervare_ILSVare (pair set)...")
                cursor.execute(_build_pairs_sql(cp))
                cursor.execute("SELECT COUNT(*) FROM [stg].[bridge_Leverandoervare_ILSVare]")
                pairs_row_count = cursor.fetchone()[0]
                cursor.execute(
                    """
                    CREATE UNIQUE NONCLUSTERED INDEX IX_stg_bridge_pairs
                    ON [stg].[bridge_Leverandoervare_ILSVare](
                        clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare
                    );
                """
                )

                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=pairs_row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.bridge_Leverandoervare_ILSVare",
            "pairs_row_count": pairs_row_count,
            "scope_row_count": scope_row_count,
            "scope_table": "stg.bridge_Leverandoervare_ILSVare_delta_entiteter",
            "checkpoint_filter": cp if cp else "none (full hub picture)",
            "mode": config.mode,
        }
    )
