# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Bridge Asset: bridge.LeverandoervareILSVare (DSA Layer)

Delta-driven, soft-delete many-to-many Vare <-> ILSVare relation.

Soft-delete scope comes ONLY from stg.bridge_Leverandoervare_ILSVare_delta_entiteter
(the hub-derived delta scope) resolved via map.Leverandoervare/map.ILSVare —
never from the pair-set's own counterparts, which would incorrectly widen
the scope (spec §5's (1|1)/(1|2)/(2|2) example). The mark (soft-delete) and
the merge (upsert) run in ONE transaction: if the run fails partway, both
roll back together and the checkpoint is not advanced. No dim deps: the
bridge has no er_primaer flag, so it never reads dim.Leverandoervare/dim.ILSVare.
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


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_bridge_Leverandoervare_ILSVare"]),
        AssetKey(["map_Leverandoervare"]),
        AssetKey(["map_ILSVare"]),
    ],
    tags={"domain": "erp", "layer": "bridge", "module": "datawarehouse"},
    description=(
        "bridge.LeverandoervareILSVare — delta-vedligeholdt many-to-many Vare <-> ILSVare "
        "fra stg.bridge_Leverandoervare_ILSVare, scopet til stg.bridge_Leverandoervare_ILSVare_delta_entiteter. "
        "Mark-then-merge soft-delete (dw_is_deleted) i én transaktion; ejer CP_Bridge."
    ),
)
def bridge_LeverandoervareILSVare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "bridge", "LeverandoervareILSVare", dagster_run_id=context.run_id, mode=config.mode
    )
    rows_affected = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Resolving delta scope + pairs via maps...")

                cursor.execute(
                    """
                    DROP TABLE IF EXISTS #scope_vare;
                    DROP TABLE IF EXISTS #scope_ilsvare;
                    DROP TABLE IF EXISTS #bridge_resolved;

                    -- (kreditor, leverandoervarenummer) er entitetens business key:
                    -- begge noegler kraeves, saa en halv noegle aldrig CONCAT'es
                    -- til et falsk map-opslag
                    SELECT DISTINCT mv.dw_ek_Leverandoervare
                    INTO #scope_vare
                    FROM [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter] e
                    INNER JOIN [map].[Leverandoervare] mv
                        ON mv.clean_code_Leverandoervare = CONCAT(e.clean_code_Kreditor, '_', e.clean_code_LeverandoerVarenummer)
                    WHERE e.clean_code_Kreditor IS NOT NULL
                      AND e.clean_code_LeverandoerVarenummer IS NOT NULL;

                    SELECT DISTINCT mi.dw_ek_ILSVare
                    INTO #scope_ilsvare
                    FROM [stg].[bridge_Leverandoervare_ILSVare_delta_entiteter] e
                    INNER JOIN [map].[ILSVare] mi
                        ON mi.clean_code_ILSVare = e.clean_code_ILSVare
                    WHERE e.clean_code_ILSVare IS NOT NULL;

                    SELECT
                        mv.dw_ek_Leverandoervare,
                        mi.dw_ek_ILSVare,
                        p.bridge_kilder
                    INTO #bridge_resolved
                    FROM [stg].[bridge_Leverandoervare_ILSVare] p
                    INNER JOIN [map].[Leverandoervare] mv
                        ON mv.clean_code_Leverandoervare = CONCAT(p.clean_code_Kreditor, '_', p.clean_code_LeverandoerVarenummer)
                    INNER JOIN [map].[ILSVare] mi
                        ON mi.clean_code_ILSVare = p.clean_code_ILSVare;

                    CREATE UNIQUE CLUSTERED INDEX IX_tmp_bridge_resolved
                    ON #bridge_resolved (dw_ek_Leverandoervare, dw_ek_ILSVare);
                """
                )

                cursor.execute("SELECT COUNT(*) FROM #bridge_resolved")
                rows_affected = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM [stg].[bridge_Leverandoervare_ILSVare]")
                staged_pairs = cursor.fetchone()[0]
                unresolved_pairs = staged_pairs - rows_affected
                if unresolved_pairs > 0:
                    # Fail-fast FOER mark+merge: hub'en genererer entiteter for alle
                    # par-noegler, saa et uoplost par betyder foraeldede maps (fx
                    # ad-hoc-koersel) — checkpointet maa ikke rykke forbi parret.
                    raise ValueError(
                        f"bridge_LeverandoervareILSVare: {unresolved_pairs} staged pairs unresolved — "
                        "vnr/vare-key not found in map.ILSVare/map.Leverandoervare. Ensure "
                        "stg_dim_Leverandoervare/stg_dim_ILSVare and map_Leverandoervare/map_ILSVare ran before "
                        "the bridge; nothing is merged and the checkpoint is not advanced."
                    )

                context.log.info("Mark-then-merge (single transaction)...")

                cursor.execute(
                    """
                    -- (1) MARK: soft-delete alle raekker for scope-entiteterne.
                    --     Kun entiteter vi har FULD historik for — modparter i
                    --     par-saettet maa ikke udvide scope (spec §5-eksemplet).
                    UPDATE b
                    SET b.dw_is_deleted = 1,
                        b.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    FROM [bridge].[LeverandoervareILSVare] b
                    WHERE (
                            b.dw_ek_Leverandoervare IN (SELECT dw_ek_Leverandoervare FROM #scope_vare)
                         OR b.dw_ek_ILSVare IN (SELECT dw_ek_ILSVare FROM #scope_ilsvare)
                    )
                      AND NOT (b.dw_ek_Leverandoervare = -1 AND b.dw_ek_ILSVare = -1);

                    -- (2) MERGE: upsert det genberegnede par-saet; genopstaaede
                    --     par afmarkeres.
                    MERGE [bridge].[LeverandoervareILSVare] AS tgt
                    USING #bridge_resolved AS src
                        ON tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare
                       AND tgt.dw_ek_ILSVare = src.dw_ek_ILSVare
                    WHEN MATCHED AND (
                            tgt.dw_is_deleted = 1
                         OR ISNULL(tgt.bridge_kilder, N'') <> ISNULL(src.bridge_kilder, N'')
                    ) THEN UPDATE SET
                        tgt.dw_is_deleted = 0,
                        tgt.bridge_kilder = src.bridge_kilder,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    WHEN NOT MATCHED BY TARGET THEN INSERT (
                        dw_ek_Leverandoervare, dw_ek_ILSVare, bridge_kilder, dw_is_deleted,
                        dw_timestamp_insert, dw_timestamp_update
                    ) VALUES (
                        src.dw_ek_Leverandoervare, src.dw_ek_ILSVare, src.bridge_kilder, 0,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    );
                """
                )

                cursor.execute("UPDATE STATISTICS [bridge].[LeverandoervareILSVare];")
                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total_pairs,
                        SUM(CASE WHEN dw_is_deleted = 1 THEN 1 ELSE 0 END) AS deleted_pairs
                    FROM [bridge].[LeverandoervareILSVare]
                    WHERE dw_ek_Leverandoervare <> -1
                """
                )
                total_pairs, deleted_pairs = cursor.fetchone()
                context.log.info(
                    f"bridge.LeverandoervareILSVare: {total_pairs} pairs, {deleted_pairs} soft-deleted"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("bridge", "LeverandoervareILSVare", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("bridge", "LeverandoervareILSVare")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "bridge.LeverandoervareILSVare",
            "rows_affected": rows_affected,
            "total_pairs": total_pairs,
            "deleted_pairs": deleted_pairs,
            "unresolved_pairs": unresolved_pairs,
            "source_table": "stg.bridge_Leverandoervare_ILSVare (+ delta_entiteter scope)",
            "grain": "one row per (dw_ek_Leverandoervare, dw_ek_ILSVare) pair; dw_is_deleted soft-delete",
            "checkpoint_after": logged_checkpoint,
            "mode": config.mode,
        }
    )
