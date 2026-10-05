# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact: fct.Lagerbeholdning (periodic snapshot)

Business logic identical to fct_Lagerbeholdning.py.
Real tables used for external FKs (map.Lager, map.ILSVare, dim.Lager).
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

RESOLVE_SQL = """
DROP TABLE IF EXISTS #Lagerbeholdning_Resolved;

SELECT
    mf.dw_ek_Lagerbeholdning,
    stg.dw_ek_Dato_snapshot,
    COALESCE(stg.dw_ek_Dato_udloeb, -1) AS dw_ek_Dato_udloeb,
    stg.dato_tid_udloeb,
    CAST(stg.antal AS DECIMAL(18,3)) AS antal,
    COALESCE(ml.dw_ek_Lager, -1) AS dw_ek_Lager,
    COALESCE(mi.dw_ek_ILSVare, -1) AS dw_ek_ILSVare,
    COALESCE(di.[dw_ek_UNSPSC], -1) AS dw_ek_UNSPSC,
    COALESCE(me.dw_ek_LagerbeholdningEgenskaber, -1)
        AS dw_ek_LagerbeholdningEgenskaber,
    stg.dato_tid_snapshot
INTO #Lagerbeholdning_Resolved
FROM [stg].[fct_Lagerbeholdning_ils] stg
INNER JOIN [map].[Lagerbeholdning] mf
    ON mf.clean_code_Lagerbeholdning = stg.clean_code_Lagerbeholdning
LEFT JOIN [map].[Lager] ml
    ON ml.clean_code_Lager = stg.clean_code_Lager
LEFT JOIN [map].[ILSVare] mi
    ON mi.clean_code_ILSVare = stg.clean_code_ILSVare
LEFT JOIN [dim].[ILSVare] di
    ON di.dw_ek_ILSVare = mi.dw_ek_ILSVare
   AND mi.dw_ek_ILSVare IS NOT NULL
   AND mi.dw_ek_ILSVare <> -1
LEFT JOIN [map].[LagerbeholdningEgenskaber] me
    ON me.clean_code_LagerbeholdningEgenskaber = stg.clean_code_LagerbeholdningEgenskaber;
"""

MERGE_SQL = """
MERGE [fct].[Lagerbeholdning] AS tgt
USING #Lagerbeholdning_Resolved AS src
ON tgt.[dw_ek_Lagerbeholdning] = src.[dw_ek_Lagerbeholdning]

WHEN MATCHED THEN UPDATE SET
    tgt.[dw_ek_Lager] = src.[dw_ek_Lager],
    tgt.[dw_ek_ILSVare] = src.[dw_ek_ILSVare],
    tgt.[dw_ek_UNSPSC] = src.[dw_ek_UNSPSC],
    tgt.[dw_ek_LagerbeholdningEgenskaber] = src.[dw_ek_LagerbeholdningEgenskaber],
    tgt.[dw_ek_Dato_snapshot] = src.[dw_ek_Dato_snapshot],
    tgt.[dw_ek_Dato_udloeb] = src.[dw_ek_Dato_udloeb],
    tgt.[dato_tid_udloeb] = src.[dato_tid_udloeb],
    tgt.[dato_tid_snapshot] = src.[dato_tid_snapshot],
    tgt.[antal] = src.[antal],
    tgt.[dw_is_deleted] = 0,
    tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN INSERT (
    [dw_ek_Lagerbeholdning], [dw_ek_Lager],
    [dw_ek_ILSVare], [dw_ek_UNSPSC], [dw_ek_LagerbeholdningEgenskaber],
    [dw_ek_Dato_snapshot], [dw_ek_Dato_udloeb], [dato_tid_udloeb],
    [dato_tid_snapshot], [antal], [dw_is_deleted],
    [dw_timestamp_insert], [dw_timestamp_update]
) VALUES (
    src.[dw_ek_Lagerbeholdning], src.[dw_ek_Lager],
    src.[dw_ek_ILSVare], src.[dw_ek_UNSPSC], src.[dw_ek_LagerbeholdningEgenskaber],
    src.[dw_ek_Dato_snapshot], src.[dw_ek_Dato_udloeb], src.[dato_tid_udloeb],
    src.[dato_tid_snapshot], src.[antal], 0,
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
)

WHEN NOT MATCHED BY SOURCE
    AND tgt.[dw_ek_Dato_snapshot] IN (
        SELECT DISTINCT [dw_ek_Dato_snapshot]
        FROM #Lagerbeholdning_Resolved
    )
THEN UPDATE SET
    tgt.[dw_is_deleted] = 1,
    tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0));
"""


@dg.asset(
    name="fct_Lagerbeholdning",
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["staging_infra_erp"]),
        dg.AssetKey(["erp", "stg_fct_Lagerbeholdning_ils"]),
        "map_Lagerbeholdning",
        "map_Lager",
        "map_ILSVare",
        "map_LagerbeholdningEgenskaber",
        "dim_ILSVare",
        "dim_LagerbeholdningEgenskaber",
    ],
    tags={"domain": "erp", "layer": "facts", "entity": "lagerbeholdning"},
    description="MERGE fct.Lagerbeholdning via stable DW_EK with soft-delete.",
)
def fct_Lagerbeholdning(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "Lagerbeholdning",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Lagerbeholdning_ils]")
                stg_count = cursor.fetchone()[0]

                if stg_count == 0:
                    context.log.info("No rows to load — skipping")
                    logged_cp: str | None = None
                    if config.should_advance_checkpoint:
                        dfm.set_checkpoint("fct", "Lagerbeholdning", mode=config.mode)
                        logged_cp = dfm.get_checkpoint("fct", "Lagerbeholdning")
                    dfm.complete_dfm_run(
                        run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=0
                    )
                    return dg.MaterializeResult(
                        metadata={"table": "fct.Lagerbeholdning", "row_count": 0}
                    )

                cursor.execute(RESOLVE_SQL)
                resolved_count = cursor.rowcount

                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Lagerbeh_Resolved;")
                cursor.execute("DROP TABLE IF EXISTS #Lagerbeh_Snapshots;")
                cursor.execute("UPDATE STATISTICS [fct].[Lagerbeholdning];")

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN dw_is_deleted = 1 THEN 1 ELSE 0 END) AS soft_deleted,
                        SUM(CASE WHEN dw_ek_Lager = -1 AND dw_is_deleted = 0 THEN 1 ELSE 0 END) AS missing_lager,
                        SUM(CASE WHEN dw_ek_ILSVare = -1 AND dw_is_deleted = 0 THEN 1 ELSE 0 END) AS missing_ilsvare,
                        SUM(CASE WHEN dw_ek_UNSPSC = -1 AND dw_is_deleted = 0 THEN 1 ELSE 0 END) AS missing_unspsc,
                        SUM(CASE WHEN dw_ek_LagerbeholdningEgenskaber = -1 AND dw_is_deleted = 0 THEN 1 ELSE 0 END) AS missing_egenskab
                    FROM [fct].[Lagerbeholdning]
                """
                )
                diag = cursor.fetchone()
                active = diag[0] - diag[1]
                context.log.info(
                    f"Total={diag[0]}, Active={active}, SoftDeleted={diag[1]}, "
                    f"FK coverage (active): "
                    f"Lager={active-diag[2]}/{active}, "
                    f"ILSVare={active-diag[3]}/{active}, "
                    f"UNSPSC={active-diag[4]}/{active}, "
                    f"Egenskab={active-diag[5]}/{active}"
                )
            finally:
                cursor.close()

        result = dg.MaterializeResult(
            metadata={
                "table": "fct.Lagerbeholdning",
                "row_count": resolved_count,
                "merged": merged,
                "pattern": "MERGE (stable DW_EK + soft-delete)",
                "total_rows": diag[0],
                "soft_deleted": diag[1],
                "missing_lager": diag[2],
                "missing_ilsvare": diag[3],
                "missing_unspsc": diag[4],
                "missing_egenskab": diag[5],
            }
        )

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Lagerbeholdning", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Lagerbeholdning")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=resolved_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
