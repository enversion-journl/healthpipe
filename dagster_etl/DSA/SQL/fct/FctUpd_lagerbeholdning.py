# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact: fct.Lagerbeholdning (periodic snapshot)

Business logic identical to FctUpd_lagerbeholdning.py.
Real tables used for external FKs (map.Lager, map.IlsVare, dim.Lager).
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

RESOLVE_SQL = """
DROP TABLE IF EXISTS #Lagerbeholdning_Resolved;

SELECT
    mf.DW_EK_Lagerbeholdning,
    stg.DW_EK_Dato_Snapshot,
    COALESCE(stg.DW_EK_Dato_Udloeb, -1) AS DW_EK_Dato_Udloeb,
    CAST(stg.antal AS DECIMAL(18,3)) AS Antal,
    COALESCE(ml.DW_EK_Lager, -1) AS DW_EK_Lager,
    COALESCE(mi.DW_EK_IlsVare, -1) AS DW_EK_IlsVare,
    COALESCE(di.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC,
    COALESCE(me.DW_EK_LagerbeholdningEgenskaber, -1)
        AS DW_EK_LagerbeholdningEgenskaber,
    stg.Datotid_Snapshot
INTO #Lagerbeholdning_Resolved
FROM [stg].[fct_Lagerbeholdning_ils] stg
INNER JOIN [map].[Lagerbeholdning] mf
    ON mf.CleanCode_Lagerbeholdning = stg.CleanCode_Lagerbeholdning
LEFT JOIN [map].[Lager] ml
    ON ml.CleanCode_Lager = stg.Lager_CleanCode
LEFT JOIN [map].[IlsVare] mi
    ON mi.CleanCode_IlsVare = stg.IlsVare_CleanCode
LEFT JOIN [dim].[IlsVare] di
    ON di.DW_EK_IlsVare = mi.DW_EK_IlsVare
   AND mi.DW_EK_IlsVare IS NOT NULL
   AND mi.DW_EK_IlsVare <> -1
LEFT JOIN [map].[LagerbeholdningEgenskaber] me
    ON me.CleanCode_LagerbeholdningEgenskaber = stg.Egenskab_CleanCode;
"""

MERGE_SQL = """
MERGE [fct].[Lagerbeholdning] AS tgt
USING #Lagerbeholdning_Resolved AS src
ON tgt.[DW_EK_Lagerbeholdning] = src.[DW_EK_Lagerbeholdning]

WHEN MATCHED THEN UPDATE SET
    tgt.[DW_EK_Lager] = src.[DW_EK_Lager],
    tgt.[DW_EK_IlsVare] = src.[DW_EK_IlsVare],
    tgt.[DW_EK_UNSPSC] = src.[DW_EK_UNSPSC],
    tgt.[DW_EK_LagerbeholdningEgenskaber] = src.[DW_EK_LagerbeholdningEgenskaber],
    tgt.[DW_EK_Dato_Snapshot] = src.[DW_EK_Dato_Snapshot],
    tgt.[DW_EK_Dato_Udloeb] = src.[DW_EK_Dato_Udloeb],
    tgt.[Datotid_Snapshot] = src.[Datotid_Snapshot],
    tgt.[Antal] = src.[Antal],
    tgt.[DW_IsDeleted] = 0,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN INSERT (
    [DW_EK_Lagerbeholdning], [DW_EK_Lager],
    [DW_EK_IlsVare], [DW_EK_UNSPSC], [DW_EK_LagerbeholdningEgenskaber],
    [DW_EK_Dato_Snapshot], [DW_EK_Dato_Udloeb],
    [Datotid_Snapshot], [Antal], [DW_IsDeleted],
    [DW_Timestamp_Insert], [DW_Timestamp_Update]
) VALUES (
    src.[DW_EK_Lagerbeholdning], src.[DW_EK_Lager],
    src.[DW_EK_IlsVare], src.[DW_EK_UNSPSC], src.[DW_EK_LagerbeholdningEgenskaber],
    src.[DW_EK_Dato_Snapshot], src.[DW_EK_Dato_Udloeb],
    src.[Datotid_Snapshot], src.[Antal], 0,
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
)

WHEN NOT MATCHED BY SOURCE
    AND tgt.[DW_EK_Dato_Snapshot] IN (
        SELECT DISTINCT [DW_EK_Dato_Snapshot]
        FROM #Lagerbeholdning_Resolved
    )
THEN UPDATE SET
    tgt.[DW_IsDeleted] = 1,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0));
"""


@dg.asset(
    name="fct_Lagerbeholdning",
    group_name="facts_lagerbeholdning",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["staging_infra_erp"]),
        dg.AssetKey(["erp", "stg_fct_Lagerbeholdning_ils"]),
        "map_Lagerbeholdning",
        "map_lager",
        "map_ilsvare",
        "map_LagerbeholdningEgenskaber",
        "dim_ilsvare",
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
                        SUM(CASE WHEN DW_IsDeleted = 1 THEN 1 ELSE 0 END) AS soft_deleted,
                        SUM(CASE WHEN DW_EK_Lager = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_lager,
                        SUM(CASE WHEN DW_EK_IlsVare = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_ilsvare,
                        SUM(CASE WHEN DW_EK_UNSPSC = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_unspsc,
                        SUM(CASE WHEN DW_EK_LagerbeholdningEgenskaber = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_egenskab
                    FROM [fct].[Lagerbeholdning]
                """
                )
                diag = cursor.fetchone()
                active = diag[0] - diag[1]
                context.log.info(
                    f"Total={diag[0]}, Active={active}, SoftDeleted={diag[1]}, "
                    f"FK coverage (active): "
                    f"Lager={active-diag[2]}/{active}, "
                    f"IlsVare={active-diag[3]}/{active}, "
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
