# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact: fct.AfdelingLagerbeholdning (periodic snapshot)

Business logic identical to FctUpd_afdeling_lagerbeholdning.py.
Real tables used for external FKs (map.Lager, map.IlsVare, dim.Lager).
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

RESOLVE_SQL = """
DROP TABLE IF EXISTS #AfdelingLagerbeholdning_Resolved;

SELECT
    mf.DW_EK_AfdelingLagerbeholdning,
    stg.DW_EK_Dato_Snapshot,
    COALESCE(stg.DW_EK_Dato_Udloeb, -1) AS DW_EK_Dato_Udloeb,
    CAST(stg.antal AS DECIMAL(18,3)) AS Antal,
    COALESCE(ml.DW_EK_Lager, -1) AS DW_EK_Lager,
    COALESCE(mr.DW_EK_Rekvirent, -1) AS DW_EK_Rekvirent,
    COALESCE(mi.DW_EK_IlsVare, -1) AS DW_EK_IlsVare,
    COALESCE(di.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC,
    COALESCE(me.DW_EK_AfdelingLagerbeholdningEgenskaber, -1)
        AS DW_EK_AfdelingLagerbeholdningEgenskaber,
    stg.Datotid_Snapshot
INTO #AfdelingLagerbeholdning_Resolved
FROM [stg].[fct_AfdelingLagerbeholdning_ils] stg
INNER JOIN [map].[AfdelingLagerbeholdning] mf
    ON mf.CleanCode_AfdelingLagerbeholdning = stg.CleanCode_AfdelingLagerbeholdning
LEFT JOIN [map].[Lager] ml
    ON ml.CleanCode_Lager = stg.Lager_CleanCode
LEFT JOIN [map].[Rekvirent] mr
    ON mr.CleanCode_Rekvirent = stg.Rekvirent_CleanCode
LEFT JOIN [map].[IlsVare] mi
    ON mi.CleanCode_IlsVare = stg.IlsVare_CleanCode
LEFT JOIN [dim].[IlsVare] di
    ON di.DW_EK_IlsVare = mi.DW_EK_IlsVare
   AND mi.DW_EK_IlsVare IS NOT NULL
   AND mi.DW_EK_IlsVare <> -1
LEFT JOIN [map].[AfdelingLagerbeholdningEgenskaber] me
    ON me.CleanCode_AfdelingLagerbeholdningEgenskaber = stg.Egenskab_CleanCode;
"""

MERGE_SQL = """
MERGE [fct].[AfdelingLagerbeholdning] AS tgt
USING #AfdelingLagerbeholdning_Resolved AS src
ON tgt.[DW_EK_AfdelingLagerbeholdning] = src.[DW_EK_AfdelingLagerbeholdning]

WHEN MATCHED THEN UPDATE SET
    tgt.[DW_EK_Lager] = src.[DW_EK_Lager],
    tgt.[DW_EK_Rekvirent] = src.[DW_EK_Rekvirent],
    tgt.[DW_EK_IlsVare] = src.[DW_EK_IlsVare],
    tgt.[DW_EK_UNSPSC] = src.[DW_EK_UNSPSC],
    tgt.[DW_EK_AfdelingLagerbeholdningEgenskaber] = src.[DW_EK_AfdelingLagerbeholdningEgenskaber],
    tgt.[DW_EK_Dato_Snapshot] = src.[DW_EK_Dato_Snapshot],
    tgt.[DW_EK_Dato_Udloeb] = src.[DW_EK_Dato_Udloeb],
    tgt.[Datotid_Snapshot] = src.[Datotid_Snapshot],
    tgt.[Antal] = src.[Antal],
    tgt.[DW_IsDeleted] = 0,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN INSERT (
    [DW_EK_AfdelingLagerbeholdning], [DW_EK_Lager],
    [DW_EK_Rekvirent], [DW_EK_IlsVare], [DW_EK_UNSPSC],
    [DW_EK_AfdelingLagerbeholdningEgenskaber],
    [DW_EK_Dato_Snapshot], [DW_EK_Dato_Udloeb],
    [Datotid_Snapshot], [Antal], [DW_IsDeleted],
    [DW_Timestamp_Insert], [DW_Timestamp_Update]
) VALUES (
    src.[DW_EK_AfdelingLagerbeholdning], src.[DW_EK_Lager],
    src.[DW_EK_Rekvirent], src.[DW_EK_IlsVare], src.[DW_EK_UNSPSC],
    src.[DW_EK_AfdelingLagerbeholdningEgenskaber],
    src.[DW_EK_Dato_Snapshot], src.[DW_EK_Dato_Udloeb],
    src.[Datotid_Snapshot], src.[Antal], 0,
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
)

WHEN NOT MATCHED BY SOURCE
    AND tgt.[DW_EK_Dato_Snapshot] IN (
        SELECT DISTINCT [DW_EK_Dato_Snapshot]
        FROM #AfdelingLagerbeholdning_Resolved
    )
THEN UPDATE SET
    tgt.[DW_IsDeleted] = 1,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0));
"""


@dg.asset(
    name="fct_AfdelingLagerbeholdning",
    group_name="facts_lagerbeholdning",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["staging_infra_erp"]),
        dg.AssetKey(["erp", "stg_fct_AfdelingLagerbeholdning_ils"]),
        "map_AfdelingLagerbeholdning",
        "map_lager",
        "map_rekvirent",
        "map_ilsvare",
        "map_AfdelingLagerbeholdningEgenskaber",
        "dim_ilsvare",
        "dim_AfdelingLagerbeholdningEgenskaber",
    ],
    tags={"domain": "erp", "layer": "facts", "entity": "afdeling_lagerbeholdning"},
    description="MERGE fct.AfdelingLagerbeholdning via stable DW_EK with soft-delete.",
)
def fct_AfdelingLagerbeholdning(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "AfdelingLagerbeholdning",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_AfdelingLagerbeholdning_ils]")
                stg_count = cursor.fetchone()[0]

                if stg_count == 0:
                    context.log.info("No rows to load — skipping")
                    logged_cp: str | None = None
                    if config.should_advance_checkpoint:
                        dfm.set_checkpoint("fct", "AfdelingLagerbeholdning", mode=config.mode)
                        logged_cp = dfm.get_checkpoint("fct", "AfdelingLagerbeholdning")
                    dfm.complete_dfm_run(
                        run_ctx.log_id,
                        checkpoint_after=logged_cp,
                        rows_affected=0,
                    )
                    return dg.MaterializeResult(
                        metadata={
                            "table": "fct.AfdelingLagerbeholdning",
                            "row_count": 0,
                        }
                    )

                cursor.execute(RESOLVE_SQL)
                resolved_count = cursor.rowcount

                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #AfdLagerbeh_Resolved;")
                cursor.execute("UPDATE STATISTICS [fct].[AfdelingLagerbeholdning];")

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN DW_IsDeleted = 1 THEN 1 ELSE 0 END) AS soft_deleted,
                        SUM(CASE WHEN DW_EK_Lager = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_lager,
                        SUM(CASE WHEN DW_EK_Rekvirent = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_rekvirent,
                        SUM(CASE WHEN DW_EK_IlsVare = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_ilsvare,
                        SUM(CASE WHEN DW_EK_UNSPSC = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_unspsc,
                        SUM(CASE WHEN DW_EK_AfdelingLagerbeholdningEgenskaber = -1 AND DW_IsDeleted = 0 THEN 1 ELSE 0 END) AS missing_egenskab
                    FROM [fct].[AfdelingLagerbeholdning]
                """
                )
                diag = cursor.fetchone()
                active = diag[0] - diag[1]
                context.log.info(
                    f"Total={diag[0]}, Active={active}, SoftDeleted={diag[1]}, "
                    f"FK coverage (active): "
                    f"Lager={active-diag[2]}/{active}, "
                    f"Rekvirent={active-diag[3]}/{active}, "
                    f"IlsVare={active-diag[4]}/{active}, "
                    f"UNSPSC={active-diag[5]}/{active}, "
                    f"Egenskab={active-diag[6]}/{active}"
                )
            finally:
                cursor.close()

        result = dg.MaterializeResult(
            metadata={
                "table": "fct.AfdelingLagerbeholdning",
                "row_count": resolved_count,
                "merged": merged,
                "pattern": "MERGE (stable DW_EK + soft-delete)",
                "total_rows": diag[0],
                "soft_deleted": diag[1],
                "missing_lager": diag[2],
                "missing_rekvirent": diag[3],
                "missing_ilsvare": diag[4],
                "missing_unspsc": diag[5],
                "missing_egenskab": diag[6],
            }
        )

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "AfdelingLagerbeholdning", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "AfdelingLagerbeholdning")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=resolved_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
