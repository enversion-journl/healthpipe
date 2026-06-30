# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: UNSPSC (Type 1)

Business logic identical to DimUpd_UNSPSC.py but uses DfmManager for checkpoint
management and run logging.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

BUILD_SQL = """
DROP TABLE IF EXISTS #UNSPSC_Resolved;

SELECT
    m.DW_EK_UNSPSC,
    m.CleanCode_UNSPSC                                                 AS UNSPSC_Kode,
    m.DW_Source_ILS                                                    AS Source_ILS,
    m.DW_Source_KMD                                                    AS Source_KMD,
    LTRIM(RTRIM(REPLACE(u.[betegn],                       CHAR(160), ''))) AS Beskrivelse_ILS,
    LTRIM(RTRIM(REPLACE(u.[un_name],                      CHAR(160), ''))) AS Beskrivelse_engelsk_ILS,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassDescription],       CHAR(160), ''))) AS Beskrivelse_KMD,
    k.[UNSPSCLevel]                                                    AS UNSPSC_Level_KMD,
    k.[UNSPSCClassLev1ID]                                              AS UNSPSC_Level_1_Kode_KMD,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev1Description],   CHAR(160), ''))) AS UNSPSC_Level_1_Beskrivelse_KMD,
    k.[UNSPSCClassLev2ID]                                              AS UNSPSC_Level_2_Kode_KMD,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev2Description],   CHAR(160), ''))) AS UNSPSC_Level_2_Beskrivelse_KMD,
    k.[UNSPSCClassLev3ID]                                              AS UNSPSC_Level_3_Kode_KMD,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev3Description],   CHAR(160), ''))) AS UNSPSC_Level_3_Beskrivelse_KMD,
    k.[UNSPSCClassLev4ID]                                              AS UNSPSC_Level_4_Kode_KMD,
    LTRIM(RTRIM(REPLACE(k.[UNSPSCClassLev4Description],   CHAR(160), ''))) AS UNSPSC_Level_4_Beskrivelse_KMD
INTO #UNSPSC_Resolved
FROM (
    SELECT CleanCode_UNSPSC FROM [stg].[dim_unspsc_ils]
    UNION
    SELECT CleanCode_UNSPSC FROM [stg].[dim_unspsc_kmd]
) delta
LEFT JOIN [map].[UNSPSC] m
    ON m.CleanCode_UNSPSC = delta.CleanCode_UNSPSC

LEFT JOIN [arc].[unspsc_ils] u
    ON u.[unspsc]         = m.CleanCode_UNSPSC

LEFT JOIN [arc].[unspsc_kmd_uddata] k
    ON k.[UNSPSCClassID] = m.CleanCode_UNSPSC AND k.[DW_IsCurrent] = 1;

CREATE UNIQUE CLUSTERED INDEX IX_tmp ON #UNSPSC_Resolved (DW_EK_UNSPSC);
"""

MERGE_SQL = """
MERGE INTO [dim].[UNSPSC] AS tgt
USING #UNSPSC_Resolved AS src
ON tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC
WHEN MATCHED THEN UPDATE SET
    tgt.UNSPSC_Kode = src.UNSPSC_Kode,
    tgt.Source_ILS = src.Source_ILS,
    tgt.Source_KMD = src.Source_KMD,
    tgt.Beskrivelse_ILS = src.Beskrivelse_ILS,
    tgt.Beskrivelse_engelsk_ILS = src.Beskrivelse_engelsk_ILS,
    tgt.Beskrivelse_KMD = src.Beskrivelse_KMD,
    tgt.UNSPSC_Level_KMD = src.UNSPSC_Level_KMD,
    tgt.UNSPSC_Level_1_Kode_KMD = src.UNSPSC_Level_1_Kode_KMD,
    tgt.UNSPSC_Level_1_Beskrivelse_KMD = src.UNSPSC_Level_1_Beskrivelse_KMD,
    tgt.UNSPSC_Level_2_Kode_KMD = src.UNSPSC_Level_2_Kode_KMD,
    tgt.UNSPSC_Level_2_Beskrivelse_KMD = src.UNSPSC_Level_2_Beskrivelse_KMD,
    tgt.UNSPSC_Level_3_Kode_KMD = src.UNSPSC_Level_3_Kode_KMD,
    tgt.UNSPSC_Level_3_Beskrivelse_KMD = src.UNSPSC_Level_3_Beskrivelse_KMD,
    tgt.UNSPSC_Level_4_Kode_KMD = src.UNSPSC_Level_4_Kode_KMD,
    tgt.UNSPSC_Level_4_Beskrivelse_KMD = src.UNSPSC_Level_4_Beskrivelse_KMD,
    tgt.DW_Timestamp_Update = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN INSERT (
    DW_EK_UNSPSC, UNSPSC_Kode, Source_ILS, Source_KMD,
    Beskrivelse_ILS, Beskrivelse_engelsk_ILS,
    Beskrivelse_KMD, UNSPSC_Level_KMD,
    UNSPSC_Level_1_Kode_KMD, UNSPSC_Level_1_Beskrivelse_KMD,
    UNSPSC_Level_2_Kode_KMD, UNSPSC_Level_2_Beskrivelse_KMD,
    UNSPSC_Level_3_Kode_KMD, UNSPSC_Level_3_Beskrivelse_KMD,
    UNSPSC_Level_4_Kode_KMD, UNSPSC_Level_4_Beskrivelse_KMD,
    DW_Timestamp_Insert, DW_Timestamp_Update
) VALUES (
    src.DW_EK_UNSPSC, src.UNSPSC_Kode, src.Source_ILS, src.Source_KMD,
    src.Beskrivelse_ILS, src.Beskrivelse_engelsk_ILS,
    src.Beskrivelse_KMD, src.UNSPSC_Level_KMD,
    src.UNSPSC_Level_1_Kode_KMD, src.UNSPSC_Level_1_Beskrivelse_KMD,
    src.UNSPSC_Level_2_Kode_KMD, src.UNSPSC_Level_2_Beskrivelse_KMD,
    src.UNSPSC_Level_3_Kode_KMD, src.UNSPSC_Level_3_Beskrivelse_KMD,
    src.UNSPSC_Level_4_Kode_KMD, src.UNSPSC_Level_4_Beskrivelse_KMD,
    SYSDATETIME(), SYSDATETIME()
);
"""


@dg.asset(
    name="dim_UNSPSC",
    group_name="dimensions_unspsc",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["map_UNSPSC"]),
        dg.AssetKey(["erp", "stg_dim_unspsc_ils"]),
        dg.AssetKey(["erp", "stg_dim_unspsc_kmd"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "scd": "type1", "entity": "unspsc"},
    description="Type 1 MERGE into dim.UNSPSC; advances DFM checkpoint on success.",
)
def dim_UNSPSC(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "UNSPSC",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL)
                cursor.execute("SELECT COUNT(*) FROM #UNSPSC_Resolved")
                resolved_count = cursor.fetchone()[0]

                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount

                cursor.execute("DROP TABLE IF EXISTS #UNSPSC_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[UNSPSC];")
                cursor.execute("SELECT COUNT(*) FROM [dim].[UNSPSC] WHERE DW_EK_UNSPSC <> -1")
                total = cursor.fetchone()[0]

                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"dim.UNSPSC: resolved={resolved_count}, merged={merged}, total={total}")

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "UNSPSC", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "UNSPSC")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merged
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "resolved": resolved_count,
            "merged": merged,
            "total": total,
            "target_table": "dim.UNSPSC",
            "scd_type": "Type 1",
        }
    )
