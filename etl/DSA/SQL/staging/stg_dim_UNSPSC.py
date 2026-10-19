# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: stg.dim_UNSPSC (consolidated delta)

Builds a single delta set of UNSPSC codes from all archive sources (KMD, three
ILS legs), each filtered on dw_timestamp_update > the shared dim.UNSPSC
checkpoint. One row per clean_code_UNSPSC, with the per-source presence flags
dw_source_kmd / dw_source_ils aggregated via MAX.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

BUILD_SQL = """
DECLARE @wm NVARCHAR(50) = ?;

DROP TABLE IF EXISTS [stg].[dim_UNSPSC];

;WITH delta AS (
    SELECT [UNSPSCClassID] AS clean_code_UNSPSC, 1 AS KMD, 0 AS ILS
    FROM [arc].[kmd_uddata_unspsc]
    WHERE [DW_IsCurrent] = 1 AND [UNSPSCClassID] IS NOT NULL
      AND (@wm IS NULL OR [dw_timestamp_update] > CAST(@wm AS DATETIME2))
    UNION ALL
    SELECT [unspsc], 0, 1
    FROM [arc].[ils_unspsc]
    WHERE [unspsc] IS NOT NULL
      AND (@wm IS NULL OR [dw_timestamp_update] > CAST(@wm AS DATETIME2))
    UNION ALL
    SELECT [unspc], 0, 1
    FROM [arc].[ils_vare]
    WHERE [unspc] IS NOT NULL
      AND (@wm IS NULL OR [dw_timestamp_update] > CAST(@wm AS DATETIME2))
    UNION ALL
    SELECT CASE WHEN [unspsc2] LIKE '%.0' THEN LEFT([unspsc2], LEN([unspsc2]) - 2) ELSE [unspsc2] END,
           0, 1
    FROM [arc].[ils_vare]
    WHERE [unspsc2] IS NOT NULL AND LTRIM(RTRIM([unspsc2])) <> ''
      AND (@wm IS NULL OR [dw_timestamp_update] > CAST(@wm AS DATETIME2))
)
SELECT
    LTRIM(RTRIM(clean_code_UNSPSC)) AS clean_code_UNSPSC,
    CAST(MAX(KMD)   AS BIT) AS dw_source_kmd,
    CAST(MAX(ILS)   AS BIT) AS dw_source_ils
INTO [stg].[dim_UNSPSC]
FROM delta
WHERE LTRIM(RTRIM(clean_code_UNSPSC)) <> ''
GROUP BY LTRIM(RTRIM(clean_code_UNSPSC));

CREATE CLUSTERED INDEX IX_stg_dim_unspsc_cc ON [stg].[dim_UNSPSC](clean_code_UNSPSC);
"""


@dg.asset(
    key=dg.AssetKey(["erp", "stg_dim_UNSPSC"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_kmd_uddata_unspsc"]),
        dg.AssetKey(["arc_ils_unspsc"]),
        dg.AssetKey(["arc_ils_vare"]),
    ],
    tags={"domain": "erp", "layer": "staging", "entity": "unspsc"},
    description="Consolidated delta set of UNSPSC codes from all arc sources with source-exists flags.",
)
def stg_dim_UNSPSC(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_UNSPSC",
        dagster_run_id=context.run_id,
        mode=config.mode,
        checkpoint_from=("dim", "UNSPSC"),
    )

    try:
        checkpoint = run_ctx.checkpoint

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL, (checkpoint,))
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_UNSPSC]")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"stg.dim_UNSPSC: {row_count} delta codes (checkpoint={checkpoint})")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "target_table": "stg.dim_UNSPSC",
            "checkpoint_used": checkpoint,
            "mode": config.mode,
        }
    )
