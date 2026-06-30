# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: stg.dim_unspsc_ils

Business logic identical to stg_dim_unspsc_ils.py. Reads from arc tables,
uses DfmManager for checkpoint and run logging.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

BUILD_SQL = """
DECLARE @wm NVARCHAR(50) = ?;

DROP TABLE IF EXISTS [stg].[dim_unspsc_ils];

WITH ils_codes AS (
    SELECT [unspsc] AS CleanCode_UNSPSC
    FROM [arc].[unspsc_ils]
    WHERE [unspsc] IS NOT NULL
      AND (@wm IS NULL OR [DW_Timestamp_Update] > CAST(@wm AS DATETIME2))
    UNION ALL
    SELECT [unspc] AS CleanCode_UNSPSC
    FROM [arc].[vare_ils]
    WHERE [unspc] IS NOT NULL
      AND (@wm IS NULL OR [DW_Timestamp_Update] > CAST(@wm AS DATETIME2))
    UNION ALL
    SELECT
        CASE WHEN [unspsc2] LIKE '%.0' THEN LEFT([unspsc2], LEN([unspsc2]) - 2) ELSE [unspsc2] END
            AS CleanCode_UNSPSC
    FROM [arc].[vare_ils]
    WHERE [unspsc2] IS NOT NULL AND LTRIM(RTRIM([unspsc2])) <> ''
      AND (@wm IS NULL OR [DW_Timestamp_Update] > CAST(@wm AS DATETIME2))
)
SELECT DISTINCT CleanCode_UNSPSC
INTO [stg].[dim_unspsc_ils]
FROM ils_codes
WHERE LTRIM(RTRIM(CleanCode_UNSPSC)) <> '';

CREATE CLUSTERED INDEX IX_stg_dim_unspsc_ils_cc
    ON [stg].[dim_unspsc_ils](CleanCode_UNSPSC);
"""


@dg.asset(
    key=dg.AssetKey(["erp", "stg_dim_unspsc_ils"]),
    group_name="staging_unspsc",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_unspsc_ils"]),
        dg.AssetKey(["arc_vare_ils"]),
    ],
    tags={"domain": "erp", "layer": "staging", "entity": "unspsc"},
    description="Build delta set of ILS UNSPSC codes from arc tables.",
)
def stg_dim_unspsc_ils(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_unspsc_ils",
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
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_unspsc_ils]")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(
            f"stg.dim_unspsc_ils: {row_count} distinct codes (checkpoint={checkpoint})"
        )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "target_table": "stg.dim_unspsc_ils",
            "checkpoint_used": checkpoint,
            "mode": config.mode,
        }
    )
