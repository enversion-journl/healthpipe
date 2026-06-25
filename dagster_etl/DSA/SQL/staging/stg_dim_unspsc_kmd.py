# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: stg.dim_unspsc_kmd

Business logic identical to stg_dim_unspsc_kmd.py.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

BUILD_SQL = """
DECLARE @wm NVARCHAR(50) = ?;

DROP TABLE IF EXISTS [stg].[dim_unspsc_kmd];

SELECT DISTINCT [UNSPSCClassID] AS CleanCode_UNSPSC
INTO [stg].[dim_unspsc_kmd]
FROM [arc].[unspsc_kmd_uddata]
WHERE [DW_IsCurrent] = 1
  AND [UNSPSCClassID] IS NOT NULL
  AND (@wm IS NULL OR [DW_Timestamp_Update] > CAST(@wm AS DATETIME2));

CREATE CLUSTERED INDEX IX_stg_dim_unspsc_kmd_cc
    ON [stg].[dim_unspsc_kmd](CleanCode_UNSPSC);
"""


@dg.asset(
    key=dg.AssetKey(["erp", "stg_dim_unspsc_kmd"]),
    group_name="staging_unspsc",
    compute_kind="mssql",
    deps=[dg.AssetKey(["arc_unspsc_kmd_uddata"])],
    tags={"domain": "erp", "layer": "staging", "entity": "unspsc"},
    description="Distinct UNSPSCClassID from arc KMD table.",
)
def stg_dim_unspsc_kmd(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_unspsc_kmd",
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
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_unspsc_kmd]")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"stg.dim_unspsc_kmd: {row_count} distinct codes")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "target_table": "stg.dim_unspsc_kmd",
            "checkpoint_used": checkpoint,
            "mode": config.mode,
        }
    )
