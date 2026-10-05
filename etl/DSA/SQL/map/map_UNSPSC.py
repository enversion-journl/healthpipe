# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: UNSPSC

Business logic identical to map_UNSPSC.py but uses DfmManager for run logging.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

MERGE_SQL = """
MERGE INTO [map].[UNSPSC] AS m
USING [stg].[dim_UNSPSC] AS s
ON m.clean_code_UNSPSC = s.clean_code_UNSPSC
WHEN MATCHED AND (
       (m.dw_source_kmd   = 0 AND s.dw_source_kmd   = 1)
    OR (m.dw_source_ils   = 0 AND s.dw_source_ils   = 1)
    OR (m.dw_source_udbud = 0 AND s.dw_source_udbud = 1)
) THEN UPDATE SET
    m.dw_source_kmd   = CASE WHEN m.dw_source_kmd   = 1 THEN 1 ELSE s.dw_source_kmd   END,
    m.dw_source_ils   = CASE WHEN m.dw_source_ils   = 1 THEN 1 ELSE s.dw_source_ils   END,
    m.dw_source_udbud = CASE WHEN m.dw_source_udbud = 1 THEN 1 ELSE s.dw_source_udbud END,
    m.dw_timestamp_update = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN INSERT (
    clean_code_UNSPSC, dw_source_ils, dw_source_kmd, dw_source_udbud,
    dw_timestamp_insert, dw_timestamp_update
) VALUES (
    s.clean_code_UNSPSC, s.dw_source_ils, s.dw_source_kmd, s.dw_source_udbud,
    SYSDATETIME(), SYSDATETIME()
);
"""


@dg.asset(
    name="map_UNSPSC",
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["erp", "stg_dim_UNSPSC"]),
    ],
    tags={"domain": "erp", "layer": "maps", "entity": "unspsc"},
    description="Map upsert for UNSPSC: insert new codes, flip sticky source flags.",
)
def map_UNSPSC(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    if config.mode != "delta":
        context.log.warning(
            f"Map asset running with mode={config.mode!r} — map tables always process "
            f"the entire upstream staging table regardless of mode."
        )

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map",
        "UNSPSC",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount
                cursor.execute("SELECT COUNT(*) FROM [map].[UNSPSC] WHERE dw_ek_UNSPSC <> -1")
                total = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"map.UNSPSC: merged={merged}, total={total}")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=merged)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "merged": merged,
            "total": total,
            "target_table": "map.UNSPSC",
        }
    )
