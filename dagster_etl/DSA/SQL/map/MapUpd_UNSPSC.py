# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: UNSPSC

Business logic identical to MapUpd_UNSPSC.py but uses DfmManager for run logging.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

INSERT_SQL = """
INSERT INTO [map].[UNSPSC] (
    CleanCode_UNSPSC, DW_Source_ILS, DW_Source_KMD,
    DW_Timestamp_Insert, DW_Timestamp_Update
)
SELECT
    f.CleanCode_UNSPSC,
    CAST(MAX(f.from_ils) AS BIT) AS DW_Source_ILS,
    CAST(MAX(f.from_kmd) AS BIT) AS DW_Source_KMD,
    SYSDATETIME(),
    SYSDATETIME()
FROM (
    SELECT CleanCode_UNSPSC, 1 AS from_ils, 0 AS from_kmd FROM [stg].[dim_unspsc_ils]
    UNION ALL
    SELECT CleanCode_UNSPSC, 0,             1             FROM [stg].[dim_unspsc_kmd]
) f
WHERE NOT EXISTS (
    SELECT 1 FROM [map].[UNSPSC] m WHERE m.CleanCode_UNSPSC = f.CleanCode_UNSPSC
)
GROUP BY f.CleanCode_UNSPSC;
"""

UPDATE_FLAG_ILS_SQL = """
UPDATE m
SET m.DW_Source_ILS = 1,
    m.DW_Timestamp_Update = SYSDATETIME()
FROM [map].[UNSPSC] m
WHERE m.DW_Source_ILS = 0
  AND EXISTS (SELECT 1 FROM [stg].[dim_unspsc_ils] s WHERE s.CleanCode_UNSPSC = m.CleanCode_UNSPSC);
"""

UPDATE_FLAG_KMD_SQL = """
UPDATE m
SET m.DW_Source_KMD = 1,
    m.DW_Timestamp_Update = SYSDATETIME()
FROM [map].[UNSPSC] m
WHERE m.DW_Source_KMD = 0
  AND EXISTS (SELECT 1 FROM [stg].[dim_unspsc_kmd] s WHERE s.CleanCode_UNSPSC = m.CleanCode_UNSPSC);
"""


@dg.asset(
    name="map_UNSPSC",
    group_name="map_unspsc",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["erp", "stg_dim_unspsc_ils"]),
        dg.AssetKey(["erp", "stg_dim_unspsc_kmd"]),
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
                cursor.execute(INSERT_SQL)
                inserted = cursor.rowcount
                cursor.execute(UPDATE_FLAG_ILS_SQL)
                flipped_ils = cursor.rowcount
                cursor.execute(UPDATE_FLAG_KMD_SQL)
                flipped_kmd = cursor.rowcount
                cursor.execute("SELECT COUNT(*) FROM [map].[UNSPSC] WHERE DW_EK_UNSPSC <> -1")
                total = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(
            f"map.UNSPSC: inserted={inserted}, flipped_ils={flipped_ils}, "
            f"flipped_kmd={flipped_kmd}, total={total}"
        )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=inserted)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "flipped_ils": flipped_ils,
            "flipped_kmd": flipped_kmd,
            "total": total,
            "target_table": "map.UNSPSC",
        }
    )
