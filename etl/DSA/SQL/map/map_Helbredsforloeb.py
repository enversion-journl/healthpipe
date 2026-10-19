# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: Helbredsforloeb.

Assigns dw_ek_Helbredsforloeb via IDENTITY on clean_code_Helbredsforloeb —
the archive's own SHA2_256 key over (CPR, the first marker's code, date,
unit and forloebselement uuid), so the grouping survives SDS' renumbering of
DW_EK_HelbredsForloeb between deliveries. Append-only: the map is never
merged, updated or deleted, so a key once minted stays minted.

EVERY helbredsforloeb in scope is minted, including one whose markers are
all soft-deleted: the fact needs its key to retire the forloeb rows that
disappeared with them (spec section 6).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[Helbredsforloeb] (
    [clean_code_Helbredsforloeb],
    [dw_timestamp_insert]
)
SELECT DISTINCT
    src.[clean_code_Helbredsforloeb],
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[pre_Helbredsforloeb] src
WHERE src.[clean_code_Helbredsforloeb] IS NOT NULL
  AND src.[clean_code_Helbredsforloeb] <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM [map].[Helbredsforloeb] m
    WHERE m.[clean_code_Helbredsforloeb] = src.[clean_code_Helbredsforloeb]
);
"""


@asset(
    group_name="udredningsret_map",
    compute_kind="mssql",
    deps=[AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"])],
    tags={"domain": "udredningsret", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_Helbredsforloeb via IDENTITY using the hash-based "
        "clean_code_Helbredsforloeb (append-only, stable across deliveries)"
    ),
)
def map_Helbredsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "Helbredsforloeb", dagster_run_id=context.run_id, mode=config.mode
    )

    new_count = 0
    total_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(INSERT_SQL)
                new_count = cursor.rowcount
                conn.commit()
                cursor.execute("SELECT COUNT(*) FROM [map].[Helbredsforloeb]")
                total_count = cursor.fetchone()[0]
                context.log.info(
                    f"map.Helbredsforloeb: {new_count} new, {total_count} total entries"
                )
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Helbredsforloeb",
            "natural_key": "clean_code_Helbredsforloeb",
            "source_table": "stg.pre_Helbredsforloeb",
            "mode": config.mode,
        }
    )
