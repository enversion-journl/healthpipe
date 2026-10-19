# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: Udredningsforloeb.

Assigns dw_ek_Udredningsforloeb via IDENTITY on clean_code_Udredningsforloeb
— the readable key 'UDREDFORL_<helbredsforloeb key>_<startdato>_<loebenummer>'
built in staging, where loebenummer is OUR per-(helbredsforloeb, startdato)
sequence and never SDS' F_ID/PK, which are renumbered per delivery. The key
is reused from the persisted map/dimension identity in staging; new
courses receive slots above the historical maximum, including retired courses.

It is the SHARED primary key of fct.Udredningsforloeb and
dim.UdredningsretEgenskaber, so both read it from this one map. Append-only
like every map: a key once minted stays minted, and a forloeb that vanishes
is retired in the fact, never unmapped.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[Udredningsforloeb] (
    [clean_code_Udredningsforloeb],
    [dw_timestamp_insert]
)
SELECT DISTINCT
    src.[clean_code_Udredningsforloeb],
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[dim_UdredningsretEgenskaber] src
WHERE src.[clean_code_Udredningsforloeb] IS NOT NULL
  AND src.[clean_code_Udredningsforloeb] <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM [map].[Udredningsforloeb] m
    WHERE m.[clean_code_Udredningsforloeb] = src.[clean_code_Udredningsforloeb]
);
"""


@asset(
    group_name="udredningsret_map",
    compute_kind="mssql",
    deps=[AssetKey(["udredningsret", "stg_dim_UdredningsretEgenskaber"])],
    tags={"domain": "udredningsret", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_Udredningsforloeb via IDENTITY using the readable "
        "clean_code_Udredningsforloeb (shared PK of the fact and the "
        "transaction dimension, append-only)"
    ),
)
def map_Udredningsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "Udredningsforloeb", dagster_run_id=context.run_id, mode=config.mode
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
                cursor.execute("SELECT COUNT(*) FROM [map].[Udredningsforloeb]")
                total_count = cursor.fetchone()[0]
                context.log.info(
                    f"map.Udredningsforloeb: {new_count} new, {total_count} total entries"
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
            "table": "map.Udredningsforloeb",
            "natural_key": "clean_code_Udredningsforloeb",
            "source_table": "stg.dim_UdredningsretEgenskaber",
            "mode": config.mode,
        }
    )
