# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: Forloebsmarkoer.

Assigns dw_ek_Forloebsmarkoer via IDENTITY on
clean_code_Forloebsmarkoer = 'FORLOEBSMARKOER_' + UPPER(TRIM(code)): one key
per distinct marker code as delivered (433 measured). The key's own
normalisation — not the collation — decides identity, so two case variants of
one code can never mint two rows. Append-only like every map; a code that
stops being delivered keeps its key.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[Forloebsmarkoer] (
    [clean_code_Forloebsmarkoer],
    [dw_timestamp_insert]
)
SELECT DISTINCT
    src.[clean_code_Forloebsmarkoer],
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[dim_Forloebsmarkoer] src
WHERE src.[clean_code_Forloebsmarkoer] IS NOT NULL
  AND src.[clean_code_Forloebsmarkoer] <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM [map].[Forloebsmarkoer] m
    WHERE m.[clean_code_Forloebsmarkoer] = src.[clean_code_Forloebsmarkoer]
);
"""


@asset(
    group_name="udredningsret_map",
    compute_kind="mssql",
    deps=[AssetKey(["udredningsret", "stg_dim_Forloebsmarkoer"])],
    tags={"domain": "udredningsret", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_Forloebsmarkoer via IDENTITY using the normalised "
        "clean_code_Forloebsmarkoer (one key per delivered marker code)"
    ),
)
def map_Forloebsmarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "Forloebsmarkoer", dagster_run_id=context.run_id, mode=config.mode
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
                cursor.execute("SELECT COUNT(*) FROM [map].[Forloebsmarkoer]")
                total_count = cursor.fetchone()[0]
                context.log.info(
                    f"map.Forloebsmarkoer: {new_count} new, {total_count} total entries"
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
            "table": "map.Forloebsmarkoer",
            "natural_key": "clean_code_Forloebsmarkoer",
            "source_table": "stg.dim_Forloebsmarkoer",
            "mode": config.mode,
        }
    )
