# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: DRGKontaktKlassifikation.

Assigns dw_ek_DRGKontaktKlassifikation via IDENTITY on the hashed
clean_code over the full classification-attribute combination (computed once
in staging, so map, dimension and fact read the exact same expression).
Append-only like every map.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[DRGKontaktKlassifikation] (
    [clean_code_DRGKontaktKlassifikation],
    [dw_timestamp_insert]
)
SELECT DISTINCT
    src.[clean_code_DRGKontaktKlassifikation],
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[dim_DRGKontaktEgenskaber] src
WHERE src.[clean_code_DRGKontaktKlassifikation] IS NOT NULL
  AND src.[clean_code_DRGKontaktKlassifikation] <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM [map].[DRGKontaktKlassifikation] m
    WHERE m.[clean_code_DRGKontaktKlassifikation] = src.[clean_code_DRGKontaktKlassifikation]
);
"""


@asset(
    group_name="drg_map",
    compute_kind="mssql",
    deps=[AssetKey(["drg", "stg_dim_DRGKontaktEgenskaber"])],
    tags={"domain": "drg", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_DRGKontaktKlassifikation via IDENTITY using the hashed "
        "clean_code over the classification-attribute combination"
    ),
)
def map_DRGKontaktKlassifikation(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "DRGKontaktKlassifikation", dagster_run_id=context.run_id, mode=config.mode
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
                cursor.execute("SELECT COUNT(*) FROM [map].[DRGKontaktKlassifikation]")
                total_count = cursor.fetchone()[0]
                context.log.info(
                    f"map.DRGKontaktKlassifikation: {new_count} new, {total_count} total"
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
            "table": "map.DRGKontaktKlassifikation",
            "natural_key": "clean_code_DRGKontaktKlassifikation",
            "source_table": "stg.dim_DRGKontaktEgenskaber",
            "mode": config.mode,
        }
    )
