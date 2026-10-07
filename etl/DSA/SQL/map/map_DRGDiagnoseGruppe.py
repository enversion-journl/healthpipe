# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: DRGDiagnoseGruppe.

Assigns dw_ek_DRGDiagnoseGruppe via IDENTITY on the hashed clean_code over a
contact's DISTINCT set of diagnoses (computed once in the group staging).
Append-only: a set-hash's meaning is immutable, so a group is minted once and
never changes.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[DRGDiagnoseGruppe] ([clean_code_DRGDiagnoseGruppe], [dw_timestamp_insert])
SELECT DISTINCT src.[clean_code_DRGDiagnoseGruppe], CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[DRGDiagnoseGruppeMedlem] src
WHERE src.[clean_code_DRGDiagnoseGruppe] IS NOT NULL
  AND src.[clean_code_DRGDiagnoseGruppe] <> ''
  AND NOT EXISTS (
    SELECT 1 FROM [map].[DRGDiagnoseGruppe] m
    WHERE m.[clean_code_DRGDiagnoseGruppe] = src.[clean_code_DRGDiagnoseGruppe]
  );
"""


@asset(
    group_name="drg_map",
    compute_kind="mssql",
    deps=[AssetKey(["drg", "stg_DRGDiagnoseGruppe"])],
    tags={"domain": "drg", "layer": "maps", "module": "datawarehouse"},
    description="Assign dw_ek_DRGDiagnoseGruppe via IDENTITY on the hashed diagnose-set code",
)
def map_DRGDiagnoseGruppe(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "DRGDiagnoseGruppe", dagster_run_id=context.run_id, mode=config.mode
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
                cursor.execute("SELECT COUNT(*) FROM [map].[DRGDiagnoseGruppe]")
                total_count = cursor.fetchone()[0]
                context.log.info(f"map.DRGDiagnoseGruppe: {new_count} new, {total_count} total")
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
            "table": "map.DRGDiagnoseGruppe",
            "natural_key": "clean_code_DRGDiagnoseGruppe",
            "source_table": "stg.DRGDiagnoseGruppeMedlem",
            "mode": config.mode,
        }
    )
