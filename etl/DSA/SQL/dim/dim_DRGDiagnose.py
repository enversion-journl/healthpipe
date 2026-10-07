# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: DRGDiagnose (code dimension).

One member per (diagnose_kode, art, sideangivelse, tillaegskoder) — the
diagnosis-as-recorded, independent of contact. The attributes are
functionally determined by the clean_code (the key IS their hash), so the
dimension is append-only: a member's attributes never change, only new
members are inserted. Shared PK dw_ek_DRGDiagnose with map.DRGDiagnose.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# One representative attribute row per new clean_code (attributes are FD of
# the key, so any representative is correct — ROW_NUMBER keeps it deterministic).
INSERT_SQL = """
INSERT INTO [dim].[DRGDiagnose] (
    [dw_ek_DRGDiagnose], [clean_code_DRGDiagnose],
    [diagnose_kode], [art], [sideangivelse], [tillaegskoder],
    [dw_timestamp_insert], [dw_timestamp_update]
)
SELECT
    m.[dw_ek_DRGDiagnose], s.[clean_code_DRGDiagnose],
    s.[diagnose_kode], s.[art], s.[sideangivelse], s.[tillaegskoder],
    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM (
    SELECT [clean_code_DRGDiagnose], [diagnose_kode], [art], [sideangivelse], [tillaegskoder],
           ROW_NUMBER() OVER (PARTITION BY [clean_code_DRGDiagnose]
                              ORDER BY (SELECT NULL)) AS rn
    FROM [stg].[bridge_DRGKontaktDiagnose]
) AS s
INNER JOIN [map].[DRGDiagnose] m ON m.[clean_code_DRGDiagnose] = s.[clean_code_DRGDiagnose]
WHERE s.rn = 1
  AND NOT EXISTS (
    SELECT 1 FROM [dim].[DRGDiagnose] d WHERE d.[dw_ek_DRGDiagnose] = m.[dw_ek_DRGDiagnose]
  );
"""

UNMAPPED_SQL = """
SELECT COUNT(*) FROM (
    SELECT DISTINCT s.[clean_code_DRGDiagnose]
    FROM [stg].[bridge_DRGKontaktDiagnose] s
    LEFT JOIN [map].[DRGDiagnose] m ON m.[clean_code_DRGDiagnose] = s.[clean_code_DRGDiagnose]
    WHERE m.[dw_ek_DRGDiagnose] IS NULL
) AS u;
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    deps=[AssetKey(["map_DRGDiagnose"]), AssetKey(["drg", "stg_bridge_DRGKontaktDiagnose"])],
    tags={"domain": "drg", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Sync dim.DRGDiagnose (code dimension) — append-only, attributes FD of the key",
)
def dim_DRGDiagnose(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "DRGDiagnose", dagster_run_id=context.run_id, mode=config.mode
    )
    rows_affected = 0
    total_rows = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped = cursor.fetchone()[0]
                if unmapped > 0:
                    raise ValueError(
                        f"dim_DRGDiagnose: {unmapped} combination codes have no match in "
                        "map.DRGDiagnose. Run map_DRGDiagnose first."
                    )
                cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.execute(
                    "SELECT COUNT(*) FROM [dim].[DRGDiagnose] WHERE [dw_ek_DRGDiagnose] <> -1"
                )
                total_rows = cursor.fetchone()[0]
                context.log.info(f"dim.DRGDiagnose: {rows_affected} new, {total_rows} members")
            finally:
                cursor.close()
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "DRGDiagnose", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "DRGDiagnose")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_members": total_rows,
            "table": "dim.DRGDiagnose",
            "source_tables": "stg.bridge_DRGKontaktDiagnose + map.DRGDiagnose",
            "scd_type": "append-only (attributes FD of key)",
        }
    )
