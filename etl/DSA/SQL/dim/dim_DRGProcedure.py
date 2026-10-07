# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: DRGProcedure (code dimension).

One member per (procedure_kode, sideangivelse, handlingsspec, kontrast,
tillaegskoder) — the procedure-as-recorded, independent of contact. No
timestamps and no producing unit (those are per-registration on the
bridge). Append-only: attributes are functionally determined by the
clean_code. Shared PK dw_ek_DRGProcedure with map.DRGProcedure.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [dim].[DRGProcedure] (
    [dw_ek_DRGProcedure], [clean_code_DRGProcedure],
    [procedure_kode], [sideangivelse], [handlingsspec], [kontrast], [tillaegskoder],
    [dw_timestamp_insert], [dw_timestamp_update]
)
SELECT
    m.[dw_ek_DRGProcedure], s.[clean_code_DRGProcedure],
    s.[procedure_kode], s.[sideangivelse], s.[handlingsspec], s.[kontrast], s.[tillaegskoder],
    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM (
    SELECT [clean_code_DRGProcedure], [procedure_kode], [sideangivelse],
           [handlingsspec], [kontrast], [tillaegskoder],
           ROW_NUMBER() OVER (PARTITION BY [clean_code_DRGProcedure]
                              ORDER BY (SELECT NULL)) AS rn
    FROM [stg].[bridge_DRGKontaktProcedure]
) AS s
INNER JOIN [map].[DRGProcedure] m ON m.[clean_code_DRGProcedure] = s.[clean_code_DRGProcedure]
WHERE s.rn = 1
  AND NOT EXISTS (
    SELECT 1 FROM [dim].[DRGProcedure] d WHERE d.[dw_ek_DRGProcedure] = m.[dw_ek_DRGProcedure]
  );
"""

UNMAPPED_SQL = """
SELECT COUNT(*) FROM (
    SELECT DISTINCT s.[clean_code_DRGProcedure]
    FROM [stg].[bridge_DRGKontaktProcedure] s
    LEFT JOIN [map].[DRGProcedure] m ON m.[clean_code_DRGProcedure] = s.[clean_code_DRGProcedure]
    WHERE m.[dw_ek_DRGProcedure] IS NULL
) AS u;
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    deps=[AssetKey(["map_DRGProcedure"]), AssetKey(["drg", "stg_bridge_DRGKontaktProcedure"])],
    tags={"domain": "drg", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Sync dim.DRGProcedure (code dimension) — append-only, attributes FD of the key",
)
def dim_DRGProcedure(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "DRGProcedure", dagster_run_id=context.run_id, mode=config.mode
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
                        f"dim_DRGProcedure: {unmapped} combination codes have no match in "
                        "map.DRGProcedure. Run map_DRGProcedure first."
                    )
                cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount
                conn.commit()
                cursor.execute(
                    "SELECT COUNT(*) FROM [dim].[DRGProcedure] WHERE [dw_ek_DRGProcedure] <> -1"
                )
                total_rows = cursor.fetchone()[0]
                context.log.info(f"dim.DRGProcedure: {rows_affected} new, {total_rows} members")
            finally:
                cursor.close()
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "DRGProcedure", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "DRGProcedure")
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
            "table": "dim.DRGProcedure",
            "source_tables": "stg.bridge_DRGKontaktProcedure + map.DRGProcedure",
            "scd_type": "append-only (attributes FD of key)",
        }
    )
