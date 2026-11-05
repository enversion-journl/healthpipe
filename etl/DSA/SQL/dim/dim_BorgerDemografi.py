# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Append-only demographic combinations from every dim.Borger version, keyed by its hash."""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.borgerdemografi import ATTRIBUTES, borger_attribute_sql

# Display values use the same normalisation as the hash input on dim.Borger.
_projection = ", ".join(f"{borger_attribute_sql(c)} AS [{c}]" for c in ATTRIBUTES)
SOURCE_SQL = f"""
SELECT DISTINCT b.dw_ek_BorgerDemografi, {_projection}
INTO #BorgerDemografi
FROM [dim].[Borger] b;
CREATE UNIQUE CLUSTERED INDEX IX_BorgerDemografi_source
    ON #BorgerDemografi(dw_ek_BorgerDemografi);
"""

LOCK_SQL = """
IF @@TRANCOUNT = 0 BEGIN TRANSACTION;
DECLARE @result INT;
EXEC @result = sys.sp_getapplock
    @Resource = N'dim.BorgerDemografi', @LockMode = 'Exclusive',
    @LockOwner = 'Transaction', @LockTimeout = 60000;
IF @result < 0 THROW 51000, 'BorgerDemografi writer lock unavailable', 1;
"""

_cols = ", ".join(f"[{c}]" for c in ATTRIBUTES)
_src_cols = ", ".join(f"s.[{c}]" for c in ATTRIBUTES)
INSERT_SQL = f"""
INSERT INTO [dim].[BorgerDemografi] (dw_ek_BorgerDemografi, {_cols})
SELECT s.dw_ek_BorgerDemografi, {_src_cols}
FROM #BorgerDemografi s
WHERE NOT EXISTS (
    SELECT 1 FROM [dim].[BorgerDemografi] d WITH (UPDLOCK, HOLDLOCK)
    WHERE d.dw_ek_BorgerDemografi = s.dw_ek_BorgerDemografi
);
"""


@dg.asset(
    group_name="shared_dimensions",
    compute_kind="mssql",
    deps=["dim_Borger", "staging_infra_shared_dimensions"],
    tags={"domain": "shared_dimensions", "layer": "dimensions", "scd": "append_only"},
    description="Stable demographic combinations from all dim.Borger versions; no citizen keys",
)
def dim_BorgerDemografi(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "BorgerDemografi", dagster_run_id=context.run_id, mode=config.mode
    )
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(LOCK_SQL)
                cursor.execute(SOURCE_SQL)
                cursor.execute(INSERT_SQL)
                new_rows = cursor.rowcount
                cursor.execute("SELECT COUNT(*) FROM [dim].[BorgerDemografi]")
                total_rows = cursor.fetchone()[0]
                cursor.execute("DROP TABLE #BorgerDemografi")
                cursor.execute("UPDATE STATISTICS [dim].[BorgerDemografi]")
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                cursor.close()
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_rows)
    except Exception as exc:
        # Database errors can carry citizen values; only the type is exposed.
        error_type = type(exc).__name__
        dfm.fail_dfm_run(run_ctx.log_id, error_message=error_type)
        raise dg.Failure(description=f"BorgerDemografi failed ({error_type})") from None
    return dg.MaterializeResult(
        metadata={
            "new_combinations": new_rows,
            "total_combinations": total_rows,
            "table": "dim.BorgerDemografi",
        }
    )
