# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Map Update: UdredningsretMarkoer.

Assigns dw_ek_UdredningsretMarkoer via IDENTITY on
clean_code_UdredningsretMarkoer = 'UDREDMRK_<markoer_noegle>_<loebenummer>' —
precisely the archive's own merge key, where markoer_noegle is SHA2_256 over
(helbredsforloeb key, marker code, marker date) and loebenummer separates the
rows that share it. The fact row and the archive row are therefore 1:1 by
construction, which is what carries a soft delete straight through to the
fact. SDS' own OBJEKTID_FORLOBSMARKOR is deliberately NOT part of the key: it
is re-issued per delivery, so a re-issue updates the same row instead of
minting a second one.

Keys are minted for live AND soft-deleted staged rows: a retired marker must
keep its dw_ek so it revives on the same fact row when the delivery brings it
back. Append-only like every map — a key once minted stays minted, and a
marker that vanishes is retired in the fact, never unmapped.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

INSERT_SQL = """
INSERT INTO [map].[UdredningsretMarkoer] (
    [clean_code_UdredningsretMarkoer],
    [dw_timestamp_insert]
)
SELECT DISTINCT
    src.[clean_code_UdredningsretMarkoer],
    CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[fct_UdredningsretMarkoer] src
WHERE src.[clean_code_UdredningsretMarkoer] IS NOT NULL
  AND src.[clean_code_UdredningsretMarkoer] <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM [map].[UdredningsretMarkoer] m
    WHERE m.[clean_code_UdredningsretMarkoer] = src.[clean_code_UdredningsretMarkoer]
);
"""


@asset(
    group_name="udredningsret_map",
    compute_kind="mssql",
    deps=[AssetKey(["udredningsret", "stg_fct_UdredningsretMarkoer"])],
    tags={"domain": "udredningsret", "layer": "maps", "module": "datawarehouse"},
    description=(
        "Assign dw_ek_UdredningsretMarkoer via IDENTITY using the archive's own "
        "merge key clean_code_UdredningsretMarkoer (live and soft-deleted "
        "markers alike, append-only)"
    ),
)
def map_UdredningsretMarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "UdredningsretMarkoer", dagster_run_id=context.run_id, mode=config.mode
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
                cursor.execute("SELECT COUNT(*) FROM [map].[UdredningsretMarkoer]")
                total_count = cursor.fetchone()[0]
                context.log.info(
                    f"map.UdredningsretMarkoer: {new_count} new, {total_count} total entries"
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
            "table": "map.UdredningsretMarkoer",
            "natural_key": "clean_code_UdredningsretMarkoer",
            "source_table": "stg.fct_UdredningsretMarkoer",
            "mode": config.mode,
        }
    )
