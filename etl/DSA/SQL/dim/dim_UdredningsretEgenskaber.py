# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: UdredningsretEgenskaber.

Transaction dimension: dw_ek_Udredningsforloeb is the SHARED PK with
fct.Udredningsforloeb, resolved from map.Udredningsforloeb (same pattern as
dim.DRGKontaktEgenskaber/fct.DRGKontakt), so there is exactly one row here
per fact row and the fact carries no separate egenskaber key. Type 1 SCD —
overwrite on change.

The attribute column list is DERIVED from EGENSKABER_ATTRIBUTTER in
udredningsret_kolonner.py, so dimension SQL and staging cannot drift; the
static DDL is held to the same list by a unit test.

The dimension never retires: it has no dw_is_deleted column. A forloeb that
vanishes from the delivery is retired on the fact side (spec section 6);
its attribute row stays as the last known description of the key.

GDPR: no borger key and no CPR — identity lives on the fact's side.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.staging.udredningsret_kolonner import EGENSKABER_ATTRIBUTTER

ATTRIBUTTER: tuple[str, ...] = EGENSKABER_ATTRIBUTTER

_ALL = ("clean_code_Udredningsforloeb", *ATTRIBUTTER)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in _ALL)
_tgt_cols = ",\n        ".join(f"tgt.[{c}]" for c in _ALL)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _ALL)
_insert_cols = ",\n        ".join(f"[{c}]" for c in _ALL)
_ds_cols = ",\n    ".join(f"ds.[{c}]" for c in _ALL)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[dim_UdredningsretEgenskaber] ds
LEFT JOIN [map].[Udredningsforloeb] m
    ON m.[clean_code_Udredningsforloeb] = ds.[clean_code_Udredningsforloeb]
WHERE m.[dw_ek_Udredningsforloeb] IS NULL;
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #UdredningsretEgenskaber;

SELECT
    m.[dw_ek_Udredningsforloeb],
    {_ds_cols}
INTO #UdredningsretEgenskaber
FROM [stg].[dim_UdredningsretEgenskaber] ds
INNER JOIN [map].[Udredningsforloeb] m
    ON m.[clean_code_Udredningsforloeb] = ds.[clean_code_Udredningsforloeb];

-- The MERGE below matches on this key against a target whose clustered PK is
-- the same column; without the key here the resolve set is sorted or hashed on
-- every run. Clustered but NOT unique on purpose, same reasoning as the fact's
-- temp table: a duplicated resolve row must reach the MERGE, not die as a
-- failed index build.
CREATE CLUSTERED INDEX [IX_UdredningsretEgenskaber]
    ON #UdredningsretEgenskaber([dw_ek_Udredningsforloeb]);
"""

MERGE_SQL = f"""
MERGE [dim].[UdredningsretEgenskaber] AS tgt
USING #UdredningsretEgenskaber AS src
ON tgt.[dw_ek_Udredningsforloeb] = src.[dw_ek_Udredningsforloeb]

WHEN MATCHED AND EXISTS (
    SELECT
        {_src_cols}
    EXCEPT
    SELECT
        {_tgt_cols}
) THEN
    UPDATE SET
        {_updates},
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [dw_ek_Udredningsforloeb],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_Udredningsforloeb],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [dim].[UdredningsretEgenskaber] WITH (TABLOCK) (
    [dw_ek_Udredningsforloeb],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_Udredningsforloeb],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #UdredningsretEgenskaber src;
"""


@asset(
    group_name="udredningsret_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Udredningsforloeb"]),
        AssetKey(["udredningsret", "stg_dim_UdredningsretEgenskaber"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Sync dim.UdredningsretEgenskaber (transaction dimension, shared PK "
        "with fct.Udredningsforloeb) from map + staged attributes, Type 1 "
        "overwrite on change"
    ),
)
def dim_UdredningsretEgenskaber(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "UdredningsretEgenskaber", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_UdredningsretEgenskaber: {unmapped_rows} staging rows have no "
                        "match in map.Udredningsforloeb. Run map_Udredningsforloeb first."
                    )

                cursor.execute(RESOLVE_SQL)

                # Short-circuit: a target holding only the -1 sentinel takes a
                # minimally logged bulk INSERT; map keys are IDENTITY >= 1 and
                # never collide with the sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[UdredningsretEgenskaber] "
                    "WHERE [dw_ek_Udredningsforloeb] <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #UdredningsretEgenskaber")
                cursor.execute("UPDATE STATISTICS [dim].[UdredningsretEgenskaber]")
                # UPDATE STATISTICS is transactional in SQL Server and the
                # connection is not autocommit, so without this commit the
                # refreshed statistics are rolled back when it closes.
                conn.commit()

                cursor.execute(
                    "SELECT COUNT(*), "
                    "SUM(CASE WHEN [dw_timestamp_update] <> [dw_timestamp_insert] THEN 1 ELSE 0 END) "
                    "FROM [dim].[UdredningsretEgenskaber] WHERE [dw_ek_Udredningsforloeb] <> -1"
                )
                total_dim_rows, updated_count = cursor.fetchone()
                context.log.info(
                    f"dim.UdredningsretEgenskaber: {rows_affected} affected, "
                    f"{total_dim_rows} total, {updated_count or 0} ever updated"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "UdredningsretEgenskaber", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "UdredningsretEgenskaber")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "table": "dim.UdredningsretEgenskaber",
            "source_tables": "stg.dim_UdredningsretEgenskaber + map.Udredningsforloeb",
            "scd_type": "Type 1 (overwrite)",
            "attribute_count": len(ATTRIBUTTER),
            "mode": config.mode,
        }
    )
