# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: DRGKontaktEgenskaber.

Transaction dimension: dw_ek_DRGKontakt is the shared PK with
fct.DRGKontakt, resolved from map.DRGKontakt (same pattern as
fct.Vareudlevering/dim.Vareudlevering). Type 1 SCD — overwrite on change.

The attribute column list is DERIVED from the staging column mapping
(drg_kontakt_kolonner.py), so dimension SQL and staging cannot drift; the
static DDL is held to the same list by a unit test.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.staging.drg_kontakt_kolonner import DIM_ATTRIBUTE_NAMES

ATTRIBUTES: tuple[str, ...] = ("leverancetype", "takstsystem", *DIM_ATTRIBUTE_NAMES)

_ALL = ("clean_code_DRGKontakt", *ATTRIBUTES)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in _ALL)
_tgt_cols = ",\n        ".join(f"tgt.[{c}]" for c in _ALL)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _ALL)
_insert_cols = ",\n        ".join(f"[{c}]" for c in _ALL)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[dim_DRGKontaktEgenskaber] ds
LEFT JOIN [map].[DRGKontakt] m
    ON m.[clean_code_DRGKontakt] = ds.[clean_code_DRGKontakt]
WHERE m.[dw_ek_DRGKontakt] IS NULL;
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #DRGKontaktEgenskaber;

SELECT
    m.[dw_ek_DRGKontakt],
    {_src_cols.replace("src.", "ds.")}
INTO #DRGKontaktEgenskaber
FROM [stg].[dim_DRGKontaktEgenskaber] ds
INNER JOIN [map].[DRGKontakt] m
    ON m.[clean_code_DRGKontakt] = ds.[clean_code_DRGKontakt];
"""

MERGE_SQL = f"""
MERGE [dim].[DRGKontaktEgenskaber] AS tgt
USING #DRGKontaktEgenskaber AS src
ON tgt.[dw_ek_DRGKontakt] = src.[dw_ek_DRGKontakt]

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
        [dw_ek_DRGKontakt],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_DRGKontakt],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [dim].[DRGKontaktEgenskaber] WITH (TABLOCK) (
    [dw_ek_DRGKontakt],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_DRGKontakt],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #DRGKontaktEgenskaber src;
"""


@asset(
    group_name="drg_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_DRGKontakt"]),
        AssetKey(["drg", "stg_dim_DRGKontaktEgenskaber"]),
    ],
    tags={"domain": "drg", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description=(
        "Sync dim.DRGKontaktEgenskaber (transaction dimension, shared PK with "
        "the fact) from map + staged attributes, Type 1 overwrite on change"
    ),
)
def dim_DRGKontaktEgenskaber(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "DRGKontaktEgenskaber", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_DRGKontaktEgenskaber: {unmapped_rows} staging rows have no "
                        "match in map.DRGKontakt. Run map_DRGKontakt first."
                    )

                cursor.execute(RESOLVE_SQL)

                # Short-circuit: a target holding only the -1 sentinel takes a
                # minimally logged bulk INSERT; map keys are IDENTITY >= 1 and
                # never collide with the sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[DRGKontaktEgenskaber] "
                    "WHERE [dw_ek_DRGKontakt] <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #DRGKontaktEgenskaber")
                cursor.execute("UPDATE STATISTICS [dim].[DRGKontaktEgenskaber]")

                cursor.execute(
                    "SELECT COUNT(*), "
                    "SUM(CASE WHEN [dw_timestamp_update] <> [dw_timestamp_insert] THEN 1 ELSE 0 END) "
                    "FROM [dim].[DRGKontaktEgenskaber] WHERE [dw_ek_DRGKontakt] <> -1"
                )
                total_dim_rows, updated_count = cursor.fetchone()
                context.log.info(
                    f"dim.DRGKontaktEgenskaber: {rows_affected} affected, "
                    f"{total_dim_rows} total, {updated_count or 0} ever updated"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "DRGKontaktEgenskaber", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "DRGKontaktEgenskaber")
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
            "table": "dim.DRGKontaktEgenskaber",
            "source_tables": "stg.dim_DRGKontaktEgenskaber + map.DRGKontakt",
            "scd_type": "Type 1 (overwrite)",
            "attribute_count": len(ATTRIBUTES),
        }
    )
