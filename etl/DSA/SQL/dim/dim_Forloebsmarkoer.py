# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dimension Update: Forloebsmarkoer.

Shared code dimension: one member per distinct marker code delivered in
arc.udredningsret_mont (433 measured), independent of forloeb and
helbredsforloeb, so both udredningsret facts point at the same member.
Type 1 SCD — a revised SKS text overwrites the member.

An unknown code is a NORMAL member, never a hard fail: staging already
mints it with tekst 'Ukendt', tekst_kilde 'ukendt' and gruppe 'oevrige'
(decision U7), so a new SDS code shows up in reports the day it arrives.

GDPR: a pure code list — no borger, no CPR, no row-level data.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# DDL order of the descriptive columns (infrastructure/ddl/tables/
# udredningsret/03_dims.sql); a unit test holds the DDL to this tuple.
ATTRIBUTTER: tuple[str, ...] = ("kode", "tekst", "gruppe", "gruppe_tekst", "tekst_kilde")

_ALL = ("clean_code_Forloebsmarkoer", *ATTRIBUTTER)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in _ALL)
_tgt_cols = ",\n        ".join(f"tgt.[{c}]" for c in _ALL)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _ALL)
_insert_cols = ",\n        ".join(f"[{c}]" for c in _ALL)
_ds_cols = ",\n    ".join(f"ds.[{c}]" for c in _ALL)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[dim_Forloebsmarkoer] ds
LEFT JOIN [map].[Forloebsmarkoer] m
    ON m.[clean_code_Forloebsmarkoer] = ds.[clean_code_Forloebsmarkoer]
WHERE m.[dw_ek_Forloebsmarkoer] IS NULL;
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #Forloebsmarkoer;

SELECT
    m.[dw_ek_Forloebsmarkoer],
    {_ds_cols}
INTO #Forloebsmarkoer
FROM [stg].[dim_Forloebsmarkoer] ds
INNER JOIN [map].[Forloebsmarkoer] m
    ON m.[clean_code_Forloebsmarkoer] = ds.[clean_code_Forloebsmarkoer];

-- The MERGE below matches on this key against a target whose clustered PK is
-- the same column; without the key here the resolve set is sorted or hashed on
-- every run. Clustered but NOT unique on purpose, same reasoning as the fact's
-- temp table: a duplicated resolve row must reach the MERGE, not die as a
-- failed index build.
CREATE CLUSTERED INDEX [IX_Forloebsmarkoer]
    ON #Forloebsmarkoer([dw_ek_Forloebsmarkoer]);
"""

MERGE_SQL = f"""
MERGE [dim].[Forloebsmarkoer] AS tgt
USING #Forloebsmarkoer AS src
ON tgt.[dw_ek_Forloebsmarkoer] = src.[dw_ek_Forloebsmarkoer]

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
        [dw_ek_Forloebsmarkoer],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_Forloebsmarkoer],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [dim].[Forloebsmarkoer] WITH (TABLOCK) (
    [dw_ek_Forloebsmarkoer],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_Forloebsmarkoer],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #Forloebsmarkoer src;
"""


@asset(
    group_name="udredningsret_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Forloebsmarkoer"]),
        AssetKey(["udredningsret", "stg_dim_Forloebsmarkoer"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Sync dim.Forloebsmarkoer (shared marker code dimension) from map + "
        "the staged code list with SKS texts and appendix-1 groups, Type 1"
    ),
)
def dim_Forloebsmarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "Forloebsmarkoer", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Forloebsmarkoer: {unmapped_rows} staging rows have no match in "
                        "map.Forloebsmarkoer. Run map_Forloebsmarkoer first."
                    )

                cursor.execute(RESOLVE_SQL)

                # Short-circuit: a target holding only the -1 sentinel takes a
                # minimally logged bulk INSERT; map keys are IDENTITY >= 1 and
                # never collide with the sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Forloebsmarkoer] "
                    "WHERE [dw_ek_Forloebsmarkoer] <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty (only -1 sentinel) - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Forloebsmarkoer")
                cursor.execute("UPDATE STATISTICS [dim].[Forloebsmarkoer]")
                # UPDATE STATISTICS is transactional in SQL Server and the
                # connection is not autocommit, so without this commit the
                # refreshed statistics are rolled back when it closes.
                conn.commit()

                cursor.execute(
                    "SELECT COUNT(*), "
                    "SUM(CASE WHEN [tekst_kilde] = 'ukendt' THEN 1 ELSE 0 END) "
                    "FROM [dim].[Forloebsmarkoer] WHERE [dw_ek_Forloebsmarkoer] <> -1"
                )
                total_dim_rows, uden_sks_tekst = cursor.fetchone()
                context.log.info(
                    f"dim.Forloebsmarkoer: {rows_affected} affected, {total_dim_rows} "
                    f"members, {uden_sks_tekst or 0} without SKS text"
                )
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Forloebsmarkoer", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Forloebsmarkoer")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_members": total_dim_rows,
            "koder_uden_sks_tekst": uden_sks_tekst or 0,
            "table": "dim.Forloebsmarkoer",
            "source_tables": "stg.dim_Forloebsmarkoer + map.Forloebsmarkoer",
            "scd_type": "Type 1 (overwrite)",
            "mode": config.mode,
        }
    )
