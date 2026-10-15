# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for SHAK-a-like (EPJI location.external_id).

Populates dim.SHAKAlike with Type 1 SCD. Resolves parent DW_EK from the
self-referencing hierarchy via map.SHAKAlike. SOR identifier changes do not
alter this dimension — they flow through conceptmap.SHAKAlikeToSOR.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.map.map_SHAKAlike import map_SHAKAlike
from DSA.SQL.staging.stg_SHAKAlike import stg_SHAKAlike


@dg.asset(
    name="dim_SHAKAlike",
    deps=["staging_infra_erp", map_SHAKAlike, stg_SHAKAlike],
    group_name="dwh_shakalike",
    compute_kind="mssql",
    description="Populate dim.SHAKAlike (Type 1 SCD)",
)
def dim_SHAKAlike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_SHAKAlike: list[dict],
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "SHAKAlike", dagster_run_id=context.run_id, mode=config.mode)
    logged_cp: str | None = None
    merge_count = 0
    inserted = 0
    updated = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT clean_code_SHAKAlike, dw_ek_SHAKAlike FROM [map].[SHAKAlike]"
                )
                clean_code_to_ek = {row[0]: row[1] for row in cursor.fetchall()}
            finally:
                cursor.close()

        context.log.info(f"Loaded {len(clean_code_to_ek)} clean_code_SHAKAlike -> dw_ek_SHAKAlike")

        missing = [
            entry["code"] for entry in stg_SHAKAlike if not clean_code_to_ek.get(entry["code"])
        ]
        if missing:
            raise dg.Failure(
                description=(
                    f"{len(missing)} SHAK-a-like code(s) have no dw_ek_SHAKAlike "
                    f"in map.SHAKAlike (sample: {missing[:5]})"
                )
            )

        dim_tuples = []
        for entry in stg_SHAKAlike:
            clean_code = entry["code"]
            dw_ek = clean_code_to_ek[clean_code]
            dw_ek_parent = -1
            if entry.get("parent_code"):
                looked_up = clean_code_to_ek.get(entry["parent_code"])
                if looked_up is not None:
                    dw_ek_parent = looked_up
            dim_tuples.append(
                (
                    dw_ek,
                    entry["code"],
                    entry.get("display"),
                    entry.get("level"),
                    dw_ek_parent,
                    entry.get("parent_code"),
                    entry["aktiv"],
                )
            )

        context.log.info(f"Processing {len(dim_tuples)} rows for dim.SHAKAlike")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                is_mssql_python = type(conn).__module__.startswith("mssql_python")
                placeholder = "?" if is_mssql_python else "%s"

                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True

                cursor.execute(
                    """
                    CREATE TABLE #staging_dim_shakalike (
                        dw_ek_SHAKAlike INT NOT NULL,
                        code_shakalike VARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL,
                        name NVARCHAR(500) COLLATE DATABASE_DEFAULT,
                        [level] INT,
                        dw_ek_SHAKAlike_parent INT,
                        parent_code VARCHAR(50) COLLATE DATABASE_DEFAULT,
                        active BIT NOT NULL
                    )
                    """
                )

                placeholders = ", ".join([placeholder] * 7)
                insert_sql = (
                    f"INSERT INTO #staging_dim_shakalike ("
                    f"dw_ek_SHAKAlike, code_shakalike, name, [level], "
                    f"dw_ek_SHAKAlike_parent, parent_code, active"
                    f") VALUES ({placeholders})"
                )

                batch_size = 1000
                for i in range(0, len(dim_tuples), batch_size):
                    cursor.executemany(insert_sql, dim_tuples[i : i + batch_size])

                cursor.execute(
                    """
                    MERGE INTO [dim].[SHAKAlike] AS target
                    USING #staging_dim_shakalike AS source
                    ON target.dw_ek_SHAKAlike = source.dw_ek_SHAKAlike
                    WHEN MATCHED THEN
                        UPDATE SET
                            code_shakalike = source.code_shakalike,
                            name = source.name,
                            [level] = source.[level],
                            dw_ek_SHAKAlike_parent = source.dw_ek_SHAKAlike_parent,
                            parent_code = source.parent_code,
                            active = source.active,
                            dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    WHEN NOT MATCHED THEN
                        INSERT (dw_ek_SHAKAlike, code_shakalike, name, [level],
                                dw_ek_SHAKAlike_parent, parent_code, active,
                                dw_timestamp_insert)
                        VALUES (source.dw_ek_SHAKAlike, source.code_shakalike,
                                source.name, source.[level],
                                source.dw_ek_SHAKAlike_parent, source.parent_code,
                                source.active,
                                CAST(SYSDATETIME() AS datetime2(0)))
                    OUTPUT $action;
                    """
                )
                merge_results = cursor.fetchall()
                inserted = sum(1 for r in merge_results if r[0] == "INSERT")
                updated = sum(1 for r in merge_results if r[0] == "UPDATE")
                merge_count = inserted + updated
                conn.commit()
            finally:
                cursor.close()

        context.log.info(f"dim.SHAKAlike: {inserted} inserted, {updated} updated")

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "SHAKAlike", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "SHAKAlike")

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=merge_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": len(dim_tuples),
            "method": "bulk_insert_with_merge",
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
