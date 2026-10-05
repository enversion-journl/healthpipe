# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for Okosted (Omkostningssted).

Populates dim.Okosted with Type 1 SCD.
Resolves parent DW_EK from the self-referencing hierarchy.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.map.map_Okosted import map_Okosted
from DSA.SQL.staging.stg_Okosted import stg_Okosted


@dg.asset(
    name="dim_Okosted",
    deps=["staging_infra_erp", map_Okosted, stg_Okosted],
    group_name="dwh_okosted",
    compute_kind="mssql",
    description="Populate dim.Okosted (Type 1 SCD)",
)
def dim_Okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_Okosted: list[dict],
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Populate dim.Okosted (Type 1 SCD)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Okosted", dagster_run_id=context.run_id, mode=config.mode)
    logged_cp: str | None = None
    merge_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT clean_code_Okosted, dw_ek_Okosted FROM [map].[Okosted]")
                clean_code_to_ek = {row[0]: row[1] for row in cursor.fetchall()}
            finally:
                cursor.close()

        context.log.info(
            f"Loaded {len(clean_code_to_ek)} clean_code_Okosted -> dw_ek_Okosted mappings"
        )

        dim_rows = []
        for entry in stg_Okosted:
            clean_code = entry["code"]
            dw_ek = clean_code_to_ek.get(clean_code)

            if not dw_ek:
                context.log.warning(f"No DW_EK found for {clean_code}")
                continue

            dw_ek_parent = -1
            if entry.get("parent_code"):
                parent_clean_code = entry["parent_code"]
                looked_up_ek = clean_code_to_ek.get(parent_clean_code)
                if looked_up_ek is not None:
                    dw_ek_parent = looked_up_ek

            dim_rows.append(
                {
                    "dw_ek": dw_ek,
                    "code_okosted": entry["code"],
                    "name": entry.get("display"),
                    "level": entry.get("level"),
                    "dw_ek_parent": dw_ek_parent,
                    "parent_code": entry.get("parent_code"),
                    "active": entry["aktiv"],
                }
            )

        context.log.info(f"Processing {len(dim_rows)} rows for dim.Okosted")

        dim_tuples = [
            (
                row["dw_ek"],
                row["code_okosted"],
                row["name"],
                row["level"],
                row["dw_ek_parent"],
                row["parent_code"],
                row["active"],
            )
            for row in dim_rows
        ]

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                is_mssql_python = type(conn).__module__.startswith("mssql_python")
                placeholder = "?" if is_mssql_python else "%s"

                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True

                cursor.execute(
                    """
                    CREATE TABLE #staging_dim_okosted (
                        dw_ek_Okosted INT NOT NULL,
                        code_okosted VARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL,
                        name NVARCHAR(500) COLLATE DATABASE_DEFAULT,
                        [level] INT,
                        dw_ek_Okosted_parent INT,
                        parent_code VARCHAR(50) COLLATE DATABASE_DEFAULT,
                        active BIT NOT NULL
                    )
                """
                )

                batch_size = 1000
                total_inserted = 0

                placeholders = ", ".join([placeholder] * 7)
                insert_sql = (
                    f"INSERT INTO #staging_dim_okosted ("
                    f"dw_ek_Okosted, code_okosted, name, [level], "
                    f"dw_ek_Okosted_parent, parent_code, active"
                    f") VALUES ({placeholders})"
                )

                for i in range(0, len(dim_tuples), batch_size):
                    batch = dim_tuples[i : i + batch_size]
                    cursor.executemany(insert_sql, batch)
                    total_inserted += len(batch)

                context.log.info(f"Staged all {total_inserted} dimension rows in temp table")

                cursor.execute(
                    """
                    MERGE INTO [dim].[Okosted] AS target
                    USING #staging_dim_okosted AS source
                    ON target.dw_ek_Okosted = source.dw_ek_Okosted
                    WHEN MATCHED THEN
                        UPDATE SET
                            code_okosted = source.code_okosted,
                            name = source.name,
                            [level] = source.[level],
                            dw_ek_Okosted_parent = source.dw_ek_Okosted_parent,
                            parent_code = source.parent_code,
                            active = source.active,
                            dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    WHEN NOT MATCHED THEN
                        INSERT (dw_ek_Okosted, code_okosted, name, [level],
                                dw_ek_Okosted_parent, parent_code, active,
                                dw_timestamp_insert)
                        VALUES (source.dw_ek_Okosted, source.code_okosted,
                                source.name, source.[level],
                                source.dw_ek_Okosted_parent, source.parent_code,
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

        context.log.info(f"dim.Okosted: {inserted} inserted, {updated} updated")

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Okosted", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "Okosted")

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=merge_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": len(dim_rows),
            "method": "bulk_insert_with_merge",
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
