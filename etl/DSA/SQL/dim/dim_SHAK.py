# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for SHAK (Sygehus-Afdelingsklassifikation).

Populates dim.SHAK with Type 1 SCD. Resolves parent DW_EK from the
self-referencing hierarchy via map.SHAK.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.map.map_SHAK import map_SHAK
from DSA.SQL.staging.stg_SHAK import stg_SHAK


@dg.asset(
    name="dim_SHAK",
    deps=["staging_infra_erp", map_SHAK, stg_SHAK],
    group_name="dwh_shak",
    compute_kind="mssql",
    description="Populate dim.SHAK (Type 1 SCD)",
)
def dim_SHAK(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_SHAK: list[dict],
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Populate dim.SHAK (Type 1 SCD)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "SHAK", dagster_run_id=context.run_id, mode=config.mode)
    logged_cp: str | None = None
    merge_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT clean_code_SHAK, dw_ek_SHAK FROM [map].[SHAK]")
                clean_code_to_ek = {row[0]: row[1] for row in cursor.fetchall()}
            finally:
                cursor.close()

        context.log.info(f"Loaded {len(clean_code_to_ek)} clean_code_SHAK -> dw_ek_SHAK mappings")

        # map_SHAK runs before dim_SHAK, so every staged code must resolve to a
        # DW_EK — a miss is a data-integrity error, not something to skip.
        missing = [entry["code"] for entry in stg_SHAK if not clean_code_to_ek.get(entry["code"])]
        if missing:
            raise dg.Failure(
                description=(
                    f"{len(missing)} SHAK code(s) have no dw_ek_SHAK in map.SHAK "
                    f"(sample: {missing[:5]})"
                )
            )

        dim_tuples = []
        for entry in stg_SHAK:
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

        context.log.info(f"Processing {len(dim_tuples)} rows for dim.SHAK")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                is_mssql_python = type(conn).__module__.startswith("mssql_python")
                placeholder = "?" if is_mssql_python else "%s"

                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True

                cursor.execute(
                    """
                    CREATE TABLE #staging_dim_shak (
                        dw_ek_SHAK INT NOT NULL,
                        code_shak VARCHAR(50) COLLATE DATABASE_DEFAULT NOT NULL,
                        name NVARCHAR(500) COLLATE DATABASE_DEFAULT,
                        [level] INT,
                        dw_ek_SHAK_parent INT,
                        parent_code VARCHAR(50) COLLATE DATABASE_DEFAULT,
                        active BIT NOT NULL
                    )
                    """
                )

                placeholders = ", ".join([placeholder] * 7)
                insert_sql = (
                    f"INSERT INTO #staging_dim_shak ("
                    f"dw_ek_SHAK, code_shak, name, [level], "
                    f"dw_ek_SHAK_parent, parent_code, active"
                    f") VALUES ({placeholders})"
                )

                batch_size = 1000
                for i in range(0, len(dim_tuples), batch_size):
                    cursor.executemany(insert_sql, dim_tuples[i : i + batch_size])

                cursor.execute(
                    """
                    MERGE INTO [dim].[SHAK] AS target
                    USING #staging_dim_shak AS source
                    ON target.dw_ek_SHAK = source.dw_ek_SHAK
                    WHEN MATCHED THEN
                        UPDATE SET
                            code_shak = source.code_shak,
                            name = source.name,
                            [level] = source.[level],
                            dw_ek_SHAK_parent = source.dw_ek_SHAK_parent,
                            parent_code = source.parent_code,
                            active = source.active,
                            dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    WHEN NOT MATCHED THEN
                        INSERT (dw_ek_SHAK, code_shak, name, [level],
                                dw_ek_SHAK_parent, parent_code, active,
                                dw_timestamp_insert)
                        VALUES (source.dw_ek_SHAK, source.code_shak,
                                source.name, source.[level],
                                source.dw_ek_SHAK_parent, source.parent_code,
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

        context.log.info(f"dim.SHAK: {inserted} inserted, {updated} updated")

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "SHAK", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "SHAK")

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
