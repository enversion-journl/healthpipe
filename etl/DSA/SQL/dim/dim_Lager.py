# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Lager

Populate dim.Lager from stg.dim_Lager with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- dw_ek_Lager from map.Lager
- dw_ek_Lager_parent from map.Lager (self-join on parent_code)
- dw_ek_Organisation from ref.Organisation (via code_sor -> sor_code)
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_Lager"]),
        AssetKey(["stg_dim_Lager"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Populate dim.Lager (Type 1 SCD) with resolved DW_EK keys",
)
def dim_Lager(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.Lager."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Lager", dagster_run_id=context.run_id, mode=config.mode)

    inserted = 0
    updated = 0
    total = 0
    has_org = 0
    has_parent = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building temp table with resolved EKs...")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Lager] stg
                LEFT JOIN [map].[Lager] m
                    ON m.clean_code_Lager = stg.clean_code_Lager
                WHERE m.dw_ek_Lager IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Lager: {unmapped_rows} staging rows have no match in map.Lager. "
                        "Ensure map_Lager runs before dim_Lager and staging CleanCodes are valid."
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Lager_Resolved;

                SELECT
                    m.dw_ek_Lager,
                    stg.code,
                    stg.display,
                    stg.parent_code,
                    stg.[level],
                    stg.entity_type,
                    stg.sgh,
                    stg.lgr,
                    stg.adr,
                    stg.land,
                    stg.postdi,
                    stg.tlf,
                    stg.code_sor,
                    COALESCE(m_parent.dw_ek_Lager, -1) AS dw_ek_Lager_parent,
                    COALESCE(m_org.dw_ek_Organisation, -1) AS dw_ek_Organisation
                INTO #Lager_Resolved
                FROM [stg].[dim_Lager] stg
                LEFT JOIN [map].[Lager] m
                    ON m.clean_code_Lager = stg.clean_code_Lager
                LEFT JOIN [map].[Lager] m_parent
                    ON m_parent.clean_code_Lager = stg.parent_code
                LEFT JOIN [ref].[Organisation] m_org
                    ON m_org.sor_code = stg.code_sor
                    AND m_org.dw_is_deleted = 0
                    AND stg.code_sor IS NOT NULL
            """
                )

                cursor.execute("SELECT COUNT(*) FROM #Lager_Resolved")
                resolved_count = cursor.fetchone()[0]
                context.log.info(f"Resolved {resolved_count} rows for dim.Lager")

                cursor.execute(
                    """
                MERGE INTO [dim].[Lager] AS target
                USING #Lager_Resolved AS source
                ON target.dw_ek_Lager = source.dw_ek_Lager
                WHEN MATCHED THEN
                    UPDATE SET
                        code = source.code,
                        display = source.display,
                        parent_code = source.parent_code,
                        [level] = source.[level],
                        entity_type = source.entity_type,
                        sgh = source.sgh,
                        lgr = source.lgr,
                        adr = source.adr,
                        land = source.land,
                        postdi = source.postdi,
                        tlf = source.tlf,
                        code_sor = source.code_sor,
                        dw_ek_Organisation = source.dw_ek_Organisation,
                        dw_ek_Lager_parent = source.dw_ek_Lager_parent,
                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Lager, code, display, parent_code, [level],
                        entity_type, sgh, lgr, adr, land, postdi, tlf,
                        code_sor, dw_ek_Organisation, dw_ek_Lager_parent,
                        dw_timestamp_insert, dw_timestamp_update
                    )
                    VALUES (
                        source.dw_ek_Lager, source.code, source.display,
                        source.parent_code, source.[level],
                        source.entity_type, source.sgh, source.lgr,
                        source.adr, source.land, source.postdi, source.tlf,
                        source.code_sor,
                        source.dw_ek_Organisation, source.dw_ek_Lager_parent,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    )
                OUTPUT $action;
            """
                )

                merge_results = cursor.fetchall()
                inserted = sum(1 for r in merge_results if r[0] == "INSERT")
                updated = sum(1 for r in merge_results if r[0] == "UPDATE")

                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Lager_Resolved")

                cursor.execute("UPDATE STATISTICS [dim].[Lager]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org,
                    SUM(CASE WHEN dw_ek_Lager_parent <> -1 THEN 1 ELSE 0 END) AS has_parent
                FROM [dim].[Lager]
                WHERE dw_ek_Lager <> -1
            """
                )
                stats = cursor.fetchone()
                total = stats[0]
                has_org = stats[1]
                has_parent = stats[2]
            finally:
                cursor.close()

        context.log.info(
            f"dim.Lager: {inserted} inserted, {updated} updated, {total} total "
            f"({has_org} with Organization FK, {has_parent} with Parent FK)"
        )

        rows_affected = inserted + updated
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Lager", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Lager")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_organization_fk": has_org,
            "has_parent_fk": has_parent,
            "table": "dim.Lager",
            "scd_type": "Type 1",
            "mode": config.mode,
        }
    )
