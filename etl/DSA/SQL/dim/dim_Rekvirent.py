# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Rekvirent

Populate dim.Rekvirent from stg.dim_Rekvirent with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- dw_ek_Rekvirent from map.Rekvirent
- dw_ek_Rekvirent_parent from map.Rekvirent (self-join on parent_code)
- dw_ek_Organisation from ref.Organisation (via code_sor -> sor_code)
- er_undtaget from ref.RekvirentUndtagelse (join on normalized sgh+rnr)
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

# Normalize dim property values the same way ref.RekvirentUndtagelse builds
# sgh/rnr — shared expression, not a hand-copy: the two sides of this join must
# move together, and hand-copying is what let the ILS keys drift apart before.
_SGH_NORM = sql_norm_code("{alias}.[sgh]")
_RNR_NORM = sql_norm_code("{alias}.[rnr]")


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_Rekvirent"]),
        AssetKey(["stg_dim_Rekvirent"]),
        AssetKey(["ref_RekvirentUndtagelse"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "rekvirent", "scd": "type1"},
    description=(
        "Populate dim.Rekvirent (Type 1 SCD) with resolved DW_EK keys and "
        "er_undtaget from ref.RekvirentUndtagelse"
    ),
)
def dim_Rekvirent(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.Rekvirent."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Rekvirent", dagster_run_id=context.run_id, mode=config.mode)

    inserted = 0
    updated = 0
    total = 0
    has_org = 0
    has_parent = 0
    undtaget_count = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building temp table with resolved EKs...")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Rekvirent] stg
                LEFT JOIN [map].[Rekvirent] m
                    ON m.clean_code_Rekvirent = stg.clean_code_Rekvirent
                WHERE m.dw_ek_Rekvirent IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Rekvirent: {unmapped_rows} staging rows have no match in map.Rekvirent. "
                        "Ensure map_Rekvirent runs before dim_Rekvirent and staging CleanCodes are valid."
                    )

                sgh_stg = _SGH_NORM.format(alias="stg")
                rnr_stg = _RNR_NORM.format(alias="stg")

                cursor.execute(
                    f"""
                DROP TABLE IF EXISTS #Rekvirent_Resolved;

                SELECT
                    m.dw_ek_Rekvirent,
                    stg.code,
                    stg.display,
                    stg.parent_code,
                    stg.[level],
                    stg.entity_type,
                    stg.sgh,
                    stg.rgrp,
                    stg.rtyp,
                    stg.rnr,
                    stg.code_sor,
                    stg.sor_relationship,
                    COALESCE(m_parent.dw_ek_Rekvirent, -1) AS dw_ek_Rekvirent_parent,
                    COALESCE(m_org.dw_ek_Organisation, -1) AS dw_ek_Organisation,
                    CAST(CASE WHEN u.clean_code_Rekvirent IS NOT NULL THEN 1 ELSE 0 END AS BIT)
                        AS er_undtaget
                INTO #Rekvirent_Resolved
                FROM [stg].[dim_Rekvirent] stg
                LEFT JOIN [map].[Rekvirent] m
                    ON m.clean_code_Rekvirent = stg.clean_code_Rekvirent
                LEFT JOIN [map].[Rekvirent] m_parent
                    ON m_parent.clean_code_Rekvirent = stg.parent_code
                LEFT JOIN [ref].[Organisation] m_org
                    ON m_org.sor_code = stg.code_sor
                    AND m_org.dw_is_deleted = 0
                    AND stg.code_sor IS NOT NULL
                LEFT JOIN [ref].[RekvirentUndtagelse] u
                    ON u.sgh = {sgh_stg}
                   AND u.rnr = {rnr_stg}
                   AND stg.sgh IS NOT NULL
                   AND stg.rnr IS NOT NULL
            """
                )

                cursor.execute("SELECT COUNT(*) FROM #Rekvirent_Resolved")
                resolved_count = cursor.fetchone()[0]
                context.log.info(f"Resolved {resolved_count} rows for dim.Rekvirent")

                cursor.execute(
                    """
                MERGE INTO [dim].[Rekvirent] AS target
                USING #Rekvirent_Resolved AS source
                ON target.dw_ek_Rekvirent = source.dw_ek_Rekvirent
                WHEN MATCHED THEN
                    UPDATE SET
                        code = source.code,
                        display = source.display,
                        parent_code = source.parent_code,
                        [level] = source.[level],
                        entity_type = source.entity_type,
                        sgh = source.sgh,
                        rgrp = source.rgrp,
                        rtyp = source.rtyp,
                        rnr = source.rnr,
                        code_sor = source.code_sor,
                        sor_relationship = source.sor_relationship,
                        dw_ek_Organisation = source.dw_ek_Organisation,
                        dw_ek_Rekvirent_parent = source.dw_ek_Rekvirent_parent,
                        er_undtaget = source.er_undtaget,
                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Rekvirent, code, display, parent_code, [level],
                        entity_type, sgh, rgrp, rtyp, rnr,
                        code_sor, sor_relationship,
                        dw_ek_Organisation, dw_ek_Rekvirent_parent, er_undtaget,
                        dw_timestamp_insert, dw_timestamp_update
                    )
                    VALUES (
                        source.dw_ek_Rekvirent, source.code, source.display,
                        source.parent_code, source.[level],
                        source.entity_type, source.sgh, source.rgrp, source.rtyp, source.rnr,
                        source.code_sor, source.sor_relationship,
                        source.dw_ek_Organisation, source.dw_ek_Rekvirent_parent,
                        source.er_undtaget,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    )
                OUTPUT $action;
            """
                )

                merge_results = cursor.fetchall()
                inserted = sum(1 for r in merge_results if r[0] == "INSERT")
                updated = sum(1 for r in merge_results if r[0] == "UPDATE")

                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Rekvirent_Resolved")

                cursor.execute("UPDATE STATISTICS [dim].[Rekvirent]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Organisation <> -1 THEN 1 ELSE 0 END) AS has_org,
                    SUM(CASE WHEN dw_ek_Rekvirent_parent <> -1 THEN 1 ELSE 0 END) AS has_parent,
                    SUM(CASE WHEN er_undtaget = 1 THEN 1 ELSE 0 END) AS undtaget_count
                FROM [dim].[Rekvirent]
                WHERE dw_ek_Rekvirent <> -1
            """
                )
                stats = cursor.fetchone()
                total = stats[0]
                has_org = stats[1]
                has_parent = stats[2]
                undtaget_count = stats[3]
            finally:
                cursor.close()

        context.log.info(
            f"dim.Rekvirent: {inserted} inserted, {updated} updated, {total} total "
            f"({has_org} with Organization FK, {has_parent} with Parent FK, "
            f"{undtaget_count} er_undtaget=1)"
        )

        rows_affected = inserted + updated
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Rekvirent", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Rekvirent")
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
            "er_undtaget_count": undtaget_count,
            "table": "dim.Rekvirent",
            "scd_type": "Type 1",
            "mode": config.mode,
        }
    )
