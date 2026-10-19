# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Finanskladde

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial journal headers.

Uses:
- map.Finanskladde: dw_ek_Finanskladde assignment via IDENTITY
- stg.dim_Finanskladde_sydax: Deduplicated finanskladde attributes at clean_code_Finanskladde grain

Business key: bk_ref_ledgerjournaltable_id
Descriptive attributes: kladdenummer, kladdetekst, kladdenavn, er_lukket, data_area_id, maskinnummer
Date keys: dw_ek_Dato_oprettet (CreatedOn), dw_ek_Dato_aendret (Modified)
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetKey
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@dg.asset(
    name="dim_Finanskladde",
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        "map_Finanskladde",
        AssetKey(["erp", "stg_dim_Finanskladde_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=("Sync dim.Finanskladde from map + add contextual attributes from SydAX staging"),
)
def dim_Finanskladde(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "Finanskladde", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Finanskladde dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladde]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Finanskladde entries to sync")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Finanskladde_sydax] fs
                LEFT JOIN [map].[Finanskladde] m
                    ON m.clean_code_Finanskladde = fs.clean_code_Finanskladde
                WHERE m.dw_ek_Finanskladde IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Finanskladde_sydax rows without map match"
                    )

                cursor.execute(
                    f"""
                DROP TABLE IF EXISTS #Finanskladde_Attributes;

                SELECT
                    m.dw_ek_Finanskladde,
                    fs.bk_ref_ledgerjournaltable_id,
                    fs.kladdenummer,
                    fs.kladdetekst,
                    fs.kladdenavn,
                    fs.er_lukket,
                    fs.data_area_id,
                    fs.maskinnummer,
                    {dato_key('fs.CreatedOn')} AS dw_ek_Dato_oprettet,
                    {dato_tid('fs.CreatedOn')} AS dato_tid_oprettet,
                    {dato_key('fs.Modified')} AS dw_ek_Dato_aendret,
                    {dato_tid('fs.Modified')} AS dato_tid_aendret
                INTO #Finanskladde_Attributes
                FROM [stg].[dim_Finanskladde_sydax] fs
                LEFT JOIN [map].[Finanskladde] m
                    ON m.clean_code_Finanskladde = fs.clean_code_Finanskladde
                WHERE m.dw_ek_Finanskladde IS NOT NULL;
            """
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladde_Attributes
                ON #Finanskladde_Attributes (dw_ek_Finanskladde)
                """
                )
                context.log.info("Temp table #Finanskladde_Attributes created")

                cursor.execute(
                    """
                MERGE [dim].[Finanskladde] AS tgt
                USING #Finanskladde_Attributes AS src
                ON tgt.dw_ek_Finanskladde = src.dw_ek_Finanskladde

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.bk_ref_ledgerjournaltable_id = src.bk_ref_ledgerjournaltable_id,
                        tgt.kladdenummer = src.kladdenummer,
                        tgt.kladdetekst = src.kladdetekst,
                        tgt.kladdenavn = src.kladdenavn,
                        tgt.er_lukket = src.er_lukket,
                        tgt.data_area_id = src.data_area_id,
                        tgt.maskinnummer = src.maskinnummer,
                        tgt.dw_ek_Dato_oprettet = src.dw_ek_Dato_oprettet,
                        tgt.dato_tid_oprettet = src.dato_tid_oprettet,
                        tgt.dw_ek_Dato_aendret = src.dw_ek_Dato_aendret,
                        tgt.dato_tid_aendret = src.dato_tid_aendret,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Finanskladde,
                        bk_ref_ledgerjournaltable_id,
                        kladdenummer,
                        kladdetekst,
                        kladdenavn,
                        er_lukket,
                        data_area_id,
                        maskinnummer,
                        dw_ek_Dato_oprettet,
                        dato_tid_oprettet,
                        dw_ek_Dato_aendret,
                        dato_tid_aendret,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    VALUES (
                        src.dw_ek_Finanskladde,
                        src.bk_ref_ledgerjournaltable_id,
                        src.kladdenummer,
                        src.kladdetekst,
                        src.kladdenavn,
                        src.er_lukket,
                        src.data_area_id,
                        src.maskinnummer,
                        src.dw_ek_Dato_oprettet,
                        src.dato_tid_oprettet,
                        src.dw_ek_Dato_aendret,
                        src.dato_tid_aendret,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """
                )

                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Finanskladde_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Finanskladde]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_dim_rows,
                    COUNT(kladdenavn) AS has_kladdenavn,
                    COUNT(kladdetekst) AS has_kladdetekst
                FROM [dim].[Finanskladde]
                WHERE dw_ek_Finanskladde <> -1
            """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                has_kladdenavn = stats[1]
                has_kladdetekst = stats[2]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows in dim.Finanskladde — "
                    f"coverage: kladdenavn={has_kladdenavn}/{total_dim_rows}, "
                    f"kladdetekst={has_kladdetekst}/{total_dim_rows}"
                )
            finally:
                cursor.close()

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Finanskladde", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Finanskladde")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merge_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "rows_affected": merge_count,
            "total_dim_rows": total_dim_rows,
            "has_kladdenavn": has_kladdenavn,
            "has_kladdetekst": has_kladdetekst,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Finanskladde",
            "source_tables": "stg.dim_Finanskladde_sydax + map.Finanskladde",
            "scd_type": "Type 1 (overwrite)",
        }
    )
