# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Finanskladdelinje

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial journal lines.

Uses:
- map.Finanskladdelinje: dw_ek_Finanskladdelinje assignment via IDENTITY
- stg.dim_Finanskladdelinje_sydax: Deduplicated attributes at clean_code_Finanskladdelinje grain

Business key: bk_ref_ledgertrans_id
Descriptive attributes: beskrivelse, linjenummer, ekspeditionsnummer,
  transaktionstypenummer, transaktionstypetekst, maskinnummer
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="dim_Finanskladdelinje",
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_Finanskladdelinje_sydax"]),
        "map_Finanskladdelinje",
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Sync dim.Finanskladdelinje from map + add contextual attributes from SydAX staging"
    ),
)
def dim_Finanskladdelinje(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "Finanskladdelinje", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Finanskladdelinje dimension sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladdelinje]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Finanskladdelinje entries to sync")

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Finanskladdelinje_Attributes;

                SELECT
                    m.dw_ek_Finanskladdelinje,
                    ls.bk_ref_ledgertrans_id,
                    ls.beskrivelse,
                    ls.linjenummer,
                    ls.ekspeditionsnummer,
                    ls.transaktionstypenummer,
                    ls.transaktionstypetekst,
                    ls.maskinnummer
                INTO #Finanskladdelinje_Attributes
                FROM [stg].[dim_Finanskladdelinje_sydax] ls
                LEFT JOIN [map].[Finanskladdelinje] m
                    ON m.clean_code_Finanskladdelinje = ls.clean_code_Finanskladdelinje
                WHERE m.dw_ek_Finanskladdelinje IS NOT NULL;
            """
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladdelinje_Attributes
                ON #Finanskladdelinje_Attributes (dw_ek_Finanskladdelinje)
                """
                )
                context.log.info("Temp table #Finanskladdelinje_Attributes created")

                cursor.execute(
                    """
                MERGE [dim].[Finanskladdelinje] AS tgt
                USING #Finanskladdelinje_Attributes AS src
                ON tgt.dw_ek_Finanskladdelinje = src.dw_ek_Finanskladdelinje

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.bk_ref_ledgertrans_id = src.bk_ref_ledgertrans_id,
                        tgt.beskrivelse = src.beskrivelse,
                        tgt.linjenummer = src.linjenummer,
                        tgt.ekspeditionsnummer = src.ekspeditionsnummer,
                        tgt.transaktionstypenummer = src.transaktionstypenummer,
                        tgt.transaktionstypetekst = src.transaktionstypetekst,
                        tgt.maskinnummer = src.maskinnummer,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Finanskladdelinje,
                        bk_ref_ledgertrans_id,
                        beskrivelse,
                        linjenummer,
                        ekspeditionsnummer,
                        transaktionstypenummer,
                        transaktionstypetekst,
                        maskinnummer,
                        dw_timestamp_insert,
                        dw_timestamp_update
                    )
                    VALUES (
                        src.dw_ek_Finanskladdelinje,
                        src.bk_ref_ledgertrans_id,
                        src.beskrivelse,
                        src.linjenummer,
                        src.ekspeditionsnummer,
                        src.transaktionstypenummer,
                        src.transaktionstypetekst,
                        src.maskinnummer,
                        CAST(SYSDATETIME() AS datetime2(0)),
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """
                )

                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Finanskladdelinje_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Finanskladdelinje]")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total_dim_rows,
                    COUNT(beskrivelse) AS has_beskrivelse,
                    COUNT(transaktionstypetekst) AS has_transaktionstypetekst
                FROM [dim].[Finanskladdelinje]
                WHERE dw_ek_Finanskladdelinje <> -1
            """
                )
                stats = cursor.fetchone()
                total_dim_rows = stats[0]
                has_beskrivelse = stats[1]
                has_transaktionstypetekst = stats[2]

                context.log.info(
                    f"Dimension sync complete: {total_dim_rows} total rows in "
                    f"dim.Finanskladdelinje — coverage: "
                    f"beskrivelse={has_beskrivelse}/{total_dim_rows}, "
                    f"transaktionstypetekst={has_transaktionstypetekst}/{total_dim_rows}"
                )
            finally:
                cursor.close()

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Finanskladdelinje", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Finanskladdelinje")

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
            "has_beskrivelse": has_beskrivelse,
            "has_transaktionstypetekst": has_transaktionstypetekst,
            "table": "dim.Finanskladdelinje",
            "source_tables": "stg.dim_Finanskladdelinje_sydax + map.Finanskladdelinje",
            "scd_type": "Type 1 (overwrite)",
        }
    )
