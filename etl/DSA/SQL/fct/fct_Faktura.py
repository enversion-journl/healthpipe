# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Faktura

Fact table for invoices with dw_ek_Kreditor, dw_ek_Faktura, dw_ek_EAN_regningsmodtager lookups from maps.
Logic from: erp_dims_and_facts.yml (grain: invoice)

Reads from stg.fct_Faktura_mysupply (Layer 1 staging) which contains
the subset of columns needed from the pre-staging table.

Grain: one row per faktura (dw_ek_Faktura from map.Faktura).
"""

import traceback

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_Faktura_mysupply"]),  # Layer 1 staging
        "map_Kreditor",
        "map_Faktura",
        "map_EAN",
        "dim_EAN",
        "fct_Postering",
        "dim_Kreditor",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "invoice",
    },
    description="Fact table for invoices - reads from fct_Faktura_mysupply, looks up dw_ek_Kreditor, dw_ek_Faktura, dw_ek_EAN_regningsmodtager from maps",
)
def fct_Faktura(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Calculate fact table with dw_ek_Kreditor, dw_ek_Faktura, dw_ek_EAN_regningsmodtager lookups from maps"""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct",
        "Faktura",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    rows_affected = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting fact table load with dw_ek_Kreditor, dw_ek_Faktura lookups and FK diagnostics..."
                )

                # DIAGNOSTIC: Count staging source records
                try:
                    cursor.execute("SELECT COUNT(*) FROM [stg].[fct_Faktura_mysupply]")
                    staging_count = cursor.fetchone()[0]

                    context.log.info(f"📊 Staging counts: Total={staging_count}")
                except Exception as e:
                    error_msg = f"Failed to count staging records: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # Create temp table with all lookups
                # NOTE: Heavy joins already done in pre-staging, this reads from stg.fct_Faktura_mysupply
                try:
                    cursor.execute(
                        f"""
                        DROP TABLE IF EXISTS #Faktura_Facts;

                        SELECT
                            -- DW_EK lookups from maps (Clean Architecture)
                            -- clean_code_Kreditor already resolved in pre-staging
                            -- COALESCE to -1 (UNKNOWN) when LEFT JOIN produces NULL (no match in map)
                            COALESCE(fak_map.dw_ek_Faktura, -1) AS dw_ek_Faktura,
                            COALESCE(kred_map.dw_ek_Kreditor, -1) AS dw_ek_Kreditor,
                            COALESCE(ean_map.dw_ek_EAN, -1) AS dw_ek_EAN_regningsmodtager,

                            -- Date key conversions
                            {dato_key('src.FakturaDato')} AS dw_ek_Dato_faktura,
                            {dato_tid('src.FakturaDato')} AS dato_tid_faktura,
                            {dato_key('src.OrdreDato')} AS dw_ek_Dato_ordre,
                            {dato_tid('src.OrdreDato')} AS dato_tid_ordre,
                            {dato_key('src.BetalingsDato')} AS dw_ek_Dato_betaling,
                            {dato_tid('src.BetalingsDato')} AS dato_tid_betaling,

                            -- Business measures from MySupply (already available in staging)
                            CAST(src.faktura_nr AS NVARCHAR(100)) AS faktura_nr,
                            TRY_CONVERT(DECIMAL(9,4), src.moms_procent) AS moms_procent,
                            TRY_CONVERT(DECIMAL(18,4), src.moms_grundlag) AS moms_grundlag,
                            TRY_CONVERT(DECIMAL(18,4), src.momsbeloeb) AS momsbeloeb,
                            TRY_CONVERT(DECIMAL(18,4), src.linje_total) AS linje_total,
                            TRY_CONVERT(DECIMAL(18,4), COALESCE(src.HeaderFakturaTotal, src.MetadataFakturaTotal)) AS faktura_total,
                            -- Currency code: truncate to 3-char ISO format, skip if numeric (data quality issue)
                            CASE WHEN ISNUMERIC(src.faktura_valuta_kode) = 0
                                 THEN LEFT(CAST(src.faktura_valuta_kode AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_faktura,

                            -- Header VAT attributes (moved from fct.Fakturalinje)
                            CAST(src.vat_afgifts_kategori AS NVARCHAR(50)) AS vat_afgifts_kategori,
                            CASE WHEN ISNUMERIC(src.MomsGrundlag_Valuta) = 0
                                 THEN LEFT(CAST(src.MomsGrundlag_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_moms_grundlag,
                            CASE WHEN ISNUMERIC(src.MomsBeloeb_Valuta) = 0
                                 THEN LEFT(CAST(src.MomsBeloeb_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_momsbeloeb,
                            CASE WHEN ISNUMERIC(src.FakturaLinjeTotal_Valuta) = 0
                                 THEN LEFT(CAST(src.FakturaLinjeTotal_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_linje_total,
                            CASE WHEN ISNUMERIC(src.FakturaTotal_Valuta) = 0
                                 THEN LEFT(CAST(src.FakturaTotal_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_faktura_total,

                            -- Zero-rated VAT measures (moved from fct.Fakturalinje)
                            TRY_CONVERT(DECIMAL(18,4), src.zero_rated_moms_grundlag) AS zero_rated_moms_grundlag,
                            CASE WHEN ISNUMERIC(src.ZeroRated_MomsGrundlag_Valuta) = 0
                                 THEN LEFT(CAST(src.ZeroRated_MomsGrundlag_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_zero_rated_moms_grundlag,
                            TRY_CONVERT(DECIMAL(18,4), src.zero_rated_momsbeloeb) AS zero_rated_momsbeloeb,
                            CASE WHEN ISNUMERIC(src.ZeroRated_MomsBeloeb_Valuta) = 0
                                 THEN LEFT(CAST(src.ZeroRated_MomsBeloeb_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS valutakode_zero_rated_momsbeloeb,

                            -- Summed allowance/charges (already DECIMAL from staging)
                            src.gebyr_beloeb,
                            src.rabatbeloeb

                        INTO #Faktura_Facts
                        FROM [stg].[fct_Faktura_mysupply] src

                        -- DW_EK lookups from map tables using pre-resolved CleanCodes
                        LEFT JOIN [map].[Kreditor] kred_map
                            ON kred_map.clean_code_Kreditor = src.clean_code_Kreditor
                            AND src.clean_code_Kreditor IS NOT NULL
                            AND src.clean_code_Kreditor <> ''

                        -- dw_ek_Faktura lookup from map.Faktura via clean_code_Faktura
                        LEFT JOIN [map].[Faktura] fak_map
                            ON fak_map.clean_code_Faktura = src.clean_code_Faktura
                           AND src.clean_code_Faktura IS NOT NULL
                           AND src.clean_code_Faktura <> ''

                        -- dw_ek_EAN_regningsmodtager lookup via clean_code_EAN_Regningsmodtager
                        LEFT JOIN [map].[ean] ean_map
                            ON ean_map.clean_code_EAN = src.clean_code_EAN_Regningsmodtager
                           AND src.clean_code_EAN_Regningsmodtager IS NOT NULL
                           AND src.clean_code_EAN_Regningsmodtager <> ''
                    """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE CLUSTERED INDEX IX_tmp_Faktura_Facts
                        ON #Faktura_Facts (dw_ek_Faktura)
                        """
                    )
                    context.log.info("✅ Temp table #Faktura_Facts created successfully")
                except Exception as e:
                    error_msg = f"Failed to create temp table with DW_EK lookups: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # MERGE into fact table using dw_ek_Faktura as primary key
                try:
                    merge_sql = """
                        MERGE [fct].[Faktura] AS tgt
                        USING #Faktura_Facts AS src
                        ON tgt.dw_ek_Faktura = src.dw_ek_Faktura

                        WHEN MATCHED AND EXISTS (
                            SELECT
                                src.dw_ek_Kreditor, src.dw_ek_EAN_regningsmodtager,
                                src.dw_ek_Dato_faktura, src.dato_tid_faktura,
                                src.dw_ek_Dato_ordre, src.dato_tid_ordre,
                                src.dw_ek_Dato_betaling, src.dato_tid_betaling,
                                src.faktura_nr, src.moms_procent, src.moms_grundlag, src.momsbeloeb,
                                src.linje_total, src.faktura_total, src.valutakode_faktura,
                                src.vat_afgifts_kategori, src.valutakode_moms_grundlag, src.valutakode_momsbeloeb,
                                src.valutakode_linje_total, src.valutakode_faktura_total,
                                src.zero_rated_moms_grundlag, src.valutakode_zero_rated_moms_grundlag,
                                src.zero_rated_momsbeloeb, src.valutakode_zero_rated_momsbeloeb,
                                src.gebyr_beloeb, src.rabatbeloeb
                            EXCEPT
                            SELECT
                                tgt.dw_ek_Kreditor, tgt.dw_ek_EAN_regningsmodtager,
                                tgt.dw_ek_Dato_faktura, tgt.dato_tid_faktura,
                                tgt.dw_ek_Dato_ordre, tgt.dato_tid_ordre,
                                tgt.dw_ek_Dato_betaling, tgt.dato_tid_betaling,
                                tgt.faktura_nr, tgt.moms_procent, tgt.moms_grundlag, tgt.momsbeloeb,
                                tgt.linje_total, tgt.faktura_total, tgt.valutakode_faktura,
                                tgt.vat_afgifts_kategori, tgt.valutakode_moms_grundlag, tgt.valutakode_momsbeloeb,
                                tgt.valutakode_linje_total, tgt.valutakode_faktura_total,
                                tgt.zero_rated_moms_grundlag, tgt.valutakode_zero_rated_moms_grundlag,
                                tgt.zero_rated_momsbeloeb, tgt.valutakode_zero_rated_momsbeloeb,
                                tgt.gebyr_beloeb, tgt.rabatbeloeb
                        ) THEN
                            UPDATE SET
                                tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
                                tgt.dw_ek_EAN_regningsmodtager = src.dw_ek_EAN_regningsmodtager,
                                tgt.dw_ek_Dato_faktura = src.dw_ek_Dato_faktura,
                                tgt.dato_tid_faktura = src.dato_tid_faktura,
                                tgt.dw_ek_Dato_ordre = src.dw_ek_Dato_ordre,
                                tgt.dato_tid_ordre = src.dato_tid_ordre,
                                tgt.dw_ek_Dato_betaling = src.dw_ek_Dato_betaling,
                                tgt.dato_tid_betaling = src.dato_tid_betaling,
                                tgt.faktura_nr = src.faktura_nr,
                                tgt.moms_procent = src.moms_procent,
                                tgt.moms_grundlag = src.moms_grundlag,
                                tgt.momsbeloeb = src.momsbeloeb,
                                tgt.linje_total = src.linje_total,
                                tgt.faktura_total = src.faktura_total,
                                tgt.valutakode_faktura = src.valutakode_faktura,
                                tgt.vat_afgifts_kategori = src.vat_afgifts_kategori,
                                tgt.valutakode_moms_grundlag = src.valutakode_moms_grundlag,
                                tgt.valutakode_momsbeloeb = src.valutakode_momsbeloeb,
                                tgt.valutakode_linje_total = src.valutakode_linje_total,
                                tgt.valutakode_faktura_total = src.valutakode_faktura_total,
                                tgt.zero_rated_moms_grundlag = src.zero_rated_moms_grundlag,
                                tgt.valutakode_zero_rated_moms_grundlag = src.valutakode_zero_rated_moms_grundlag,
                                tgt.zero_rated_momsbeloeb = src.zero_rated_momsbeloeb,
                                tgt.valutakode_zero_rated_momsbeloeb = src.valutakode_zero_rated_momsbeloeb,
                                tgt.gebyr_beloeb = src.gebyr_beloeb,
                                tgt.rabatbeloeb = src.rabatbeloeb,
                                tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (
                                dw_ek_Faktura, dw_ek_Kreditor, dw_ek_EAN_regningsmodtager,
                                dw_ek_Dato_faktura, dato_tid_faktura,
                                dw_ek_Dato_ordre, dato_tid_ordre,
                                dw_ek_Dato_betaling, dato_tid_betaling,
                                faktura_nr, moms_procent, moms_grundlag, momsbeloeb,
                                linje_total, faktura_total, valutakode_faktura,
                                vat_afgifts_kategori, valutakode_moms_grundlag, valutakode_momsbeloeb,
                                valutakode_linje_total, valutakode_faktura_total,
                                zero_rated_moms_grundlag, valutakode_zero_rated_moms_grundlag,
                                zero_rated_momsbeloeb, valutakode_zero_rated_momsbeloeb,
                                gebyr_beloeb, rabatbeloeb,
                                dw_timestamp_insert, dw_timestamp_update
                            )
                            VALUES (
                                src.dw_ek_Faktura, src.dw_ek_Kreditor, src.dw_ek_EAN_regningsmodtager,
                                src.dw_ek_Dato_faktura, src.dato_tid_faktura,
                                src.dw_ek_Dato_ordre, src.dato_tid_ordre,
                                src.dw_ek_Dato_betaling, src.dato_tid_betaling,
                                src.faktura_nr, src.moms_procent, src.moms_grundlag, src.momsbeloeb,
                                src.linje_total, src.faktura_total, src.valutakode_faktura,
                                src.vat_afgifts_kategori, src.valutakode_moms_grundlag, src.valutakode_momsbeloeb,
                                src.valutakode_linje_total, src.valutakode_faktura_total,
                                src.zero_rated_moms_grundlag, src.valutakode_zero_rated_moms_grundlag,
                                src.zero_rated_momsbeloeb, src.valutakode_zero_rated_momsbeloeb,
                                src.gebyr_beloeb, src.rabatbeloeb,
                                CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                            );
                        """

                    insert_sql = """
                        INSERT INTO [fct].[Faktura] WITH (TABLOCK) (
                            dw_ek_Faktura, dw_ek_Kreditor, dw_ek_EAN_regningsmodtager,
                            dw_ek_Dato_faktura, dato_tid_faktura,
                            dw_ek_Dato_ordre, dato_tid_ordre,
                            dw_ek_Dato_betaling, dato_tid_betaling,
                            faktura_nr, moms_procent, moms_grundlag, momsbeloeb,
                            linje_total, faktura_total, valutakode_faktura,
                            vat_afgifts_kategori, valutakode_moms_grundlag, valutakode_momsbeloeb,
                            valutakode_linje_total, valutakode_faktura_total,
                            zero_rated_moms_grundlag, valutakode_zero_rated_moms_grundlag,
                            zero_rated_momsbeloeb, valutakode_zero_rated_momsbeloeb,
                            gebyr_beloeb, rabatbeloeb,
                            dw_timestamp_insert, dw_timestamp_update
                        )
                        SELECT
                            src.dw_ek_Faktura, src.dw_ek_Kreditor, src.dw_ek_EAN_regningsmodtager,
                            src.dw_ek_Dato_faktura, src.dato_tid_faktura,
                            src.dw_ek_Dato_ordre, src.dato_tid_ordre,
                            src.dw_ek_Dato_betaling, src.dato_tid_betaling,
                            src.faktura_nr, src.moms_procent, src.moms_grundlag, src.momsbeloeb,
                            src.linje_total, src.faktura_total, src.valutakode_faktura,
                            src.vat_afgifts_kategori, src.valutakode_moms_grundlag, src.valutakode_momsbeloeb,
                            src.valutakode_linje_total, src.valutakode_faktura_total,
                            src.zero_rated_moms_grundlag, src.valutakode_zero_rated_moms_grundlag,
                            src.zero_rated_momsbeloeb, src.valutakode_zero_rated_momsbeloeb,
                            src.gebyr_beloeb, src.rabatbeloeb,
                            CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                        FROM #Faktura_Facts src;
                        """

                    # Short-circuit: fct.Faktura has no -1 sentinel row, so on a fresh
                    # or rebuilt (empty) target a bulk INSERT ... WITH (TABLOCK) can be
                    # minimally logged, whereas MERGE is always fully logged.
                    cursor.execute(
                        "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Faktura]) "
                        "THEN 1 ELSE 0 END"
                    )
                    if cursor.fetchone()[0]:
                        context.log.info("Target has existing rows - MERGE upsert path")
                        cursor.execute(merge_sql)
                    else:
                        context.log.info(
                            "Target empty - bulk INSERT WITH (TABLOCK), minimally logged"
                        )
                        cursor.execute(insert_sql)
                    context.log.info("✅ MERGE operation completed successfully")
                except Exception as e:
                    error_msg = f"Failed to MERGE into fct.Faktura: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                rows_affected = cursor.rowcount
                conn.commit()
                context.log.info("✅ Database transaction committed")

                # DIAGNOSTIC: FK matching statistics
                try:
                    cursor.execute(
                        """
                        SELECT
                            COUNT(*) AS total_facts,
                            SUM(CASE WHEN dw_ek_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
                            SUM(CASE WHEN dw_ek_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
                            SUM(CASE WHEN dw_ek_EAN_regningsmodtager = -1 THEN 1 ELSE 0 END) AS missing_ean_fk
                        FROM [fct].[Faktura]
                        """
                    )
                    columns = [col[0] for col in cursor.description]
                    diag = dict(zip(columns, cursor.fetchone(), strict=False))

                    context.log.info(
                        f"🔍 FK Matching: Total={diag['total_facts']}, "
                        f"Missing Kreditor={diag['missing_kreditor_fk']}, "
                        f"Missing Faktura={diag['missing_faktura_fk']}, "
                        f"Missing ean={diag['missing_ean_fk']}"
                    )

                    for key, label in [
                        ("missing_kreditor_fk", "Kreditor"),
                        ("missing_faktura_fk", "Faktura"),
                        ("missing_ean_fk", "ean"),
                    ]:
                        if diag[key] and diag[key] > 0:
                            context.log.warning(
                                f"⚠️  {diag[key]} invoices without {label} FK (using -1/UNKNOWN)"
                            )
                except Exception as e:
                    error_msg = f"Failed to query fact table for FK diagnostics: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # Clean up temp table
                try:
                    cursor.execute("DROP TABLE IF EXISTS #Faktura_Facts")
                    context.log.info("✅ Temp table cleaned up")
                except Exception as e:
                    # Log but don't fail - temp tables are cleaned up automatically
                    context.log.warning(f"⚠️ Failed to drop temp table (may auto-cleanup): {str(e)}")

                # Update statistics
                try:
                    cursor.execute("UPDATE STATISTICS [fct].[Faktura]")
                    context.log.info("✅ Table statistics updated")
                except Exception as e:
                    # Log but don't fail - statistics update is not critical
                    context.log.warning(f"⚠️ Failed to update statistics: {str(e)}")

                context.log.info(f"✅ Fact table load complete: {rows_affected} rows processed")
            finally:
                cursor.close()

        result: MaterializeResult = MaterializeResult(
            metadata={
                "rows_affected": rows_affected,
                "table": "fct.Faktura",
                "grain": "dw_ek_Faktura (one row per faktura)",
                "staging_source": "stg.fct_Faktura_mysupply (Layer 1)",
                "dw_ek_lookups": "dw_ek_Kreditor, dw_ek_Faktura, dw_ek_EAN_regningsmodtager from maps",
                "date_conversions": 3,
                "clean_architecture_note": "DW_EK lookups from maps, reads from staging",
                "primary_key": "dw_ek_Faktura (from map.Faktura, NOT IDENTITY)",
                "organization_lookup": "vw_fct.Faktura.DW_EK_Organisation_Regningsmodtager via conceptmap.ean_to_sor",
            }
        )

        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Faktura", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Faktura")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return result
