# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Faktura

Fact table for invoices with DW_EK_Kreditor, DW_EK_Faktura, DW_EK_EAN_Regningsmodtager lookups from maps.
Logic from: erp_dims_and_facts.yml (grain: invoice)

Reads from stg.fct_faktura_mysupply (Layer 1 staging) which contains
the subset of columns needed from the pre-staging table.

Grain: one row per faktura (DW_EK_Faktura from map.Faktura).
"""

import traceback

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_facts",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_faktura_mysupply"]),  # Layer 1 staging
        "map_kreditor",
        "map_faktura",
        "map_ean",
        "dim_ean",
        "fct_postering",
        "dim_kreditor",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "invoice",
    },
    description="Fact table for invoices - reads from fct_faktura_mysupply, looks up DW_EK_Kreditor, DW_EK_Faktura, DW_EK_EAN_Regningsmodtager from maps",
)
def fct_faktura(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Calculate fact table with DW_EK_Kreditor, DW_EK_Faktura, DW_EK_EAN_Regningsmodtager lookups from maps"""

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting fact table load with DW_EK_Kreditor, DW_EK_Faktura lookups and FK diagnostics..."
                )

                # DIAGNOSTIC: Count staging source records
                try:
                    cursor.execute("SELECT COUNT(*) FROM [stg].[fct_faktura_mysupply]")
                    staging_count = cursor.fetchone()[0]

                    context.log.info(f"📊 Staging counts: Total={staging_count}")
                except Exception as e:
                    error_msg = f"Failed to count staging records: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # Create temp table with all lookups
                # NOTE: Heavy joins already done in pre-staging, this reads from stg.fct_faktura_mysupply
                try:
                    cursor.execute(
                        """
                        DROP TABLE IF EXISTS #Faktura_Facts;

                        SELECT
                            -- DW_EK lookups from maps (Clean Architecture)
                            -- CleanCode_Kreditor already resolved in pre-staging
                            -- COALESCE to -1 (UNKNOWN) when LEFT JOIN produces NULL (no match in map)
                            COALESCE(fak_map.DW_EK_Faktura, -1) AS DW_EK_Faktura,
                            COALESCE(kred_map.DW_EK_Kreditor, -1) AS DW_EK_Kreditor,
                            COALESCE(ean_map.DW_EK_EAN, -1) AS DW_EK_EAN_Regningsmodtager,

                            -- Date key conversions
                            COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.FakturaDato)), -1) AS DW_EK_Dato_Faktura,
                            COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.OrdreDato)), -1) AS DW_EK_Dato_Ordre,
                            COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.BetalingsDato)), -1) AS DW_EK_Dato_Betaling,

                            -- Business measures from MySupply (already available in staging)
                            CAST(src.FakturaNr AS NVARCHAR(100)) AS FakturaNr,
                            TRY_CONVERT(DECIMAL(9,4), src.MomsProcent) AS MomsProcent,
                            TRY_CONVERT(DECIMAL(18,4), src.MomsGrundlag) AS MomsGrundlag,
                            TRY_CONVERT(DECIMAL(18,4), src.MomsBeloeb) AS MomsBeloeb,
                            TRY_CONVERT(DECIMAL(18,4), src.LinjeTotal) AS LinjeTotal,
                            TRY_CONVERT(DECIMAL(18,4), COALESCE(src.HeaderFakturaTotal, src.MetadataFakturaTotal)) AS FakturaTotal,
                            -- Currency code: truncate to 3-char ISO format, skip if numeric (data quality issue)
                            CASE WHEN ISNUMERIC(src.FakturaValutaKode) = 0
                                 THEN LEFT(CAST(src.FakturaValutaKode AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_Faktura,

                            -- Header VAT attributes (moved from fct.Fakturalinje)
                            CAST(src.VAT_AfgiftsKategori AS NVARCHAR(50)) AS VAT_AfgiftsKategori,
                            CASE WHEN ISNUMERIC(src.MomsGrundlag_Valuta) = 0
                                 THEN LEFT(CAST(src.MomsGrundlag_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_MomsGrundlag,
                            CASE WHEN ISNUMERIC(src.MomsBeloeb_Valuta) = 0
                                 THEN LEFT(CAST(src.MomsBeloeb_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_MomsBeloeb,
                            CASE WHEN ISNUMERIC(src.FakturaLinjeTotal_Valuta) = 0
                                 THEN LEFT(CAST(src.FakturaLinjeTotal_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_LinjeTotal,
                            CASE WHEN ISNUMERIC(src.FakturaTotal_Valuta) = 0
                                 THEN LEFT(CAST(src.FakturaTotal_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_FakturaTotal,

                            -- Zero-rated VAT measures (moved from fct.Fakturalinje)
                            TRY_CONVERT(DECIMAL(18,4), src.ZeroRated_MomsGrundlag) AS ZeroRated_MomsGrundlag,
                            CASE WHEN ISNUMERIC(src.ZeroRated_MomsGrundlag_Valuta) = 0
                                 THEN LEFT(CAST(src.ZeroRated_MomsGrundlag_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_ZeroRated_MomsGrundlag,
                            TRY_CONVERT(DECIMAL(18,4), src.ZeroRated_MomsBeloeb) AS ZeroRated_MomsBeloeb,
                            CASE WHEN ISNUMERIC(src.ZeroRated_MomsBeloeb_Valuta) = 0
                                 THEN LEFT(CAST(src.ZeroRated_MomsBeloeb_Valuta AS NVARCHAR(10)), 3)
                                 ELSE NULL END AS Valutakode_ZeroRated_MomsBeloeb,

                            -- Summed allowance/charges (already DECIMAL from staging)
                            src.GebyrBeloeb,
                            src.RabatBeloeb

                        INTO #Faktura_Facts
                        FROM [stg].[fct_faktura_mysupply] src

                        -- DW_EK lookups from map tables using pre-resolved CleanCodes
                        LEFT JOIN [map].[Kreditor] kred_map
                            ON kred_map.CleanCode_Kreditor = src.CleanCode_Kreditor
                            AND src.CleanCode_Kreditor IS NOT NULL
                            AND src.CleanCode_Kreditor <> ''

                        -- DW_EK_Faktura lookup from map.Faktura via CleanCode_Faktura
                        LEFT JOIN [map].[Faktura] fak_map
                            ON fak_map.CleanCode_Faktura = src.CleanCode_Faktura
                           AND src.CleanCode_Faktura IS NOT NULL
                           AND src.CleanCode_Faktura <> ''

                        -- DW_EK_EAN_Regningsmodtager lookup via CleanCode_EAN_Regningsmodtager
                        LEFT JOIN [map].[EAN] ean_map
                            ON ean_map.CleanCode_EAN = src.CleanCode_EAN_Regningsmodtager
                           AND src.CleanCode_EAN_Regningsmodtager IS NOT NULL
                           AND src.CleanCode_EAN_Regningsmodtager <> ''
                    """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE CLUSTERED INDEX IX_tmp_Faktura_Facts
                        ON #Faktura_Facts (DW_EK_Faktura)
                        """
                    )
                    context.log.info("✅ Temp table #Faktura_Facts created successfully")
                except Exception as e:
                    error_msg = f"Failed to create temp table with DW_EK lookups: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # MERGE into fact table using DW_EK_Faktura as primary key
                try:
                    cursor.execute(
                        """
                        MERGE [fct].[Faktura] AS tgt
                        USING #Faktura_Facts AS src
                        ON tgt.DW_EK_Faktura = src.DW_EK_Faktura

                        WHEN MATCHED THEN
                            UPDATE SET
                                tgt.DW_EK_Kreditor = src.DW_EK_Kreditor,
                                tgt.DW_EK_EAN_Regningsmodtager = src.DW_EK_EAN_Regningsmodtager,
                                tgt.DW_EK_Dato_Faktura = src.DW_EK_Dato_Faktura,
                                tgt.DW_EK_Dato_Ordre = src.DW_EK_Dato_Ordre,
                                tgt.DW_EK_Dato_Betaling = src.DW_EK_Dato_Betaling,
                                tgt.FakturaNr = src.FakturaNr,
                                tgt.MomsProcent = src.MomsProcent,
                                tgt.MomsGrundlag = src.MomsGrundlag,
                                tgt.MomsBeloeb = src.MomsBeloeb,
                                tgt.LinjeTotal = src.LinjeTotal,
                                tgt.FakturaTotal = src.FakturaTotal,
                                tgt.Valutakode_Faktura = src.Valutakode_Faktura,
                                tgt.VAT_AfgiftsKategori = src.VAT_AfgiftsKategori,
                                tgt.Valutakode_MomsGrundlag = src.Valutakode_MomsGrundlag,
                                tgt.Valutakode_MomsBeloeb = src.Valutakode_MomsBeloeb,
                                tgt.Valutakode_LinjeTotal = src.Valutakode_LinjeTotal,
                                tgt.Valutakode_FakturaTotal = src.Valutakode_FakturaTotal,
                                tgt.ZeroRated_MomsGrundlag = src.ZeroRated_MomsGrundlag,
                                tgt.Valutakode_ZeroRated_MomsGrundlag = src.Valutakode_ZeroRated_MomsGrundlag,
                                tgt.ZeroRated_MomsBeloeb = src.ZeroRated_MomsBeloeb,
                                tgt.Valutakode_ZeroRated_MomsBeloeb = src.Valutakode_ZeroRated_MomsBeloeb,
                                tgt.GebyrBeloeb = src.GebyrBeloeb,
                                tgt.RabatBeloeb = src.RabatBeloeb,
                                tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (
                                DW_EK_Faktura, DW_EK_Kreditor, DW_EK_EAN_Regningsmodtager,
                                DW_EK_Dato_Faktura, DW_EK_Dato_Ordre, DW_EK_Dato_Betaling,
                                FakturaNr, MomsProcent, MomsGrundlag, MomsBeloeb,
                                LinjeTotal, FakturaTotal, Valutakode_Faktura,
                                VAT_AfgiftsKategori, Valutakode_MomsGrundlag, Valutakode_MomsBeloeb,
                                Valutakode_LinjeTotal, Valutakode_FakturaTotal,
                                ZeroRated_MomsGrundlag, Valutakode_ZeroRated_MomsGrundlag,
                                ZeroRated_MomsBeloeb, Valutakode_ZeroRated_MomsBeloeb,
                                GebyrBeloeb, RabatBeloeb,
                                DW_Timestamp_Insert
                            )
                            VALUES (
                                src.DW_EK_Faktura, src.DW_EK_Kreditor, src.DW_EK_EAN_Regningsmodtager,
                                src.DW_EK_Dato_Faktura, src.DW_EK_Dato_Ordre, src.DW_EK_Dato_Betaling,
                                src.FakturaNr, src.MomsProcent, src.MomsGrundlag, src.MomsBeloeb,
                                src.LinjeTotal, src.FakturaTotal, src.Valutakode_Faktura,
                                src.VAT_AfgiftsKategori, src.Valutakode_MomsGrundlag, src.Valutakode_MomsBeloeb,
                                src.Valutakode_LinjeTotal, src.Valutakode_FakturaTotal,
                                src.ZeroRated_MomsGrundlag, src.Valutakode_ZeroRated_MomsGrundlag,
                                src.ZeroRated_MomsBeloeb, src.Valutakode_ZeroRated_MomsBeloeb,
                                src.GebyrBeloeb, src.RabatBeloeb,
                                CAST(SYSDATETIME() AS datetime2(0))
                            );
                        """
                    )
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
                            SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
                            SUM(CASE WHEN DW_EK_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
                            SUM(CASE WHEN DW_EK_EAN_Regningsmodtager = -1 THEN 1 ELSE 0 END) AS missing_ean_fk
                        FROM [fct].[Faktura]
                        """
                    )
                    columns = [col[0] for col in cursor.description]
                    diag = dict(zip(columns, cursor.fetchone(), strict=False))

                    context.log.info(
                        f"🔍 FK Matching: Total={diag['total_facts']}, "
                        f"Missing Kreditor={diag['missing_kreditor_fk']}, "
                        f"Missing Faktura={diag['missing_faktura_fk']}, "
                        f"Missing EAN={diag['missing_ean_fk']}"
                    )

                    for key, label in [
                        ("missing_kreditor_fk", "Kreditor"),
                        ("missing_faktura_fk", "Faktura"),
                        ("missing_ean_fk", "EAN"),
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

    except Exception as e:
        # Catch-all error handler for any unhandled exceptions
        error_msg = f"Faktura fact table load failed: {str(e)}"
        context.log.error(f"❌ {error_msg}")
        context.log.error(f"Full traceback: {traceback.format_exc()}")
        raise ValueError(error_msg) from e

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Faktura",
            "grain": "DW_EK_Faktura (one row per faktura)",
            "staging_source": "stg.fct_faktura_mysupply (Layer 1)",
            "dw_ek_lookups": "DW_EK_Kreditor, DW_EK_Faktura, DW_EK_EAN_Regningsmodtager from maps",
            "date_conversions": 3,
            "clean_architecture_note": "DW_EK lookups from maps, reads from staging",
            "primary_key": "DW_EK_Faktura (from map.Faktura, NOT IDENTITY)",
            "organization_lookup": "vw_fct.Faktura.DW_EK_Organisation_Regningsmodtager via conceptmap.ean_to_sor",
        }
    )
