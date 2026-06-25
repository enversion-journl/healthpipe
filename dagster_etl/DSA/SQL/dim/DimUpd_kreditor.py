# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Kreditor

Sync dimension from map + add descriptive attributes.
Logic from: erp_dims_and_facts.yml (Type 1 SCD with COALESCE priority)

Uses dimension staging tables for simplified joins:
- stg.dim_kreditor_ils: Deduplicated ILS kreditor data with is_k_prefix flag
- stg.dim_kreditor_mysupply: Merged MySupply metadata + invoice headers
"""

import traceback

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_kreditor"]),
        AssetKey(["erp", "stg_dim_kreditor_sydax"]),
        AssetKey(["erp", "stg_dim_kreditor_ils"]),
        AssetKey(["erp", "stg_dim_kreditor_mysupply"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Kreditor from map + add attributes with COALESCE priority: SydAX → ILS → MySupply",
)
def dim_kreditor(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add attributes with priority logic"""

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Kreditor dimension sync with COALESCE priority logic...")

                # DIAGNOSTIC: Count map entries before sync
                try:
                    cursor.execute("SELECT COUNT(*) FROM [map].[Kreditor]")
                    map_count = cursor.fetchone()[0]
                    context.log.info(f"📊 Map has {map_count} Kreditor entries to sync")
                except Exception as e:
                    error_msg = f"Failed to count map entries: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # Create temp table with COALESCE logic for all attributes
                # All three staging tables are already deduplicated and use CleanCode_Kreditor:
                #   - stg.dim_kreditor_sydax: CleanCode_Kreditor, KreditorId, Kreditornummer, Kreditornavn, JuridiskEnhed
                #   - stg.dim_kreditor_ils: CleanCode_Kreditor, se_nr, lnr, navn, adr, adr2, postdi, kontakt
                #   - stg.dim_kreditor_mysupply: CleanCode_Kreditor, LeverandorCVR, FirmaNavn, ByNavn, Adresse, Postnummer, FakturaValutaKode
                try:
                    cursor.execute(
                        """
                        DROP TABLE IF EXISTS #Kreditor_Attributes;

            ;WITH staging_keys AS (
                SELECT DISTINCT CleanCode_Kreditor
                FROM [stg].[dim_kreditor_sydax]
                WHERE CleanCode_Kreditor IS NOT NULL
                UNION
                SELECT DISTINCT CleanCode_Kreditor
                FROM [stg].[dim_kreditor_ils]
                WHERE CleanCode_Kreditor IS NOT NULL
                UNION
                SELECT DISTINCT CleanCode_Kreditor
                FROM [stg].[dim_kreditor_mysupply]
                WHERE CleanCode_Kreditor IS NOT NULL
            )

            SELECT
                m.DW_EK_Kreditor,
                m.CleanCode_Kreditor,
                syd.KreditorId,

                -- Raw source attributes
                syd.Kreditornavn AS SydAX_Kreditornavn,
                syd.Kreditornummer AS SydAX_Kreditornummer,
                syd.CleanCode_Kreditor AS SydAX_CleanCode,

                ils.navn AS ILS_Kreditor_Navn,
                ils.adr AS ILS_Adresse1,
                ils.adr2 AS ILS_Adresse2,
                ils.postdi AS ILS_Postdistrikt,
                COALESCE(ils.se_nr, ils.cvr_nr) AS ILS_Cvrnr,
                ils.lnr AS ILS_Kreditornummer,
                ils.kontakt AS ILS_Kontakt,

                mys.LeverandorCVR AS MySupply_CVR,
                mys.FirmaNavn AS MySupply_FirmaNavn,
                mys.ByNavn AS MySupply_By,
                mys.Adresse AS MySupply_Adresse,
                mys.Postnummer AS MySupply_Postnummer,

                -- Derived COALESCE fields
                -- Priority: SydAX JuridiskEnhed -> ILS CVR -> MySupply CVR -> UNKNOWN
                COALESCE(
                    LTRIM(RTRIM(syd.JuridiskEnhed)),
                    LTRIM(RTRIM(COALESCE(ils.se_nr, ils.cvr_nr))),
                    LTRIM(RTRIM(mys.LeverandorCVR)),
                    'UNKNOWN'
                ) AS Cvrnr,

                -- Source-specific CVR columns
                LTRIM(RTRIM(COALESCE(ils.se_nr, ils.cvr_nr))) AS Cvrnr_Ils,
                LTRIM(RTRIM(mys.LeverandorCVR)) AS Cvrnr_MySupply,
                LTRIM(RTRIM(syd.JuridiskEnhed)) AS Cvrnr_SydAX,

                -- Name enrichment: Priority SydAX -> ILS -> MySupply
                COALESCE(
                    syd.Kreditornavn,
                    ils.navn,
                    mys.FirmaNavn
                ) AS KreditorNavn,

                COALESCE(
                    ils.adr2,
                    mys.ByNavn
                ) AS ByNavn,

                mys.FakturaValutaKode AS Valutakode,

                COALESCE(
                    ils.adr,
                    mys.Adresse
                ) AS Vejnavn,

                COALESCE(
                    LTRIM(RTRIM(ils.postdi)),
                    CAST(mys.Postnummer AS NVARCHAR(50))
                ) AS Postnummer,

                CAST(NULL AS NVARCHAR(50)) AS SprogId,
                CAST(NULL AS NVARCHAR(50)) AS BrancheId,

                COALESCE(
                    syd.Kreditornummer,
                    CAST(ils.lnr AS NVARCHAR(255))
                ) AS Kreditornummer,

                COALESCE(ils.kontakt, mys.FirmaNavn) AS KontaktNavn

            INTO #Kreditor_Attributes
            FROM staging_keys sk

            LEFT JOIN [map].[Kreditor] m
                ON m.CleanCode_Kreditor = sk.CleanCode_Kreditor

            LEFT JOIN [stg].[dim_kreditor_sydax] syd
                ON syd.CleanCode_Kreditor = sk.CleanCode_Kreditor

            LEFT JOIN [stg].[dim_kreditor_ils] ils
                ON ils.CleanCode_Kreditor = sk.CleanCode_Kreditor

            LEFT JOIN [stg].[dim_kreditor_mysupply] mys
                ON mys.CleanCode_Kreditor = sk.CleanCode_Kreditor;
        """
                    )
                    cursor.execute(
                        """
                        CREATE UNIQUE CLUSTERED INDEX IX_tmp_Kreditor_Attributes
                        ON #Kreditor_Attributes (DW_EK_Kreditor)
                        """
                    )
                    context.log.info("✅ Temp table #Kreditor_Attributes created successfully")
                except Exception as e:
                    error_msg = f"Failed to create temp table with COALESCE joins: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # DIAGNOSTIC: Check attribute coverage
                try:
                    cursor.execute(
                        """
                        SELECT
                            COUNT(*) AS total,
                            COUNT(Cvrnr) AS has_cvr,
                            COUNT(KreditorNavn) AS has_navn,
                            COUNT(CASE WHEN Cvrnr IS NOT NULL OR KreditorNavn IS NOT NULL THEN 1 END) AS has_any
                        FROM #Kreditor_Attributes
                    """
                    )
                    attr_stats = cursor.fetchone()
                    context.log.info(
                        f"📈 Attribute coverage: CVR={attr_stats[1]}/{attr_stats[0]}, "
                        f"Navn={attr_stats[2]}/{attr_stats[0]}, Any={attr_stats[3]}/{attr_stats[0]}"
                    )
                except Exception as e:
                    error_msg = f"Failed to query temp table for diagnostics: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # MERGE into dimension (Type 1 SCD)
                try:
                    cursor.execute(
                        """
            MERGE [dim].[Kreditor] AS tgt
            USING #Kreditor_Attributes AS src
            ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.CleanCode_Kreditor = src.CleanCode_Kreditor,
                    tgt.KreditorId = src.KreditorId,
                    tgt.Kreditorkonto = NULL,
                    tgt.Cvrnr = src.Cvrnr,
                    tgt.Cvrnr_Ils = src.Cvrnr_Ils,
                    tgt.Cvrnr_MySupply = src.Cvrnr_MySupply,
                    tgt.Cvrnr_SydAX = src.Cvrnr_SydAX,
                    tgt.KreditorNavn = src.KreditorNavn,
                    tgt.ByNavn = src.ByNavn,
                    tgt.Valutakode = src.Valutakode,
                    tgt.SprogId = src.SprogId,
                    tgt.BrancheId = src.BrancheId,
                    tgt.Vejnavn = src.Vejnavn,
                    tgt.Postnummer = src.Postnummer,
                    tgt.ILS_Kreditor_Navn = src.ILS_Kreditor_Navn,
                    tgt.ILS_Adresse1 = src.ILS_Adresse1,
                    tgt.ILS_Adresse2 = src.ILS_Adresse2,
                    tgt.ILS_Postdistrikt = src.ILS_Postdistrikt,
                    tgt.ILS_ByNavn = COALESCE(src.ILS_Adresse2, src.ILS_Kreditor_Navn),
                    tgt.ILS_Kontakt = src.ILS_Kontakt,
                    tgt.Kreditornummer = src.Kreditornummer,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Kreditor, CleanCode_Kreditor, KreditorId, Kreditorkonto, Cvrnr,
                    Cvrnr_Ils, Cvrnr_MySupply, Cvrnr_SydAX, KreditorNavn,
                    ByNavn, Valutakode, SprogId, BrancheId, Vejnavn, Postnummer,
                    ILS_Kreditor_Navn, ILS_Adresse1, ILS_Adresse2, ILS_Postdistrikt,
                    ILS_ByNavn, ILS_Kontakt, Kreditornummer, DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Kreditor, src.CleanCode_Kreditor, src.KreditorId, NULL, src.Cvrnr,
                    src.Cvrnr_Ils, src.Cvrnr_MySupply, src.Cvrnr_SydAX, src.KreditorNavn,
                    src.ByNavn, src.Valutakode, src.SprogId, src.BrancheId, src.Vejnavn, src.Postnummer,
                    src.ILS_Kreditor_Navn, src.ILS_Adresse1, src.ILS_Adresse2, src.ILS_Postdistrikt,
                    COALESCE(src.ILS_Adresse2, src.ILS_Kreditor_Navn), src.ILS_Kontakt, src.Kreditornummer,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
                    )
                    context.log.info("✅ MERGE operation completed successfully")
                except Exception as e:
                    error_msg = f"Failed to MERGE into dim.Kreditor: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                rows_affected = cursor.rowcount
                context.log.info(f"🔄 MERGE complete: {rows_affected} rows affected")

                # DIAGNOSTIC: Check final attribute population in dimension
                try:
                    cursor.execute(
                        """
                        SELECT
                            COUNT(*) AS total,
                            COUNT(Cvrnr) AS has_cvr,
                            COUNT(KreditorNavn) AS has_navn,
                            COUNT(CASE WHEN COALESCE(KreditorNavn, ByNavn, Vejnavn) IS NOT NULL THEN 1 END) AS has_any_attr
                        FROM [dim].[Kreditor]
                    """
                    )
                    attr_stats = cursor.fetchone()
                    context.log.info(
                        f"📈 Final attribute coverage: CVR={attr_stats[1]}/{attr_stats[0]}, "
                        f"Navn={attr_stats[2]}/{attr_stats[0]}, Any={attr_stats[3]}/{attr_stats[0]}"
                    )
                except Exception as e:
                    error_msg = f"Failed to query final dimension for diagnostics: {str(e)}"
                    context.log.error(f"❌ {error_msg}")
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                # Clean up temp table
                try:
                    cursor.execute("DROP TABLE IF EXISTS #Kreditor_Attributes")
                    context.log.info("✅ Temp table cleaned up")
                except Exception as e:
                    # Log but don't fail - temp tables are cleaned up automatically
                    context.log.warning(f"⚠️ Failed to drop temp table (may auto-cleanup): {str(e)}")

                conn.commit()
                context.log.info("✅ Database transaction committed")

                # Update statistics
                try:
                    cursor.execute("UPDATE STATISTICS [dim].[Kreditor]")
                    context.log.info("✅ Table statistics updated")
                except Exception as e:
                    # Log but don't fail - statistics update is not critical
                    context.log.warning(f"⚠️ Failed to update statistics: {str(e)}")

                context.log.info(f"✅ Dimension sync complete: {rows_affected} rows merged")
            finally:
                cursor.close()

    except Exception as e:
        # Catch-all error handler for any unhandled exceptions
        error_msg = f"Kreditor dimension sync failed: {str(e)}"
        context.log.error(f"❌ {error_msg}")
        context.log.error(f"Full traceback: {traceback.format_exc()}")
        raise ValueError(error_msg) from e

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.Kreditor",
            "staging_tables_joined": 4,
            "coalesce_sources": "SydAX → ILS → MySupply",
            "scd_type": "Type 1 (update in place)",
            "clean_architecture_note": "DW_EK_Kreditor synced from map table",
        }
    )
