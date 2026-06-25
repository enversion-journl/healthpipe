# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Fakturalinje

Sync dimension from map + add textual attributes from MySupply staging.
Logic: Type 1 SCD - update textual information for invoice lines.

Uses:
- map.Fakturalinje: DW_EK_Fakturalinje assignment via IDENTITY
- stg.dim_fakturalinje_mysupply: Deduplicated fakturalinje attributes at CleanCode grain
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_fakturalinje"]),
        AssetKey(["erp", "stg_dim_fakturalinje_mysupply"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Fakturalinje from map + add textual attributes from MySupply staging",
)
def dim_fakturalinje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add textual attributes from MySupply."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Fakturalinje dimension sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Fakturalinje]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Fakturalinje entries to sync")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_fakturalinje_mysupply] ps
            LEFT JOIN [map].[Fakturalinje] m
                ON m.CleanCode_Fakturalinje = ps.CleanCode_Fakturalinje
            WHERE m.DW_EK_Fakturalinje IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(
                    f"Found {unmapped_rows} stg.dim_fakturalinje_mysupply rows without map match"
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Fakturalinje_Attributes;

            SELECT
                m.DW_EK_Fakturalinje,
                LEFT(ps.Item_FakturaUdstederVareNr, 255) AS Item_FakturaUdstederVareNr,
                ps.Item_FakturaUdstederVareNavn,
                ps.FakturaLinjeNr,
                ps.AntalEnhederFaktureret_Type,
                ps.Item_FakturaUdstederensVareNr,
                ps.Item_FakturaUdstederensVareNr_Type,
                ps.BasePrice_EnhedsStørrelse,
                ps.BasePrice_EnhedsStørrelse_Type,
                ps.Item_FakturaUdstederVareNavnKort,
                ps.Line_DimensionsKontoStreng,
                ps.LeveranceKrav,
                ps.LeveringsDato_2,
                ps.Item_KundensVareNr,
                ps.Item_KundensVareNr_Type,
                ps.Item_KundensVareBeskrivelse,
                ps.Item_FakturaUdstederensVareBeskrivelse,
                ps.Item_FabrikantensVareNr,
                ps.Item_FabrikantensVareNr_Type,
                ps.Item_FabrikantensVareBeskrivelse,
                ps.Item_StandardiseretVareNr,
                ps.Item_StandardiseretVareNr_Type,
                ps.Item_StandardiseretVareBeskrivelse,
                ps.Item_VareKlassifikation,
                ps.Item_VareKlassifikation_Type,
                ps.Item_AfgiftsKategori,
                ps.Item_AfgiftsKategoriProcent,
                ps.FakturaUdstederGebyrRabatAarsag,
                ps.FakturaUdstederGebyrRabatKategori,
                ps.FakturaUdstederGebyrRabatFinanskonto,
                ps.FakturaUdstederGebyrTillaegAarsag,
                ps.FakturaUdstederGebyrTillaegKategori,
                ps.FakturaUdstederGebyrTillaegFinanskonto
            INTO #Fakturalinje_Attributes
            FROM [stg].[dim_fakturalinje_mysupply] ps
            LEFT JOIN [map].[Fakturalinje] m
                ON m.CleanCode_Fakturalinje = ps.CleanCode_Fakturalinje
            WHERE m.DW_EK_Fakturalinje IS NOT NULL;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Fakturalinje_Attributes
            ON #Fakturalinje_Attributes (DW_EK_Fakturalinje)
            """
            )
            context.log.info("Temp table #Fakturalinje_Attributes created")

            cursor.execute(
                """
            MERGE [dim].[Fakturalinje] WITH (TABLOCK) AS tgt
            USING #Fakturalinje_Attributes AS src
            ON tgt.DW_EK_Fakturalinje = src.DW_EK_Fakturalinje

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.Item_FakturaUdstederVareNr = src.Item_FakturaUdstederVareNr,
                    tgt.Item_FakturaUdstederVareNavn = src.Item_FakturaUdstederVareNavn,
                    tgt.FakturaLinjeNr = src.FakturaLinjeNr,
                    tgt.AntalEnhederFaktureret_Type = src.AntalEnhederFaktureret_Type,
                    tgt.Item_FakturaUdstederensVareNr = src.Item_FakturaUdstederensVareNr,
                    tgt.Item_FakturaUdstederensVareNr_Type = src.Item_FakturaUdstederensVareNr_Type,
                    tgt.BasePrice_EnhedsStørrelse = src.BasePrice_EnhedsStørrelse,
                    tgt.BasePrice_EnhedsStørrelse_Type = src.BasePrice_EnhedsStørrelse_Type,
                    tgt.Item_FakturaUdstederVareNavnKort = src.Item_FakturaUdstederVareNavnKort,
                    tgt.Line_DimensionsKontoStreng = src.Line_DimensionsKontoStreng,
                    tgt.LeveranceKrav = src.LeveranceKrav,
                    tgt.LeveringsDato_2 = src.LeveringsDato_2,
                    tgt.Item_KundensVareNr = src.Item_KundensVareNr,
                    tgt.Item_KundensVareNr_Type = src.Item_KundensVareNr_Type,
                    tgt.Item_KundensVareBeskrivelse = src.Item_KundensVareBeskrivelse,
                    tgt.Item_FakturaUdstederensVareBeskrivelse = src.Item_FakturaUdstederensVareBeskrivelse,
                    tgt.Item_FabrikantensVareNr = src.Item_FabrikantensVareNr,
                    tgt.Item_FabrikantensVareNr_Type = src.Item_FabrikantensVareNr_Type,
                    tgt.Item_FabrikantensVareBeskrivelse = src.Item_FabrikantensVareBeskrivelse,
                    tgt.Item_StandardiseretVareNr = src.Item_StandardiseretVareNr,
                    tgt.Item_StandardiseretVareNr_Type = src.Item_StandardiseretVareNr_Type,
                    tgt.Item_StandardiseretVareBeskrivelse = src.Item_StandardiseretVareBeskrivelse,
                    tgt.Item_VareKlassifikation = src.Item_VareKlassifikation,
                    tgt.Item_VareKlassifikation_Type = src.Item_VareKlassifikation_Type,
                    tgt.Item_AfgiftsKategori = src.Item_AfgiftsKategori,
                    tgt.Item_AfgiftsKategoriProcent = src.Item_AfgiftsKategoriProcent,
                    tgt.FakturaUdstederGebyrRabatAarsag = src.FakturaUdstederGebyrRabatAarsag,
                    tgt.FakturaUdstederGebyrRabatKategori = src.FakturaUdstederGebyrRabatKategori,
                    tgt.FakturaUdstederGebyrRabatFinanskonto = src.FakturaUdstederGebyrRabatFinanskonto,
                    tgt.FakturaUdstederGebyrTillaegAarsag = src.FakturaUdstederGebyrTillaegAarsag,
                    tgt.FakturaUdstederGebyrTillaegKategori = src.FakturaUdstederGebyrTillaegKategori,
                    tgt.FakturaUdstederGebyrTillaegFinanskonto = src.FakturaUdstederGebyrTillaegFinanskonto,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Fakturalinje, Item_FakturaUdstederVareNr, Item_FakturaUdstederVareNavn,
                    FakturaLinjeNr, AntalEnhederFaktureret_Type,
                    Item_FakturaUdstederensVareNr, Item_FakturaUdstederensVareNr_Type,
                    BasePrice_EnhedsStørrelse, BasePrice_EnhedsStørrelse_Type,
                    Item_FakturaUdstederVareNavnKort,
                    Line_DimensionsKontoStreng, LeveranceKrav, LeveringsDato_2,
                    Item_KundensVareNr, Item_KundensVareNr_Type, Item_KundensVareBeskrivelse,
                    Item_FakturaUdstederensVareBeskrivelse,
                    Item_FabrikantensVareNr, Item_FabrikantensVareNr_Type, Item_FabrikantensVareBeskrivelse,
                    Item_StandardiseretVareNr, Item_StandardiseretVareNr_Type, Item_StandardiseretVareBeskrivelse,
                    Item_VareKlassifikation, Item_VareKlassifikation_Type,
                    Item_AfgiftsKategori, Item_AfgiftsKategoriProcent,
                    FakturaUdstederGebyrRabatAarsag, FakturaUdstederGebyrRabatKategori,
                    FakturaUdstederGebyrRabatFinanskonto,
                    FakturaUdstederGebyrTillaegAarsag, FakturaUdstederGebyrTillaegKategori,
                    FakturaUdstederGebyrTillaegFinanskonto,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Fakturalinje, src.Item_FakturaUdstederVareNr, src.Item_FakturaUdstederVareNavn,
                    src.FakturaLinjeNr, src.AntalEnhederFaktureret_Type,
                    src.Item_FakturaUdstederensVareNr, src.Item_FakturaUdstederensVareNr_Type,
                    src.BasePrice_EnhedsStørrelse, src.BasePrice_EnhedsStørrelse_Type,
                    src.Item_FakturaUdstederVareNavnKort,
                    src.Line_DimensionsKontoStreng, src.LeveranceKrav, src.LeveringsDato_2,
                    src.Item_KundensVareNr, src.Item_KundensVareNr_Type, src.Item_KundensVareBeskrivelse,
                    src.Item_FakturaUdstederensVareBeskrivelse,
                    src.Item_FabrikantensVareNr, src.Item_FabrikantensVareNr_Type, src.Item_FabrikantensVareBeskrivelse,
                    src.Item_StandardiseretVareNr, src.Item_StandardiseretVareNr_Type, src.Item_StandardiseretVareBeskrivelse,
                    src.Item_VareKlassifikation, src.Item_VareKlassifikation_Type,
                    src.Item_AfgiftsKategori, src.Item_AfgiftsKategoriProcent,
                    src.FakturaUdstederGebyrRabatAarsag, src.FakturaUdstederGebyrRabatKategori,
                    src.FakturaUdstederGebyrRabatFinanskonto,
                    src.FakturaUdstederGebyrTillaegAarsag, src.FakturaUdstederGebyrTillaegKategori,
                    src.FakturaUdstederGebyrTillaegFinanskonto,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Fakturalinje_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Fakturalinje]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN DW_Timestamp_Update IS NULL THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN DW_Timestamp_Update IS NOT NULL THEN 1 ELSE 0 END) AS updated,
                COUNT(Item_FakturaUdstederVareNavn) AS has_vare_navn
            FROM [dim].[Fakturalinje]
            WHERE DW_EK_Fakturalinje <> -1
        """
            )
            stats = cursor.fetchone()
            total_dim_rows = stats[0]
            inserts_only = stats[1]
            updated_count = stats[2]
            has_vare_navn = stats[3]

            context.log.info(
                f"Dimension sync complete: {total_dim_rows} total rows "
                f"({inserts_only} insert-only, {updated_count} updated)"
            )
            context.log.info(f"Coverage: {has_vare_navn}/{total_dim_rows} with VareNavn")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_vare_navn": has_vare_navn,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Fakturalinje",
            "source_tables": "stg.dim_fakturalinje_mysupply + map.Fakturalinje",
            "scd_type": "Type 1 (overwrite)",
        }
    )
