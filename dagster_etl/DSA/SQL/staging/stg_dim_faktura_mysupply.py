# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Faktura from MySupply (Dimension Layer)

Aggregate MySupply faktura data at CleanCode_Faktura level (one row per faktura).
Derived from pre_stg_fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Allowance/charges are 1:n per header, split into Rabat (Indikering='false')
# and Tillaeg (Indikering='true'/NULL). Rows with zero/null Total are excluded.
# Aarsag is a composite '{Aarsag}:{Total}' per position. Columns are '#'-delimited,
# position-aligned (ORDER BY id), '#' stripped from source values and ':' stripped
# from Aarsag so the composite stays parseable.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
HAC_DIM_CTE_SQL = """\
;WITH hac_base AS (
    SELECT *
    FROM [ext].[statistic_invoice_header_allowance_charge_mysupply]
    WHERE GebyrKategoriTotal IS NOT NULL
      AND TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)) <> 0
),
hac_rabat AS (
    SELECT
        statistic_invoice_header_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([GebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(GebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrRabatAarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrRabatKategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategoriFinanskonto, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrRabatFinanskonto
    FROM hac_base
    WHERE LOWER(GebyrKategoriIndikering) = 'false'
    GROUP BY statistic_invoice_header_id
),
hac_tillaeg AS (
    SELECT
        statistic_invoice_header_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([GebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(GebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrTillaegAarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrTillaegKategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(GebyrKategoriFinanskonto, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS GebyrTillaegFinanskonto
    FROM hac_base
    WHERE LOWER(GebyrKategoriIndikering) <> 'false' OR GebyrKategoriIndikering IS NULL
    GROUP BY statistic_invoice_header_id
)
"""


@asset(
    key=AssetKey(["erp", "stg_dim_faktura_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "faktura",
    },
    description="Aggregate MySupply faktura data at CleanCode_Faktura level (one row per faktura) - derived from pre_stg_fakturalinje_mysupply",
)
def stg_dim_faktura_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply faktura (invoice) data.

    Aggregates pre_stg_fakturalinje_mysupply at header key grain:
    - CleanCode_Faktura as DimUpd join key (header key, unique)
    - CleanCode_Kreditor carried from the winning row
    - Selects most recent record (by Header_DW_LoadTimestamp DESC)
    - Deduplicates to one row per CleanCode_Faktura (matches map.Faktura grain)
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_faktura_mysupply from base layer...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_faktura_mysupply];")

            # Create staging table aggregating from base layer
            cursor.execute(
                HAC_DIM_CTE_SQL
                + """
                SELECT
                    ranked.CleanCode_Faktura,
                    ranked.CleanCode_Kreditor,
                    ranked.CleanCode_Faktura AS bk_Faktura,
                    ranked.CleanCode_Kreditor AS bk_Kreditor,
                    ranked.FakturaNr,
                    ranked.FakturaValutaKode,
                    ranked.FirmaNavn,
                    ranked.LeverandorCVR,
                    ranked.Adresse,
                    ranked.ByNavn,
                    ranked.Postnummer,
                    CASE
                        WHEN ranked.MetadataType IN ('PIE', 'PIP', 'OIOUBL Invoice') THEN 'Faktura'
                        WHEN ranked.MetadataType IN ('PCM', 'PCP', 'OIOUBL CreditNote') THEN 'Kreditnota'
                        ELSE 'Ukendt'
                    END AS DokumentType,
                    ranked.RegningsModtagerID,
                    ranked.RegningsModtagerID_Type,
                    ranked.KøbsOrdreNummer,
                    ranked.SalgsOrdreNummer,
                    ranked.KundeIdentification,
                    ranked.KundeIdentification_Type,
                    ranked.DimensionKontoStreng,
                    ranked.KundeNavn,
                    ranked.KundeVejNavn,
                    ranked.KundeHusNr,
                    ranked.KundeByNavn,
                    ranked.KundePostNr,
                    ranked.KundeLandeKode,
                    ranked.KundeReferenceID,
                    ranked.KundeReferenceNavn,
                    ranked.FakturaUdstederIdentifikation,
                    ranked.FakturaUdstederIdentifikation_Type,
                    ranked.FakturaUdstederNavn,
                    ranked.FakturaUdstederVejNavn_Vareafsendelse,
                    ranked.FakturaUdstederHusNr_Vareafsendelse,
                    ranked.FakturaUdstederByNavn_Vareafsendelse,
                    ranked.FakturaUdstederPostNr_Vareafsendelse,
                    ranked.FakturaUdstederLandKode_Vareafsendelse,
                    ranked.FakturaUdstederCVRNummer,
                    ranked.FakturaUdstederCVRNummer_Type,
                    ranked.FakturaUdstederReferenceIdenrifikation,
                    ranked.FakturaUdstederReferenceNavn,
                    ranked.FakturaUdstederReferenceTelefonNr,
                    ranked.FakturaUdstederReferenceEMail,
                    ranked.KortArtsKode,
                    ranked.BetalingsType,
                    ranked.BetalingsID,
                    ranked.Kreditnummer,
                    ranked.Kontonummer,
                    ranked.Kontotype,
                    ranked.AfdelingsID,
                    ranked.ToldKurs,
                    ranked.ToldKurs_Type,
                    ranked.KundeReferenceTelefonNr,
                    ranked.KundeReferenceFaxNr,
                    ranked.KundeReferenceEMail,
                    ranked.KundeReferenceRolle,
                    ranked.Juridisk_KundeIdentification,
                    ranked.Juridisk_KundeIdentification_Type,
                    ranked.Juridisk_DimensionKontoStreng,
                    ranked.Juridisk_KundeNavn,
                    ranked.Juridisk_KundeVejNavn,
                    ranked.Juridisk_KundeHusNr,
                    ranked.Juridisk_KundeByNavn,
                    ranked.Juridisk_KundePostNr,
                    ranked.Juridisk_KundeLandeKode,
                    ranked.Juridisk_KundeReferenceID,
                    ranked.Juridisk_KundeReferenceNavn,
                    ranked.Juridisk_KundeReferenceTelefonNr,
                    ranked.Juridisk_KundeReferenceFaxNr,
                    ranked.Juridisk_KundeReferenceEMail,
                    ranked.Juridisk_KundeReferenceRolle,
                    ranked.FakturaUdstederAdresseID_Vareafsendelse,
                    ranked.FakturaUdstederReferenceFaxNr,
                    ranked.BankID,
                    ranked.BankNavn,
                    ranked.Kontantrabatsats,
                    ranked.Strafrentesats,
                    ranked.KontantRabatDato,
                    ranked.StartRenteDato,
                    ranked.ZeroRated_AfgiftsKategori,
                    ranked.ZeroRated_AfgiftsKategoriProcent,
                    ranked.statistic_invoice_header_id,
                    ranked.Header_DW_LoadTimestamp,
                    hac_r.GebyrRabatAarsag,
                    hac_r.GebyrRabatKategori,
                    hac_r.GebyrRabatFinanskonto,
                    hac_t.GebyrTillaegAarsag,
                    hac_t.GebyrTillaegKategori,
                    hac_t.GebyrTillaegFinanskonto
                INTO [stg].[dim_faktura_mysupply]
                FROM (
                    SELECT
                        -- Header key — MUST match stg_fct_faktura_mysupply / map.Faktura
                        -- ('CleanCode_{metadata_id}_{header_id}'). pre_fakturalinje.CleanCode_Faktura
                        -- is the FakturaNr-based BUSINESS key ('FAKTURA_..'), which is NOT unique per
                        -- header and would never join map.Faktura — do not use it as the dim key.
                        CONCAT('CleanCode_',
                            CAST(metadata_id AS NVARCHAR(50)), '_',
                            CAST(statistic_invoice_header_id AS NVARCHAR(50))
                        ) AS CleanCode_Faktura,
                        CleanCode_Kreditor,
                        FakturaNr,
                        FakturaValutaKode,
                        FirmaNavn,
                        LeverandorCVR,
                        Adresse,
                        ByNavn,
                        Postnummer,
                        MetadataType,
                        RegningsModtagerID,
                        RegningsModtagerID_Type,
                        KøbsOrdreNummer,
                        SalgsOrdreNummer,
                        KundeIdentification,
                        KundeIdentification_Type,
                        DimensionKontoStreng,
                        KundeNavn,
                        KundeVejNavn,
                        KundeHusNr,
                        KundeByNavn,
                        KundePostNr,
                        KundeLandeKode,
                        KundeReferenceID,
                        KundeReferenceNavn,
                        FakturaUdstederIdentifikation,
                        FakturaUdstederIdentifikation_Type,
                        FakturaUdstederNavn,
                        FakturaUdstederVejNavn_Vareafsendelse,
                        FakturaUdstederHusNr_Vareafsendelse,
                        FakturaUdstederByNavn_Vareafsendelse,
                        FakturaUdstederPostNr_Vareafsendelse,
                        FakturaUdstederLandKode_Vareafsendelse,
                        FakturaUdstederCVRNummer,
                        FakturaUdstederCVRNummer_Type,
                        FakturaUdstederReferenceIdenrifikation,
                        FakturaUdstederReferenceNavn,
                        FakturaUdstederReferenceTelefonNr,
                        FakturaUdstederReferenceEMail,
                        KortArtsKode,
                        BetalingsType,
                        BetalingsID,
                        Kreditnummer,
                        Kontonummer,
                        Kontotype,
                        AfdelingsID,
                        ToldKurs,
                        ToldKurs_Type,
                        KundeReferenceTelefonNr,
                        KundeReferenceFaxNr,
                        KundeReferenceEMail,
                        KundeReferenceRolle,
                        Juridisk_KundeIdentification,
                        Juridisk_KundeIdentification_Type,
                        Juridisk_DimensionKontoStreng,
                        Juridisk_KundeNavn,
                        Juridisk_KundeVejNavn,
                        Juridisk_KundeHusNr,
                        Juridisk_KundeByNavn,
                        Juridisk_KundePostNr,
                        Juridisk_KundeLandeKode,
                        Juridisk_KundeReferenceID,
                        Juridisk_KundeReferenceNavn,
                        Juridisk_KundeReferenceTelefonNr,
                        Juridisk_KundeReferenceFaxNr,
                        Juridisk_KundeReferenceEMail,
                        Juridisk_KundeReferenceRolle,
                        FakturaUdstederAdresseID_Vareafsendelse,
                        FakturaUdstederReferenceFaxNr,
                        BankID,
                        BankNavn,
                        Kontantrabatsats,
                        Strafrentesats,
                        KontantRabatDato,
                        StartRenteDato,
                        ZeroRated_AfgiftsKategori,
                        ZeroRated_AfgiftsKategoriProcent,
                        statistic_invoice_header_id,
                        Header_DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY metadata_id, statistic_invoice_header_id
                            ORDER BY Header_DW_LoadTimestamp DESC, statistic_invoice_header_id DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    -- Header grain, matching stg_fct_faktura_mysupply / map.Faktura. The non-null
                    -- id filter also drops the degenerate FakturaNr-only rows (e.g. 'FAKTURA_-').
                    WHERE metadata_id IS NOT NULL
                      AND statistic_invoice_header_id IS NOT NULL
                ) ranked
                LEFT JOIN hac_rabat hac_r
                    ON hac_r.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                LEFT JOIN hac_tillaeg hac_t
                    ON hac_t.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                WHERE ranked.rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_faktura_mysupply with {row_count} rows")

            # Create unique clustered index on header key
            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_faktura_mysupply_CleanCode
                ON [stg].[dim_faktura_mysupply](CleanCode_Faktura);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            # Diagnostic: Check attribute coverage
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(FakturaNr) AS has_faktura_nr,
                    COUNT(FirmaNavn) AS has_firma_navn,
                    COUNT(LeverandorCVR) AS has_cvr,
                    COUNT(FakturaValutaKode) AS has_valuta,
                    COUNT(GebyrRabatAarsag) AS has_gebyr_rabat,
                    COUNT(GebyrTillaegAarsag) AS has_gebyr_tillaeg
                FROM [stg].[dim_faktura_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with FakturaNr, "
                f"{stats[2]}/{stats[0]} with FirmaNavn, "
                f"{stats[3]}/{stats[0]} with CVR, {stats[4]}/{stats[0]} with Valutakode, "
                f"{stats[5]}/{stats[0]} with GebyrRabat, {stats[6]}/{stats[0]} with GebyrTillaeg"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_faktura_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply + ext.statistic_invoice_header_allowance_charge_mysupply",
            "composite_key": "CleanCode_Faktura",
            "deduplication": "ROW_NUMBER() on (CleanCode_Faktura) ORDER BY Header_DW_LoadTimestamp DESC, statistic_invoice_header_id DESC (deterministic tiebreaker)",
            "gebyr_aggregation": "STRING_AGG '#'-delimited, split by Indikering (Rabat/Tillaeg), zero/null Total excluded, Aarsag={Aarsag}:{Total}",
            "has_faktura_nr": stats[1],
            "has_firma_navn": stats[2],
            "has_cvr": stats[3],
            "has_valuta": stats[4],
            "has_gebyr_rabat": stats[5],
            "has_gebyr_tillaeg": stats[6],
            "layer": "dim (Layer 1)",
        }
    )
