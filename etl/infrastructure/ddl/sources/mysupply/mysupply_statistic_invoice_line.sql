-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_line_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_line'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_line] (
        [id]                                          INT             NOT NULL,
        [statistic_invoice_header_id]                 INT             NOT NULL,
        [FakturaLinjeNr]                              NVARCHAR(512)   NULL,
        [AntalEnhederFaktureret]                      NVARCHAR(255)   NULL,
        [AntalEnhederFaktureret_Type]                 NVARCHAR(255)   NULL,
        [AntalEnhederFaktureret_List]                 NVARCHAR(255)   NULL,
        [LinjeBeløb]                                  NVARCHAR(255)   NULL,
        [LinjeBeløb_Valuta]                           NVARCHAR(255)   NULL,
        [KøbsOrdreNummer]                             NVARCHAR(255)   NULL,
        [SalgsOrdreNummer]                            NVARCHAR(255)   NULL,
        [KundeLeveringsIdentifikation]                NVARCHAR(255)   NULL,
        [KundeLeveringsIdentifikation_Type]           NVARCHAR(255)   NULL,
        [DimensionsKontoStreng]                       NVARCHAR(255)   NULL,
        [KundeNavn]                                   NVARCHAR(255)   NULL,
        [KundeReferenceIdentifikation]                NVARCHAR(255)   NULL,
        [KundeReferenceNavn]                          NVARCHAR(255)   NULL,
        [KundeReferenceTelefonNr]                     NVARCHAR(255)   NULL,
        [KundeReferenceFaxNr]                         NVARCHAR(255)   NULL,
        [KundeReferenceEMail]                         NVARCHAR(255)   NULL,
        [Kundesprog]                                  NVARCHAR(255)   NULL,
        [KundeLeveringsAdresseID]                     NVARCHAR(255)   NULL,
        [KundeLeveringsVejNavn]                       NVARCHAR(255)   NULL,
        [KundeLeveringsHusNr]                         NVARCHAR(255)   NULL,
        [KundeLeveringsDueslag]                       NVARCHAR(255)   NULL,
        [KundeLeveringsByNavn]                        NVARCHAR(255)   NULL,
        [KundeLeveringsPostNr]                        NVARCHAR(255)   NULL,
        [KundeLeveringsLandeKode]                     NVARCHAR(255)   NULL,
        [KundeVareNr]                                 NVARCHAR(255)   NULL,
        [KundeVareBeskrivelse]                        NVARCHAR(1000)  NULL,
        [KundensVareNr]                               NVARCHAR(255)   NULL,
        [KundensVareNr_Type]                          NVARCHAR(255)   NULL,
        [KundensVareBeskrivelse]                      NVARCHAR(255)   NULL,
        [FakturaUdstederensVareNr]                    NVARCHAR(255)   NULL,
        [FakturaUdstederensVareNr_Type]               NVARCHAR(255)   NULL,
        [FakturaUdstederensVareBeskrivelse]           NVARCHAR(512)   NULL,
        [FabrikantensVareNr]                          NVARCHAR(255)   NULL,
        [FabrikantensVareNr_Type]                     NVARCHAR(255)   NULL,
        [FabrikantensVareBeskrivelse]                 NVARCHAR(255)   NULL,
        [StandardiseretVareNr]                        NVARCHAR(255)   NULL,
        [StandardiseretVareNr_Type]                   NVARCHAR(255)   NULL,
        [StandardiseretVareBeskrivelse]               NVARCHAR(255)   NULL,
        [KatalogVareNr]                               NVARCHAR(255)   NULL,
        [KatalogVareNr_Type]                          NVARCHAR(255)   NULL,
        [KatalogVareBeskrivelse]                      NVARCHAR(255)   NULL,
        [KatalogIdentifikation]                       NVARCHAR(255)   NULL,
        [VareKlassifikation]                          NVARCHAR(255)   NULL,
        [VareKlassifikation_Type]                     NVARCHAR(255)   NULL,
        [VareKlassifikation_List]                     NVARCHAR(255)   NULL,
        [AfgiftsKategori]                             NVARCHAR(255)   NULL,
        [AfgiftsKategoriKode]                         NVARCHAR(255)   NULL,
        [AfgiftsKategoriProcent]                      NVARCHAR(255)   NULL,
        [EnhedsPris]                                  NVARCHAR(255)   NULL,
        [EnhedsPris_Valuta]                           NVARCHAR(255)   NULL,
        [EnhedsStørrelse]                             NVARCHAR(255)   NULL,
        [EnhedsStørrelse_Type]                        NVARCHAR(255)   NULL,
        [EnhedsStørrelse_List]                        NVARCHAR(255)   NULL,
        [LeveranceKrav]                               NVARCHAR(255)   NULL,
        [LeveringsDatoTypeIdentifikation_1]           NVARCHAR(255)   NULL,
        [LeveringsDato_1]                             NVARCHAR(255)   NULL,
        [LeveringsDatoTypeIdentifikation_2]           NVARCHAR(255)   NULL,
        [LeveringsDato_2]                             NVARCHAR(255)   NULL,
        [LeveringsDatoTypeIdentifikation_3]           NVARCHAR(255)   NULL,
        [LeveringsDato_3]                             NVARCHAR(255)   NULL,
        [LeveringsDatoTypeIdentifikation_4]           NVARCHAR(255)   NULL,
        [LeveringsDato_4]                             NVARCHAR(255)   NULL,
        [Item_FakturaUdstederVareNr]                  NVARCHAR(512)   NULL,
        [Item_FakturaUdstederVareNavn]                NVARCHAR(MAX)   NULL,
        [Item_KundensVareNr]                          NVARCHAR(255)   NULL,
        [Item_KundensVareNr_Type]                     NVARCHAR(255)   NULL,
        [Item_KundensVareBeskrivelse]                 NVARCHAR(255)   NULL,
        [Item_FakturaUdstederensVareNr]               NVARCHAR(512)   NULL,
        [Item_FakturaUdstederensVareNr_Type]          NVARCHAR(255)   NULL,
        [Item_FakturaUdstederensVareBeskrivelse]      NVARCHAR(2000)  NULL,
        [Item_FabrikantensVareNr]                     NVARCHAR(255)   NULL,
        [Item_FabrikantensVareNr_Type]                NVARCHAR(255)   NULL,
        [Item_FabrikantensVareBeskrivelse]            NVARCHAR(255)   NULL,
        [Item_StandardiseretVareNr]                   NVARCHAR(255)   NULL,
        [Item_StandardiseretVareNr_Type]              NVARCHAR(255)   NULL,
        [Item_StandardiseretVareBeskrivelse]          NVARCHAR(255)   NULL,
        [Item_KatalogVareNr]                          NVARCHAR(255)   NULL,
        [Item_KatalogVareNr_Type]                     NVARCHAR(255)   NULL,
        [Item_KatalogVareBeskrivelse]                 NVARCHAR(255)   NULL,
        [Item_KatalogIdentifikation]                  NVARCHAR(255)   NULL,
        [Item_VareKlassifikation]                     NVARCHAR(255)   NULL,
        [Item_VareKlassifikation_Type]                NVARCHAR(255)   NULL,
        [Item_VareKlassifikation_List]                NVARCHAR(255)   NULL,
        [Item_AfgiftsKategori]                        NVARCHAR(255)   NULL,
        [Item_AfgiftsKategoriKode]                    NVARCHAR(255)   NULL,
        [Item_AfgiftsKategoriProcent]                 NVARCHAR(255)   NULL,
        [Item_EnhedsPris]                             NVARCHAR(255)   NULL,
        [Item_EnhedsPris_Valuta]                      NVARCHAR(255)   NULL,
        [Item_EnhedsStørrelse]                        NVARCHAR(255)   NULL,
        [Item_EnhedsStørrelse_Type]                   NVARCHAR(255)   NULL,
        [Item_EnhedsStørrelse_List]                   NVARCHAR(255)   NULL,
        [BasePrice_EnhedsPris]                        NVARCHAR(255)   NULL,
        [BasePrice_EnhedsPris_Valuta]                 NVARCHAR(255)   NULL,
        [BasePrice_EnhedsStørrelse]                   NVARCHAR(255)   NULL,
        [BasePrice_EnhedsStørrelse_Type]              NVARCHAR(255)   NULL,
        [BasePrice_EnhedsStørrelse_List]              NVARCHAR(255)   NULL,
        [Item_FakturaUdstederVareNavnKort]            NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_line]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_dw_timestamp_update]
        ON [arc].[mysupply_statistic_invoice_line] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_statistic_invoice_header_id'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_statistic_invoice_header_id]
        ON [arc].[mysupply_statistic_invoice_line] ([statistic_invoice_header_id]);
END
GO
