-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Add full CSV specification columns to dim.FakturaEgenskaber and
-- dim.Fakturalinje.
--
-- AB#28293 — Tilføj Kreditnota (DokumentType) + full MySupply field coverage.
--
-- Adds:
--   dim.FakturaEgenskaber: 30 new columns (29 high-NULL header + DokumentType)
--   dim.Fakturalinje: 17 new columns (high-NULL line descriptive fields)
--
-- Idempotent: safe to re-run (all ADD COLUMN guarded by sys.columns check).
-- ============================================================================

-- ---------- dim.FakturaEgenskaber: Phase 2 descriptive columns ---------------

DECLARE @tbl_egenskaber INT = OBJECT_ID(N'[dim].[FakturaEgenskaber]');

IF @tbl_egenskaber IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'RegningsModtagerID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [RegningsModtagerID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'RegningsModtagerID_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [RegningsModtagerID_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KøbsOrdreNummer')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KøbsOrdreNummer] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'SalgsOrdreNummer')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [SalgsOrdreNummer] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeIdentification')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeIdentification] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeIdentification_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeIdentification_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'DimensionKontoStreng')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [DimensionKontoStreng] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeVejNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeVejNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeHusNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeHusNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeByNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeByNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundePostNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundePostNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeLandeKode')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeLandeKode] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederIdentifikation')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederIdentifikation] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederIdentifikation_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederIdentifikation_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederVejNavn_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederVejNavn_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederHusNr_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederHusNr_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederByNavn_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederByNavn_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederPostNr_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederPostNr_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederLandKode_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederLandKode_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederCVRNummer')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederCVRNummer] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederCVRNummer_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederCVRNummer_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederReferenceIdenrifikation')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederReferenceIdenrifikation] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederReferenceNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederReferenceNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederReferenceTelefonNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederReferenceTelefonNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederReferenceEMail')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederReferenceEMail] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KortArtsKode')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KortArtsKode] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'BetalingsType')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [BetalingsType] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'BetalingsID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [BetalingsID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Kreditnummer')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Kreditnummer] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Kontonummer')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Kontonummer] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Kontotype')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Kontotype] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'AfdelingsID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [AfdelingsID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'ToldKurs')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [ToldKurs] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'ToldKurs_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [ToldKurs_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'DokumentType')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [DokumentType] NVARCHAR(20) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceTelefonNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceTelefonNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceFaxNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceFaxNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceEMail')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceEMail] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KundeReferenceRolle')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KundeReferenceRolle] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeIdentification')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeIdentification] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeIdentification_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeIdentification_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_DimensionKontoStreng')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_DimensionKontoStreng] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeVejNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeVejNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeHusNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeHusNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeByNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeByNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundePostNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundePostNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeLandeKode')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeLandeKode] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceTelefonNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceTelefonNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceFaxNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceFaxNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceEMail')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceEMail] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Juridisk_KundeReferenceRolle')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Juridisk_KundeReferenceRolle] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederAdresseID_Vareafsendelse')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederAdresseID_Vareafsendelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'FakturaUdstederReferenceFaxNr')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [FakturaUdstederReferenceFaxNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'BankID')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [BankID] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'BankNavn')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [BankNavn] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Kontantrabatsats')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Kontantrabatsats] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'Strafrentesats')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [Strafrentesats] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'KontantRabatDato')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [KontantRabatDato] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'StartRenteDato')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [StartRenteDato] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'ZeroRated_AfgiftsKategori')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [ZeroRated_AfgiftsKategori] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'ZeroRated_AfgiftsKategoriProcent')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [ZeroRated_AfgiftsKategoriProcent] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategori')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategori] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriIndikering')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriIndikering] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriÅrsag')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriÅrsag] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriKvantitet')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriKvantitet] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriKvantitet_Type')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriKvantitet_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriKvantitet_List')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriKvantitet_List] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriTotal')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriTotal] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_egenskaber AND name = 'GebyrKategoriFinanskonto')
        ALTER TABLE [dim].[FakturaEgenskaber] ADD [GebyrKategoriFinanskonto] NVARCHAR(MAX) NULL;
END
GO

-- ---------- dim.Fakturalinje: Phase 2 + high-NULL line columns ---------------

DECLARE @tbl_linje INT = OBJECT_ID(N'[dim].[Fakturalinje]');

IF @tbl_linje IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaLinjeNr')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaLinjeNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'AntalEnhederFaktureret_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [AntalEnhederFaktureret_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FakturaUdstederensVareNr')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FakturaUdstederensVareNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FakturaUdstederensVareNr_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FakturaUdstederensVareNr_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'BasePrice_EnhedsStørrelse')
        ALTER TABLE [dim].[Fakturalinje] ADD [BasePrice_EnhedsStørrelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'BasePrice_EnhedsStørrelse_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [BasePrice_EnhedsStørrelse_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FakturaUdstederVareNavnKort')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FakturaUdstederVareNavnKort] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Line_DimensionsKontoStreng')
        ALTER TABLE [dim].[Fakturalinje] ADD [Line_DimensionsKontoStreng] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'LeveranceKrav')
        ALTER TABLE [dim].[Fakturalinje] ADD [LeveranceKrav] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'LeveringsDato_2')
        ALTER TABLE [dim].[Fakturalinje] ADD [LeveringsDato_2] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_KundensVareNr')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_KundensVareNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_KundensVareNr_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_KundensVareNr_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_KundensVareBeskrivelse')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_KundensVareBeskrivelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FakturaUdstederensVareBeskrivelse')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FakturaUdstederensVareBeskrivelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FabrikantensVareNr')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FabrikantensVareNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FabrikantensVareNr_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FabrikantensVareNr_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_FabrikantensVareBeskrivelse')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_FabrikantensVareBeskrivelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_StandardiseretVareNr')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_StandardiseretVareNr] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_StandardiseretVareNr_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_StandardiseretVareNr_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_StandardiseretVareBeskrivelse')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_StandardiseretVareBeskrivelse] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_VareKlassifikation')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_VareKlassifikation] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_VareKlassifikation_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_VareKlassifikation_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_AfgiftsKategori')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_AfgiftsKategori] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'Item_AfgiftsKategoriProcent')
        ALTER TABLE [dim].[Fakturalinje] ADD [Item_AfgiftsKategoriProcent] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategori')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategori] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriIndikering')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriIndikering] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriÅrsag')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriÅrsag] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriKvantitet')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriKvantitet] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriKvantitet_Type')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriKvantitet_Type] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriKvantitet_List')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriKvantitet_List] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriTotal')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriTotal] NVARCHAR(MAX) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_linje AND name = 'FakturaUdstederGebyrKategoriFinanskonto')
        ALTER TABLE [dim].[Fakturalinje] ADD [FakturaUdstederGebyrKategoriFinanskonto] NVARCHAR(MAX) NULL;
END
GO

-- ---------- fct.Fakturalinje: VAT valuta codes + zero-rated measures ----------
-- Must match the columns written by FctUpd_fakturalinje.py's MERGE.

DECLARE @tbl_fct INT = OBJECT_ID(N'[fct].[Fakturalinje]');

IF @tbl_fct IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_MomsGrundlag] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_MomsBeloeb] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'VAT_AfgiftsKategori')
        ALTER TABLE [fct].[Fakturalinje] ADD [VAT_AfgiftsKategori] NVARCHAR(50) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'ZeroRated_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [ZeroRated_MomsGrundlag] DECIMAL(18, 4) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_ZeroRated_MomsGrundlag')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_ZeroRated_MomsGrundlag] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'ZeroRated_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [ZeroRated_MomsBeloeb] DECIMAL(18, 4) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_ZeroRated_MomsBeloeb')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_ZeroRated_MomsBeloeb] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_LinjeTotal')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_LinjeTotal] NVARCHAR(10) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = @tbl_fct AND name = 'Valutakode_FakturaTotal')
        ALTER TABLE [fct].[Fakturalinje] ADD [Valutakode_FakturaTotal] NVARCHAR(10) NULL;
END
GO
