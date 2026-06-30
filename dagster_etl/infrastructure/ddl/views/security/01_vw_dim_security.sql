-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security: Dimension Views (vw_dim schema)
--
-- Security-focused views for PII masking and scoped organization access.
-- ============================================================================

-- vw_dim.Borger (always masked)
CREATE OR ALTER VIEW [vw_dim].[Borger]
AS
SELECT
    [DW_EK_Borger],
    [DW_SK_Borger],
    -- Masked fields (always hidden)
    [CPR]               = CAST([DW_EK_Borger] AS NVARCHAR(10)),  -- Pseudonymized (INT key)
    [Fornavne]          = N'[MASKERET]',
    [Efternavn]         = N'[MASKERET]',
    [Adresseringsnavn]  = N'[MASKERET]',
    -- Non-sensitive fields
    [KoenNavn],
    [Active],
    [Deceased],
    [DW_ValidFrom],
    [DW_ValidTo],
    [DW_IsCurrent],
    [DW_IsDeleted],
    [DW_Timestamp_Insert],
    [DW_Timestamp_Update],
    -- Security metadata
    [DPASikkerhed] = 'KB'  -- Kolonnebegrænset
FROM [dim].[Borger];
GO

-- vw_dim.Organisation
CREATE OR ALTER VIEW [vw_dim].[Organisation]
AS
SELECT
    [DW_EK_Organisation],
    [Code],
    [DW_EK_Org_Parent],
    [ParentCode],
    [Name],
    [Type],
    [Active],
    [DW_Timestamp_Insert],
    [DW_Timestamp_Update]
FROM [dim].[Organisation];
GO

-- vw_dim.Okosted
CREATE OR ALTER VIEW [vw_dim].[Okosted]
AS
SELECT
    [DW_EK_Okosted],
    [CodeOkosted],
    [Name],
    [Level],
    [DW_EK_Okosted_Parent],
    [ParentCode],
    [Active],
    [DW_Timestamp_Insert],
    [DW_Timestamp_Update]
FROM [dim].[Okosted];
GO

-- vw_dim.Vare (free-text columns column-restricted)
--
-- Varenavn/VarenavnKort can hold invoice free-text (MySupply) that may contain
-- names/CPR, and VarenummerKreditor can be CPR-formatted. These columns are masked
-- unless the caller has been granted sec.BrugerRoller.Fritekst (requestable access).
-- Clean product names remain available via dim.IlsVare (FK DW_EK_IlsVare) / vw_dim.IlsVare.
-- Downstream reporting joins use the surrogate key DW_EK_Vare, so masking is safe.
CREATE OR ALTER VIEW [vw_dim].[Vare]
AS
SELECT
    v.[DW_EK_Vare],
    v.[CVR],
    -- PII-risk free-text columns (masked unless caller has Fritekst access)
    [VarenummerKreditor] = beskyt.[VarenummerKreditor],
    [Varenavn]           = beskyt.[Varenavn],
    [VarenavnKort]       = beskyt.[VarenavnKort],
    -- Structured attributes (ILS-sourced, non-sensitive)
    v.[Maerke],
    v.[ILS_Varenummer],
    v.[FaellesVarenummer],
    v.[Stregkode],
    v.[StoerrelseTekst],
    v.[SammensatNavn],
    v.[Basisenhed],
    v.[Pakkestoerrelse1],
    v.[Pakkestoerrelse2],
    v.[Pakkestoerrelse3],
    v.[VareartKode],
    v.[VareartNavn],
    v.[VaregruppeKode],
    v.[VaregruppeNavn],
    v.[Varetype],
    v.[Aftale],
    v.[AftaleType],
    v.[Aftaletekst],
    v.[Kreditorvarenummer1],
    v.[Kreditorvarenummer2],
    v.[Kreditorvarenummer3],
    v.[Pris1],
    v.[Pris2],
    v.[Pris3],
    v.[ils_dsa_aftale_varenummer],
    v.[Kreditornummer],
    v.[DW_EK_Aftale],
    v.[DW_EK_IlsVare],
    v.[momskd],
    v.[afgiftkd],
    v.[DW_Timestamp_Insert],
    v.[DW_Timestamp_Update],
    -- Security metadata
    [DPASikkerhed] = beskyt.[DPASikkerhed]
FROM [dim].[Vare] AS v
CROSS APPLY [sec].[HentFritekstAdgang_v1](DB_NAME(), SUSER_SNAME()) AS adgang
CROSS APPLY [sec].[BeskytDimVare_v1](
    adgang.[Fritekst],
    v.[VarenummerKreditor],
    v.[Varenavn],
    v.[VarenavnKort]
) AS beskyt;
GO

-- vw_dim.Fakturalinje (free-text dimension, column-restricted)
--
-- dim.Fakturalinje consists entirely of MySupply invoice free-text (user-entered
-- item numbers, names and descriptions) which may contain names/CPR. All descriptive
-- columns are masked ([MASKERET]) unless the caller has been granted
-- sec.BrugerRoller.Fritekst (requestable access, evt. with 2-factor approval).
-- Only the surrogate key and audit timestamps are always visible; reporting joins
-- use DW_EK_Fakturalinje, so masking does not break joins.
CREATE OR ALTER VIEW [vw_dim].[Fakturalinje]
AS
SELECT
    f.[DW_EK_Fakturalinje],
    -- Free-text / PII-risk columns (masked unless caller has Fritekst access)
    [Item_FakturaUdstederVareNr]            = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederVareNr]            ELSE N'[MASKERET]' END,
    [Item_FakturaUdstederVareNavn]          = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederVareNavn]          ELSE N'[MASKERET]' END,
    [FakturaLinjeNr]                        = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaLinjeNr]                        ELSE N'[MASKERET]' END,
    [AntalEnhederFaktureret_Type]           = CASE WHEN a.[Fritekst] = 1 THEN f.[AntalEnhederFaktureret_Type]           ELSE N'[MASKERET]' END,
    [Item_FakturaUdstederensVareNr]         = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederensVareNr]         ELSE N'[MASKERET]' END,
    [Item_FakturaUdstederensVareNr_Type]    = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederensVareNr_Type]    ELSE N'[MASKERET]' END,
    [BasePrice_EnhedsStørrelse]             = CASE WHEN a.[Fritekst] = 1 THEN f.[BasePrice_EnhedsStørrelse]             ELSE N'[MASKERET]' END,
    [BasePrice_EnhedsStørrelse_Type]        = CASE WHEN a.[Fritekst] = 1 THEN f.[BasePrice_EnhedsStørrelse_Type]        ELSE N'[MASKERET]' END,
    [Item_FakturaUdstederVareNavnKort]      = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederVareNavnKort]      ELSE N'[MASKERET]' END,
    [Line_DimensionsKontoStreng]            = CASE WHEN a.[Fritekst] = 1 THEN f.[Line_DimensionsKontoStreng]            ELSE N'[MASKERET]' END,
    [LeveranceKrav]                         = CASE WHEN a.[Fritekst] = 1 THEN f.[LeveranceKrav]                         ELSE N'[MASKERET]' END,
    [LeveringsDato_2]                       = CASE WHEN a.[Fritekst] = 1 THEN f.[LeveringsDato_2]                       ELSE N'[MASKERET]' END,
    [Item_KundensVareNr]                    = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_KundensVareNr]                    ELSE N'[MASKERET]' END,
    [Item_KundensVareNr_Type]               = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_KundensVareNr_Type]               ELSE N'[MASKERET]' END,
    [Item_KundensVareBeskrivelse]           = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_KundensVareBeskrivelse]           ELSE N'[MASKERET]' END,
    [Item_FakturaUdstederensVareBeskrivelse]= CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FakturaUdstederensVareBeskrivelse]ELSE N'[MASKERET]' END,
    [Item_FabrikantensVareNr]               = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FabrikantensVareNr]               ELSE N'[MASKERET]' END,
    [Item_FabrikantensVareNr_Type]          = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FabrikantensVareNr_Type]          ELSE N'[MASKERET]' END,
    [Item_FabrikantensVareBeskrivelse]      = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_FabrikantensVareBeskrivelse]      ELSE N'[MASKERET]' END,
    [Item_StandardiseretVareNr]             = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_StandardiseretVareNr]             ELSE N'[MASKERET]' END,
    [Item_StandardiseretVareNr_Type]        = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_StandardiseretVareNr_Type]        ELSE N'[MASKERET]' END,
    [Item_StandardiseretVareBeskrivelse]    = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_StandardiseretVareBeskrivelse]    ELSE N'[MASKERET]' END,
    [Item_VareKlassifikation]               = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_VareKlassifikation]               ELSE N'[MASKERET]' END,
    [Item_VareKlassifikation_Type]          = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_VareKlassifikation_Type]          ELSE N'[MASKERET]' END,
    [Item_AfgiftsKategori]                  = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_AfgiftsKategori]                  ELSE N'[MASKERET]' END,
    [Item_AfgiftsKategoriProcent]           = CASE WHEN a.[Fritekst] = 1 THEN f.[Item_AfgiftsKategoriProcent]           ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrRabatAarsag]       = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrRabatAarsag]       ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrRabatKategori]     = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrRabatKategori]     ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrRabatFinanskonto]  = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrRabatFinanskonto]  ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrTillaegAarsag]     = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrTillaegAarsag]     ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrTillaegKategori]   = CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrTillaegKategori]   ELSE N'[MASKERET]' END,
    [FakturaUdstederGebyrTillaegFinanskonto]= CASE WHEN a.[Fritekst] = 1 THEN f.[FakturaUdstederGebyrTillaegFinanskonto]ELSE N'[MASKERET]' END,
    -- Audit (always visible)
    f.[DW_Timestamp_Insert],
    f.[DW_Timestamp_Update],
    -- Security metadata
    [DPASikkerhed] = CASE WHEN a.[Fritekst] = 1 THEN 'UB' ELSE 'KB' END
FROM [dim].[Fakturalinje] AS f
CROSS APPLY [sec].[HentFritekstAdgang_v1](DB_NAME(), SUSER_SNAME()) AS a;
GO
