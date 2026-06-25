-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Fact Views (vw_fct schema)
--
-- Reporting views for fact tables.
-- SOR organisation FKs are resolved via conceptmap.* (not stored on fct).
-- DW_EK_Organisation_Okosted is resolved via conceptmap.okosted_to_sor.
-- Enriched views use f.* plus explicit computed/joined columns only.
-- ============================================================================

-- vw_fct.Postering
-- Adds newest-row indicator per invoice.
-- Postering relates to an invoice via DW_EK_Faktura (grain is ref_LedgertransId).
-- fct.DW_EK_Okosted = okosted source; view exposes SOR target as DW_EK_Organisation_Okosted.
CREATE OR ALTER VIEW [vw_fct].[Postering]
AS
SELECT
    p.*,
    COALESCE(cm_okosted.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Okosted],
    ROW_NUMBER() OVER (
        PARTITION BY p.DW_EK_Faktura
        ORDER BY p.DW_haendelsestidspunkt DESC, p.ref_LedgertransId DESC
    ) AS rn_newest_per_faktura
FROM [fct].[Postering] p
LEFT JOIN [conceptmap].[OkostedToSOR] cm_okosted
    ON cm_okosted.[DW_EK_Okosted] = p.[DW_EK_Okosted];
GO

-- vw_fct.Faktura
-- DW_EK_Organisation_Regningsmodtager from ConceptMap EAN -> SOR.
-- DW_EK_Kontoplan_Postering / DW_EK_Postering resolved from the NEWEST related
-- posting via DW_EK_Faktura (ref.Faktura_To_Postering stamps fct.Postering.DW_EK_Faktura).
-- These are single-valued (newest posting) and descriptive only — fct.Faktura
-- stores no account/posting FK (finding 2.2). For account-weighted measures,
-- aggregate fct.Postering by DW_EK_Kontoplan instead of reading this column.
CREATE OR ALTER VIEW [vw_fct].[Faktura]
AS
SELECT
    f.*,
    COALESCE(p.[DW_EK_Kontoplan], -1) AS [DW_EK_Kontoplan_Postering],
    COALESCE(p.[DW_EK_Postering], -1) AS [DW_EK_Postering],
    COALESCE(cm_ean.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Regningsmodtager]
FROM [fct].[Faktura] f
LEFT JOIN [conceptmap].[EANToSOR] cm_ean
    ON cm_ean.[DW_EK_EAN] = f.[DW_EK_EAN_Regningsmodtager]
LEFT JOIN [vw_fct].[Postering] p
    ON p.[DW_EK_Faktura] = f.[DW_EK_Faktura]
   AND f.[DW_EK_Faktura] <> -1
   AND p.[rn_newest_per_faktura] = 1;
GO

-- vw_fct.Fakturalinje
-- Enriches Fakturalinje with DW_EK_Kontoplan from newest related Postering,
-- related via DW_EK_Faktura (excluding the UKENDT sentinel -1).
-- DW_EK_Organisation_Regningsmodtager from ConceptMap EAN -> SOR.
CREATE OR ALTER VIEW [vw_fct].[Fakturalinje]
AS
SELECT
    f.*,
    COALESCE(p.[DW_EK_Kontoplan], -1) AS [DW_EK_Kontoplan_Postering],
    COALESCE(cm_ean.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Regningsmodtager]
FROM [fct].[Fakturalinje] f
LEFT JOIN [conceptmap].[EANToSOR] cm_ean
    ON cm_ean.[DW_EK_EAN] = f.[DW_EK_EAN_Regningsmodtager]
LEFT JOIN [vw_fct].[Postering] p
    ON p.[DW_EK_Faktura] = f.[DW_EK_Faktura]
    AND f.[DW_EK_Faktura] <> -1
    AND p.[rn_newest_per_faktura] = 1;
GO

-- vw_fct.Vareudlevering
-- Organisation via ConceptMap ref tables (SOR targets).
CREATE OR ALTER VIEW [vw_fct].[Vareudlevering]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Lager],
    COALESCE(cm_rekvirent.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Rekvirent]
FROM [fct].[Vareudlevering] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[DW_EK_Lager] = f.[DW_EK_Lager]
LEFT JOIN [conceptmap].[RekvirentToSOR] cm_rekvirent
    ON cm_rekvirent.[DW_EK_Rekvirent] = f.[DW_EK_Rekvirent];
GO

-- vw_fct.Lagerbeholdning (active rows only)
CREATE OR ALTER VIEW [vw_fct].[Lagerbeholdning]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Lager]
FROM [fct].[Lagerbeholdning] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[DW_EK_Lager] = f.[DW_EK_Lager]
WHERE f.[DW_IsDeleted] = 0;
GO

-- vw_fct.AfdelingLagerbeholdning (active rows only)
CREATE OR ALTER VIEW [vw_fct].[AfdelingLagerbeholdning]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Lager],
    COALESCE(cm_rekvirent.[DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Rekvirent]
FROM [fct].[AfdelingLagerbeholdning] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[DW_EK_Lager] = f.[DW_EK_Lager]
LEFT JOIN [conceptmap].[RekvirentToSOR] cm_rekvirent
    ON cm_rekvirent.[DW_EK_Rekvirent] = f.[DW_EK_Rekvirent]
WHERE f.[DW_IsDeleted] = 0;
GO

CREATE OR ALTER VIEW [vw_fct].[Finanskladdelinje] AS SELECT * FROM [fct].[Finanskladdelinje];
GO

-- vw_fct.Budget
CREATE OR ALTER VIEW [vw_fct].[Budget]
AS
SELECT *
FROM [fct].[Budget];
GO

-- vw_fct.IndkoebsanalyseLinje
-- Exposes IsCreditnote flag for filtering credit notes in spend analysis.
CREATE OR ALTER VIEW [vw_fct].[IndkoebsanalyseLinje]
AS
SELECT *
FROM [fct].[IndkoebsanalyseLinje];
GO
