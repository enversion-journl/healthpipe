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
-- dw_ek_Organisation_okosted is resolved via conceptmap.okosted_to_sor.
-- Enriched views use f.* plus explicit computed/joined columns only.
-- ============================================================================

-- vw_fct.Postering
-- Adds newest-row indicator per invoice.
-- Postering relates to an invoice via dw_ek_Faktura (grain is ref_ledgertrans_id).
-- fct.dw_ek_Okosted = okosted source; view exposes SOR target as dw_ek_Organisation_okosted.
CREATE OR ALTER VIEW [vw_fct].[Postering]
AS
SELECT
    p.*,
    COALESCE(cm_okosted.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_okosted],
    ROW_NUMBER() OVER (
        PARTITION BY p.dw_ek_Faktura
        ORDER BY p.dw_haendelsestidspunkt DESC, p.ref_ledgertrans_id DESC
    ) AS rn_newest_per_faktura
FROM [fct].[Postering] p
LEFT JOIN [conceptmap].[OkostedToSOR] cm_okosted
    ON cm_okosted.[dw_ek_Okosted] = p.[dw_ek_Okosted];
GO

-- vw_fct.Faktura
-- dw_ek_Organisation_regningsmodtager from ConceptMap ean -> SOR.
-- dw_ek_Kontoplan_postering / dw_ek_Postering resolved from the NEWEST related
-- posting via dw_ek_Faktura (ref.FakturaToPostering stamps fct.Postering.dw_ek_Faktura).
-- These are single-valued (newest posting) and descriptive only — fct.Faktura
-- stores no account/posting FK (finding 2.2). For account-weighted measures,
-- aggregate fct.Postering by dw_ek_Kontoplan instead of reading this column.
CREATE OR ALTER VIEW [vw_fct].[Faktura]
AS
SELECT
    f.*,
    COALESCE(p.[dw_ek_Kontoplan], -1) AS [dw_ek_Kontoplan_postering],
    COALESCE(p.[dw_ek_Postering], -1) AS [dw_ek_Postering],
    COALESCE(cm_ean.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_regningsmodtager]
FROM [fct].[Faktura] f
LEFT JOIN [conceptmap].[EANToSOR] cm_ean
    ON cm_ean.[dw_ek_EAN] = f.[dw_ek_EAN_regningsmodtager]
LEFT JOIN [vw_fct].[Postering] p
    ON p.[dw_ek_Faktura] = f.[dw_ek_Faktura]
   AND f.[dw_ek_Faktura] <> -1
   AND p.[rn_newest_per_faktura] = 1;
GO

-- vw_fct.Fakturalinje
-- Enriches Fakturalinje with dw_ek_Kontoplan from newest related Postering,
-- related via dw_ek_Faktura (excluding the UKENDT sentinel -1).
-- dw_ek_Organisation_regningsmodtager from ConceptMap ean -> SOR.
CREATE OR ALTER VIEW [vw_fct].[Fakturalinje]
AS
SELECT
    f.*,
    COALESCE(p.[dw_ek_Kontoplan], -1) AS [dw_ek_Kontoplan_postering],
    COALESCE(cm_ean.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_regningsmodtager]
FROM [fct].[Fakturalinje] f
LEFT JOIN [conceptmap].[EANToSOR] cm_ean
    ON cm_ean.[dw_ek_EAN] = f.[dw_ek_EAN_regningsmodtager]
LEFT JOIN [vw_fct].[Postering] p
    ON p.[dw_ek_Faktura] = f.[dw_ek_Faktura]
    AND f.[dw_ek_Faktura] <> -1
    AND p.[rn_newest_per_faktura] = 1;
GO

-- vw_fct.Vareudlevering
-- Organisation via ConceptMap ref tables (SOR targets).
CREATE OR ALTER VIEW [vw_fct].[Vareudlevering]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_lager],
    COALESCE(cm_rekvirent.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_rekvirent]
FROM [fct].[Vareudlevering] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[dw_ek_Lager] = f.[dw_ek_Lager]
LEFT JOIN [conceptmap].[RekvirentToSOR] cm_rekvirent
    ON cm_rekvirent.[dw_ek_Rekvirent] = f.[dw_ek_Rekvirent];
GO

-- vw_fct.Ordre
-- Organisation via ConceptMap ref table (SOR target of the ordering warehouse).
CREATE OR ALTER VIEW [vw_fct].[Ordre]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_lager]
FROM [fct].[Ordre] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[dw_ek_Lager] = f.[dw_ek_Lager];
GO

-- vw_fct.Vareudlevering_ReeltForbrug
-- Anti-join ref.RekvirentUndtagelse on raw clean_code_Rekvirent (via dim.sgh/rnr).
-- Trim-only, mirroring DSA/SQL/common/ils_clean_code_shared.sql_norm_code: the
-- key must be spelled the way map.Rekvirent minted it. A view cannot import, so
-- this is a documented mirror — keep the two in step.
CREATE OR ALTER VIEW [vw_fct].[Vareudlevering_ReeltForbrug]
AS
SELECT
    v.*
FROM [vw_fct].[Vareudlevering] v
WHERE NOT EXISTS (
    SELECT 1
    FROM [dim].[Rekvirent] d
    INNER JOIN [ref].[RekvirentUndtagelse] u
        ON u.[clean_code_Rekvirent] =
            'SGH_'
            + LTRIM(RTRIM(CAST(d.[sgh] AS NVARCHAR(50))))
            + '_RNR_'
            + LTRIM(RTRIM(CAST(d.[rnr] AS NVARCHAR(50))))
    WHERE d.[dw_ek_Rekvirent] = v.[dw_ek_Rekvirent]
      AND d.[dw_ek_Rekvirent] <> -1
      AND d.[sgh] IS NOT NULL
      AND d.[rnr] IS NOT NULL
);
GO

-- vw_fct.Lagerbeholdning (active rows only)
CREATE OR ALTER VIEW [vw_fct].[Lagerbeholdning]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_lager]
FROM [fct].[Lagerbeholdning] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[dw_ek_Lager] = f.[dw_ek_Lager]
WHERE f.[dw_is_deleted] = 0;
GO

-- vw_fct.AfdelingLagerbeholdning (active rows only)
CREATE OR ALTER VIEW [vw_fct].[AfdelingLagerbeholdning]
AS
SELECT
    f.*,
    COALESCE(cm_lager.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_lager],
    COALESCE(cm_rekvirent.[dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_rekvirent]
FROM [fct].[AfdelingLagerbeholdning] f
LEFT JOIN [conceptmap].[LagerToSOR] cm_lager
    ON cm_lager.[dw_ek_Lager] = f.[dw_ek_Lager]
LEFT JOIN [conceptmap].[RekvirentToSOR] cm_rekvirent
    ON cm_rekvirent.[dw_ek_Rekvirent] = f.[dw_ek_Rekvirent]
WHERE f.[dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_fct].[Finanskladdelinje] AS SELECT * FROM [fct].[Finanskladdelinje];
GO

-- vw_fct.Budget
CREATE OR ALTER VIEW [vw_fct].[Budget]
AS
SELECT *
FROM [fct].[Budget];
GO

-- vw_fct.AftaleVarePris
-- Prisversioner (+ syntetiske daekningsraekker for prisloese aftalelinjer) pr.
-- aftalelinje: én raekke pr. prisversion-ELLER-syntetisk-linje (grain
-- bk_prrowid). De fire forpakningsvariant-slots ligger som kolonner:
-- dw_ek_Leverandoervare_1-4 parvis med pris1-4/pakkestoerrelse1-4
-- (se etl/docs/contracts/beslutningslog_vareaftaler.md).
-- dw_is_current = 1 giver nyeste ikrafttraadte pris ELLER en prisloes linje
-- (som altid er dw_is_current = 1, jf. har_prisversion = 0) — view-paritet
-- med ILS-referencen (som ikke kender prisloese linjer) kraever derfor
-- filteret har_prisversion = 1 AND dw_is_current = 1, ikke dw_is_current
-- alene.
CREATE OR ALTER VIEW [vw_fct].[AftaleVarePris]
AS
SELECT *
FROM [fct].[AftaleVarePris]
WHERE [dw_is_deleted] = 0;
GO
