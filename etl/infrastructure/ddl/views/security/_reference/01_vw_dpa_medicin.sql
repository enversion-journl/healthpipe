-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- REFERENCE ONLY — NOT DEPLOYED.
-- Implementation pattern for DPA per-domain secured views ([internal ticket], Medicin).
-- These views require a real fct.Medicinadministration (a later pipeline). They
-- live under views/security/_reference/ specifically so the non-recursive
-- views/security/*.sql glob in dso_infra_views_security does NOT execute them.
-- ============================================================================

-- dbo.TilladteBorgere_Medicin
-- Helper view: patients the current user may see based on row-based
-- organisation security on Medicinadministration.
CREATE OR ALTER VIEW [dbo].[TilladteBorgere_Medicin] AS
WITH TilladteFraAdm AS (
    SELECT fma.[dw_ek_Borger]
    FROM [fct].[Medicinadministration] AS fma
    WHERE EXISTS (
              SELECT 1
              FROM [sec].[HentRBOrganisation]('DPA_Medicin', SUSER_SNAME(), fma.[DW_EK_Organisation_adm]) AS r1
              WHERE r1.[DW_EK_RBOrganisation] IS NOT NULL)
          OR EXISTS (
              SELECT 1
              FROM [sec].[HentRBOrganisation]('DPA_Medicin', SUSER_SNAME(), fma.[DW_EK_Organisation_ord]) AS r2
              WHERE r2.[DW_EK_RBOrganisation] IS NOT NULL
          )
    GROUP BY fma.[dw_ek_Borger]
)
SELECT [dw_ek_Borger] FROM TilladteFraAdm
;
GO


-- vw_dim.Borger_Medicin
-- Secured dimension view: only patients the user has access to.
CREATE OR ALTER VIEW [vw_dim].[Borger_Medicin] AS
SELECT
    DimBorger.*
    ,'' AS [Populationsansvar]
FROM [dim].[Borger] AS DimBorger
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), 'DPA_Medicin') AS R
WHERE EXISTS (
          SELECT 1
          FROM [dbo].[TilladteBorgere_Medicin] AS tb
          WHERE tb.[dw_ek_Borger] = DimBorger.[dw_ek_Borger]
      )
   OR R.[Rolle] = 'U'
   OR DimBorger.[dw_ek_Borger] IN (-1, -99);
GO


-- vw_fct.Medicinadministration
-- Secured fact view with row-based security and PII masking on Notat.
CREATE OR ALTER VIEW [vw_fct].[Medicinadministration] AS
SELECT
    p.[DW_SK_Medicinadministration],
    p.[dw_ek_Borger] AS [DW_EK_Borger_Medicinadm],
    CASE
        WHEN R.[Rolle] = 'U'
             OR (R.[Populationsansvar] != '' AND R.[Populationsansvar] = p.[Populationsansvar])
            THEN p.[dw_sk_Borger]
        WHEN RB1.[DW_EK_RBOrganisation] IS NOT NULL THEN p.[dw_sk_Borger]
        WHEN RB2.[DW_EK_RBOrganisation] IS NOT NULL THEN p.[dw_sk_Borger]
        ELSE -1
    END AS [DW_SK_Borger_Medicinadm],
    p.[DW_EK_Organisation_adm],
    p.[DW_EK_Organisation_ord],
    p.[antal],
    CASE
        WHEN R.[Populationsansvar] != '' AND R.[Populationsansvar] = p.[Populationsansvar]
            THEN p.[Notat]
        ELSE SecNotat.[Personoplysning]
    END AS [Notat],
    [RBMatch] = CASE
        WHEN COALESCE(RB1.[DW_EK_RBOrganisation], RB2.[DW_EK_RBOrganisation]) IS NULL THEN 0
        ELSE 1
    END,
    p.[DW_Update],
    p.[dw_is_deleted]
FROM [fct].[Medicinadministration] AS p
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), 'DPA_Medicin') AS R
OUTER APPLY [sec].[HentRBOrganisation]('DPA_Medicin', SUSER_SNAME(), p.[DW_EK_Organisation_adm]) AS RB1
OUTER APPLY [sec].[HentRBOrganisation]('DPA_Medicin', SUSER_SNAME(), p.[DW_EK_Organisation_ord]) AS RB2
CROSS APPLY [sec].[BeskytPersonoplysning](
    R.[Rolle],
    p.[Populationsansvar],
    R.[Sikkerhedsdomaene],
    COALESCE(RB1.[DW_EK_RBOrganisation], RB2.[DW_EK_RBOrganisation]),
    p.[Notat],
    N'Maskeret'
) AS SecNotat;
GO
