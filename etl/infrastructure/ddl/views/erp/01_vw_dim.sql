-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Dimension Views (vw_dim schema)
--
-- Simple pass-through views for dimension tables.
-- Used for security/access control layer on DSO.
-- ============================================================================

-- vw_dim.Kontoplan
CREATE OR ALTER VIEW [vw_dim].[Kontoplan]
AS
SELECT *
FROM [dim].[Kontoplan];
GO

-- vw_dim.Kreditor
CREATE OR ALTER VIEW [vw_dim].[Kreditor]
AS
SELECT *
FROM [dim].[Kreditor];
GO

-- vw_dim.Leverandoervare is defined in views/security/01_vw_dim_security.sql
-- (free-text/PII-risk columns are column-restricted, not a SELECT * pass-through).

-- vw_dim.Aftale (Type 1 — one row per agreement)
-- No filtering: the dimension carries active, expired and deleted agreements,
-- because historical purchases reference them. Expired = hoved_slut in the past.
CREATE OR ALTER VIEW [vw_dim].[Aftale]
AS
SELECT *
FROM [dim].[Aftale];
GO

-- vw_dim.ILSLeverandoer (Type 1 — one row per ILS supplier number, lnr)
-- Pass-through; giro = 'Person' rows are unmasked (masking is datamart responsibility).
CREATE OR ALTER VIEW [vw_dim].[ILSLeverandoer]
AS
SELECT *
FROM [dim].[ILSLeverandoer];
GO

-- vw_dim.Vareudlevering
CREATE OR ALTER VIEW [vw_dim].[Vareudlevering]
AS
SELECT *
FROM [dim].[Vareudlevering];
GO

-- vw_dim.Faktura
CREATE OR ALTER VIEW [vw_dim].[Faktura]
AS
SELECT *
FROM [dim].[Faktura];
GO

-- vw_dim.Fakturalinje is defined in views/security/01_vw_dim_security.sql
-- (dim.Fakturalinje is entirely MySupply free-text and is column-restricted).

-- vw_dim.Lager
CREATE OR ALTER VIEW [vw_dim].[Lager]
AS
SELECT *
FROM [dim].[Lager];
GO

-- vw_dim.Postering is defined in views/security/01_vw_dim_security.sql
-- (posteringstekst/transaktionstekst are free-text and are column-restricted).

-- vw_dim.Rekvirent is defined in views/security/01_vw_dim_security.sql
-- (rekvirent name for HP/OHP home patients is column-restricted).

-- vw_dim.ILSBruger
-- Presents the generic dim.ILSBruger columns under their ILS headings.
CREATE OR ALTER VIEW [vw_dim].[ILSBruger]
AS
SELECT
    [dw_ek_ILSBruger],
    [BrugerID]    = [bruger_id],
    [ILSinit]     = [initialer],
    [Navn]        = [name_text],
    [Sygehus]     = [primaer_sygehus],
    [Sygehusnavn] = [primaer_sygehus_navn],
    [Aktiv]       = [active]
FROM [dim].[ILSBruger];
GO

-- vw_dim.ean
CREATE OR ALTER VIEW [vw_dim].[ean]
AS
SELECT *
FROM [dim].[ean];
GO

CREATE OR ALTER VIEW [vw_dim].[Finanskladde] AS SELECT * FROM [dim].[Finanskladde];
GO
-- vw_dim.Finanskladdelinje is defined in views/security/01_vw_dim_security.sql
-- (beskrivelse is free-text and is column-restricted).

-- vw_dim.BudgetEgenskaber
CREATE OR ALTER VIEW [vw_dim].[BudgetEgenskaber]
AS
SELECT *
FROM [dim].[BudgetEgenskaber];
GO

-- vw_dim.UNSPSC
CREATE OR ALTER VIEW [vw_dim].[UNSPSC]
AS
SELECT *
FROM [dim].[UNSPSC];
GO

-- vw_dim.Dato
CREATE OR ALTER VIEW [vw_dim].[Dato]
AS
SELECT
    [dw_ek_Dato] AS [Dato ID],
    [dato] AS [Dato],
    [aar] AS [År],
    [aar_startdato] AS [Års startdato],
    [aar_slutdato] AS [Års slutdato],
    [aar_afsluttet] AS [År afsluttet],
    [kvartal_id] AS [Kvartal ID],
    [kvartal] AS [Kvartal],
    [kvartal_startdato] AS [Kvartals startdato],
    [kvartal_slutdato] AS [Kvartals slutdato],
    [kvartal_afsluttet] AS [Kvartal afsluttet],
    [kvartal_aar_id] AS [Kvartal og år ID],
    [kvartal_aar] AS [Kvartal og år],
    [maaned_id] AS [Måned ID],
    [maaned_lang] AS [Måned (lang)],
    [maaned] AS [Måned],
    [maaned_startdato] AS [Måneds startdato],
    [maaned_slutdato] AS [Måneds slutdato],
    [maaned_afsluttet] AS [Måned afsluttet],
    [maaned_aar_id] AS [Måned og år ID],
    [maaned_aar] AS [Måned og år],
    [uge_id] AS [Uge ID],
    [uge] AS [Uge],
    [uge_startdato] AS [Uges startdato],
    [uge_slutdato] AS [Uges slutdato],
    [uge_afsluttet] AS [Uge afsluttet],
    [uge_aar_id] AS [Uge og år ID],
    [uge_aar] AS [Uge og år],
    [dag_aar] AS [Dag i året],
    [dag_kvartal] AS [Dag i kvartalet],
    [dag_maaned] AS [Dag i måneden],
    [dag_uge] AS [Dag i ugen],
    [ugedag_lang] AS [Ugedag (lang)],
    [ugedag] AS [Ugedag],
    [er_hverdag] AS [Er hverdag],
    [hverdag_weekend] AS [Hverdag/Weekend],
    [er_helligdag] AS [Er helligdag],
    [helligdag] AS [Helligdag],
    [aar_antal_dage] AS [Antal dage i året],
    [kvartal_antal_dage] AS [Antal dage i kvartalet],
    [maaned_antal_dage] AS [Antal dage i måneden],
    [aar_relativ] AS [Antal år fra i dag],
    [kvartal_relativ] AS [Antal kvartaler fra i dag],
    [maaned_relativ] AS [Antal måneder fra i dag],
    [uge_relativ] AS [Antal uger fra i dag],
    [dag_relativ] AS [Antal dage fra i dag],
    [er_beredskabshelligdag] AS [Er beredskabshelligdag],
    [beredskabshelligdag] AS [Beredskabshelligdag]
FROM [dim].[Dato];
GO
