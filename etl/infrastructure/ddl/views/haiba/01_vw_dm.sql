-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- HAIBA konsum-views (vw_dm-skemaet) — kun DSO
--
-- dm.HAIBA_* publicerer revisionssporet: SDS's vindue er forankret i et
-- kalenderår og ruller én gang om året, så arkivet soft-sletter ca. en
-- femtedel af tabellen hver januar — og de rækker forlader aldrig dm. En
-- ufiltreret SELECT over-tæller derfor mere år for år, og fejler stille i
-- retningen "flere infektioner".
--
-- Disse views gør den sikre vej til default: vw_dm.HAIBA_* filtrerer
-- dw_is_deleted = 0, så en konsument der ikke kender konventionen får de
-- aktive rækker. Revisionssporet (hvad SDS har trukket tilbage) læses fortsat
-- fra dm-tabellerne direkte.
-- ============================================================================

IF SCHEMA_ID('vw_dm') IS NULL
    EXEC('CREATE SCHEMA [vw_dm]');
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_AggInfektion]
AS
SELECT *
FROM [dm].[HAIBA_AggInfektion]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_AggInfektionEgenskaber]
AS
SELECT *
FROM [dm].[HAIBA_AggInfektionEgenskaber]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_AggOrtopaedi]
AS
SELECT *
FROM [dm].[HAIBA_AggOrtopaedi]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_AggOrtopaediEgenskaber]
AS
SELECT *
FROM [dm].[HAIBA_AggOrtopaediEgenskaber]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_CasesInfektion]
AS
SELECT *
FROM [dm].[HAIBA_CasesInfektion]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_CasesInfektionEgenskaber]
AS
SELECT *
FROM [dm].[HAIBA_CasesInfektionEgenskaber]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_CasesSSI]
AS
SELECT *
FROM [dm].[HAIBA_CasesSSI]
WHERE [dw_is_deleted] = 0;
GO

CREATE OR ALTER VIEW [vw_dm].[HAIBA_CasesSSIEgenskaber]
AS
SELECT *
FROM [dm].[HAIBA_CasesSSIEgenskaber]
WHERE [dw_is_deleted] = 0;
GO
