-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw_ek_Borger på dm's HAIBA-case-tabeller
--
-- Tabellerne er oprettet uden kolonnen af [internal ticket]. Landes både her og i
-- tables/haiba/01_dm.sql: migrationen
-- retter eksisterende databaser, base-DDL'en er alt en frisk database får.
--
-- Kolonnen er NOT NULL DEFAULT (-1): dm genopbygges fuldt ved hver kørsel, så
-- eksisterende rækker får sentinellen indtil næste rebuild fylder den ud.
-- ============================================================================

IF OBJECT_ID(N'[dm].[HAIBA_Cases_Infektion]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_Infektion]', 'dw_ek_Borger') IS NULL
BEGIN
    ALTER TABLE [dm].[HAIBA_Cases_Infektion]
        ADD [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_dm_HAIBA_Cases_Infektion_Borger] DEFAULT (-1);
END
GO

IF OBJECT_ID(N'[dm].[HAIBA_Cases_SSI]', 'U') IS NOT NULL
   AND COL_LENGTH('[dm].[HAIBA_Cases_SSI]', 'dw_ek_Borger') IS NULL
BEGIN
    ALTER TABLE [dm].[HAIBA_Cases_SSI]
        ADD [dw_ek_Borger] INT NOT NULL CONSTRAINT [DF_dm_HAIBA_Cases_SSI_Borger] DEFAULT (-1);
END
GO
