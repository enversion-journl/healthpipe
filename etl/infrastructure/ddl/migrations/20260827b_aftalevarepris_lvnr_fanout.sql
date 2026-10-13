-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- fct.AftaleVarePris lvnr-fanout (spec 2026-08-27-alternate-lvnr-rows):
--   grain aendres fra bk_prrowid til (bk_prrowid, clean_code_AftaleVarePris),
--   kilder tilfoejes, kreditorvarenummer1/2/3 fjernes (raa lvnr bor i
--   dim.Leverandoervare), unikt indeks udskiftes.
-- Koerer mod BAADE staging og DSO (tabellen findes i begge). Idempotent:
-- alle guards taaler gentagen koersel og no-op'er paa friske databaser
-- (basis-DDL'en baerer netto-tilstanden). Ingen DML — kolonnereferencer i
-- guards gaar via sys.columns/COL_LENGTH og kompilerer uafhaengigt af skema.
-- Eksisterende raekker beholder basisformat-CleanCodes; fuld genopbygning
-- (full_load) efter deploy afloeser dem med udvidet format.

-- 1) kilder tilfoejes
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NULL
BEGIN
    ALTER TABLE [fct].[AftaleVarePris] ADD [kilder] NVARCHAR(100) NULL;
END
GO

-- 2) Gammelt unikt indeks paa bk_prrowid alene droppes
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                 AND name = 'IX_Fct_AftaleVarePris_bk_prrowid')
BEGIN
    DROP INDEX [IX_Fct_AftaleVarePris_bk_prrowid] ON [fct].[AftaleVarePris];
END
GO

-- 3) Sammensat unikt grain-indeks oprettes. Eksisterende data kan ikke
--    bryde det: bk_prrowid var unik foer denne migration.
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_grain')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_grain]
    ON [fct].[AftaleVarePris] ([bk_prrowid] ASC, [clean_code_AftaleVarePris] ASC) ON [PRIMARY];
END
GO

-- 4) kreditorvarenummer1/2/3 droppes. Auto-navngivne DEFAULT-constraints
--    droppes defensivt foerst (kolonnerne er NULL uden defaults i basis-DDL'en,
--    men aeldre miljoer kan afvige).
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kreditorvarenummer1') IS NOT NULL
BEGIN
    DECLARE @dc1 NVARCHAR(256);
    SELECT @dc1 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'kreditorvarenummer1';
    IF @dc1 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc1 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [kreditorvarenummer1];
END
GO

IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kreditorvarenummer2') IS NOT NULL
BEGIN
    DECLARE @dc2 NVARCHAR(256);
    SELECT @dc2 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'kreditorvarenummer2';
    IF @dc2 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc2 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [kreditorvarenummer2];
END
GO

IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kreditorvarenummer3') IS NOT NULL
BEGIN
    DECLARE @dc3 NVARCHAR(256);
    SELECT @dc3 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'kreditorvarenummer3';
    IF @dc3 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc3 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [kreditorvarenummer3];
END
GO
