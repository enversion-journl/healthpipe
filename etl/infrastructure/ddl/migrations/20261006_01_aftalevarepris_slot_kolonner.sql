-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- fct.AftaleVarePris slot-kolonner (se etl/docs/contracts/beslutningslog_vareaftaler.md):
-- grain aendres fra (bk_prrowid, clean_code_AftaleVarePris) tilbage til bk_prrowid
-- alene — lvnr-fan-out'en fjernes, og de fire forpakningsvariant-slots ligger som
-- kolonner (dw_ek_Leverandoervare_1-4 parvis med pris1-4/pakkestoerrelse1-4).
-- kilder, maerke, linje_start og linje_slut udgaar.
-- Koerer mod BAADE staging og DSO; map- og meta-blokke no-op'er paa DSO via
-- OBJECT_ID-guards. Idempotent: de destruktive blokke (DELETE + checkpoint-
-- nulstilling) er guardet paa den GAMLE skematilstand (kilder-kolonnen findes)
-- og koerer derfor praecis een gang, FOER kolonne-drops'ene; alt andet taaler
-- gentagen koersel.
--
-- Grain-skiftet goer alle eksisterende fakta-raekker og map-identiteter
-- foraeldede. fct.AftaleVarePris og dim.Leverandoervare/bridge.LeverandoervareILSVare
-- toemmes (UNKNOWN-raekken/-parret bevares), og checkpoint-nulstillingen faar
-- foerste ordinaere koersel til selv at genopbygge dim.Leverandoervare,
-- bridge.LeverandoervareILSVare og fct.AftaleVarePris fuldt fra det aktuelle
-- kildebillede — laftd-sourcede entiteter, hvis eneste kilde var laftds egne
-- lvnr-kolonner, udgaar dermed. map.Leverandoervare toemmes
-- IKKE: dens IDENTITY dw_ek-vaerdier refereres af andre fakta (fct.Fakturalinje),
-- og at bevare den stabil betyder at genopstagede entiteter faar deres gamle
-- noegler tilbage igen; en foraeldet map-raekke, der aldrig refereres igen, er
-- harmloes.

-- 1) Toem fakta, map, dim og bridge (kun mens den gamle skematilstand findes)
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC(N'DELETE FROM [fct].[AftaleVarePris]');
GO
IF OBJECT_ID(N'[map].[AftaleVarePris]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC(N'DELETE FROM [map].[AftaleVarePris]');
GO
IF OBJECT_ID(N'[dim].[Leverandoervare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC(N'DELETE FROM [dim].[Leverandoervare] WHERE [dw_ek_Leverandoervare] <> -1');
GO
IF OBJECT_ID(N'[bridge].[LeverandoervareILSVare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC(N'DELETE FROM [bridge].[LeverandoervareILSVare] WHERE NOT ([dw_ek_Leverandoervare] = -1 AND [dw_ek_ILSVare] = -1)');
GO

-- 2) Nulstil forbruger-checkpoints (kun staging har meta.DFMState)
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''dim'' AND table_name = ''Leverandoervare''');
GO
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''bridge'' AND table_name = ''LeverandoervareILSVare''');
GO
IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL
   AND OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
    EXEC('DELETE FROM meta.DFMState WHERE scope = ''staging'' AND schema_name = ''fct'' AND table_name = ''AftaleVarePris''');
GO

-- 3) Indeks paa dw_ek_Leverandoervare droppes FOER kolonnen
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                 AND name = 'IX_Fct_AftaleVarePris_Leverandoervare')
BEGIN
    DROP INDEX [IX_Fct_AftaleVarePris_Leverandoervare] ON [fct].[AftaleVarePris];
END
GO

-- 4) Gamle kolonner droppes (defensive drops af auto-navngivne DEFAULTs foerst)
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'kilder') IS NOT NULL
BEGIN
    DECLARE @dc1 NVARCHAR(256);
    SELECT @dc1 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'kilder';
    IF @dc1 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc1 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [kilder];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'maerke') IS NOT NULL
BEGIN
    DECLARE @dc2 NVARCHAR(256);
    SELECT @dc2 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'maerke';
    IF @dc2 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc2 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [maerke];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'linje_start') IS NOT NULL
BEGIN
    DECLARE @dc3 NVARCHAR(256);
    SELECT @dc3 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'linje_start';
    IF @dc3 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc3 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [linje_start];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'linje_slut') IS NOT NULL
BEGIN
    DECLARE @dc4 NVARCHAR(256);
    SELECT @dc4 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'linje_slut';
    IF @dc4 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc4 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [linje_slut];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'dw_ek_Leverandoervare') IS NOT NULL
BEGIN
    DECLARE @dc5 NVARCHAR(256);
    SELECT @dc5 = dc.name FROM sys.default_constraints dc
    JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE c.object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = 'dw_ek_Leverandoervare';
    IF @dc5 IS NOT NULL EXEC(N'ALTER TABLE [fct].[AftaleVarePris] DROP CONSTRAINT [' + @dc5 + N']');
    ALTER TABLE [fct].[AftaleVarePris] DROP COLUMN [dw_ek_Leverandoervare];
END
GO

-- 5) Slot-FK-kolonner tilfoejes
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND COL_LENGTH('[fct].[AftaleVarePris]', 'dw_ek_Leverandoervare_1') IS NULL
BEGIN
    ALTER TABLE [fct].[AftaleVarePris] ADD
        [dw_ek_Leverandoervare_1] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Leverandoervare_1] DEFAULT -1,
        [dw_ek_Leverandoervare_2] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Leverandoervare_2] DEFAULT -1,
        [dw_ek_Leverandoervare_3] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Leverandoervare_3] DEFAULT -1,
        [dw_ek_Leverandoervare_4] INT NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_ek_Leverandoervare_4] DEFAULT -1;
END
GO

-- 6) Grain-indekset skiftes: sammensat ud, unikt bk_prrowid ind
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                 AND name = 'IX_Fct_AftaleVarePris_grain')
BEGIN
    DROP INDEX [IX_Fct_AftaleVarePris_grain] ON [fct].[AftaleVarePris];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_bk_prrowid')
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_bk_prrowid]
    ON [fct].[AftaleVarePris] ([bk_prrowid] ASC) ON [PRIMARY];
END
GO

-- 7) FK-indeks pr. slot
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_Leverandoervare_1')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Leverandoervare_1]
    ON [fct].[AftaleVarePris] ([dw_ek_Leverandoervare_1] ASC) ON [PRIMARY];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_Leverandoervare_2')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Leverandoervare_2]
    ON [fct].[AftaleVarePris] ([dw_ek_Leverandoervare_2] ASC) ON [PRIMARY];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_Leverandoervare_3')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Leverandoervare_3]
    ON [fct].[AftaleVarePris] ([dw_ek_Leverandoervare_3] ASC) ON [PRIMARY];
END
GO
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
                     AND name = 'IX_Fct_AftaleVarePris_Leverandoervare_4')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_Leverandoervare_4]
    ON [fct].[AftaleVarePris] ([dw_ek_Leverandoervare_4] ASC) ON [PRIMARY];
END
GO
