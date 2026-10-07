-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG: narrow clean_code_DRGKontakt from NVARCHAR(255) to VARCHAR(255).
--
-- The key is DRGKONTAKT_<leverancetype>_<takstsystem>_<LPR3-uuid> — every part
-- is ASCII, so a single-byte type carries the value byte-for-byte identically.
-- Halving the key width halves the clean_code indexes on ~35.6M rows:
--   * fct.DRGKontakt : unique filtered IX_Fct_DRGKontakt_clean_code (the big one)
--   * dim.DRGKontaktEgenskaber : unique filtered IX_Dim_..._clean_code
--   * map.DRGKontakt : the clustered PK IS this column
-- Fresh deploys get VARCHAR straight from tables/drg/{02_maps,03_dims,04_facts}.sql
-- and the staging chain (DSA/SQL/staging/drg_kontakt_kolonner.py::clean_code_sql
-- now CASTs to VARCHAR(255)); this migration brings already-deployed databases
-- to the same schema WITHOUT re-keying — the values are unchanged, so every
-- dw_ek_DRGKontakt and every fact FK stays valid.
--
-- Cost: retyping an index/PK-key column requires dropping the dependent index
-- first, so map.DRGKontakt drops and rebuilds its CLUSTERED PK — a full table
-- rewrite on ~35.6M rows under a Sch-M lock. fct/dim only rebuild one filtered
-- nonclustered index each. Schedule the deploy window accordingly.
--
-- Idempotent: each table is a "retype" batch guarded on the column still being
-- nvarchar, plus separate index-/PK-recreate batches guarded on the object
-- being absent — so a partially applied file (e.g. PK dropped, not yet re-added)
-- re-runs cleanly. ASCII-safe: if any value were non-ASCII the ALTER would fail
-- loudly rather than lose data, but LPR3 uuids and takstsystem codes are ASCII.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- fct.DRGKontakt
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[fct].[DRGKontakt]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns c
               WHERE c.object_id = OBJECT_ID(N'[fct].[DRGKontakt]')
                 AND c.name = 'clean_code_DRGKontakt'
                 AND c.system_type_id = TYPE_ID('nvarchar'))
BEGIN
    EXEC('DROP INDEX IF EXISTS [IX_Fct_DRGKontakt_clean_code] ON [fct].[DRGKontakt]');
    EXEC('ALTER TABLE [fct].[DRGKontakt] ALTER COLUMN [clean_code_DRGKontakt] VARCHAR(255) NULL');
END
GO

IF OBJECT_ID(N'[fct].[DRGKontakt]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[DRGKontakt]') AND name = 'IX_Fct_DRGKontakt_clean_code')
BEGIN
    EXEC('CREATE UNIQUE NONCLUSTERED INDEX [IX_Fct_DRGKontakt_clean_code] ON [fct].[DRGKontakt] ([clean_code_DRGKontakt] ASC) WHERE [clean_code_DRGKontakt] IS NOT NULL ON [PRIMARY]');
END
GO

-- ---------------------------------------------------------------------------
-- dim.DRGKontaktEgenskaber
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns c
               WHERE c.object_id = OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]')
                 AND c.name = 'clean_code_DRGKontakt'
                 AND c.system_type_id = TYPE_ID('nvarchar'))
BEGIN
    EXEC('DROP INDEX IF EXISTS [IX_Dim_DRGKontaktEgenskaber_clean_code] ON [dim].[DRGKontaktEgenskaber]');
    EXEC('ALTER TABLE [dim].[DRGKontaktEgenskaber] ALTER COLUMN [clean_code_DRGKontakt] VARCHAR(255) NULL');
END
GO

IF OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]') AND name = 'IX_Dim_DRGKontaktEgenskaber_clean_code')
BEGIN
    EXEC('CREATE UNIQUE NONCLUSTERED INDEX [IX_Dim_DRGKontaktEgenskaber_clean_code] ON [dim].[DRGKontaktEgenskaber] ([clean_code_DRGKontakt] ASC) WHERE [clean_code_DRGKontakt] IS NOT NULL ON [PRIMARY]');
END
GO

-- ---------------------------------------------------------------------------
-- map.DRGKontakt — clean_code is the CLUSTERED PK, so the PK and the DW_EK
-- include-index must both come off before the column can be retyped.
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[map].[DRGKontakt]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns c
               WHERE c.object_id = OBJECT_ID(N'[map].[DRGKontakt]')
                 AND c.name = 'clean_code_DRGKontakt'
                 AND c.system_type_id = TYPE_ID('nvarchar'))
BEGIN
    EXEC('DROP INDEX IF EXISTS [IX_Map_DRGKontakt_DW_EK] ON [map].[DRGKontakt]');
    EXEC('ALTER TABLE [map].[DRGKontakt] DROP CONSTRAINT IF EXISTS [PK_Map_DRGKontakt]');
    EXEC('ALTER TABLE [map].[DRGKontakt] ALTER COLUMN [clean_code_DRGKontakt] VARCHAR(255) NOT NULL');
END
GO

IF OBJECT_ID(N'[map].[DRGKontakt]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND name = 'PK_Map_DRGKontakt')
BEGIN
    EXEC('ALTER TABLE [map].[DRGKontakt] ADD CONSTRAINT [PK_Map_DRGKontakt] PRIMARY KEY CLUSTERED ([clean_code_DRGKontakt] ASC) ON [PRIMARY]');
END
GO

IF OBJECT_ID(N'[map].[DRGKontakt]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND name = 'IX_Map_DRGKontakt_DW_EK')
BEGIN
    EXEC('CREATE UNIQUE NONCLUSTERED INDEX [IX_Map_DRGKontakt_DW_EK] ON [map].[DRGKontakt] ([dw_ek_DRGKontakt] ASC) INCLUDE ([clean_code_DRGKontakt]) ON [PRIMARY]');
END
GO
