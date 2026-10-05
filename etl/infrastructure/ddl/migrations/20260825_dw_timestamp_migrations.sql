-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dw_timestamp_insert / dw_timestamp_update -> NOT NULL: retrofit for EXISTING
-- staging + DSO databases (the standard audit columns; see rule 04).
--
-- WHY 20260813_dw_columns_not_null.sql did NOT achieve this:
--   * staging_migrations / dso_migrations run BEFORE the map/dim/fct tables are
--     created (upstream of *_infra_erp). On first build every IF OBJECT_ID(...)
--     guard was false -> each block a no-op -> file stamped 'applied' in
--     [meta].[MigrationHistory] -> it never re-runs.
--   * The watermark indexes (IX_<schema>_<table>_Watermark) key on the audit
--     columns, so a plain ALTER COLUMN ... NOT NULL now fails with error 5074.
--
-- Per in-scope table, guarded so a re-run / already-correct table is a no-op:
--   1. drop indexes keyed on the audit columns (canonical watermark + observed
--      stale duplicates from old renames + bespoke arc/ref names),
--   2. backfill NULLs (insert first; update falls back to insert),
--   3. add DEFAULT SYSDATETIME() if missing,
--   3b. on the three fhir delta-discovery tables (Observation,
--      QuestionnaireResponse, QuestionnaireResponse_item): drop the PERSISTED
--      computed column dw_timestamp_effective and the index keyed on it, since
--      ALTER COLUMN on a column a computed column references fails with error
--      5074 ("The column 'dw_timestamp_effective' is dependent on column
--      'dw_timestamp_insert'"); both are recreated after the ALTERs,
--   4. ALTER both columns to NOT NULL (guarded on is_nullable = 1),
--   5. recreate the correct index: canonical watermark for watermark-sync
--      tables; the exact bespoke index (from base DDL) for arc.ils_* and
--      ref.KMDFakturalinjeUNSPSCMatch.
--
-- SCOPE: model tables (dim/fct/fhir/map/ref/bridge/conceptmap/codesystem/meta/
-- sec) + the 17 nullable arc.ils_* archive tables. EXCLUDED: stg.* (transient
-- SELECT INTO tables; enforce in the staging build SQL instead) and the other
-- arc.* tables (already NOT NULL everywhere). dim.Faktura.[header_dw_timestamp_
-- update] is intentionally nullable (not an audit column) and is NOT touched.
--
-- Stale duplicate indexes dropped WITHOUT recreation (leftovers from renames):
--   IX_dim_FakturaEgenskaber_Watermark, IX_dim_Vare_Watermark,
--   IX_map_Organization_Watermark (English), IX_fhir_CarePlanActivity_Watermark,
--   IX_fhir_EncounterLocations_Watermark, and map.Organisation's watermark index
--   (map.Organisation is no longer a watermark-sync target).
--
-- INDEX NAMES: post-rename form only. 20260824_reapply_index_renames.sql sorts
-- ahead of this file, so by the time this runs the bespoke arc.ils_* and
-- ref.KMDFakturalinjeUNSPSCMatch indexes already carry their new names
-- (IX_arc_ils_<t>_dw_timestamp_update and
-- IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp).
-- The drop guards still match a pre-rename index because the
-- database collation is case-insensitive; the recreate always emits the new
-- name, matching ddl/tables/. Never reintroduce IX_..._DW_Timestamp_Update
-- here - it would undo 20260824.
--
-- Idempotent + guarded. Runs on BOTH staging and DSO via the shared migrations/
-- directory (guard no-ops tables absent on a given DB). Base DDL already
-- declares these columns NOT NULL, so fresh databases never need this.
-- ============================================================================


-- arc.ils_indkfak
IF OBJECT_ID(N'[arc].[ils_indkfak]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]') AND name = N'IX_arc_ils_indkfak_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_indkfak_Watermark] ON [arc].[ils_indkfak];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]') AND name = N'IX_arc_ils_indkfak_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_indkfak_dw_timestamp_update] ON [arc].[ils_indkfak];');

    EXEC(N'UPDATE [arc].[ils_indkfak]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_indkfak]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_indkfak]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_indkfak] ADD CONSTRAINT [DF_ils_indkfak_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_indkfak]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_indkfak] ADD CONSTRAINT [DF_ils_indkfak_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_indkfak] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_indkfak] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_indkfak]') AND name = N'IX_arc_ils_indkfak_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_indkfak_dw_timestamp_update] ON [arc].[ils_indkfak] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_laftd
IF OBJECT_ID(N'[arc].[ils_laftd]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]') AND name = N'IX_arc_ils_laftd_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_laftd_Watermark] ON [arc].[ils_laftd];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]') AND name = N'IX_arc_ils_laftd_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_laftd_dw_timestamp_update] ON [arc].[ils_laftd];');

    EXEC(N'UPDATE [arc].[ils_laftd]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_laftd]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftd]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_laftd] ADD CONSTRAINT [DF_ils_laftd_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftd]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_laftd] ADD CONSTRAINT [DF_ils_laftd_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftd] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftd] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftd]') AND name = N'IX_arc_ils_laftd_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_laftd_dw_timestamp_update] ON [arc].[ils_laftd] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_laftdpris
IF OBJECT_ID(N'[arc].[ils_laftdpris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]') AND name = N'IX_arc_ils_laftdpris_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_laftdpris_Watermark] ON [arc].[ils_laftdpris];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]') AND name = N'IX_arc_ils_laftdpris_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_laftdpris_dw_timestamp_update] ON [arc].[ils_laftdpris];');

    EXEC(N'UPDATE [arc].[ils_laftdpris]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_laftdpris]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftdpris]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_laftdpris] ADD CONSTRAINT [DF_ils_laftdpris_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftdpris]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_laftdpris] ADD CONSTRAINT [DF_ils_laftdpris_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftdpris] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftdpris] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdpris]') AND name = N'IX_arc_ils_laftdpris_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdpris_dw_timestamp_update] ON [arc].[ils_laftdpris] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_laftdstam
IF OBJECT_ID(N'[arc].[ils_laftdstam]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]') AND name = N'IX_arc_ils_laftdstam_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_laftdstam_Watermark] ON [arc].[ils_laftdstam];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]') AND name = N'IX_arc_ils_laftdstam_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_laftdstam_dw_timestamp_update] ON [arc].[ils_laftdstam];');

    EXEC(N'UPDATE [arc].[ils_laftdstam]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_laftdstam]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftdstam]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_laftdstam] ADD CONSTRAINT [DF_ils_laftdstam_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_laftdstam]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_laftdstam] ADD CONSTRAINT [DF_ils_laftdstam_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftdstam] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_laftdstam] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_laftdstam]') AND name = N'IX_arc_ils_laftdstam_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_laftdstam_dw_timestamp_update] ON [arc].[ils_laftdstam] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_lafth
IF OBJECT_ID(N'[arc].[ils_lafth]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]') AND name = N'IX_arc_ils_lafth_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_lafth_Watermark] ON [arc].[ils_lafth];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]') AND name = N'IX_arc_ils_lafth_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_lafth_dw_timestamp_update] ON [arc].[ils_lafth];');

    EXEC(N'UPDATE [arc].[ils_lafth]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_lafth]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lafth]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_lafth] ADD CONSTRAINT [DF_ils_lafth_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lafth]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_lafth] ADD CONSTRAINT [DF_ils_lafth_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lafth] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lafth] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lafth]') AND name = N'IX_arc_ils_lafth_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_lafth_dw_timestamp_update] ON [arc].[ils_lafth] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_lev
IF OBJECT_ID(N'[arc].[ils_lev]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]') AND name = N'IX_arc_ils_lev_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_lev_Watermark] ON [arc].[ils_lev];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]') AND name = N'IX_arc_ils_lev_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_lev_dw_timestamp_update] ON [arc].[ils_lev];');

    EXEC(N'UPDATE [arc].[ils_lev]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_lev]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lev]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_lev] ADD CONSTRAINT [DF_ils_lev_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lev]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_lev] ADD CONSTRAINT [DF_ils_lev_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lev] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lev] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lev]') AND name = N'IX_arc_ils_lev_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_lev_dw_timestamp_update] ON [arc].[ils_lev] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_lgr
IF OBJECT_ID(N'[arc].[ils_lgr]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]') AND name = N'IX_arc_ils_lgr_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_lgr_Watermark] ON [arc].[ils_lgr];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]') AND name = N'IX_arc_ils_lgr_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_lgr_dw_timestamp_update] ON [arc].[ils_lgr];');

    EXEC(N'UPDATE [arc].[ils_lgr]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_lgr]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lgr]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_lgr] ADD CONSTRAINT [DF_ils_lgr_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lgr]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_lgr] ADD CONSTRAINT [DF_ils_lgr_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lgr] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lgr] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgr]') AND name = N'IX_arc_ils_lgr_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_lgr_dw_timestamp_update] ON [arc].[ils_lgr] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_lgrvare
IF OBJECT_ID(N'[arc].[ils_lgrvare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]') AND name = N'IX_arc_ils_lgrvare_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_lgrvare_Watermark] ON [arc].[ils_lgrvare];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]') AND name = N'IX_arc_ils_lgrvare_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_lgrvare_dw_timestamp_update] ON [arc].[ils_lgrvare];');

    EXEC(N'UPDATE [arc].[ils_lgrvare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_lgrvare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lgrvare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_lgrvare] ADD CONSTRAINT [DF_ils_lgrvare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_lgrvare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_lgrvare] ADD CONSTRAINT [DF_ils_lgrvare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lgrvare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_lgrvare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_lgrvare]') AND name = N'IX_arc_ils_lgrvare_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_lgrvare_dw_timestamp_update] ON [arc].[ils_lgrvare] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_moms
IF OBJECT_ID(N'[arc].[ils_moms]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]') AND name = N'IX_arc_ils_moms_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_moms_Watermark] ON [arc].[ils_moms];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]') AND name = N'IX_arc_ils_moms_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_moms_dw_timestamp_update] ON [arc].[ils_moms];');

    EXEC(N'UPDATE [arc].[ils_moms]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_moms]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_moms]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_moms] ADD CONSTRAINT [DF_ils_moms_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_moms]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_moms] ADD CONSTRAINT [DF_ils_moms_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_moms] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_moms] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_moms]') AND name = N'IX_arc_ils_moms_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_moms_dw_timestamp_update] ON [arc].[ils_moms] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_pmdm
IF OBJECT_ID(N'[arc].[ils_pmdm]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]') AND name = N'IX_arc_ils_pmdm_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_pmdm_Watermark] ON [arc].[ils_pmdm];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]') AND name = N'IX_arc_ils_pmdm_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_pmdm_dw_timestamp_update] ON [arc].[ils_pmdm];');

    EXEC(N'UPDATE [arc].[ils_pmdm]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_pmdm]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_pmdm]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_pmdm] ADD CONSTRAINT [DF_ils_pmdm_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_pmdm]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_pmdm] ADD CONSTRAINT [DF_ils_pmdm_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_pmdm] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_pmdm] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_pmdm]') AND name = N'IX_arc_ils_pmdm_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_pmdm_dw_timestamp_update] ON [arc].[ils_pmdm] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_rekv
IF OBJECT_ID(N'[arc].[ils_rekv]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]') AND name = N'IX_arc_ils_rekv_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_rekv_Watermark] ON [arc].[ils_rekv];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]') AND name = N'IX_arc_ils_rekv_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_rekv_dw_timestamp_update] ON [arc].[ils_rekv];');

    EXEC(N'UPDATE [arc].[ils_rekv]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_rekv]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekv]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_rekv] ADD CONSTRAINT [DF_ils_rekv_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekv]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_rekv] ADD CONSTRAINT [DF_ils_rekv_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_rekv] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_rekv] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekv]') AND name = N'IX_arc_ils_rekv_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_rekv_dw_timestamp_update] ON [arc].[ils_rekv] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_rekvgrp
IF OBJECT_ID(N'[arc].[ils_rekvgrp]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]') AND name = N'IX_arc_ils_rekvgrp_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_rekvgrp_Watermark] ON [arc].[ils_rekvgrp];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]') AND name = N'IX_arc_ils_rekvgrp_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_rekvgrp_dw_timestamp_update] ON [arc].[ils_rekvgrp];');

    EXEC(N'UPDATE [arc].[ils_rekvgrp]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_rekvgrp]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_rekvgrp] ADD CONSTRAINT [DF_ils_rekvgrp_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_rekvgrp] ADD CONSTRAINT [DF_ils_rekvgrp_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_rekvgrp] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_rekvgrp] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_rekvgrp]') AND name = N'IX_arc_ils_rekvgrp_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_rekvgrp_dw_timestamp_update] ON [arc].[ils_rekvgrp] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_sgh
IF OBJECT_ID(N'[arc].[ils_sgh]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]') AND name = N'IX_arc_ils_sgh_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_sgh_Watermark] ON [arc].[ils_sgh];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]') AND name = N'IX_arc_ils_sgh_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_sgh_dw_timestamp_update] ON [arc].[ils_sgh];');

    EXEC(N'UPDATE [arc].[ils_sgh]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_sgh]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_sgh]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_sgh] ADD CONSTRAINT [DF_ils_sgh_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_sgh]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_sgh] ADD CONSTRAINT [DF_ils_sgh_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_sgh] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_sgh] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_sgh]') AND name = N'IX_arc_ils_sgh_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_sgh_dw_timestamp_update] ON [arc].[ils_sgh] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_udlev
IF OBJECT_ID(N'[arc].[ils_udlev]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]') AND name = N'IX_arc_ils_udlev_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_udlev_Watermark] ON [arc].[ils_udlev];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]') AND name = N'IX_arc_ils_udlev_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_udlev_dw_timestamp_update] ON [arc].[ils_udlev];');

    EXEC(N'UPDATE [arc].[ils_udlev]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_udlev]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_udlev]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_udlev] ADD CONSTRAINT [DF_ils_udlev_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_udlev]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_udlev] ADD CONSTRAINT [DF_ils_udlev_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_udlev] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_udlev] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_udlev]') AND name = N'IX_arc_ils_udlev_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_udlev_dw_timestamp_update] ON [arc].[ils_udlev] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_vareart
IF OBJECT_ID(N'[arc].[ils_vareart]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]') AND name = N'IX_arc_ils_vareart_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_vareart_Watermark] ON [arc].[ils_vareart];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]') AND name = N'IX_arc_ils_vareart_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_vareart_dw_timestamp_update] ON [arc].[ils_vareart];');

    EXEC(N'UPDATE [arc].[ils_vareart]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_vareart]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_vareart]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_vareart] ADD CONSTRAINT [DF_ils_vareart_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_vareart]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_vareart] ADD CONSTRAINT [DF_ils_vareart_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_vareart] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_vareart] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_vareart]') AND name = N'IX_arc_ils_vareart_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_vareart_dw_timestamp_update] ON [arc].[ils_vareart] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_varegrp
IF OBJECT_ID(N'[arc].[ils_varegrp]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]') AND name = N'IX_arc_ils_varegrp_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_varegrp_Watermark] ON [arc].[ils_varegrp];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]') AND name = N'IX_arc_ils_varegrp_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_varegrp_dw_timestamp_update] ON [arc].[ils_varegrp];');

    EXEC(N'UPDATE [arc].[ils_varegrp]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_varegrp]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_varegrp]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_varegrp] ADD CONSTRAINT [DF_ils_varegrp_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_varegrp]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_varegrp] ADD CONSTRAINT [DF_ils_varegrp_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_varegrp] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_varegrp] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varegrp]') AND name = N'IX_arc_ils_varegrp_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_varegrp_dw_timestamp_update] ON [arc].[ils_varegrp] ([dw_timestamp_update] DESC);');
END
GO

-- arc.ils_varetype
IF OBJECT_ID(N'[arc].[ils_varetype]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]') AND name = N'IX_arc_ils_varetype_Watermark')
        EXEC(N'DROP INDEX [IX_arc_ils_varetype_Watermark] ON [arc].[ils_varetype];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]') AND name = N'IX_arc_ils_varetype_dw_timestamp_update')
        EXEC(N'DROP INDEX [IX_arc_ils_varetype_dw_timestamp_update] ON [arc].[ils_varetype];');

    EXEC(N'UPDATE [arc].[ils_varetype]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [arc].[ils_varetype]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_varetype]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [arc].[ils_varetype] ADD CONSTRAINT [DF_ils_varetype_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_varetype]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [arc].[ils_varetype] ADD CONSTRAINT [DF_ils_varetype_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_varetype] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [arc].[ils_varetype] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype]') AND name = N'IX_arc_ils_varetype_dw_timestamp_update')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_arc_ils_varetype_dw_timestamp_update] ON [arc].[ils_varetype] ([dw_timestamp_update] DESC);');
END
GO

-- bridge.LeverandoervareILSVare
IF OBJECT_ID(N'[bridge].[LeverandoervareILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND name = N'IX_bridge_LeverandoervareILSVare_Watermark')
        EXEC(N'DROP INDEX [IX_bridge_LeverandoervareILSVare_Watermark] ON [bridge].[LeverandoervareILSVare];');

    EXEC(N'UPDATE [bridge].[LeverandoervareILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [bridge].[LeverandoervareILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ADD CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ADD CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND name = N'IX_bridge_LeverandoervareILSVare_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_bridge_LeverandoervareILSVare_Watermark] ON [bridge].[LeverandoervareILSVare] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- codesystem.EANConcept
IF OBJECT_ID(N'[codesystem].[EANConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANConcept]') AND name = N'IX_codesystem_EANConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_EANConcept_Watermark] ON [codesystem].[EANConcept];');

    EXEC(N'UPDATE [codesystem].[EANConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[EANConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[EANConcept] ADD CONSTRAINT [DF_EANConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[EANConcept] ADD CONSTRAINT [DF_EANConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.EANHierarchy
IF OBJECT_ID(N'[codesystem].[EANHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND name = N'IX_codesystem_EANHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_EANHierarchy_Watermark] ON [codesystem].[EANHierarchy];');

    EXEC(N'UPDATE [codesystem].[EANHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[EANHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[EANHierarchy] ADD CONSTRAINT [DF_EANHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[EANHierarchy] ADD CONSTRAINT [DF_EANHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.EANProperties
IF OBJECT_ID(N'[codesystem].[EANProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND name = N'IX_codesystem_EANProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_EANProperties_Watermark] ON [codesystem].[EANProperties];');

    EXEC(N'UPDATE [codesystem].[EANProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[EANProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[EANProperties] ADD CONSTRAINT [DF_EANProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[EANProperties] ADD CONSTRAINT [DF_EANProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[EANProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.LagerConcept
IF OBJECT_ID(N'[codesystem].[LagerConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerConcept]') AND name = N'IX_codesystem_LagerConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_LagerConcept_Watermark] ON [codesystem].[LagerConcept];');

    EXEC(N'UPDATE [codesystem].[LagerConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[LagerConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[LagerConcept] ADD CONSTRAINT [DF_LagerConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[LagerConcept] ADD CONSTRAINT [DF_LagerConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.LagerHierarchy
IF OBJECT_ID(N'[codesystem].[LagerHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND name = N'IX_codesystem_LagerHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_LagerHierarchy_Watermark] ON [codesystem].[LagerHierarchy];');

    EXEC(N'UPDATE [codesystem].[LagerHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[LagerHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[LagerHierarchy] ADD CONSTRAINT [DF_LagerHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[LagerHierarchy] ADD CONSTRAINT [DF_LagerHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.LagerProperties
IF OBJECT_ID(N'[codesystem].[LagerProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND name = N'IX_codesystem_LagerProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_LagerProperties_Watermark] ON [codesystem].[LagerProperties];');

    EXEC(N'UPDATE [codesystem].[LagerProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[LagerProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[LagerProperties] ADD CONSTRAINT [DF_LagerProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[LagerProperties] ADD CONSTRAINT [DF_LagerProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[LagerProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.OkostedConcept
IF OBJECT_ID(N'[codesystem].[OkostedConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]') AND name = N'IX_codesystem_OkostedConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_OkostedConcept_Watermark] ON [codesystem].[OkostedConcept];');

    EXEC(N'UPDATE [codesystem].[OkostedConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[OkostedConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[OkostedConcept] ADD CONSTRAINT [DF_OkostedConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[OkostedConcept] ADD CONSTRAINT [DF_OkostedConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.OkostedHierarchy
IF OBJECT_ID(N'[codesystem].[OkostedHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND name = N'IX_codesystem_OkostedHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_OkostedHierarchy_Watermark] ON [codesystem].[OkostedHierarchy];');

    EXEC(N'UPDATE [codesystem].[OkostedHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[OkostedHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[OkostedHierarchy] ADD CONSTRAINT [DF_OkostedHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[OkostedHierarchy] ADD CONSTRAINT [DF_OkostedHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.OkostedProperties
IF OBJECT_ID(N'[codesystem].[OkostedProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND name = N'IX_codesystem_OkostedProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_OkostedProperties_Watermark] ON [codesystem].[OkostedProperties];');

    EXEC(N'UPDATE [codesystem].[OkostedProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[OkostedProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[OkostedProperties] ADD CONSTRAINT [DF_OkostedProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[OkostedProperties] ADD CONSTRAINT [DF_OkostedProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[OkostedProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.RekvirentConcept
IF OBJECT_ID(N'[codesystem].[RekvirentConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]') AND name = N'IX_codesystem_RekvirentConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_RekvirentConcept_Watermark] ON [codesystem].[RekvirentConcept];');

    EXEC(N'UPDATE [codesystem].[RekvirentConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[RekvirentConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentConcept] ADD CONSTRAINT [DF_RekvirentConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentConcept] ADD CONSTRAINT [DF_RekvirentConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.RekvirentHierarchy
IF OBJECT_ID(N'[codesystem].[RekvirentHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND name = N'IX_codesystem_RekvirentHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_RekvirentHierarchy_Watermark] ON [codesystem].[RekvirentHierarchy];');

    EXEC(N'UPDATE [codesystem].[RekvirentHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[RekvirentHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentHierarchy] ADD CONSTRAINT [DF_RekvirentHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentHierarchy] ADD CONSTRAINT [DF_RekvirentHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.RekvirentProperties
IF OBJECT_ID(N'[codesystem].[RekvirentProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND name = N'IX_codesystem_RekvirentProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_RekvirentProperties_Watermark] ON [codesystem].[RekvirentProperties];');

    EXEC(N'UPDATE [codesystem].[RekvirentProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[RekvirentProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentProperties] ADD CONSTRAINT [DF_RekvirentProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[RekvirentProperties] ADD CONSTRAINT [DF_RekvirentProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[RekvirentProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFIAlias
IF OBJECT_ID(N'[codesystem].[SFIAlias]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND name = N'IX_codesystem_SFIAlias_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFIAlias_Watermark] ON [codesystem].[SFIAlias];');

    EXEC(N'UPDATE [codesystem].[SFIAlias]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIAlias]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ADD CONSTRAINT [DF_SFIAlias_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ADD CONSTRAINT [DF_SFIAlias_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFIAliasMember
IF OBJECT_ID(N'[codesystem].[SFIAliasMember]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND name = N'IX_codesystem_SFIAliasMember_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFIAliasMember_Watermark] ON [codesystem].[SFIAliasMember];');

    EXEC(N'UPDATE [codesystem].[SFIAliasMember]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIAliasMember]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ADD CONSTRAINT [DF_SFIAliasMember_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ADD CONSTRAINT [DF_SFIAliasMember_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFIConcept
IF OBJECT_ID(N'[codesystem].[SFIConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND name = N'IX_codesystem_SFIConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFIConcept_Watermark] ON [codesystem].[SFIConcept];');

    EXEC(N'UPDATE [codesystem].[SFIConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIConcept] ADD CONSTRAINT [DF_SFIConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIConcept] ADD CONSTRAINT [DF_SFIConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFIHierarchy
IF OBJECT_ID(N'[codesystem].[SFIHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND name = N'IX_codesystem_SFIHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFIHierarchy_Watermark] ON [codesystem].[SFIHierarchy];');

    EXEC(N'UPDATE [codesystem].[SFIHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIHierarchy] ADD CONSTRAINT [DF_SFIHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIHierarchy] ADD CONSTRAINT [DF_SFIHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFIProperties
IF OBJECT_ID(N'[codesystem].[SFIProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND name = N'IX_codesystem_SFIProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFIProperties_Watermark] ON [codesystem].[SFIProperties];');

    EXEC(N'UPDATE [codesystem].[SFIProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIProperties] ADD CONSTRAINT [DF_SFIProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIProperties] ADD CONSTRAINT [DF_SFIProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFISearchDocument
IF OBJECT_ID(N'[codesystem].[SFISearchDocument]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND name = N'IX_codesystem_SFISearchDocument_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFISearchDocument_Watermark] ON [codesystem].[SFISearchDocument];');

    EXEC(N'UPDATE [codesystem].[SFISearchDocument]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFISearchDocument]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ADD CONSTRAINT [DF_SFISearchDocument_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ADD CONSTRAINT [DF_SFISearchDocument_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SFISearchOverride
IF OBJECT_ID(N'[codesystem].[SFISearchOverride]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND name = N'IX_codesystem_SFISearchOverride_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SFISearchOverride_Watermark] ON [codesystem].[SFISearchOverride];');

    EXEC(N'UPDATE [codesystem].[SFISearchOverride]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFISearchOverride]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ADD CONSTRAINT [DF_SFISearchOverride_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ADD CONSTRAINT [DF_SFISearchOverride_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SHAKConcept
IF OBJECT_ID(N'[codesystem].[SHAKConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]') AND name = N'IX_codesystem_SHAKConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SHAKConcept_Watermark] ON [codesystem].[SHAKConcept];');

    EXEC(N'UPDATE [codesystem].[SHAKConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SHAKConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SHAKConcept] ADD CONSTRAINT [DF_SHAKConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SHAKConcept] ADD CONSTRAINT [DF_SHAKConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SHAKHierarchy
IF OBJECT_ID(N'[codesystem].[SHAKHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]') AND name = N'IX_codesystem_SHAKHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SHAKHierarchy_Watermark] ON [codesystem].[SHAKHierarchy];');

    EXEC(N'UPDATE [codesystem].[SHAKHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SHAKHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SHAKHierarchy] ADD CONSTRAINT [DF_SHAKHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SHAKHierarchy] ADD CONSTRAINT [DF_SHAKHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SHAKProperties
IF OBJECT_ID(N'[codesystem].[SHAKProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]') AND name = N'IX_codesystem_SHAKProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SHAKProperties_Watermark] ON [codesystem].[SHAKProperties];');

    EXEC(N'UPDATE [codesystem].[SHAKProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SHAKProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SHAKProperties] ADD CONSTRAINT [DF_SHAKProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SHAKProperties] ADD CONSTRAINT [DF_SHAKProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SHAKProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SHAKProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SKSConcept
IF OBJECT_ID(N'[codesystem].[SKSConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSConcept]') AND name = N'IX_codesystem_SKSConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SKSConcept_Watermark] ON [codesystem].[SKSConcept];');

    EXEC(N'UPDATE [codesystem].[SKSConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SKSConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SKSConcept] ADD CONSTRAINT [DF_SKSConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SKSConcept] ADD CONSTRAINT [DF_SKSConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SKSHierarchy
IF OBJECT_ID(N'[codesystem].[SKSHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND name = N'IX_codesystem_SKSHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SKSHierarchy_Watermark] ON [codesystem].[SKSHierarchy];');

    EXEC(N'UPDATE [codesystem].[SKSHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SKSHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SKSHierarchy] ADD CONSTRAINT [DF_SKSHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SKSHierarchy] ADD CONSTRAINT [DF_SKSHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SKSProperties
IF OBJECT_ID(N'[codesystem].[SKSProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND name = N'IX_codesystem_SKSProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SKSProperties_Watermark] ON [codesystem].[SKSProperties];');

    EXEC(N'UPDATE [codesystem].[SKSProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SKSProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SKSProperties] ADD CONSTRAINT [DF_SKSProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SKSProperties] ADD CONSTRAINT [DF_SKSProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SKSProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SORConcept
IF OBJECT_ID(N'[codesystem].[SORConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORConcept]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORConcept]') AND name = N'IX_codesystem_SORConcept_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SORConcept_Watermark] ON [codesystem].[SORConcept];');

    EXEC(N'UPDATE [codesystem].[SORConcept]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SORConcept]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORConcept]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SORConcept] ADD CONSTRAINT [DF_SORConcept_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORConcept]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SORConcept] ADD CONSTRAINT [DF_SORConcept_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORConcept]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORConcept] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORConcept]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORConcept] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SORHierarchy
IF OBJECT_ID(N'[codesystem].[SORHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND name = N'IX_codesystem_SORHierarchy_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SORHierarchy_Watermark] ON [codesystem].[SORHierarchy];');

    EXEC(N'UPDATE [codesystem].[SORHierarchy]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SORHierarchy]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SORHierarchy] ADD CONSTRAINT [DF_SORHierarchy_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SORHierarchy] ADD CONSTRAINT [DF_SORHierarchy_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORHierarchy] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORHierarchy] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- codesystem.SORProperties
IF OBJECT_ID(N'[codesystem].[SORProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND name = N'IX_codesystem_SORProperties_Watermark')
        EXEC(N'DROP INDEX [IX_codesystem_SORProperties_Watermark] ON [codesystem].[SORProperties];');

    EXEC(N'UPDATE [codesystem].[SORProperties]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SORProperties]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SORProperties] ADD CONSTRAINT [DF_SORProperties_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SORProperties] ADD CONSTRAINT [DF_SORProperties_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORProperties] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SORProperties] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- conceptmap.EANToSOR
IF OBJECT_ID(N'[conceptmap].[EANToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND name = N'IX_conceptmap_EANToSOR_Watermark')
        EXEC(N'DROP INDEX [IX_conceptmap_EANToSOR_Watermark] ON [conceptmap].[EANToSOR];');

    EXEC(N'UPDATE [conceptmap].[EANToSOR]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [conceptmap].[EANToSOR]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [conceptmap].[EANToSOR] ADD CONSTRAINT [DF_EANToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [conceptmap].[EANToSOR] ADD CONSTRAINT [DF_EANToSOR_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[EANToSOR] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[EANToSOR] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND name = N'IX_conceptmap_EANToSOR_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_conceptmap_EANToSOR_Watermark] ON [conceptmap].[EANToSOR] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- conceptmap.LagerToSOR
IF OBJECT_ID(N'[conceptmap].[LagerToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND name = N'IX_conceptmap_LagerToSOR_Watermark')
        EXEC(N'DROP INDEX [IX_conceptmap_LagerToSOR_Watermark] ON [conceptmap].[LagerToSOR];');

    EXEC(N'UPDATE [conceptmap].[LagerToSOR]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [conceptmap].[LagerToSOR]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [conceptmap].[LagerToSOR] ADD CONSTRAINT [DF_LagerToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [conceptmap].[LagerToSOR] ADD CONSTRAINT [DF_LagerToSOR_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[LagerToSOR] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[LagerToSOR] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND name = N'IX_conceptmap_LagerToSOR_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_conceptmap_LagerToSOR_Watermark] ON [conceptmap].[LagerToSOR] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- conceptmap.OkostedToSOR
IF OBJECT_ID(N'[conceptmap].[OkostedToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND name = N'IX_conceptmap_OkostedToSOR_Watermark')
        EXEC(N'DROP INDEX [IX_conceptmap_OkostedToSOR_Watermark] ON [conceptmap].[OkostedToSOR];');

    EXEC(N'UPDATE [conceptmap].[OkostedToSOR]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [conceptmap].[OkostedToSOR]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [conceptmap].[OkostedToSOR] ADD CONSTRAINT [DF_OkostedToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [conceptmap].[OkostedToSOR] ADD CONSTRAINT [DF_OkostedToSOR_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[OkostedToSOR] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[OkostedToSOR] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND name = N'IX_conceptmap_OkostedToSOR_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_conceptmap_OkostedToSOR_Watermark] ON [conceptmap].[OkostedToSOR] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- conceptmap.RekvirentToSOR
IF OBJECT_ID(N'[conceptmap].[RekvirentToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND name = N'IX_conceptmap_RekvirentToSOR_Watermark')
        EXEC(N'DROP INDEX [IX_conceptmap_RekvirentToSOR_Watermark] ON [conceptmap].[RekvirentToSOR];');

    EXEC(N'UPDATE [conceptmap].[RekvirentToSOR]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [conceptmap].[RekvirentToSOR]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [conceptmap].[RekvirentToSOR] ADD CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [conceptmap].[RekvirentToSOR] ADD CONSTRAINT [DF_RekvirentToSOR_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[RekvirentToSOR] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[RekvirentToSOR] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND name = N'IX_conceptmap_RekvirentToSOR_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_conceptmap_RekvirentToSOR_Watermark] ON [conceptmap].[RekvirentToSOR] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- conceptmap.SHAKToSOR
IF OBJECT_ID(N'[conceptmap].[SHAKToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND name = N'IX_conceptmap_SHAKToSOR_Watermark')
        EXEC(N'DROP INDEX [IX_conceptmap_SHAKToSOR_Watermark] ON [conceptmap].[SHAKToSOR];');

    EXEC(N'UPDATE [conceptmap].[SHAKToSOR]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [conceptmap].[SHAKToSOR]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [conceptmap].[SHAKToSOR] ADD CONSTRAINT [DF_SHAKToSOR_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [conceptmap].[SHAKToSOR] ADD CONSTRAINT [DF_SHAKToSOR_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[SHAKToSOR] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [conceptmap].[SHAKToSOR] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[SHAKToSOR]') AND name = N'IX_conceptmap_SHAKToSOR_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_conceptmap_SHAKToSOR_Watermark] ON [conceptmap].[SHAKToSOR] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.AfdelingLagerbeholdningEgenskaber
IF OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND name = N'IX_dim_AfdelingLagerbeholdningEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_dim_AfdelingLagerbeholdningEgenskaber_Watermark] ON [dim].[AfdelingLagerbeholdningEgenskaber];');

    EXEC(N'UPDATE [dim].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND name = N'IX_dim_AfdelingLagerbeholdningEgenskaber_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_AfdelingLagerbeholdningEgenskaber_Watermark] ON [dim].[AfdelingLagerbeholdningEgenskaber] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Aftale
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Aftale]') AND name = N'IX_dim_Aftale_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Aftale_Watermark] ON [dim].[Aftale];');

    EXEC(N'UPDATE [dim].[Aftale]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Aftale]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Aftale] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Aftale] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Borger
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Borger]') AND name = N'IX_dim_Borger_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Borger_Watermark] ON [dim].[Borger];');

    EXEC(N'UPDATE [dim].[Borger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Borger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Borger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Borger] ADD CONSTRAINT [DF_Borger_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Borger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Borger] ADD CONSTRAINT [DF_Borger_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Borger] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Borger] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.BudgetEgenskaber
IF OBJECT_ID(N'[dim].[BudgetEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND name = N'IX_dim_BudgetEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_dim_BudgetEgenskaber_Watermark] ON [dim].[BudgetEgenskaber];');

    EXEC(N'UPDATE [dim].[BudgetEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[BudgetEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND name = N'IX_dim_BudgetEgenskaber_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_BudgetEgenskaber_Watermark] ON [dim].[BudgetEgenskaber] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Dato
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Dato]') AND name = N'IX_dim_Dato_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Dato_Watermark] ON [dim].[Dato];');

    EXEC(N'UPDATE [dim].[Dato]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Dato]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Dato]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Dato] ADD CONSTRAINT [DF_Dato_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Dato]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Dato] ADD CONSTRAINT [DF_Dato_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Dato] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Dato] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Faktura
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'IX_dim_Faktura_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Faktura_Watermark] ON [dim].[Faktura];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'IX_dim_FakturaEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_dim_FakturaEgenskaber_Watermark] ON [dim].[Faktura];');

    EXEC(N'UPDATE [dim].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'IX_dim_Faktura_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Faktura_Watermark] ON [dim].[Faktura] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Fakturalinje
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'IX_dim_Fakturalinje_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Fakturalinje_Watermark] ON [dim].[Fakturalinje];');

    EXEC(N'UPDATE [dim].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'IX_dim_Fakturalinje_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Fakturalinje_Watermark] ON [dim].[Fakturalinje] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Finanskladde
IF OBJECT_ID(N'[dim].[Finanskladde]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = N'IX_dim_Finanskladde_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Finanskladde_Watermark] ON [dim].[Finanskladde];');

    EXEC(N'UPDATE [dim].[Finanskladde]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Finanskladde]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = N'IX_dim_Finanskladde_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Finanskladde_Watermark] ON [dim].[Finanskladde] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Finanskladdelinje
IF OBJECT_ID(N'[dim].[Finanskladdelinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND name = N'IX_dim_Finanskladdelinje_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Finanskladdelinje_Watermark] ON [dim].[Finanskladdelinje];');

    EXEC(N'UPDATE [dim].[Finanskladdelinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Finanskladdelinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND name = N'IX_dim_Finanskladdelinje_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Finanskladdelinje_Watermark] ON [dim].[Finanskladdelinje] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.ILSVare
IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'IX_dim_ILSVare_Watermark')
        EXEC(N'DROP INDEX [IX_dim_ILSVare_Watermark] ON [dim].[ILSVare];');

    EXEC(N'UPDATE [dim].[ILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[ILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'IX_dim_ILSVare_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_ILSVare_Watermark] ON [dim].[ILSVare] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Kontoplan
IF OBJECT_ID(N'[dim].[Kontoplan]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND name = N'IX_dim_Kontoplan_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Kontoplan_Watermark] ON [dim].[Kontoplan];');

    EXEC(N'UPDATE [dim].[Kontoplan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Kontoplan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND name = N'IX_dim_Kontoplan_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Kontoplan_Watermark] ON [dim].[Kontoplan] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Kreditor
IF OBJECT_ID(N'[dim].[Kreditor]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = N'IX_dim_Kreditor_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Kreditor_Watermark] ON [dim].[Kreditor];');

    EXEC(N'UPDATE [dim].[Kreditor]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Kreditor]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kreditor]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kreditor]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kreditor] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kreditor] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]') AND name = N'IX_dim_Kreditor_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Kreditor_Watermark] ON [dim].[Kreditor] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Lager
IF OBJECT_ID(N'[dim].[Lager]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = N'IX_dim_Lager_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Lager_Watermark] ON [dim].[Lager];');

    EXEC(N'UPDATE [dim].[Lager]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Lager]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Lager]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Lager]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Lager] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Lager] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Lager]') AND name = N'IX_dim_Lager_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Lager_Watermark] ON [dim].[Lager] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.LagerbeholdningEgenskaber
IF OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND name = N'IX_dim_LagerbeholdningEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_dim_LagerbeholdningEgenskaber_Watermark] ON [dim].[LagerbeholdningEgenskaber];');

    EXEC(N'UPDATE [dim].[LagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[LagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND name = N'IX_dim_LagerbeholdningEgenskaber_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_LagerbeholdningEgenskaber_Watermark] ON [dim].[LagerbeholdningEgenskaber] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Leverandoervare
IF OBJECT_ID(N'[dim].[Leverandoervare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND name = N'IX_dim_Leverandoervare_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Leverandoervare_Watermark] ON [dim].[Leverandoervare];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND name = N'IX_dim_Vare_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Vare_Watermark] ON [dim].[Leverandoervare];');

    EXEC(N'UPDATE [dim].[Leverandoervare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Leverandoervare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND name = N'IX_dim_Leverandoervare_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Leverandoervare_Watermark] ON [dim].[Leverandoervare] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Okosted
IF OBJECT_ID(N'[dim].[Okosted]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Okosted]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Okosted]') AND name = N'IX_dim_Okosted_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Okosted_Watermark] ON [dim].[Okosted];');

    EXEC(N'UPDATE [dim].[Okosted]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Okosted]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Okosted]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Okosted]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Okosted]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Okosted] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Okosted]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Okosted] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Okosted]') AND name = N'IX_dim_Okosted_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Okosted_Watermark] ON [dim].[Okosted] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Organisation
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = N'IX_dim_Organisation_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Organisation_Watermark] ON [dim].[Organisation];');

    EXEC(N'UPDATE [dim].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = N'IX_dim_Organisation_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Organisation_Watermark] ON [dim].[Organisation] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Postering
IF OBJECT_ID(N'[dim].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = N'IX_dim_Postering_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Postering_Watermark] ON [dim].[Postering];');

    EXEC(N'UPDATE [dim].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = N'IX_dim_Postering_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Postering_Watermark] ON [dim].[Postering] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Rekvirent
IF OBJECT_ID(N'[dim].[Rekvirent]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = N'IX_dim_Rekvirent_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Rekvirent_Watermark] ON [dim].[Rekvirent];');

    EXEC(N'UPDATE [dim].[Rekvirent]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Rekvirent]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND name = N'IX_dim_Rekvirent_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Rekvirent_Watermark] ON [dim].[Rekvirent] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.SHAK
IF OBJECT_ID(N'[dim].[SHAK]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[SHAK]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[SHAK]') AND name = N'IX_dim_SHAK_Watermark')
        EXEC(N'DROP INDEX [IX_dim_SHAK_Watermark] ON [dim].[SHAK];');

    EXEC(N'UPDATE [dim].[SHAK]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[SHAK]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[SHAK]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[SHAK]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[SHAK]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[SHAK] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[SHAK]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[SHAK] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[SHAK]') AND name = N'IX_dim_SHAK_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_SHAK_Watermark] ON [dim].[SHAK] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.UNSPSC
IF OBJECT_ID(N'[dim].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = N'IX_dim_UNSPSC_Watermark')
        EXEC(N'DROP INDEX [IX_dim_UNSPSC_Watermark] ON [dim].[UNSPSC];');

    EXEC(N'UPDATE [dim].[UNSPSC]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[UNSPSC]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[UNSPSC] ADD CONSTRAINT [DF_UNSPSC_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[UNSPSC] ADD CONSTRAINT [DF_UNSPSC_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[UNSPSC] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[UNSPSC] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[UNSPSC]') AND name = N'IX_dim_UNSPSC_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_UNSPSC_Watermark] ON [dim].[UNSPSC] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.Vareudlevering
IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = N'IX_dim_Vareudlevering_Watermark')
        EXEC(N'DROP INDEX [IX_dim_Vareudlevering_Watermark] ON [dim].[Vareudlevering];');

    EXEC(N'UPDATE [dim].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND name = N'IX_dim_Vareudlevering_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_Vareudlevering_Watermark] ON [dim].[Vareudlevering] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- dim.ean
IF OBJECT_ID(N'[dim].[ean]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ean]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND name = N'IX_dim_ean_Watermark')
        EXEC(N'DROP INDEX [IX_dim_ean_Watermark] ON [dim].[ean];');

    EXEC(N'UPDATE [dim].[ean]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[ean]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ean]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ean]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ean]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ean] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ean]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ean] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[ean]') AND name = N'IX_dim_ean_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_dim_ean_Watermark] ON [dim].[ean] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.AfdelingLagerbeholdning
IF OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND name = N'IX_fct_AfdelingLagerbeholdning_Watermark')
        EXEC(N'DROP INDEX [IX_fct_AfdelingLagerbeholdning_Watermark] ON [fct].[AfdelingLagerbeholdning];');

    EXEC(N'UPDATE [fct].[AfdelingLagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[AfdelingLagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND name = N'IX_fct_AfdelingLagerbeholdning_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_AfdelingLagerbeholdning_Watermark] ON [fct].[AfdelingLagerbeholdning] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.AftaleVarePris
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND name = N'IX_fct_AftaleVarePris_Watermark')
        EXEC(N'DROP INDEX [IX_fct_AftaleVarePris_Watermark] ON [fct].[AftaleVarePris];');

    EXEC(N'UPDATE [fct].[AftaleVarePris]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[AftaleVarePris]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND name = N'IX_fct_AftaleVarePris_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_AftaleVarePris_Watermark] ON [fct].[AftaleVarePris] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Budget
IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = N'IX_fct_Budget_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Budget_Watermark] ON [fct].[Budget];');

    EXEC(N'UPDATE [fct].[Budget]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Budget]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Budget]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Budget]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Budget] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Budget] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Budget]') AND name = N'IX_fct_Budget_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Budget_Watermark] ON [fct].[Budget] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Faktura
IF OBJECT_ID(N'[fct].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = N'IX_fct_Faktura_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Faktura_Watermark] ON [fct].[Faktura];');

    EXEC(N'UPDATE [fct].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = N'IX_fct_Faktura_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Faktura_Watermark] ON [fct].[Faktura] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Fakturalinje
IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = N'IX_fct_Fakturalinje_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Fakturalinje_Watermark] ON [fct].[Fakturalinje];');

    EXEC(N'UPDATE [fct].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = N'IX_fct_Fakturalinje_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Fakturalinje_Watermark] ON [fct].[Fakturalinje] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Finanskladdelinje
IF OBJECT_ID(N'[fct].[Finanskladdelinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = N'IX_fct_Finanskladdelinje_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Finanskladdelinje_Watermark] ON [fct].[Finanskladdelinje];');

    EXEC(N'UPDATE [fct].[Finanskladdelinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Finanskladdelinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Finanskladdelinje]') AND name = N'IX_fct_Finanskladdelinje_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Finanskladdelinje_Watermark] ON [fct].[Finanskladdelinje] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Lagerbeholdning
IF OBJECT_ID(N'[fct].[Lagerbeholdning]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND name = N'IX_fct_Lagerbeholdning_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Lagerbeholdning_Watermark] ON [fct].[Lagerbeholdning];');

    EXEC(N'UPDATE [fct].[Lagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Lagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND name = N'IX_fct_Lagerbeholdning_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Lagerbeholdning_Watermark] ON [fct].[Lagerbeholdning] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Postering
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = N'IX_fct_Postering_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Postering_Watermark] ON [fct].[Postering];');

    EXEC(N'UPDATE [fct].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = N'IX_fct_Postering_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Postering_Watermark] ON [fct].[Postering] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fct.Vareudlevering
IF OBJECT_ID(N'[fct].[Vareudlevering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = N'IX_fct_Vareudlevering_Watermark')
        EXEC(N'DROP INDEX [IX_fct_Vareudlevering_Watermark] ON [fct].[Vareudlevering];');

    EXEC(N'UPDATE [fct].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND name = N'IX_fct_Vareudlevering_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fct_Vareudlevering_Watermark] ON [fct].[Vareudlevering] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Appointment
IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND name = N'IX_fhir_Appointment_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Appointment_Watermark] ON [fhir].[Appointment];');

    EXEC(N'UPDATE [fhir].[Appointment]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Appointment]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Appointment]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Appointment] ADD CONSTRAINT [DF_Appointment_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Appointment]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Appointment] ADD CONSTRAINT [DF_Appointment_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Appointment] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Appointment] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]') AND name = N'IX_fhir_Appointment_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Appointment_Watermark] ON [fhir].[Appointment] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.CarePlan
IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND name = N'IX_fhir_CarePlan_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_CarePlan_Watermark] ON [fhir].[CarePlan];');

    EXEC(N'UPDATE [fhir].[CarePlan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[CarePlan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD CONSTRAINT [DF_CarePlan_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD CONSTRAINT [DF_CarePlan_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND name = N'IX_fhir_CarePlan_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_CarePlan_Watermark] ON [fhir].[CarePlan] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.CarePlan_Activity
IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND name = N'IX_fhir_CarePlan_Activity_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_CarePlan_Activity_Watermark] ON [fhir].[CarePlan_Activity];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND name = N'IX_fhir_CarePlanActivity_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_CarePlanActivity_Watermark] ON [fhir].[CarePlan_Activity];');

    EXEC(N'UPDATE [fhir].[CarePlan_Activity]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[CarePlan_Activity]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND name = N'IX_fhir_CarePlan_Activity_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_CarePlan_Activity_Watermark] ON [fhir].[CarePlan_Activity] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Condition
IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND name = N'IX_fhir_Condition_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Condition_Watermark] ON [fhir].[Condition];');

    EXEC(N'UPDATE [fhir].[Condition]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Condition]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Condition] ADD CONSTRAINT [DF_Condition_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Condition] ADD CONSTRAINT [DF_Condition_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND name = N'IX_fhir_Condition_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Condition_Watermark] ON [fhir].[Condition] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Condition_tillaegskode
IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND name = N'IX_fhir_Condition_tillaegskode_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Condition_tillaegskode_Watermark] ON [fhir].[Condition_tillaegskode];');

    EXEC(N'UPDATE [fhir].[Condition_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Condition_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ADD CONSTRAINT [DF_Condition_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ADD CONSTRAINT [DF_Condition_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND name = N'IX_fhir_Condition_tillaegskode_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Condition_tillaegskode_Watermark] ON [fhir].[Condition_tillaegskode] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.DocumentReference
IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND name = N'IX_fhir_DocumentReference_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_DocumentReference_Watermark] ON [fhir].[DocumentReference];');

    EXEC(N'UPDATE [fhir].[DocumentReference]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[DocumentReference]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ADD CONSTRAINT [DF_DocumentReference_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ADD CONSTRAINT [DF_DocumentReference_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.DocumentReference_Recipient
IF OBJECT_ID(N'[fhir].[DocumentReference_Recipient]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND name = N'IX_fhir_DocumentReference_Recipient_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_DocumentReference_Recipient_Watermark] ON [fhir].[DocumentReference_Recipient];');

    EXEC(N'UPDATE [fhir].[DocumentReference_Recipient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[DocumentReference_Recipient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ADD CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ADD CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Encounter
IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]') AND name = N'IX_fhir_Encounter_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Encounter_Watermark] ON [fhir].[Encounter];');

    EXEC(N'UPDATE [fhir].[Encounter]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Encounter]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Encounter] ADD CONSTRAINT [DF_Encounter_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Encounter] ADD CONSTRAINT [DF_Encounter_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]') AND name = N'IX_fhir_Encounter_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Encounter_Watermark] ON [fhir].[Encounter] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Encounter_Location
IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND name = N'IX_fhir_Encounter_Location_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Encounter_Location_Watermark] ON [fhir].[Encounter_Location];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND name = N'IX_fhir_EncounterLocations_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_EncounterLocations_Watermark] ON [fhir].[Encounter_Location];');

    EXEC(N'UPDATE [fhir].[Encounter_Location]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Encounter_Location]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ADD CONSTRAINT [DF_Encounter_Location_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ADD CONSTRAINT [DF_Encounter_Location_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND name = N'IX_fhir_Encounter_Location_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Encounter_Location_Watermark] ON [fhir].[Encounter_Location] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.EpisodeOfCare
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND name = N'IX_fhir_EpisodeOfCare_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_EpisodeOfCare_Watermark] ON [fhir].[EpisodeOfCare];');

    EXEC(N'UPDATE [fhir].[EpisodeOfCare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[EpisodeOfCare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND name = N'IX_fhir_EpisodeOfCare_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_EpisodeOfCare_Watermark] ON [fhir].[EpisodeOfCare] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Location
IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Location]') AND name = N'IX_fhir_Location_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Location_Watermark] ON [fhir].[Location];');

    EXEC(N'UPDATE [fhir].[Location]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Location]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Location]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Location] ADD CONSTRAINT [DF_Location_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Location]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Location] ADD CONSTRAINT [DF_Location_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Location] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Location] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Location]') AND name = N'IX_fhir_Location_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Location_Watermark] ON [fhir].[Location] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Observation
IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_fhir_Observation_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Observation_Watermark] ON [fhir].[Observation];');

    EXEC(N'UPDATE [fhir].[Observation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Observation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Observation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Observation] ADD CONSTRAINT [DF_Observation_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Observation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Observation] ADD CONSTRAINT [DF_Observation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    -- dw_timestamp_effective is a PERSISTED computed column over both audit
    -- columns, so ALTER COLUMN fails with error 5074 while it exists. Drop it
    -- (and the index keyed on it) here; both are recreated below.
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_SystemDWEffective')
        EXEC(N'DROP INDEX [IX_FHIR_Observation_SystemDWEffective] ON [fhir].[Observation];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[Observation] DROP COLUMN [dw_timestamp_effective];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Observation] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Observation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[Observation] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_SystemDWEffective')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_SystemDWEffective] ON [fhir].[Observation] ([element_system] ASC, [dw_timestamp_effective] ASC) INCLUDE ([observation_id]) ON [PRIMARY];');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_fhir_Observation_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Observation_Watermark] ON [fhir].[Observation] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Organization
IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Organization]') AND name = N'IX_fhir_Organization_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Organization_Watermark] ON [fhir].[Organization];');

    EXEC(N'UPDATE [fhir].[Organization]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Organization]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Organization] ADD CONSTRAINT [DF_Organization_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Organization] ADD CONSTRAINT [DF_Organization_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Organization]') AND name = N'IX_fhir_Organization_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Organization_Watermark] ON [fhir].[Organization] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Patient
IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Patient]') AND name = N'IX_fhir_Patient_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Patient_Watermark] ON [fhir].[Patient];');

    EXEC(N'UPDATE [fhir].[Patient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Patient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Patient] ADD CONSTRAINT [DF_Patient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Patient] ADD CONSTRAINT [DF_Patient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Patient]') AND name = N'IX_fhir_Patient_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Patient_Watermark] ON [fhir].[Patient] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Patient_Address
IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND name = N'IX_fhir_Patient_Address_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Patient_Address_Watermark] ON [fhir].[Patient_Address];');

    EXEC(N'UPDATE [fhir].[Patient_Address]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Patient_Address]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ADD CONSTRAINT [DF_Patient_Address_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ADD CONSTRAINT [DF_Patient_Address_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND name = N'IX_fhir_Patient_Address_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Patient_Address_Watermark] ON [fhir].[Patient_Address] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Procedure
IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]') AND name = N'IX_fhir_Procedure_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Procedure_Watermark] ON [fhir].[Procedure];');

    EXEC(N'UPDATE [fhir].[Procedure]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Procedure]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Procedure] ADD CONSTRAINT [DF_Procedure_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Procedure] ADD CONSTRAINT [DF_Procedure_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]') AND name = N'IX_fhir_Procedure_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Procedure_Watermark] ON [fhir].[Procedure] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Procedure_tillaegskode
IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND name = N'IX_fhir_Procedure_tillaegskode_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Procedure_tillaegskode_Watermark] ON [fhir].[Procedure_tillaegskode];');

    EXEC(N'UPDATE [fhir].[Procedure_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Procedure_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ADD CONSTRAINT [DF_Procedure_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ADD CONSTRAINT [DF_Procedure_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND name = N'IX_fhir_Procedure_tillaegskode_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Procedure_tillaegskode_Watermark] ON [fhir].[Procedure_tillaegskode] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Questionnaire
IF OBJECT_ID(N'[fhir].[Questionnaire]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND name = N'IX_fhir_Questionnaire_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Questionnaire_Watermark] ON [fhir].[Questionnaire];');

    EXEC(N'UPDATE [fhir].[Questionnaire]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Questionnaire]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ADD CONSTRAINT [DF_Questionnaire_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ADD CONSTRAINT [DF_Questionnaire_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND name = N'IX_fhir_Questionnaire_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Questionnaire_Watermark] ON [fhir].[Questionnaire] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.QuestionnaireResponse
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_fhir_QuestionnaireResponse_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_QuestionnaireResponse_Watermark] ON [fhir].[QuestionnaireResponse];');

    EXEC(N'UPDATE [fhir].[QuestionnaireResponse]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    -- dw_timestamp_effective is a PERSISTED computed column over both audit
    -- columns, so ALTER COLUMN fails with error 5074 while it exists. Drop it
    -- (and the index keyed on it) here; both are recreated below.
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_FHIR_QR_DWEffective')
        EXEC(N'DROP INDEX [IX_FHIR_QR_DWEffective] ON [fhir].[QuestionnaireResponse];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] DROP COLUMN [dw_timestamp_effective];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_FHIR_QR_DWEffective')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_QR_DWEffective] ON [fhir].[QuestionnaireResponse] ([dw_timestamp_effective] ASC) INCLUDE ([qr_id]) ON [PRIMARY];');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_fhir_QuestionnaireResponse_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_QuestionnaireResponse_Watermark] ON [fhir].[QuestionnaireResponse] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.QuestionnaireResponse_item
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_fhir_QuestionnaireResponse_item_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_QuestionnaireResponse_item_Watermark] ON [fhir].[QuestionnaireResponse_item];');

    EXEC(N'UPDATE [fhir].[QuestionnaireResponse_item]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse_item]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    -- dw_timestamp_effective is a PERSISTED computed column over both audit
    -- columns, so ALTER COLUMN fails with error 5074 while it exists. Drop it
    -- (and the index keyed on it) here; both are recreated below.
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_FHIR_QRI_DWEffective')
        EXEC(N'DROP INDEX [IX_FHIR_QRI_DWEffective] ON [fhir].[QuestionnaireResponse_item];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] DROP COLUMN [dw_timestamp_effective];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'dw_timestamp_effective')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_FHIR_QRI_DWEffective')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_QRI_DWEffective] ON [fhir].[QuestionnaireResponse_item] ([dw_timestamp_effective] ASC) INCLUDE ([qr_id]) ON [PRIMARY];');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_fhir_QuestionnaireResponse_item_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_QuestionnaireResponse_item_Watermark] ON [fhir].[QuestionnaireResponse_item] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.Questionnaire_item
IF OBJECT_ID(N'[fhir].[Questionnaire_item]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND name = N'IX_fhir_Questionnaire_item_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_Questionnaire_item_Watermark] ON [fhir].[Questionnaire_item];');

    EXEC(N'UPDATE [fhir].[Questionnaire_item]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Questionnaire_item]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ADD CONSTRAINT [DF_Questionnaire_item_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ADD CONSTRAINT [DF_Questionnaire_item_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND name = N'IX_fhir_Questionnaire_item_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_Questionnaire_item_Watermark] ON [fhir].[Questionnaire_item] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.ServiceRequest
IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND name = N'IX_fhir_ServiceRequest_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_ServiceRequest_Watermark] ON [fhir].[ServiceRequest];');

    EXEC(N'UPDATE [fhir].[ServiceRequest]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD CONSTRAINT [DF_ServiceRequest_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD CONSTRAINT [DF_ServiceRequest_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND name = N'IX_fhir_ServiceRequest_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_ServiceRequest_Watermark] ON [fhir].[ServiceRequest] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.ServiceRequest_Recipient
IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND name = N'IX_fhir_ServiceRequest_Recipient_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_ServiceRequest_Recipient_Watermark] ON [fhir].[ServiceRequest_Recipient];');

    EXEC(N'UPDATE [fhir].[ServiceRequest_Recipient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest_Recipient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ADD CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ADD CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND name = N'IX_fhir_ServiceRequest_Recipient_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_ServiceRequest_Recipient_Watermark] ON [fhir].[ServiceRequest_Recipient] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- fhir.ServiceRequest_tillaegskode
IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND name = N'IX_fhir_ServiceRequest_tillaegskode_Watermark')
        EXEC(N'DROP INDEX [IX_fhir_ServiceRequest_tillaegskode_Watermark] ON [fhir].[ServiceRequest_tillaegskode];');

    EXEC(N'UPDATE [fhir].[ServiceRequest_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ADD CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ADD CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND name = N'IX_fhir_ServiceRequest_tillaegskode_Watermark')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_fhir_ServiceRequest_tillaegskode_Watermark] ON [fhir].[ServiceRequest_tillaegskode] ([dw_timestamp_update] ASC, [dw_timestamp_insert] ASC);');
END
GO

-- map.AfdelingLagerbeholdning
IF OBJECT_ID(N'[map].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND name = N'IX_map_AfdelingLagerbeholdning_Watermark')
        EXEC(N'DROP INDEX [IX_map_AfdelingLagerbeholdning_Watermark] ON [map].[AfdelingLagerbeholdning];');

    EXEC(N'UPDATE [map].[AfdelingLagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.AfdelingLagerbeholdningEgenskaber
IF OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND name = N'IX_map_AfdelingLagerbeholdningEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_map_AfdelingLagerbeholdningEgenskaber_Watermark] ON [map].[AfdelingLagerbeholdningEgenskaber];');

    EXEC(N'UPDATE [map].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Aftale
IF OBJECT_ID(N'[map].[Aftale]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Aftale]') AND name = N'IX_map_Aftale_Watermark')
        EXEC(N'DROP INDEX [IX_map_Aftale_Watermark] ON [map].[Aftale];');

    EXEC(N'UPDATE [map].[Aftale]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Aftale]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Aftale] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Aftale] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.AftaleVarePris
IF OBJECT_ID(N'[map].[AftaleVarePris]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND name = N'IX_map_AftaleVarePris_Watermark')
        EXEC(N'DROP INDEX [IX_map_AftaleVarePris_Watermark] ON [map].[AftaleVarePris];');

    EXEC(N'UPDATE [map].[AftaleVarePris]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AftaleVarePris]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Budget
IF OBJECT_ID(N'[map].[Budget]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Budget]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Budget]') AND name = N'IX_map_Budget_Watermark')
        EXEC(N'DROP INDEX [IX_map_Budget_Watermark] ON [map].[Budget];');

    EXEC(N'UPDATE [map].[Budget]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Budget]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Budget]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Budget]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Budget]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Budget] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Budget]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Budget] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.BudgetEgenskaber
IF OBJECT_ID(N'[map].[BudgetEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND name = N'IX_map_BudgetEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_map_BudgetEgenskaber_Watermark] ON [map].[BudgetEgenskaber];');

    EXEC(N'UPDATE [map].[BudgetEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[BudgetEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Faktura
IF OBJECT_ID(N'[map].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Faktura]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Faktura]') AND name = N'IX_map_Faktura_Watermark')
        EXEC(N'DROP INDEX [IX_map_Faktura_Watermark] ON [map].[Faktura];');

    EXEC(N'UPDATE [map].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Fakturalinje
IF OBJECT_ID(N'[map].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND name = N'IX_map_Fakturalinje_Watermark')
        EXEC(N'DROP INDEX [IX_map_Fakturalinje_Watermark] ON [map].[Fakturalinje];');

    EXEC(N'UPDATE [map].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Finanskladde
IF OBJECT_ID(N'[map].[Finanskladde]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]') AND name = N'IX_map_Finanskladde_Watermark')
        EXEC(N'DROP INDEX [IX_map_Finanskladde_Watermark] ON [map].[Finanskladde];');

    EXEC(N'UPDATE [map].[Finanskladde]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Finanskladde]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladde]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladde]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladde] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladde] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Finanskladdelinje
IF OBJECT_ID(N'[map].[Finanskladdelinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND name = N'IX_map_Finanskladdelinje_Watermark')
        EXEC(N'DROP INDEX [IX_map_Finanskladdelinje_Watermark] ON [map].[Finanskladdelinje];');

    EXEC(N'UPDATE [map].[Finanskladdelinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Finanskladdelinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.ILSVare
IF OBJECT_ID(N'[map].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ILSVare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[ILSVare]') AND name = N'IX_map_ILSVare_Watermark')
        EXEC(N'DROP INDEX [IX_map_ILSVare_Watermark] ON [map].[ILSVare];');

    EXEC(N'UPDATE [map].[ILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[ILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Kontoplan
IF OBJECT_ID(N'[map].[Kontoplan]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]') AND name = N'IX_map_Kontoplan_Watermark')
        EXEC(N'DROP INDEX [IX_map_Kontoplan_Watermark] ON [map].[Kontoplan];');

    EXEC(N'UPDATE [map].[Kontoplan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Kontoplan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kontoplan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kontoplan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Kreditor
IF OBJECT_ID(N'[map].[Kreditor]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Kreditor]') AND name = N'IX_map_Kreditor_Watermark')
        EXEC(N'DROP INDEX [IX_map_Kreditor_Watermark] ON [map].[Kreditor];');

    EXEC(N'UPDATE [map].[Kreditor]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Kreditor]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kreditor] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kreditor] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Lager
IF OBJECT_ID(N'[map].[Lager]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Lager]') AND name = N'IX_map_Lager_Watermark')
        EXEC(N'DROP INDEX [IX_map_Lager_Watermark] ON [map].[Lager];');

    EXEC(N'UPDATE [map].[Lager]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Lager]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lager]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lager]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lager] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lager] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Lagerbeholdning
IF OBJECT_ID(N'[map].[Lagerbeholdning]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND name = N'IX_map_Lagerbeholdning_Watermark')
        EXEC(N'DROP INDEX [IX_map_Lagerbeholdning_Watermark] ON [map].[Lagerbeholdning];');

    EXEC(N'UPDATE [map].[Lagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Lagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.LagerbeholdningEgenskaber
IF OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND name = N'IX_map_LagerbeholdningEgenskaber_Watermark')
        EXEC(N'DROP INDEX [IX_map_LagerbeholdningEgenskaber_Watermark] ON [map].[LagerbeholdningEgenskaber];');

    EXEC(N'UPDATE [map].[LagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[LagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Leverandoervare
IF OBJECT_ID(N'[map].[Leverandoervare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]') AND name = N'IX_map_Leverandoervare_Watermark')
        EXEC(N'DROP INDEX [IX_map_Leverandoervare_Watermark] ON [map].[Leverandoervare];');

    EXEC(N'UPDATE [map].[Leverandoervare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Leverandoervare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Leverandoervare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Leverandoervare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Leverandoervare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Leverandoervare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Leverandoervare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Okosted
IF OBJECT_ID(N'[map].[Okosted]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Okosted]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Okosted]') AND name = N'IX_map_Okosted_Watermark')
        EXEC(N'DROP INDEX [IX_map_Okosted_Watermark] ON [map].[Okosted];');

    EXEC(N'UPDATE [map].[Okosted]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Okosted]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Okosted]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Okosted]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Okosted]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Okosted] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Okosted]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Okosted] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Organisation
IF OBJECT_ID(N'[map].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = N'IX_map_Organisation_Watermark')
        EXEC(N'DROP INDEX [IX_map_Organisation_Watermark] ON [map].[Organisation];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Organisation]') AND name = N'IX_map_Organization_Watermark')
        EXEC(N'DROP INDEX [IX_map_Organization_Watermark] ON [map].[Organisation];');

    EXEC(N'UPDATE [map].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Postering
IF OBJECT_ID(N'[map].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Postering]') AND name = N'IX_map_Postering_Watermark')
        EXEC(N'DROP INDEX [IX_map_Postering_Watermark] ON [map].[Postering];');

    EXEC(N'UPDATE [map].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Rekvirent
IF OBJECT_ID(N'[map].[Rekvirent]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]') AND name = N'IX_map_Rekvirent_Watermark')
        EXEC(N'DROP INDEX [IX_map_Rekvirent_Watermark] ON [map].[Rekvirent];');

    EXEC(N'UPDATE [map].[Rekvirent]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Rekvirent]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Rekvirent]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Rekvirent]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Rekvirent] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Rekvirent] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.SHAK
IF OBJECT_ID(N'[map].[SHAK]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[SHAK]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[SHAK]') AND name = N'IX_map_SHAK_Watermark')
        EXEC(N'DROP INDEX [IX_map_SHAK_Watermark] ON [map].[SHAK];');

    EXEC(N'UPDATE [map].[SHAK]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[SHAK]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[SHAK]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[SHAK]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[SHAK]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[SHAK] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[SHAK]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[SHAK] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.UNSPSC
IF OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]') AND name = N'IX_map_UNSPSC_Watermark')
        EXEC(N'DROP INDEX [IX_map_UNSPSC_Watermark] ON [map].[UNSPSC];');

    EXEC(N'UPDATE [map].[UNSPSC]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[UNSPSC]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[UNSPSC]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[UNSPSC] ADD CONSTRAINT [DF_UNSPSC_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[UNSPSC]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[UNSPSC] ADD CONSTRAINT [DF_UNSPSC_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[UNSPSC] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[UNSPSC]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[UNSPSC] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Vareudlevering
IF OBJECT_ID(N'[map].[Vareudlevering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND name = N'IX_map_Vareudlevering_Watermark')
        EXEC(N'DROP INDEX [IX_map_Vareudlevering_Watermark] ON [map].[Vareudlevering];');

    EXEC(N'UPDATE [map].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.ean
IF OBJECT_ID(N'[map].[ean]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ean]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[map].[ean]') AND name = N'IX_map_ean_Watermark')
        EXEC(N'DROP INDEX [IX_map_ean_Watermark] ON [map].[ean];');

    EXEC(N'UPDATE [map].[ean]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[ean]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ean]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ean]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ean]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ean] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ean]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ean] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- meta.IFDBAktivitetsresultatRoutingDecision
IF OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]') AND name = N'IX_meta_IFDBAktivitetsresultatRoutingDecision_Watermark')
        EXEC(N'DROP INDEX [IX_meta_IFDBAktivitetsresultatRoutingDecision_Watermark] ON [meta].[IFDBAktivitetsresultatRoutingDecision];');

    EXEC(N'UPDATE [meta].[IFDBAktivitetsresultatRoutingDecision]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [meta].[IFDBAktivitetsresultatRoutingDecision]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ADD CONSTRAINT [DF_IFDBAktivitetsresultatRoutingDecision_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ADD CONSTRAINT [DF_IFDBAktivitetsresultatRoutingDecision_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- meta.IFDBAktivitetstypeRouting
IF OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]') AND name = N'IX_meta_IFDBAktivitetstypeRouting_Watermark')
        EXEC(N'DROP INDEX [IX_meta_IFDBAktivitetstypeRouting_Watermark] ON [meta].[IFDBAktivitetstypeRouting];');

    EXEC(N'UPDATE [meta].[IFDBAktivitetstypeRouting]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [meta].[IFDBAktivitetstypeRouting]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ADD CONSTRAINT [DF_IFDBAktivitetstypeRouting_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ADD CONSTRAINT [DF_IFDBAktivitetstypeRouting_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.FakturaToPostering
IF OBJECT_ID(N'[ref].[FakturaToPostering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND name = N'IX_ref_FakturaToPostering_Watermark')
        EXEC(N'DROP INDEX [IX_ref_FakturaToPostering_Watermark] ON [ref].[FakturaToPostering];');

    EXEC(N'UPDATE [ref].[FakturaToPostering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[FakturaToPostering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ADD CONSTRAINT [DF_FakturaToPostering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ADD CONSTRAINT [DF_FakturaToPostering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.FakturalinjeMatchKeys
IF OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND name = N'IX_ref_FakturalinjeMatchKeys_Watermark')
        EXEC(N'DROP INDEX [IX_ref_FakturalinjeMatchKeys_Watermark] ON [ref].[FakturalinjeMatchKeys];');

    EXEC(N'UPDATE [ref].[FakturalinjeMatchKeys]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[FakturalinjeMatchKeys]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[FakturalinjeMatchKeys] ADD CONSTRAINT [DF_FakturalinjeMatchKeys_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[FakturalinjeMatchKeys] ADD CONSTRAINT [DF_FakturalinjeMatchKeys_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturalinjeMatchKeys] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturalinjeMatchKeys]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturalinjeMatchKeys] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.KMDFakturalinjeUNSPSCMatch
IF OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND name = N'IX_ref_KMDFakturalinjeUNSPSCMatch_Watermark')
        EXEC(N'DROP INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_Watermark] ON [ref].[KMDFakturalinjeUNSPSCMatch];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND name = N'IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp')
        EXEC(N'DROP INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp] ON [ref].[KMDFakturalinjeUNSPSCMatch];');
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND name = N'IX_ref_kmd_fakturalinje_unspsc_match_timestamp')
        EXEC(N'DROP INDEX [IX_ref_kmd_fakturalinje_unspsc_match_timestamp] ON [ref].[KMDFakturalinjeUNSPSCMatch];');

    EXEC(N'UPDATE [ref].[KMDFakturalinjeUNSPSCMatch]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[KMDFakturalinjeUNSPSCMatch]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[KMDFakturalinjeUNSPSCMatch] ADD CONSTRAINT [DF_KMDFakturalinjeUNSPSCMatch_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[KMDFakturalinjeUNSPSCMatch] ADD CONSTRAINT [DF_KMDFakturalinjeUNSPSCMatch_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[KMDFakturalinjeUNSPSCMatch] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[KMDFakturalinjeUNSPSCMatch] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[KMDFakturalinjeUNSPSCMatch]') AND name = N'IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp')
        EXEC(N'CREATE NONCLUSTERED INDEX [IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp] ON [ref].[KMDFakturalinjeUNSPSCMatch] ([dw_timestamp_update], [dw_timestamp_insert]) WHERE [dw_is_deleted] = 0;');
END
GO

-- ref.Organisation
IF OBJECT_ID(N'[ref].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND name = N'IX_ref_Organisation_Watermark')
        EXEC(N'DROP INDEX [IX_ref_Organisation_Watermark] ON [ref].[Organisation];');

    EXEC(N'UPDATE [ref].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[Organisation] ADD CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.PatientToBorger
IF OBJECT_ID(N'[ref].[PatientToBorger]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND name = N'IX_ref_PatientToBorger_Watermark')
        EXEC(N'DROP INDEX [IX_ref_PatientToBorger_Watermark] ON [ref].[PatientToBorger];');

    EXEC(N'UPDATE [ref].[PatientToBorger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[PatientToBorger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ADD CONSTRAINT [DF_PatientToBorger_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ADD CONSTRAINT [DF_PatientToBorger_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.Patient_Address_To_Borger
IF OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND name = N'IX_ref_Patient_Address_To_Borger_Watermark')
        EXEC(N'DROP INDEX [IX_ref_Patient_Address_To_Borger_Watermark] ON [ref].[Patient_Address_To_Borger];');

    EXEC(N'UPDATE [ref].[Patient_Address_To_Borger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[Patient_Address_To_Borger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ADD CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ADD CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.RekvirentUndtagelse
IF OBJECT_ID(N'[ref].[RekvirentUndtagelse]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
               AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND name = N'IX_ref_RekvirentUndtagelse_Watermark')
        EXEC(N'DROP INDEX [IX_ref_RekvirentUndtagelse_Watermark] ON [ref].[RekvirentUndtagelse];');

    EXEC(N'UPDATE [ref].[RekvirentUndtagelse]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[RekvirentUndtagelse]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[RekvirentUndtagelse] ADD CONSTRAINT [DF_RekvirentUndtagelse_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[RekvirentUndtagelse] ADD CONSTRAINT [DF_RekvirentUndtagelse_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[RekvirentUndtagelse] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[RekvirentUndtagelse] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- sec.RBOrganisation
IF OBJECT_ID(N'[sec].[RBOrganisation]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation]')
               AND name IN (N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation]') AND name = N'IX_sec_RBOrganisation_Watermark')
        EXEC(N'DROP INDEX [IX_sec_RBOrganisation_Watermark] ON [sec].[RBOrganisation];');

    EXEC(N'UPDATE [sec].[RBOrganisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[sec].[RBOrganisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [sec].[RBOrganisation] ADD CONSTRAINT [DF_RBOrganisation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [sec].[RBOrganisation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- sec.RolleDomaene
IF OBJECT_ID(N'[sec].[RolleDomaene]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[RolleDomaene]')
               AND name IN (N'dw_timestamp_update') AND is_nullable = 1)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[sec].[RolleDomaene]') AND name = N'IX_sec_RolleDomaene_Watermark')
        EXEC(N'DROP INDEX [IX_sec_RolleDomaene_Watermark] ON [sec].[RolleDomaene];');

    EXEC(N'UPDATE [sec].[RolleDomaene]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[sec].[RolleDomaene]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [sec].[RolleDomaene] ADD CONSTRAINT [DF_RolleDomaene_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[RolleDomaene]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [sec].[RolleDomaene] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO
