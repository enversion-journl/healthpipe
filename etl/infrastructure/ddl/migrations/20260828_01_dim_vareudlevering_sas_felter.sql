-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- SAS ILS_GRD_DB_Forbrug parity fields on dim.Vareudlevering.
-- Archives: arc.ils_projekt, arc.ils_varetype2 (full-load lookups).
-- Dim columns: lgrlok, ekspeditionsnummer, projektnavn, lager_varetype,
-- lager_varetype_navn (ILS varetype2 code/name from lgrvare + varetype2).
--
-- Also NULL dim/fct Vareudlevering DFM checkpoints so the next scheduled run
-- backfills the new columns without a manual full_load. pre_stg takes MIN of
-- the two checkpoints; an early date is still a delta path — only NULL is a
-- true full reconstruction (see 20260813a_borger_checkpoint_null_and_protection_flag.sql).

-- ----------------------------------------------------------------------------
-- arc.ils_varetype2
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_varetype2'
)
BEGIN
    CREATE TABLE [arc].[ils_varetype2] (
        [prrowid]      varchar(36)    NOT NULL,
        [varetype2]    varchar(12)    NULL,
        [navn]         varchar(80)    NULL,
        [sw_synk]      bit            NULL,
        [aktiv]        bit            NULL,
        [sysklk]       int            NULL,
        [sysdato]      datetime       NULL,
        [seqnr]        int            NULL,
        [bruger]       varchar(6)     NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_varetype2_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_varetype2_dw_timestamp_update] DEFAULT SYSDATETIME(),

        CONSTRAINT [PK_arc_ils_varetype2] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF OBJECT_ID(N'[arc].[ils_varetype2]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[arc].[ils_varetype2]')
         AND name = N'IX_arc_ils_varetype2_dw_timestamp_update'
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_varetype2_dw_timestamp_update]
        ON [arc].[ils_varetype2] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- arc.ils_projekt
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_projekt'
)
BEGIN
    CREATE TABLE [arc].[ils_projekt] (
        [prrowid]      varchar(36)    NOT NULL,
        [sgh]          varchar(8)     NULL,
        [projnr]       varchar(20)    NULL,
        [navn]         varchar(80)    NULL,
        [ktosted]      varchar(20)    NULL,
        [projgr]       varchar(16)    NULL,
        [sw_niveau]    bit            NULL,
        [startdato]    datetime       NULL,
        [slutdato]     datetime       NULL,
        [rekvirent]    varchar(200)   NULL,
        [aktiv]        bit            NULL,
        [sysdato]      datetime       NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_projekt_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_projekt_dw_timestamp_update] DEFAULT SYSDATETIME(),

        CONSTRAINT [PK_arc_ils_projekt] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF OBJECT_ID(N'[arc].[ils_projekt]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE object_id = OBJECT_ID(N'[arc].[ils_projekt]')
         AND name = N'IX_arc_ils_projekt_dw_timestamp_update'
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_projekt_dw_timestamp_update]
        ON [arc].[ils_projekt] ([dw_timestamp_update] DESC);
END
GO

-- ----------------------------------------------------------------------------
-- dim.Vareudlevering — five SAS parity columns
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Vareudlevering]', N'lgrlok') IS NULL
BEGIN
    ALTER TABLE [dim].[Vareudlevering] ADD [lgrlok] NVARCHAR(16) NULL;
END
GO

IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Vareudlevering]', N'ekspeditionsnummer') IS NULL
BEGIN
    ALTER TABLE [dim].[Vareudlevering] ADD [ekspeditionsnummer] INT NULL;
END
GO

IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Vareudlevering]', N'projektnavn') IS NULL
BEGIN
    ALTER TABLE [dim].[Vareudlevering] ADD [projektnavn] NVARCHAR(80) NULL;
END
GO

IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Vareudlevering]', N'lager_varetype') IS NULL
BEGIN
    ALTER TABLE [dim].[Vareudlevering] ADD [lager_varetype] NVARCHAR(12) NULL;
END
GO

IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Vareudlevering]', N'lager_varetype_navn') IS NULL
BEGIN
    ALTER TABLE [dim].[Vareudlevering] ADD [lager_varetype_navn] NVARCHAR(80) NULL;
END
GO

-- ----------------------------------------------------------------------------
-- DFM checkpoints → NULL (true full reconstruction on the next scheduled run)
-- Both dim and fct owners must be NULL: pre_stg_Vareudlevering_ils filters on
-- MIN(dim, fct). An early date still takes the delta path.
-- RUNS ON BOTH DATABASES. Checkpoint reset no-ops where meta.DFMState is absent (DSO).
-- ----------------------------------------------------------------------------
IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50011,
        N'meta.DFMState exists but meta.usp_dfm_reset does not - materialise staging_infra_dfm before this migration. Skipping the checkpoint reset would leave dim/fct.Vareudlevering with a current checkpoint after adding SAS parity columns.',
        1;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'No meta.DFMState on this database (expected on DSO) - no checkpoints to reset.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''NULL dim/fct.Vareudlevering checkpoints so the next scheduled run backfills [internal ticket] SAS parity columns (true full reconstruction via pre_stg).'';
        DECLARE @reset INT = 0;

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''dim'' AND [table_name] = ''Vareudlevering'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''dim'', @table_name = ''Vareudlevering'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''fct'' AND [table_name] = ''Vareudlevering'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fct'', @table_name = ''Vareudlevering'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        PRINT CONCAT(''Reset '', @reset, '' DFM checkpoint(s) to NULL for Vareudlevering.'');
';
GO
