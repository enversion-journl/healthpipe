-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DB space snapshots
-- One row per capture of file + volume occupancy, plus the largest objects
-- at that moment. Written by infra_db_space_* (daily, no ETL deps) so we can
-- forecast weeks-until-full instead of discovering a full PRIMARY filegroup
-- from a failed MERGE.
-- Lives on both staging (DSA) and DSO -- each database records itself.
--
-- cap_kind records WHAT bounded the database at capture time, because it
-- decides which numbers mean anything:
--   'database_quota' - Azure max size or a fixed data-file max_size. Owned by
--                      this database alone, so used_pct is its real occupancy.
--   'volume'         - unlimited autogrowth. remaining_gb is in-file free
--                      space plus volume free space; used_pct is NULL because
--                      a shared volume's percentage tracks the neighbours.
--                      volume_* describe the FULLEST volume holding a data
--                      file (what stops the next autogrowth), while
--                      volume_free_total_gb sums every such volume -- QA DSO
--                      spans three, one of them full.
--   'unknown'        - neither could be read; headroom is not known.
-- This file is the authoritative definition AND its own migration: the guarded
-- ALTERs at the bottom bring an already-created table up to the shape above.
-- That is deliberate. The daily monitor must not depend on staging_migrations
-- (a full filegroup has to surface as disk space, not as a failed ETL step), so
-- it runs this one file itself -- and a bare CREATE-if-missing would no-op on a
-- deployed table and then INSERT into columns that do not exist yet.
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'meta')
    EXEC (N'CREATE SCHEMA [meta]');
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = N'meta' AND TABLE_NAME = N'DbSpaceSnapshot'
)
BEGIN
    CREATE TABLE [meta].[DbSpaceSnapshot] (
        [snapshot_id]           BIGINT          IDENTITY(1, 1) NOT NULL,
        [captured_at]           DATETIME2(0)    NOT NULL
            CONSTRAINT [DF_DbSpaceSnapshot_captured_at] DEFAULT SYSUTCDATETIME(),
        [database_name]         NVARCHAR(128)   NOT NULL,
        [volume_mount_point]    NVARCHAR(256)   NULL,
        [volume_total_gb]       DECIMAL(12, 2)  NULL,
        [volume_free_gb]        DECIMAL(12, 2)  NULL,
        [volume_free_total_gb]  DECIMAL(12, 2)  NULL,
        [data_file_size_gb]     DECIMAL(12, 2)  NOT NULL,
        [data_file_used_gb]     DECIMAL(12, 2)  NOT NULL,
        [data_file_max_size_gb] DECIMAL(12, 2)  NULL,
        [log_file_size_gb]      DECIMAL(12, 2)  NULL,
        [log_file_used_gb]      DECIMAL(12, 2)  NULL,
        [azure_max_size_gb]     DECIMAL(12, 2)  NULL,
        [cap_kind]              NVARCHAR(32)    NULL,
        [used_pct]              DECIMAL(5, 2)   NULL,
        [volume_used_pct]       DECIMAL(5, 2)   NULL,
        [remaining_gb]          DECIMAL(12, 2)  NULL,
        [weekly_growth_gb]      DECIMAL(12, 2)  NULL,
        [weeks_until_full]      DECIMAL(12, 2)  NULL,
        [headroom_level]        NVARCHAR(8)     NULL,
        CONSTRAINT [PK_DbSpaceSnapshot] PRIMARY KEY CLUSTERED ([snapshot_id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_DbSpaceSnapshot_DatabaseCaptured'
      AND object_id = OBJECT_ID(N'[meta].[DbSpaceSnapshot]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_DbSpaceSnapshot_DatabaseCaptured]
        ON [meta].[DbSpaceSnapshot] ([database_name], [captured_at] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = N'meta' AND TABLE_NAME = N'DbSpaceTopObject'
)
BEGIN
    CREATE TABLE [meta].[DbSpaceTopObject] (
        [snapshot_id]    BIGINT         NOT NULL,
        [rank_no]        INT            NOT NULL,
        [object_schema]  NVARCHAR(128)  NOT NULL,
        [object_table]   NVARCHAR(128)  NOT NULL,
        [index_name]     NVARCHAR(128)  NULL,
        [index_type]     NVARCHAR(60)   NULL,
        [used_mb]        DECIMAL(12, 1) NOT NULL,
        CONSTRAINT [PK_DbSpaceTopObject] PRIMARY KEY CLUSTERED ([snapshot_id], [rank_no]),
        CONSTRAINT [FK_DbSpaceTopObject_Snapshot]
            FOREIGN KEY ([snapshot_id]) REFERENCES [meta].[DbSpaceSnapshot] ([snapshot_id])
    );
END
GO

-- ----------------------------------------------------------------------------
-- Column additions for tables created by an earlier version of this file.
-- Guarded and idempotent; a no-op on a table created above.
-- ----------------------------------------------------------------------------

IF OBJECT_ID(N'[meta].[DbSpaceSnapshot]', 'U') IS NOT NULL
   AND COL_LENGTH('[meta].[DbSpaceSnapshot]', 'cap_kind') IS NULL
BEGIN
    ALTER TABLE [meta].[DbSpaceSnapshot]
        ADD [cap_kind] NVARCHAR(32) NULL;
END
GO

IF OBJECT_ID(N'[meta].[DbSpaceSnapshot]', 'U') IS NOT NULL
   AND COL_LENGTH('[meta].[DbSpaceSnapshot]', 'volume_used_pct') IS NULL
BEGIN
    ALTER TABLE [meta].[DbSpaceSnapshot]
        ADD [volume_used_pct] DECIMAL(5, 2) NULL;
END
GO

IF OBJECT_ID(N'[meta].[DbSpaceSnapshot]', 'U') IS NOT NULL
   AND COL_LENGTH('[meta].[DbSpaceSnapshot]', 'remaining_gb') IS NULL
BEGIN
    ALTER TABLE [meta].[DbSpaceSnapshot]
        ADD [remaining_gb] DECIMAL(12, 2) NULL;
END
GO

IF OBJECT_ID(N'[meta].[DbSpaceSnapshot]', 'U') IS NOT NULL
   AND COL_LENGTH('[meta].[DbSpaceSnapshot]', 'headroom_level') IS NULL
BEGIN
    ALTER TABLE [meta].[DbSpaceSnapshot]
        ADD [headroom_level] NVARCHAR(8) NULL;
END
GO

IF OBJECT_ID(N'[meta].[DbSpaceSnapshot]', 'U') IS NOT NULL
   AND COL_LENGTH('[meta].[DbSpaceSnapshot]', 'volume_free_total_gb') IS NULL
BEGIN
    ALTER TABLE [meta].[DbSpaceSnapshot]
        ADD [volume_free_total_gb] DECIMAL(12, 2) NULL;
END
GO
