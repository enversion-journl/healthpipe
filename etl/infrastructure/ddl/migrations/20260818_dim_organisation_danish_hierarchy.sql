-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Organisation: Danish business columns, drop type, persist SOR hierarchy.
--
-- Renames code/name/parent_code/active → kode/navn/parent_kode/aktiv, drops
-- type, adds hierarki_niveau + kode_niveau1..9 / navn_niveau1..9.
--
-- Drops vw_dim.Organisation (and a local scratch unpack view if present) so
-- they are recreated from base DDL against the new columns. Hierarchy values
-- are filled by dim_Organisation (pass 2), not this migration.
--
-- Idempotent. Runs on staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[vw_dim].[Organisation]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Organisation];
GO

IF OBJECT_ID(N'[vw_dim].[Organisation_udpakket]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Organisation_udpakket];
GO

-- ---------------------------------------------------------------------------
-- Column renames
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'code') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'kode') IS NULL
    EXEC sp_rename N'[dim].[Organisation].[code]', N'kode', N'COLUMN';
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'name') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'navn') IS NULL
    EXEC sp_rename N'[dim].[Organisation].[name]', N'navn', N'COLUMN';
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'parent_code') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'parent_kode') IS NULL
    EXEC sp_rename N'[dim].[Organisation].[parent_code]', N'parent_kode', N'COLUMN';
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'active') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'aktiv') IS NULL
    EXEC sp_rename N'[dim].[Organisation].[active]', N'aktiv', N'COLUMN';
GO

-- ---------------------------------------------------------------------------
-- Drop type (and any DEFAULT on it)
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'type') IS NOT NULL
BEGIN
    EXEC(N'
        DECLARE @sql NVARCHAR(MAX) = N'''';
        SELECT @sql = @sql + N''ALTER TABLE [dim].[Organisation] DROP CONSTRAINT [''
            + dc.name + N''];''
        FROM sys.default_constraints dc
        JOIN sys.columns c
            ON c.object_id = dc.parent_object_id
           AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N''[dim].[Organisation]'')
          AND c.name = N''type'';
        IF @sql <> N'''' EXEC sp_executesql @sql;
        ALTER TABLE [dim].[Organisation] DROP COLUMN [type];
    ');
END
GO

-- ---------------------------------------------------------------------------
-- Index renames (column rename already retargeted the key lists)
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'IX_dim_Organisation_Code'
   )
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'IX_dim_Organisation_kode'
   )
    EXEC sp_rename N'[dim].[Organisation].[IX_dim_Organisation_Code]',
        N'IX_dim_Organisation_kode', N'INDEX';
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'IX_dim_Organisation_code'
   )
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'IX_dim_Organisation_kode'
   )
    EXEC sp_rename N'[dim].[Organisation].[IX_dim_Organisation_code]',
        N'IX_dim_Organisation_kode', N'INDEX';
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'UQ_dim_Organisation_code_Live'
   )
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'UQ_dim_Organisation_kode_Live'
   )
    EXEC sp_rename N'[dim].[Organisation].[UQ_dim_Organisation_code_Live]',
        N'UQ_dim_Organisation_kode_Live', N'INDEX';
GO

-- ---------------------------------------------------------------------------
-- Hierarchy columns
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'hierarki_niveau') IS NULL
BEGIN
    ALTER TABLE [dim].[Organisation] ADD
        [hierarki_niveau] [int] NULL,
        [kode_niveau1] [varchar](50) NULL,
        [navn_niveau1] [nvarchar](500) NULL,
        [kode_niveau2] [varchar](50) NULL,
        [navn_niveau2] [nvarchar](500) NULL,
        [kode_niveau3] [varchar](50) NULL,
        [navn_niveau3] [nvarchar](500) NULL,
        [kode_niveau4] [varchar](50) NULL,
        [navn_niveau4] [nvarchar](500) NULL,
        [kode_niveau5] [varchar](50) NULL,
        [navn_niveau5] [nvarchar](500) NULL,
        [kode_niveau6] [varchar](50) NULL,
        [navn_niveau6] [nvarchar](500) NULL,
        [kode_niveau7] [varchar](50) NULL,
        [navn_niveau7] [nvarchar](500) NULL,
        [kode_niveau8] [varchar](50) NULL,
        [navn_niveau8] [nvarchar](500) NULL,
        [kode_niveau9] [varchar](50) NULL,
        [navn_niveau9] [nvarchar](500) NULL;
END
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'hierarki_niveau') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE parent_object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'CK_Dim_Organisation_hierarki_niveau'
   )
    ALTER TABLE [dim].[Organisation] ADD CONSTRAINT [CK_Dim_Organisation_hierarki_niveau]
        CHECK (
            [hierarki_niveau] IS NULL
            OR ([hierarki_niveau] >= 1 AND [hierarki_niveau] <= 9)
        );
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'kode_niveau1') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'IX_dim_Organisation_kode_niveau1'
   )
    CREATE INDEX IX_dim_Organisation_kode_niveau1
        ON [dim].[Organisation] ([kode_niveau1]);
GO
