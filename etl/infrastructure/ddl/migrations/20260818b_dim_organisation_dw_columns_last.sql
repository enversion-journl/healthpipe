-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Organisation: last three columns are the DW audit columns, in order
--   dw_timestamp_insert, dw_timestamp_update, dw_is_deleted.
--
-- 20260818 appended the hierarchy columns after dw_is_deleted, so existing
-- tables end with kode_niveau / navn_niveau. SQL Server cannot reorder columns
-- in place. dim.Organisation has no FKs and is Type 1 from ref — drop and
-- recreate (same DDL as tables/organisation/02_dims.sql). dim_Organisation
-- (then dso_sync_dim_organisation) refills the rows.
--
-- Drops vw_dim.Organisation so it is recreated from base view DDL.
-- Idempotent: skips the drop when the last three columns are already correct.
-- Runs on staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[vw_dim].[Organisation]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Organisation];
GO

IF OBJECT_ID(N'[vw_dim].[Organisation_udpakket]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Organisation_udpakket];
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND NOT (
        (SELECT name FROM (
            SELECT name, ROW_NUMBER() OVER (ORDER BY column_id DESC) AS rn
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
        ) x WHERE rn = 1) = N'dw_is_deleted'
        AND (SELECT name FROM (
            SELECT name, ROW_NUMBER() OVER (ORDER BY column_id DESC) AS rn
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
        ) x WHERE rn = 2) = N'dw_timestamp_update'
        AND (SELECT name FROM (
            SELECT name, ROW_NUMBER() OVER (ORDER BY column_id DESC) AS rn
            FROM sys.columns
            WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
        ) x WHERE rn = 3) = N'dw_timestamp_insert'
   )
    DROP TABLE [dim].[Organisation];
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NULL
BEGIN
    CREATE TABLE [dim].[Organisation] (
        [dw_ek_Organisation] [int] PRIMARY KEY,
        [kode] [varchar](50) NOT NULL,
        [dw_ek_Organisation_parent] [int],
        [parent_kode] [varchar](50),
        [navn] [nvarchar](500),
        [aktiv] [bit] DEFAULT 1,
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
        [navn_niveau9] [nvarchar](500) NULL,
        [dw_timestamp_insert] [datetime2](7) NOT NULL DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL DEFAULT GETDATE(),
        [dw_is_deleted] [bit] NOT NULL CONSTRAINT [DF_Dim_Organisation_DwIsDeleted] DEFAULT 0,
        CONSTRAINT [CK_Dim_Organisation_hierarki_niveau] CHECK (
            [hierarki_niveau] IS NULL
            OR ([hierarki_niveau] >= 1 AND [hierarki_niveau] <= 9)
        )
    ) ON [PRIMARY];

    CREATE INDEX IX_dim_Organisation_kode
        ON [dim].[Organisation] ([kode]);

    CREATE UNIQUE INDEX UQ_dim_Organisation_kode_Live
        ON [dim].[Organisation] ([kode])
        WHERE [dw_is_deleted] = 0;

    CREATE INDEX IX_dim_Organisation_Parent
        ON [dim].[Organisation] ([dw_ek_Organisation_parent])
        WHERE [dw_ek_Organisation_parent] IS NOT NULL;

    CREATE INDEX IX_dim_Organisation_kode_niveau1
        ON [dim].[Organisation] ([kode_niveau1]);
END
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] WHERE dw_ek_Organisation = -1)
BEGIN
    EXEC(N'
        INSERT INTO [dim].[Organisation] (
            dw_ek_Organisation, kode,
            dw_ek_Organisation_parent, parent_kode,
            navn, aktiv,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (
            -1, ''UNKNOWN'',
            NULL, NULL,
            ''Unknown Organisation'', 1,
            GETDATE(), GETDATE()
        )
    ');
END
GO
