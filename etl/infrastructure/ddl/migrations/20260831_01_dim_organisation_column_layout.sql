-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Organisation: new column names and nominal order.
--   * parent_kode            -> kode_parent
--   * fhir_Organization_id   new — ref.Organisation.organization_id, i.e. the
--                            fhir.Organization resource id behind the DW_EK
--   * order: keys (dw_ek_Organisation, dw_ek_Organisation_parent,
--     fhir_Organization_id) -> attributes -> hierarchy unfolding -> DW audit
--     columns (dw_is_deleted, dw_timestamp_insert, dw_timestamp_update)
--
-- SQL Server cannot reorder columns in place and the nominal order is part of
-- the change, so the table is dropped and recreated (same DDL as
-- tables/organisation/02_dims.sql). dim.Organisation has no FKs and is Type 1
-- from ref.Organisation + fhir.Organization: dim_Organisation refills every
-- row, dso_sync_dim_organisation carries them on to DSO.
--
-- That DSO sync is watermark-based, so its checkpoint row in
-- meta.SyncWatermarks is deleted here: the next run must be a full sync into
-- the emptied DSO table, not a delta from the pre-migration timestamp.
-- SyncWatermarks only exists on DSO — the delete is guarded, not skipped, so
-- the same file is correct on both databases.
--
-- Drops vw_dim.Organisation so views/security/01_vw_dim_security.sql recreates
-- it over the new column list.
--
-- Idempotent: the drop only fires when the column layout differs from the
-- end state, and every other statement is existence-guarded.
-- Runs on staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[vw_dim].[Organisation]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[Organisation];
GO

-- Compare the deployed column list (names in nominal order) with the end state.
-- Anything else — old name, missing fhir_Organization_id, or right names in the
-- wrong order — means the table has to be rebuilt.
DECLARE @expected NVARCHAR(MAX) =
      N'dw_ek_Organisation,dw_ek_Organisation_parent,fhir_Organization_id,'
    + N'kode,kode_parent,navn,aktiv,hierarki_niveau,'
    + N'kode_niveau1,navn_niveau1,kode_niveau2,navn_niveau2,'
    + N'kode_niveau3,navn_niveau3,kode_niveau4,navn_niveau4,'
    + N'kode_niveau5,navn_niveau5,kode_niveau6,navn_niveau6,'
    + N'kode_niveau7,navn_niveau7,kode_niveau8,navn_niveau8,'
    + N'kode_niveau9,navn_niveau9,'
    + N'dw_is_deleted,dw_timestamp_insert,dw_timestamp_update';

DECLARE @actual NVARCHAR(MAX) = (
    SELECT STRING_AGG(CONVERT(NVARCHAR(MAX), c.name), N',')
               WITHIN GROUP (ORDER BY c.column_id)
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'[dim].[Organisation]')
);

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND @actual <> @expected
BEGIN
    PRINT 'dim.Organisation column layout differs — rebuilding.';
    DROP TABLE [dim].[Organisation];
END
GO

IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NULL
BEGIN
    CREATE TABLE [dim].[Organisation] (
        [dw_ek_Organisation] [int] PRIMARY KEY,
        [dw_ek_Organisation_parent] [int],
        [fhir_Organization_id] [nvarchar](255) NULL,
        [kode] [varchar](50) NOT NULL,
        [kode_parent] [varchar](50),
        [navn] [nvarchar](500),
        [aktiv] [bit] CONSTRAINT [DF_Organisation_aktiv] DEFAULT 1,
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
        [dw_is_deleted] [bit] NOT NULL CONSTRAINT [DF_Organisation_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] [datetime2](7) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update] [datetime2](7) NOT NULL CONSTRAINT [DF_Organisation_dw_timestamp_update] DEFAULT GETDATE(),
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

-- Sentinel row for "Unknown" organisation (DW_EK = -1), as in the base DDL.
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [dim].[Organisation] WHERE dw_ek_Organisation = -1)
BEGIN
    EXEC(N'
        INSERT INTO [dim].[Organisation] (
            dw_ek_Organisation, kode,
            dw_ek_Organisation_parent, kode_parent,
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

-- DSO only: forget the delta checkpoint so the next dso_sync_dim_organisation
-- refills the emptied table in full instead of syncing rows changed since the
-- pre-migration timestamp.
IF OBJECT_ID(N'[meta].[SyncWatermarks]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[SyncWatermarks]
    WHERE schema_name = N'dim' AND table_name = N'Organisation';

    PRINT 'meta.SyncWatermarks: dim.Organisation checkpoint cleared ('
        + CAST(@@ROWCOUNT AS NVARCHAR(10)) + ' row(s)).';
END
GO
