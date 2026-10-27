-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Organisation: Dimension Table
-- dim.Organisation - Analytics-ready organisation dimension (SOR only)
-- Self-referencing parent-child hierarchy from SOR registry, unfolded into
-- kode_niveau / navn_niveau columns (niveau1 = root).
--
-- The nominal column order is deliberate: keys
-- first (dw_ek_Organisation, dw_ek_Organisation_parent, fhir_Organization_id),
-- then business attributes, then the hierarchy unfolding, then the DW audit
-- columns.
--
-- This file contains ONLY the desired end-state schema (CREATE TABLE + indexes).
-- Schema migrations (column renames, type changes, etc.) belong in migrations/.
-- ============================================================================

-- Create dim.Organisation if it does not exist
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND type = 'U'
)
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

-- dw-key lookup from the flat fhir.* tables
-- (kept in sync with migrations/20261012_fhir_dw_keys_kolonner_og_indeks.sql)
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dim].[Organisation]') AND name = N'IX_dim_Organisation_FhirOrganizationId')
BEGIN
    CREATE INDEX IX_dim_Organisation_FhirOrganizationId
        ON [dim].[Organisation] ([fhir_Organization_id])
        INCLUDE ([dw_is_deleted]);
END
GO

-- Sentinel row for "Unknown" organisation (DW_EK = -1)
IF NOT EXISTS (SELECT 1 FROM [dim].[Organisation] WHERE dw_ek_Organisation = -1)
BEGIN
    EXEC('
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
