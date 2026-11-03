-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security Tables
-- RBOrganisation (AD-fed). sec.RolleDomaene lives in 04_dpa_tables.sql.
--
-- sec.RBOrganisation is loaded via SWAP (load_healthpipe_ad_to_mssql.py): _New is
-- filled and rotated metadata-only (TRUNCATE _Old; live SWITCH TO _Old;
-- _New SWITCH TO live). The three tables MUST be structurally identical —
-- SWITCH requires it. No dw_is_deleted: the content is fully overwritten.
--
-- Clustered on (bruger, DW_EK_RBOrganisation): the DPA lookup probes exactly
-- that pair, so the seek covers SOR_kode without key lookups.
-- bruger = bare sAMAccountName.
--
-- SOR_kode is VARCHAR(50) like dim.Organisation.kode — the expansion feeds
-- SORHierarchy.child_code (NVARCHAR(100)) into the column. The width must
-- match across all three tables and #SrcRBOrganisation in the loader.
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[RBOrganisation] (
        [bruger] NVARCHAR(50) NOT NULL,
        [DW_EK_RBOrganisation] INT NOT NULL,
        [SOR_kode] VARCHAR(50) NOT NULL,
        [dw_timestamp_update] DATETIME2(7) NOT NULL CONSTRAINT [DF_RBOrganisation_dw_timestamp_update] DEFAULT GETUTCDATE(),
        CONSTRAINT [PK_RBOrganisation] PRIMARY KEY CLUSTERED ([bruger] ASC, [DW_EK_RBOrganisation] ASC)
    );

    -- Reverse lookups only (admin/audit: "who can see org X?")
    CREATE INDEX IX_RBOrganisation_DW_EK ON [sec].[RBOrganisation]([DW_EK_RBOrganisation]);
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation_New]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[RBOrganisation_New] (
        [bruger] NVARCHAR(50) NOT NULL,
        [DW_EK_RBOrganisation] INT NOT NULL,
        [SOR_kode] VARCHAR(50) NOT NULL,
        [dw_timestamp_update] DATETIME2(7) NOT NULL CONSTRAINT [DF_RBOrganisation_New_dw_timestamp_update] DEFAULT GETUTCDATE(),
        CONSTRAINT [PK_RBOrganisation_New] PRIMARY KEY CLUSTERED ([bruger] ASC, [DW_EK_RBOrganisation] ASC)
    );

    CREATE INDEX IX_RBOrganisation_New_DW_EK ON [sec].[RBOrganisation_New]([DW_EK_RBOrganisation]);
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation_Old]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[RBOrganisation_Old] (
        [bruger] NVARCHAR(50) NOT NULL,
        [DW_EK_RBOrganisation] INT NOT NULL,
        [SOR_kode] VARCHAR(50) NOT NULL,
        [dw_timestamp_update] DATETIME2(7) NOT NULL CONSTRAINT [DF_RBOrganisation_Old_dw_timestamp_update] DEFAULT GETUTCDATE(),
        CONSTRAINT [PK_RBOrganisation_Old] PRIMARY KEY CLUSTERED ([bruger] ASC, [DW_EK_RBOrganisation] ASC)
    );

    CREATE INDEX IX_RBOrganisation_Old_DW_EK ON [sec].[RBOrganisation_Old]([DW_EK_RBOrganisation]);
END
GO

-- Old/new metrics per load — input for a later manual acceptance gate.
-- Written by load_healthpipe_ad_to_mssql.py, also when the size guard blocks.
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisationLoadLog]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[RBOrganisationLoadLog] (
        [LoadLogId] INT IDENTITY(1,1) NOT NULL,
        [LoadTidspunkt] DATETIME2(7) NOT NULL CONSTRAINT [DF_RBOrganisationLoadLog_LoadTidspunkt] DEFAULT GETDATE(),
        [RaekkerGammel] INT NOT NULL,
        [RaekkerNy] INT NOT NULL,
        [BrugereGammel] INT NOT NULL,
        [BrugereNy] INT NOT NULL,
        [SwapUdfoert] BIT NOT NULL,
        CONSTRAINT [PK_RBOrganisationLoadLog] PRIMARY KEY CLUSTERED ([LoadLogId])
    );
END
GO
