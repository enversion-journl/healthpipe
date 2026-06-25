-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security Tables
-- BrugerRoller, PersonaleOrg, BrugerOrganisation
-- ============================================================================

-- sec.BrugerRoller - User role assignments
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[BrugerRoller]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[BrugerRoller] (
        [BrugerRollerId] INT IDENTITY(1,1) NOT NULL,
        [Database] NVARCHAR(128) NOT NULL,
        [Bruger] NVARCHAR(128) NOT NULL,
        [Ubegraenset] BIT NOT NULL DEFAULT 0,
        [RaekkeBegraenset] BIT NOT NULL DEFAULT 0,
        [CreatedDate] DATETIME2(7) DEFAULT GETDATE(),
        [ModifiedDate] DATETIME2(7) DEFAULT GETDATE(),
        CONSTRAINT [PK_BrugerRoller] PRIMARY KEY CLUSTERED ([BrugerRollerId] ASC),
        CONSTRAINT [UQ_BrugerRoller_DB_Bruger] UNIQUE ([Database], [Bruger])
    );
END
GO

-- Fritekst: dedicated, requestable access to PII-risk free-text columns
-- (e.g. invoice line descriptions surfaced in dim.Vare). Users must explicitly
-- be granted this flag (access request, possibly with 2-factor approval).
-- Idempotent: add column on existing installations.
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[sec].[BrugerRoller]') AND name = 'Fritekst'
)
BEGIN
    ALTER TABLE [sec].[BrugerRoller] ADD [Fritekst] BIT NOT NULL CONSTRAINT [DF_BrugerRoller_Fritekst] DEFAULT 0;
END
GO

-- sec.PersonaleOrg - User-to-SOR code affiliations (source of truth)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[PersonaleOrg]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[PersonaleOrg] (
        [PersonaleOrgId] INT IDENTITY(1,1) NOT NULL,
        [Bruger] NVARCHAR(128) NOT NULL,
        [CodeSOR] VARCHAR(50) NOT NULL,
        [GivAdgangTilUnderorganisationer] BIT NOT NULL DEFAULT 0,
        [Beskrivelse] NVARCHAR(500) NULL,
        [GyldigFra] DATE NULL,
        [GyldigTil] DATE NULL,
        [CreatedDate] DATETIME2(7) DEFAULT GETDATE(),
        [CreatedBy] NVARCHAR(128) DEFAULT SUSER_SNAME(),
        [ModifiedDate] DATETIME2(7) DEFAULT GETDATE(),
        [ModifiedBy] NVARCHAR(128) DEFAULT SUSER_SNAME(),
        CONSTRAINT [PK_PersonaleOrg] PRIMARY KEY CLUSTERED ([PersonaleOrgId] ASC),
        CONSTRAINT [UQ_PersonaleOrg_Bruger_CodeSOR] UNIQUE ([Bruger], [CodeSOR])
    );

    CREATE INDEX IX_PersonaleOrg_Bruger ON [sec].[PersonaleOrg]([Bruger]);
    CREATE INDEX IX_PersonaleOrg_CodeSOR ON [sec].[PersonaleOrg]([CodeSOR]);
END
GO

-- sec.BrugerOrganisation - Expanded user-to-DW_EK_Organisation (populated by procedure)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[BrugerOrganisation]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[BrugerOrganisation] (
        [BrugerOrganizationId] INT IDENTITY(1,1) NOT NULL,
        [Bruger] NVARCHAR(128) NOT NULL,
        [DW_EK_Organisation] INT NOT NULL,
        [PersonaleOrgId] INT NULL,
        [IsDirectAffiliation] BIT NOT NULL DEFAULT 0,
        [HierarchyLevel] INT NOT NULL DEFAULT 0,
        [CreatedDate] DATETIME2(7) DEFAULT GETDATE(),
        CONSTRAINT [PK_BrugerOrganisation] PRIMARY KEY CLUSTERED ([BrugerOrganizationId] ASC),
        CONSTRAINT [UQ_BrugerOrganisation_Bruger_Org] UNIQUE ([Bruger], [DW_EK_Organisation])
    );

    CREATE INDEX IX_BrugerOrganisation_Bruger ON [sec].[BrugerOrganisation]([Bruger]);
    CREATE INDEX IX_BrugerOrganisation_Org ON [sec].[BrugerOrganisation]([DW_EK_Organisation]);
END
GO
