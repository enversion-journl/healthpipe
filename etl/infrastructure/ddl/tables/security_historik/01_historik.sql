-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- sec.RolleDomaeneHistorik and sec.SORTildelingHistorik — the history of the
-- users' security domain roles and their direct SOR grants from AD, so a
-- lookup resolves the roles and the SOR unit a user had at a given time.
--
-- Loaded daily by load_healthpipe_ad_to_mssql.py from the same AD extract as
-- sec.RolleDomaene and sec.RBOrganisation, which stay full refreshes. A
-- changed value closes the open row and inserts a new one; a key that leaves
-- the extract gets its open row closed. When an AD member lookup failed in the
-- extract, no row closes and only new keys get a row, because a partial
-- extract can also change a value (a role from K to U).
--
-- sec.SORTildelingHistorik holds the grant as AD states it, before the
-- codesystem.SORHierarchy expansion that sec.RBOrganisation applies. It has
-- no value columns, so a grant row only opens and closes.
--
-- Half-open intervals [dw_valid_from, dw_valid_to); the open row has
-- dw_valid_to = '9999-12-31 23:59:59'. dw_run_id_from/dw_run_id_to hold the
-- run that opened/closed the row (the Dagster run id, or cli-<uuid> for a
-- CLI load). The tables live on staging only: staging_infra_security runs
-- this folder, dso_infra_security does not, and nothing syncs them to DSO.
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[RolleDomaeneHistorik]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[RolleDomaeneHistorik] (
        [bruger] NVARCHAR(50) NOT NULL,
        [Sikkerhedsdomaene] NVARCHAR(50) NOT NULL,
        [Rolle] CHAR(1) NOT NULL,
        [Populationsansvar] NVARCHAR(50) NOT NULL,
        [dw_valid_from] DATETIME2(0) NOT NULL,
        [dw_valid_to] DATETIME2(0) NOT NULL CONSTRAINT [DF_RolleDomaeneHistorik_dw_valid_to] DEFAULT '9999-12-31 23:59:59',
        [dw_run_id_from] NVARCHAR(64) NOT NULL,
        [dw_run_id_to] NVARCHAR(64) NULL,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_RolleDomaeneHistorik_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_RolleDomaeneHistorik_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_RolleDomaeneHistorik] PRIMARY KEY CLUSTERED ([bruger] ASC, [Sikkerhedsdomaene] ASC, [dw_valid_from] ASC)
    );

    CREATE UNIQUE INDEX [UX_RolleDomaeneHistorik_aaben]
        ON [sec].[RolleDomaeneHistorik]([bruger], [Sikkerhedsdomaene])
        WHERE [dw_valid_to] = '9999-12-31 23:59:59';
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[sec].[SORTildelingHistorik]') AND type = 'U')
BEGIN
    CREATE TABLE [sec].[SORTildelingHistorik] (
        [bruger] NVARCHAR(50) NOT NULL,
        [SOR_kode] VARCHAR(50) NOT NULL,
        [dw_valid_from] DATETIME2(0) NOT NULL,
        [dw_valid_to] DATETIME2(0) NOT NULL CONSTRAINT [DF_SORTildelingHistorik_dw_valid_to] DEFAULT '9999-12-31 23:59:59',
        [dw_run_id_from] NVARCHAR(64) NOT NULL,
        [dw_run_id_to] NVARCHAR(64) NULL,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL CONSTRAINT [DF_SORTildelingHistorik_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2(0) NOT NULL CONSTRAINT [DF_SORTildelingHistorik_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_SORTildelingHistorik] PRIMARY KEY CLUSTERED ([bruger] ASC, [SOR_kode] ASC, [dw_valid_from] ASC)
    );

    CREATE UNIQUE INDEX [UX_SORTildelingHistorik_aaben]
        ON [sec].[SORTildelingHistorik]([bruger], [SOR_kode])
        WHERE [dw_valid_to] = '9999-12-31 23:59:59';
END
GO
