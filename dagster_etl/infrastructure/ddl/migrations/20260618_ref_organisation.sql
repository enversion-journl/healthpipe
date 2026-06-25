-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ref.Organisation — create + seed from map.Organisation (preserve DW_EK).
--
-- Part of US #28193 (fhir.Organization as the basis for dim.Organisation).
-- ref.Organisation REPLACES map.Organisation as the source of the INT
-- surrogate DW_EK_Organisation. To keep every existing fact / dim FK valid
-- (sec.BrugerOrganisation, dim.Lager, dim.Rekvirent, fct.*), existing keys are
-- migrated 1:1 via IDENTITY_INSERT, matched to the fhir.Organization resource
-- id through the shared SOR code (map.Code == fhir.identifier_value_sor).
--
-- ORDERING: this migration is self-contained — it creates ref.Organisation if
-- the infra DDL has not run yet, then seeds it. The canonical end-state DDL
-- lives in tables/ref/01_tables.sql.
--
-- Safe to re-run: the seed only fires when ref.Organisation is empty.
-- ============================================================================

-- ============================================================================
-- STEP 1: Create ref.Organisation if it does not exist (mirror canonical DDL)
-- ============================================================================
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE TABLE [ref].[Organisation] (
        [DW_EK_Organisation] INT IDENTITY(1,1) NOT NULL,
        [organization_id] NVARCHAR(255) NOT NULL,
        [sor_code] VARCHAR(50) NULL,
        [DW_IsDeleted] BIT NOT NULL CONSTRAINT [DF_Ref_Organisation_DW_IsDeleted] DEFAULT 0,
        [DW_Timestamp_Insert] DATETIME2(0) NULL DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [DW_Timestamp_Update] DATETIME2(0) NULL,
        CONSTRAINT [PK_Ref_Organisation] PRIMARY KEY CLUSTERED ([DW_EK_Organisation], [organization_id]),
        CONSTRAINT [UQ_Ref_Organisation_OrganizationId] UNIQUE ([organization_id]),
        CONSTRAINT [CK_Ref_Organisation_HasIdentifier] CHECK (
            [DW_IsDeleted] = 1
            OR [sor_code] IS NOT NULL
        )
    ) ON [PRIMARY];
    PRINT 'Created ref.Organisation';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND name = 'IX_Ref_Organisation_SorCode')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Organisation]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_Organisation_SorCode]
    ON [ref].[Organisation] ([sor_code] ASC) ON [PRIMARY];
END
GO

-- ============================================================================
-- STEP 2: Seed DW_EK from map.Organisation (only when ref is empty)
--
-- Match map row -> fhir.Organization on the SOR code (map.Code is the SORId,
-- fhir.identifier_value_sor is the same SORId). ROW_NUMBER guards against a
-- (theoretical) duplicate sor_code in fhir.Organization so a single map DW_EK
-- never seeds two ref rows. Map rows with no matching fhir.Organization are
-- intentionally NOT seeded — their old dim.Organisation rows survive the
-- Type-1 MERGE (no delete), so historical fact FKs still resolve.
-- ============================================================================
IF OBJECT_ID(N'[ref].[Organisation]') IS NOT NULL
   AND OBJECT_ID(N'[map].[Organisation]') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Organization]') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [ref].[Organisation])
BEGIN
    SET IDENTITY_INSERT [ref].[Organisation] ON;

    ;WITH src AS (
        SELECT
            m.DW_EK_Organisation,
            f.organization_id,
            f.identifier_value_sor AS sor_code,
            ROW_NUMBER() OVER (
                PARTITION BY m.DW_EK_Organisation
                ORDER BY f.organization_id
            ) AS rn
        FROM [map].[Organisation] m
        JOIN [fhir].[Organization] f
            ON f.identifier_value_sor = m.Code
        WHERE m.DW_EK_Organisation > 0
    )
    INSERT INTO [ref].[Organisation]
        (DW_EK_Organisation, organization_id, sor_code, DW_IsDeleted, DW_Timestamp_Insert)
    SELECT
        src.DW_EK_Organisation,
        src.organization_id,
        src.sor_code,
        0,
        CAST(SYSDATETIME() AS datetime2(0))
    FROM src
    WHERE src.rn = 1;

    DECLARE @seeded INT = @@ROWCOUNT;

    SET IDENTITY_INSERT [ref].[Organisation] OFF;

    -- Reseed IDENTITY so new orgs get DW_EK above the migrated maximum.
    IF EXISTS (SELECT 1 FROM [ref].[Organisation])
        DBCC CHECKIDENT ('[ref].[Organisation]', RESEED);

    PRINT CONCAT('Seeded ref.Organisation from map.Organisation: ', @seeded, ' rows');
END
ELSE
BEGIN
    PRINT 'Skipped ref.Organisation seed (already populated or source tables missing)';
END
GO

-- ============================================================================
-- ROLLBACK (manual — run only to undo this migration)
-- ============================================================================
-- ref.Organisation is additive; map.Organisation is left intact by this
-- migration, so a rollback simply drops the new table. Downstream code that
-- has been switched to ref.Organisation must be reverted first.
--
--   IF OBJECT_ID(N'[ref].[Organisation]') IS NOT NULL
--       DROP TABLE [ref].[Organisation];
-- ============================================================================
