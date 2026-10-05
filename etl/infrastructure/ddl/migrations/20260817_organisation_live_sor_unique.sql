-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Organisation live-SOR uniqueness + dw_is_deleted plumbing.
--
-- fhir.Organization.dw_is_deleted becomes NOT NULL DEFAULT 0 (warehouse flag;
-- SoF resource-grain reconcile sets 1 for UUIDs gone from Aidbox).
-- Filtered UNIQUE indexes enforce one live row per SOR code on
-- fhir.Organization, ref.Organisation, and dim.Organisation.
-- dim.Organisation gains dw_is_deleted, copied from ref.
--
-- Grain of ref/dim stays organization_id / dw_ek_Organisation (UUID). Deleted
-- rows may share a SOR code; live rows may not.
--
-- Idempotent. Runs on staging and DSO. Live-duplicate checks THROW before
-- creating a unique index — clean the data first.
-- ============================================================================

-- fhir.Organization: backfill NULL -> 0, DEFAULT, NOT NULL
IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'dw_is_deleted') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Organization] SET [dw_is_deleted] = 0 WHERE [dw_is_deleted] IS NULL;');

    IF NOT EXISTS (
        SELECT 1 FROM sys.default_constraints dc
        JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
        WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]')
          AND c.name = N'dw_is_deleted'
    )
        EXEC(N'ALTER TABLE [fhir].[Organization]
               ADD CONSTRAINT [DF_FHIR_Organization_DwIsDeleted] DEFAULT 0 FOR [dw_is_deleted];');

    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
          AND name = N'dw_is_deleted' AND is_nullable = 1
    )
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_is_deleted] INT NOT NULL;');
END
GO

-- fhir.Organization: refuse live SOR duplicates, then unique index
IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'dw_is_deleted') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Organization]', N'identifier_value_sor') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
          AND name = N'UQ_FHIR_Organization_IdentifierValueSor_Live'
   )
BEGIN
    EXEC(N'
        IF EXISTS (
            SELECT 1
            FROM [fhir].[Organization]
            WHERE [dw_is_deleted] = 0 AND [identifier_value_sor] IS NOT NULL
            GROUP BY [identifier_value_sor]
            HAVING COUNT(*) > 1
        )
            THROW 50017, N''Cannot create UQ_FHIR_Organization_IdentifierValueSor_Live: live duplicate identifier_value_sor remain in fhir.Organization. Clean them first.'', 1;

        CREATE UNIQUE NONCLUSTERED INDEX [UQ_FHIR_Organization_IdentifierValueSor_Live]
            ON [fhir].[Organization] ([identifier_value_sor])
            WHERE [dw_is_deleted] = 0 AND [identifier_value_sor] IS NOT NULL;
    ');
END
GO

-- ref.Organisation: refuse live SOR duplicates, then unique index
IF OBJECT_ID(N'[ref].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[ref].[Organisation]', N'sor_code') IS NOT NULL
   AND COL_LENGTH(N'[ref].[Organisation]', N'dw_is_deleted') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
          AND name = N'UQ_Ref_Organisation_SorCode_Live'
   )
BEGIN
    EXEC(N'
        IF EXISTS (
            SELECT 1
            FROM [ref].[Organisation]
            WHERE [dw_is_deleted] = 0 AND [sor_code] IS NOT NULL
            GROUP BY [sor_code]
            HAVING COUNT(*) > 1
        )
            THROW 50017, N''Cannot create UQ_Ref_Organisation_SorCode_Live: live duplicate sor_code remain in ref.Organisation. Clean them first.'', 1;

        CREATE UNIQUE NONCLUSTERED INDEX [UQ_Ref_Organisation_SorCode_Live]
            ON [ref].[Organisation] ([sor_code])
            WHERE [dw_is_deleted] = 0 AND [sor_code] IS NOT NULL;
    ');
END
GO

-- dim.Organisation: add dw_is_deleted
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'dw_is_deleted') IS NULL
BEGIN
    EXEC(N'ALTER TABLE [dim].[Organisation]
           ADD [dw_is_deleted] BIT NOT NULL
           CONSTRAINT [DF_Dim_Organisation_DwIsDeleted] DEFAULT 0;');
END
GO

-- dim.Organisation: copy flag from ref (UUID grain; -1 sentinel stays 0)
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[ref].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'dw_is_deleted') IS NOT NULL
BEGIN
    EXEC(N'UPDATE d
           SET d.[dw_is_deleted] = r.[dw_is_deleted]
           FROM [dim].[Organisation] d
           INNER JOIN [ref].[Organisation] r
               ON r.[dw_ek_Organisation] = d.[dw_ek_Organisation]
           WHERE d.[dw_is_deleted] <> r.[dw_is_deleted];');
END
GO

-- dim.Organisation: refuse live code duplicates, then unique index
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'dw_is_deleted') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Organisation]', N'code') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
          AND name = N'UQ_dim_Organisation_code_Live'
   )
BEGIN
    EXEC(N'
        IF EXISTS (
            SELECT 1
            FROM [dim].[Organisation]
            WHERE [dw_is_deleted] = 0
            GROUP BY [code]
            HAVING COUNT(*) > 1
        )
            THROW 50017, N''Cannot create UQ_dim_Organisation_code_Live: live duplicate code remain in dim.Organisation. Clean them first.'', 1;

        CREATE UNIQUE INDEX [UQ_dim_Organisation_code_Live]
            ON [dim].[Organisation] ([code])
            WHERE [dw_is_deleted] = 0;
    ');
END
GO
