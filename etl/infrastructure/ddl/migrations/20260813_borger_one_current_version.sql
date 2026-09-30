-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- At most one current version per borger (ref + dim)
--
-- WHY: dw_is_current is the Type 2 "this is the live row" flag. Two live rows
-- for the same patient_id / dw_ek_Borger make fact joins and "aktuel adresse"
-- denormalisation ambiguous. A UNIQUE CONSTRAINT cannot take a WHERE filter;
-- a filtered unique index is the SQL Server equivalent.
--
-- Soft-deleted rows must not occupy that slot: CHECK
-- (dw_is_deleted = 0 OR dw_is_current = 0). The MERGE NOT MATCHED BY SOURCE
-- branches already set both flags; this migration backfills any rows that
-- were left current=1 after a delete, then installs the constraints.
--
-- RUNS ON BOTH DATABASES (staging_migrations and dso_migrations share this
-- folder). Base DDL in tables/ref/01_tables.sql and
-- tables/shared_dimensions/02_dims.sql is the fresh-database source of truth.
--
-- If more than one live current row still exists after the backfill, skip
-- the unique index (do not THROW — that would block the rest of cutover).
-- Rebuild the Borger chain and re-run staging_infra_ref /
-- staging_infra_shared_dimensions; the base DDL creates the index once
-- duplicates are gone.
-- =============================================================================

-- ---------- ref.Patient_Address_To_Borger ----------
IF OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE [ref].[Patient_Address_To_Borger]
        SET [dw_is_current] = 0,
            [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        WHERE [dw_is_deleted] = 1
          AND [dw_is_current] = 1
    ');
END
GO

IF OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
BEGIN
    DECLARE @ref_dupes INT = (
        SELECT COUNT(*)
        FROM (
            SELECT [patient_id]
            FROM [ref].[Patient_Address_To_Borger]
            WHERE [dw_is_current] = 1
            GROUP BY [patient_id]
            HAVING COUNT(*) > 1
        ) d
    );
    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = N'CK_Ref_Patient_Address_Borger_CurrentNotDeleted'
          AND parent_object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
    )
    BEGIN
        ALTER TABLE [ref].[Patient_Address_To_Borger] WITH CHECK
        ADD CONSTRAINT [CK_Ref_Patient_Address_Borger_CurrentNotDeleted]
        CHECK ([dw_is_deleted] = 0 OR [dw_is_current] = 0);
    END

    IF @ref_dupes > 0
    BEGIN
        DECLARE @ref_msg NVARCHAR(400) = CONCAT(
            N'Skipping UX_Ref_Patient_Address_Borger_CurrentPatient: ',
            @ref_dupes,
            N' patient_id value(s) have more than one dw_is_current = 1 row. ',
            N'Rebuild the Borger chain then re-run staging_infra_ref.'
        );
        RAISERROR(@ref_msg, 10, 1) WITH NOWAIT;
    END
    ELSE IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
          AND name = N'UX_Ref_Patient_Address_Borger_CurrentPatient'
    )
    BEGIN
        CREATE UNIQUE NONCLUSTERED INDEX [UX_Ref_Patient_Address_Borger_CurrentPatient]
        ON [ref].[Patient_Address_To_Borger] ([patient_id] ASC)
        WHERE [dw_is_current] = 1
        ON [PRIMARY];
    END
END
GO

-- ---------- dim.Borger ----------
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE [dim].[Borger]
        SET [dw_is_current] = 0,
            [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        WHERE [dw_is_deleted] = 1
          AND [dw_is_current] = 1
    ');
END
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    DECLARE @dim_dupes INT = (
        SELECT COUNT(*)
        FROM (
            SELECT [dw_ek_Borger]
            FROM [dim].[Borger]
            WHERE [dw_is_current] = 1
            GROUP BY [dw_ek_Borger]
            HAVING COUNT(*) > 1
        ) d
    );
    IF NOT EXISTS (
        SELECT 1 FROM sys.check_constraints
        WHERE name = N'CK_Dim_Borger_CurrentNotDeleted'
          AND parent_object_id = OBJECT_ID(N'[dim].[Borger]')
    )
    BEGIN
        ALTER TABLE [dim].[Borger] WITH CHECK
        ADD CONSTRAINT [CK_Dim_Borger_CurrentNotDeleted]
        CHECK ([dw_is_deleted] = 0 OR [dw_is_current] = 0);
    END

    IF @dim_dupes > 0
    BEGIN
        DECLARE @dim_msg NVARCHAR(400) = CONCAT(
            N'Skipping UX_Dim_Borger_CurrentEntity: ',
            @dim_dupes,
            N' dw_ek_Borger value(s) have more than one dw_is_current = 1 row. ',
            N'Rebuild the Borger chain then re-run staging_infra_shared_dimensions.'
        );
        RAISERROR(@dim_msg, 10, 1) WITH NOWAIT;
    END
    ELSE IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
          AND name = N'UX_Dim_Borger_CurrentEntity'
    )
    BEGIN
        CREATE UNIQUE NONCLUSTERED INDEX [UX_Dim_Borger_CurrentEntity]
        ON [dim].[Borger] ([dw_ek_Borger] ASC)
        WHERE [dw_is_current] = 1
        ON [PRIMARY];
    END
END
GO
