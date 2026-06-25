-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Shared ERP Migrations
--
-- Cross-cutting migrations that affect multiple table types (map, dim, fct).
-- All statements are idempotent. Safe to delete once applied to all environments.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Replace "__" with "_" in composite CleanCode keys (map.Vare, map.FakturaEgenskaber)
-- ---------------------------------------------------------------------------

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @vare_key_col SYSNAME = NULL;
    DECLARE @faktura_key_col SYSNAME = NULL;
    DECLARE @pk_name_vare SYSNAME = NULL;
    DECLARE @pk_name_faktura SYSNAME = NULL;
    DECLARE @sql NVARCHAR(MAX);

    IF OBJECT_ID(N'[map].[Vare]', N'U') IS NOT NULL
    BEGIN
        SET @vare_key_col = CASE
            WHEN COL_LENGTH(N'map.Vare', N'CleanCode_Vare') IS NOT NULL THEN N'CleanCode_Vare'
            WHEN COL_LENGTH(N'map.Vare', N'Vare_CleanCode') IS NOT NULL THEN N'Vare_CleanCode'
            ELSE NULL
        END;
    END;

    IF OBJECT_ID(N'[map].[FakturaEgenskaber]', N'U') IS NOT NULL
    BEGIN
        SET @faktura_key_col = CASE
            WHEN COL_LENGTH(N'map.FakturaEgenskaber', N'CleanCode_Faktura') IS NOT NULL THEN N'CleanCode_Faktura'
            WHEN COL_LENGTH(N'map.FakturaEgenskaber', N'CleanCode_Kreditor_Faktura') IS NOT NULL THEN N'CleanCode_Kreditor_Faktura'
            WHEN COL_LENGTH(N'map.FakturaEgenskaber', N'Faktura_CVR_CleanCode') IS NOT NULL THEN N'Faktura_CVR_CleanCode'
            WHEN COL_LENGTH(N'map.FakturaEgenskaber', N'CleanCode_Faktura_Kreditor') IS NOT NULL
                THEN N'CleanCode_Faktura_Kreditor'
            ELSE NULL
        END;
    END;

    IF @vare_key_col IS NOT NULL
    BEGIN
        SET @sql = N'
            DELETE dup
            FROM [map].[Vare] dup
            INNER JOIN [map].[Vare] canon
                ON canon.[' + @vare_key_col + N'] = REPLACE(dup.[' + @vare_key_col + N'], ''__'', ''_'')
               AND canon.[' + @vare_key_col + N'] <> dup.[' + @vare_key_col + N']
            WHERE dup.[' + @vare_key_col + N'] LIKE ''%[_][_]%'';
        ';
        EXEC sp_executesql @sql;
    END;

    IF @faktura_key_col IS NOT NULL
    BEGIN
        SET @sql = N'
            DELETE dup
            FROM [map].[FakturaEgenskaber] dup
            INNER JOIN [map].[FakturaEgenskaber] canon
                ON canon.[' + @faktura_key_col + N'] = REPLACE(dup.[' + @faktura_key_col + N'], ''__'', ''_'')
               AND canon.[' + @faktura_key_col + N'] <> dup.[' + @faktura_key_col + N']
            WHERE dup.[' + @faktura_key_col + N'] LIKE ''%[_][_]%'';
        ';
        EXEC sp_executesql @sql;
    END;

    SELECT @pk_name_vare = kc.name
    FROM sys.key_constraints kc
    WHERE kc.parent_object_id = OBJECT_ID(N'[map].[Vare]')
      AND kc.type = 'PK';

    SELECT @pk_name_faktura = kc.name
    FROM sys.key_constraints kc
    WHERE kc.parent_object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
      AND kc.type = 'PK';

    IF @pk_name_vare IS NOT NULL
    BEGIN
        EXEC(N'ALTER TABLE [map].[Vare] DROP CONSTRAINT [' + @pk_name_vare + N'];');
    END;

    IF @pk_name_faktura IS NOT NULL
    BEGIN
        EXEC(N'ALTER TABLE [map].[FakturaEgenskaber] DROP CONSTRAINT [' + @pk_name_faktura + N'];');
    END;

    IF @vare_key_col IS NOT NULL
    BEGIN
        SET @sql = N'
            UPDATE [map].[Vare]
            SET [' + @vare_key_col + N'] = REPLACE([' + @vare_key_col + N'], ''__'', ''_'')
            WHERE [' + @vare_key_col + N'] LIKE ''%__%'';
        ';
        EXEC sp_executesql @sql;

        IF @vare_key_col = N'Vare_CleanCode'
        BEGIN
            EXEC sp_rename '[map].[Vare].[Vare_CleanCode]', 'CleanCode_Vare', 'COLUMN';
            SET @vare_key_col = N'CleanCode_Vare';
        END;
    END;

    IF @faktura_key_col IS NOT NULL
    BEGIN
        SET @sql = N'
            UPDATE [map].[FakturaEgenskaber]
            SET [' + @faktura_key_col + N'] = REPLACE([' + @faktura_key_col + N'], ''__'', ''_'')
            WHERE [' + @faktura_key_col + N'] LIKE ''%__%'';
        ';
        EXEC sp_executesql @sql;

        IF @faktura_key_col = N'Faktura_CVR_CleanCode'
        BEGIN
            EXEC sp_rename '[map].[FakturaEgenskaber].[Faktura_CVR_CleanCode]', 'CleanCode_Faktura', 'COLUMN';
            SET @faktura_key_col = N'CleanCode_Faktura';
        END;
        ELSE IF @faktura_key_col = N'CleanCode_Faktura_Kreditor'
        BEGIN
            EXEC sp_rename '[map].[FakturaEgenskaber].[CleanCode_Faktura_Kreditor]', 'CleanCode_Faktura', 'COLUMN';
            SET @faktura_key_col = N'CleanCode_Faktura';
        END;
        ELSE IF @faktura_key_col = N'CleanCode_Kreditor_Faktura'
        BEGIN
            EXEC sp_rename '[map].[FakturaEgenskaber].[CleanCode_Kreditor_Faktura]', 'CleanCode_Faktura', 'COLUMN';
            SET @faktura_key_col = N'CleanCode_Faktura';
        END;
    END;

    IF OBJECT_ID(N'[map].[Vare]', N'U') IS NOT NULL
       AND @vare_key_col IS NOT NULL
       AND NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'[map].[Vare]')
          AND type = 'PK'
    )
    BEGIN
        SET @sql = N'
            ALTER TABLE [map].[Vare]
            ADD CONSTRAINT [PK_Map_Vare]
                PRIMARY KEY CLUSTERED ([' + @vare_key_col + N'] ASC);
        ';
        EXEC sp_executesql @sql;
    END;

    IF OBJECT_ID(N'[map].[FakturaEgenskaber]', N'U') IS NOT NULL
       AND @faktura_key_col IS NOT NULL
       AND NOT EXISTS (
        SELECT 1
        FROM sys.key_constraints
        WHERE parent_object_id = OBJECT_ID(N'[map].[FakturaEgenskaber]')
          AND type = 'PK'
    )
    BEGIN
        SET @sql = N'
            ALTER TABLE [map].[FakturaEgenskaber]
            ADD CONSTRAINT [PK_Map_FakturaEgenskaber]
                PRIMARY KEY CLUSTERED ([' + @faktura_key_col + N'] ASC);
        ';
        EXEC sp_executesql @sql;
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
GO
