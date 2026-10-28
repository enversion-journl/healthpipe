-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: rekey arc.ifdb_rehabilitation_plan_receiver on (OBJECT_ID, modtager_noegle).
--
-- The old key (OBJECT_ID, RECEIVER_YDER_NUMMER, RECEIVER_AFDELING_KODE, TRANSMISSION_TIME)
-- could not tell apart receivers that only carry a SOR code, so the extract collapsed them
-- and dropped real receivers. modtager_noegle = COALESCE(RECEIVER_YDER_NUMMER,
-- RECEIVER_AFDELING_KODE, RECEIVER_AFDELING_SORKODE). The extract no longer writes the
-- "__NULL__" token, so existing rows get NULL back in the two identity columns.
--
-- The receivers the old extract dropped arrive with the next full-load extract + archive
-- run. A soft-deleted row that collides with another row on the new key is superseded and
-- is deleted. A guard batch raises before the old PK is dropped if any row would get a NULL
-- or over-long modtager_noegle, or if two live rows share a new key; the table then keeps its
-- old PK.
-- The table exists only in staging; on DSO every block is a no-op.

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND COL_LENGTH(N'arc.ifdb_rehabilitation_plan_receiver', N'modtager_noegle') IS NULL
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver] ADD [modtager_noegle] NVARCHAR(255) NULL;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.key_constraints kc
       JOIN sys.index_columns ic
         ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
       JOIN sys.columns c
         ON c.object_id = ic.object_id AND c.column_id = ic.column_id
       WHERE kc.name = N'PK_arc_rehabilitation_plan_receiver_ifdb'
         AND kc.parent_object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
         AND c.name = N'modtager_noegle'
   )
BEGIN
    DECLARE @null_modtager INT, @too_long_modtager INT, @colliding_keys INT;
    EXEC sp_executesql
        N'WITH k AS (
              SELECT [OBJECT_ID], [dw_is_deleted],
                     COALESCE(
                         NULLIF([RECEIVER_YDER_NUMMER], N''__NULL__''),
                         NULLIF([RECEIVER_AFDELING_KODE], N''__NULL__''),
                         [RECEIVER_AFDELING_SORKODE]
                     ) AS modtager
              FROM [arc].[ifdb_rehabilitation_plan_receiver]
          )
          SELECT @n = ISNULL(SUM(CASE WHEN modtager IS NULL THEN 1 ELSE 0 END), 0),
                 @l = ISNULL(SUM(CASE WHEN LEN(modtager) > 255 THEN 1 ELSE 0 END), 0),
                 @c = (SELECT COUNT(*) FROM (
                          SELECT 1 AS x FROM k
                          WHERE modtager IS NOT NULL AND [dw_is_deleted] = 0
                          GROUP BY [OBJECT_ID], modtager
                          HAVING COUNT(*) >= 2
                      ) AS g)
          FROM k;',
        N'@n INT OUTPUT, @l INT OUTPUT, @c INT OUTPUT',
        @n = @null_modtager OUTPUT, @l = @too_long_modtager OUTPUT, @c = @colliding_keys OUTPUT;
    IF @null_modtager > 0 OR @too_long_modtager > 0 OR @colliding_keys > 0
    BEGIN
        DECLARE @msg NVARCHAR(400) = CONCAT(
            'arc.ifdb_rehabilitation_plan_receiver cannot be rekeyed on (OBJECT_ID, modtager_noegle): ',
            @null_modtager, ' rows without modtager_noegle, ',
            @too_long_modtager, ' rows with modtager_noegle over 255 characters, ',
            @colliding_keys, ' keys shared by two or more live rows.'
        );
        THROW 50000, @msg, 1;
    END
END
GO

IF EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE name = N'PK_arc_rehabilitation_plan_receiver_ifdb'
      AND parent_object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
)
AND NOT EXISTS (
    SELECT 1
    FROM sys.key_constraints kc
    JOIN sys.index_columns ic
      ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
    JOIN sys.columns c
      ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE kc.name = N'PK_arc_rehabilitation_plan_receiver_ifdb'
      AND kc.parent_object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
      AND c.name = N'modtager_noegle'
)
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver] DROP CONSTRAINT [PK_arc_rehabilitation_plan_receiver_ifdb];');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
         AND name = N'RECEIVER_YDER_NUMMER' AND is_nullable = 0
   )
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver] ALTER COLUMN [RECEIVER_YDER_NUMMER] NVARCHAR(50) NULL;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
         AND name = N'RECEIVER_AFDELING_KODE' AND is_nullable = 0
   )
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver] ALTER COLUMN [RECEIVER_AFDELING_KODE] NVARCHAR(512) NULL;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
    EXEC(N'UPDATE [arc].[ifdb_rehabilitation_plan_receiver]
           SET [RECEIVER_YDER_NUMMER] = NULL
           WHERE [RECEIVER_YDER_NUMMER] = N''__NULL__'';');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
    EXEC(N'UPDATE [arc].[ifdb_rehabilitation_plan_receiver]
           SET [RECEIVER_AFDELING_KODE] = NULL
           WHERE [RECEIVER_AFDELING_KODE] = N''__NULL__'';');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND COL_LENGTH(N'arc.ifdb_rehabilitation_plan_receiver', N'modtager_noegle') IS NOT NULL
    EXEC(N'UPDATE [arc].[ifdb_rehabilitation_plan_receiver]
           SET [modtager_noegle] = COALESCE([RECEIVER_YDER_NUMMER], [RECEIVER_AFDELING_KODE], [RECEIVER_AFDELING_SORKODE])
           WHERE [modtager_noegle] IS NULL;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND COL_LENGTH(N'arc.ifdb_rehabilitation_plan_receiver', N'modtager_noegle') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.key_constraints
       WHERE name = N'PK_arc_rehabilitation_plan_receiver_ifdb'
         AND parent_object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
   )
    EXEC(N'WITH ranked AS (
               SELECT [dw_is_deleted],
                      ROW_NUMBER() OVER (
                          PARTITION BY [OBJECT_ID], [modtager_noegle]
                          ORDER BY [dw_is_deleted], [dw_timestamp_update] DESC
                      ) AS rn
               FROM [arc].[ifdb_rehabilitation_plan_receiver]
               WHERE [modtager_noegle] IS NOT NULL
           )
           DELETE FROM ranked WHERE rn > 1 AND [dw_is_deleted] = 1;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.columns
       WHERE object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
         AND name = N'modtager_noegle' AND is_nullable = 1
   )
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver] ALTER COLUMN [modtager_noegle] NVARCHAR(255) NOT NULL;');
GO

IF OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.key_constraints
       WHERE name = N'PK_arc_rehabilitation_plan_receiver_ifdb'
         AND parent_object_id = OBJECT_ID(N'[arc].[ifdb_rehabilitation_plan_receiver]')
   )
    EXEC(N'ALTER TABLE [arc].[ifdb_rehabilitation_plan_receiver]
           ADD CONSTRAINT [PK_arc_rehabilitation_plan_receiver_ifdb]
           PRIMARY KEY CLUSTERED ([OBJECT_ID], [modtager_noegle]);');
GO
