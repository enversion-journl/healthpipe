-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: give arc.ils_rekvundtagne named DEFAULT constraints on its audit
-- timestamps, matching ref.RekvirentUndtagelse. Base DDL (tables/erp/00_archive.sql)
-- carries these for fresh builds; this migration back-fills databases where the
-- table was created by 20260811_arc_ils_rekvundtagne.sql (anonymous default on
-- dw_timestamp_insert, no default on dw_timestamp_update).

-- 1) dw_timestamp_insert: replace the anonymous default with a named one.
IF OBJECT_ID(N'[arc].[ils_rekvundtagne]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.default_constraints
       WHERE parent_object_id = OBJECT_ID(N'[arc].[ils_rekvundtagne]')
         AND name = N'DF_arc_ils_rekvundtagne_DW_Timestamp_Insert'
   )
BEGIN
    DECLARE @insert_df sysname;
    SELECT @insert_df = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekvundtagne]')
      AND c.name = N'dw_timestamp_insert';

    IF @insert_df IS NOT NULL
        EXEC(N'ALTER TABLE [arc].[ils_rekvundtagne] DROP CONSTRAINT [' + @insert_df + N'];');

    ALTER TABLE [arc].[ils_rekvundtagne]
        ADD CONSTRAINT [DF_arc_ils_rekvundtagne_DW_Timestamp_Insert]
        DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];
END
GO

-- 2) dw_timestamp_update: add a named default (none existed before).
IF OBJECT_ID(N'[arc].[ils_rekvundtagne]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.default_constraints dc
       INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
       WHERE dc.parent_object_id = OBJECT_ID(N'[arc].[ils_rekvundtagne]')
         AND c.name = N'dw_timestamp_update'
   )
BEGIN
    ALTER TABLE [arc].[ils_rekvundtagne]
        ADD CONSTRAINT [DF_arc_ils_rekvundtagne_DW_Timestamp_Update]
        DEFAULT SYSDATETIME() FOR [dw_timestamp_update];
END
GO
