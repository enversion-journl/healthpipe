-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: add DEFAULT constraint on ref.RekvirentUndtagelse.dw_timestamp_update.
-- Base DDL (tables/ref/01_tables.sql) already carries this default for fresh builds;
-- this migration back-fills the constraint on databases where the table was created by
-- 20260811_ref_rekvirentundtagelse.sql (dw_timestamp_update had no default).

IF OBJECT_ID(N'[ref].[RekvirentUndtagelse]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1
       FROM sys.default_constraints dc
       INNER JOIN sys.columns c ON c.default_object_id = dc.object_id
       WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
         AND c.name = N'dw_timestamp_update'
   )
BEGIN
    ALTER TABLE [ref].[RekvirentUndtagelse]
        ADD CONSTRAINT [DF_Ref_RekvirentUndtagelse_DW_Timestamp_Update]
        DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];
END
GO
