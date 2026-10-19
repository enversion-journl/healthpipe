-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: add the source connection of an extract run to meta.DFMRunLog
--
-- source_user  — the user or service account the extraction authenticated with
-- source_host  — the host of the source connection
-- source_table — schema.table for a database, the stable dataset name for a file,
--                the URL path for HTTP, the base DN for LDAP
--
-- Every extraction and every flow start that reads its source fill the columns. A value is NULL when the source has
-- none (anonymous HTTP, a folder that is not on a CIFS mount). Other DFM runs leave all
-- three NULL.
-- Runs on BOTH staging and DSO via the centralized migrations runner
-- (staging_migrations / dso_migrations), before the base DDL. Idempotent: a
-- no-op when the table does not exist yet (the base DDL then creates it with
-- the columns) or already has a column.

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[DFMRunLog]') AND type = 'U')
   AND COL_LENGTH(N'meta.DFMRunLog', N'source_user') IS NULL
BEGIN
    ALTER TABLE [meta].[DFMRunLog] ADD [source_user] NVARCHAR(128) NULL;
    PRINT 'Added meta.DFMRunLog.source_user';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[DFMRunLog]') AND type = 'U')
   AND COL_LENGTH(N'meta.DFMRunLog', N'source_host') IS NULL
BEGIN
    ALTER TABLE [meta].[DFMRunLog] ADD [source_host] NVARCHAR(255) NULL;
    PRINT 'Added meta.DFMRunLog.source_host';
END
GO

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[meta].[DFMRunLog]') AND type = 'U')
   AND COL_LENGTH(N'meta.DFMRunLog', N'source_table') IS NULL
BEGIN
    ALTER TABLE [meta].[DFMRunLog] ADD [source_table] NVARCHAR(256) NULL;
    PRINT 'Added meta.DFMRunLog.source_table';
END
GO
