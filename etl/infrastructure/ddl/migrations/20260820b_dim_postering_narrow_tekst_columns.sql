-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- dim.Postering: narrow transaktionstekst / posteringstekst from NVARCHAR(MAX)
-- to sized NVARCHAR (paired with tables/erp/02_dims.sql).
--
-- Both columns originate 1:1 from the SydAX archive source columns
-- arc.*.Transaktionstekst / .Posteringstekst, which are VARCHAR(100) (see
-- tables/erp/00_archive.sql). They pass straight through pre_stg -> stg -> the
-- temp source table without concatenation, so no value can exceed 100 chars.
-- Leaving the dim columns as NVARCHAR(MAX) forced potential off-row LOB storage
-- and made the MERGE change-detection (EXCEPT) compare LOBs; sizing them keeps
-- the data in-row.
--
-- The matching CREATE TABLE in tables/erp/02_dims.sql already declares the sized
-- types, so on a fresh install this migration is a no-op. On an existing database
-- it narrows in place.
--
-- NOTE: narrowing NVARCHAR(MAX) is a size-of-data operation -- on a populated
-- table SQL Server rewrites and logs every row, so run this in a maintenance
-- window, or rebuild dim.Postering from empty (the full-rebuild path recreates it
-- from the sized DDL). Neither column takes part in a key or index, so a plain
-- ALTER COLUMN is sufficient. Each ALTER is guarded to run only while the column
-- is still NVARCHAR(MAX), so the script is idempotent; a missing table/column is
-- skipped via the sys.columns guard. SET XACT_ABORT ON ensures that if any value
-- would be truncated the ALTER fails atomically rather than silently dropping data.

SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'[dim].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = N'transaktionstekst' AND max_length = -1)
    ALTER TABLE [dim].[Postering] ALTER COLUMN [transaktionstekst] NVARCHAR(100) NULL;
GO
IF OBJECT_ID(N'[dim].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]') AND name = N'posteringstekst' AND max_length = -1)
    ALTER TABLE [dim].[Postering] ALTER COLUMN [posteringstekst] NVARCHAR(100) NULL;
GO
