-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- dim.ILSVare: narrow oversized business columns from NVARCHAR(MAX) to sized
-- NVARCHAR (paired with tables/erp/02_dims.sql).
--
-- These columns trace 1:1 from arc.ils_vare through stg.dim_ILSVare and the
-- #IlsVare_Resolved temp source into the dim, so the values already fit the arc
-- source sizes; only the dim columns were still declared as MAX, forcing
-- potential off-row LOB storage and making the MERGE change-detection (EXCEPT)
-- compare LOBs. Widths mirror the arc.ils_vare source:
--   varenavn   <- arc.ils_vare.navn      NVARCHAR(80)
--   varenavn2  <- arc.ils_vare.navn2     NVARCHAR(80)
--   billede    <- arc.ils_vare.billed    NVARCHAR(104)
--   weblink    <- arc.ils_vare.billedli  NVARCHAR(140)
--
-- The matching CREATE TABLE in tables/erp/02_dims.sql already declares the sized
-- types, so on a fresh install this migration is a no-op. On an existing database
-- it narrows in place.
--
-- NOTE: narrowing NVARCHAR(MAX) is a size-of-data operation -- on a populated
-- table SQL Server rewrites and logs every row, so run this in a maintenance
-- window, or rebuild dim.ILSVare from empty (the full-rebuild path recreates it
-- from the sized DDL). None of these columns take part in a key or index, so a
-- plain ALTER COLUMN is sufficient. Each ALTER is guarded to run only while the
-- column is still NVARCHAR(MAX), so the script is idempotent; a missing
-- table/column is skipped via the sys.columns guard. SET XACT_ABORT ON ensures
-- that if any value would be truncated the ALTER fails atomically rather than
-- silently dropping data.

SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'varenavn' AND max_length = -1)
    ALTER TABLE [dim].[ILSVare] ALTER COLUMN [varenavn] NVARCHAR(80) NULL;
GO
IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'varenavn2' AND max_length = -1)
    ALTER TABLE [dim].[ILSVare] ALTER COLUMN [varenavn2] NVARCHAR(80) NULL;
GO
IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'billede' AND max_length = -1)
    ALTER TABLE [dim].[ILSVare] ALTER COLUMN [billede] NVARCHAR(104) NULL;
GO
IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]') AND name = N'weblink' AND max_length = -1)
    ALTER TABLE [dim].[ILSVare] ALTER COLUMN [weblink] NVARCHAR(140) NULL;
GO
