-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- dim.Fakturalinje performance cleanup (paired with tables/erp/02_dims.sql):
--
--   1. Drop the redundant UNIQUE NONCLUSTERED index IX_Dim_Fakturalinje_DW_EK.
--      It duplicates the clustered PK (PK_Dim_Fakturalinje) on the same single
--      column [dw_ek_Fakturalinje] — zero read benefit, but a second B-tree that
--      must be maintained on every insert. On a 60M-row (re)load that is a whole
--      extra index build for nothing.
--
--   2. Narrow the business-attribute columns from NVARCHAR(MAX) to sized NVARCHAR,
--      matching the already-narrowed arc.mysupply_statistic_invoice_line source
--      columns (see 20260703_narrow_mysupply_invoice_nvarchar.sql). The values
--      arrive pre-sized through pre_stg -> stg -> the temp source table, but the
--      dim columns were still MAX, forcing potential off-row LOB storage and
--      making the MERGE change-detection (EXCEPT) compare LOBs. Sizing them keeps
--      the data in-row and shrinks the row footprint.
--
--      Columns kept as NVARCHAR(MAX) on purpose:
--        item_faktura_udsteder_vare_navn        -- source max ~10072 chars
--        item_faktura_udsteder_vare_navn_kort   -- source max ~4157 chars
--        faktura_udsteder_gebyr_rabat_*         -- '#'-delimited STRING_AGG
--        faktura_udsteder_gebyr_tillaeg_*          composites, unbounded arity
--
-- The matching CREATE TABLE in tables/erp/02_dims.sql already declares the sized
-- types and omits the redundant index, so on a fresh install this migration is a
-- no-op. On an existing database it narrows in place / drops the index.
--
-- NOTE: narrowing NVARCHAR(MAX) is a size-of-data operation -- on a populated
-- table SQL Server rewrites and logs every row, so run this in a maintenance
-- window, or simply rebuild dim.Fakturalinje from empty (the full-rebuild path
-- recreates it from the sized DDL). None of these columns take part in a key or
-- index, so a plain ALTER COLUMN is sufficient. Each ALTER is guarded to run only
-- while the column is still NVARCHAR(MAX), so the script is idempotent; a missing
-- table/column is skipped via the sys.columns guard. SET XACT_ABORT ON ensures
-- that if any value would be truncated the ALTER fails atomically rather than
-- silently dropping data.

SET XACT_ABORT ON;
GO

-- 1. Drop the redundant duplicate-of-PK index.
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
                 AND name = N'IX_Dim_Fakturalinje_DW_EK')
    DROP INDEX [IX_Dim_Fakturalinje_DW_EK] ON [dim].[Fakturalinje];
GO

-- 2. Narrow business-attribute columns (only while still NVARCHAR(MAX)).
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'faktura_linje_nr' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [faktura_linje_nr] NVARCHAR(512) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'antal_enheder_faktureret_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [antal_enheder_faktureret_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_faktura_udstederens_vare_nr' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_faktura_udstederens_vare_nr] NVARCHAR(512) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_faktura_udstederens_vare_nr_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_faktura_udstederens_vare_nr_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'base_price_enheds_stoerrelse' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [base_price_enheds_stoerrelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'base_price_enheds_stoerrelse_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [base_price_enheds_stoerrelse_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'line_dimensions_konto_streng' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [line_dimensions_konto_streng] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'leverance_krav' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [leverance_krav] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'leverings_dato_2' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [leverings_dato_2] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_kundens_vare_nr' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_kundens_vare_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_kundens_vare_nr_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_kundens_vare_nr_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_kundens_vare_beskrivelse' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_kundens_vare_beskrivelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_faktura_udstederens_vare_beskrivelse' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_faktura_udstederens_vare_beskrivelse] NVARCHAR(2000) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_fabrikantens_vare_nr' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_fabrikantens_vare_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_fabrikantens_vare_nr_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_fabrikantens_vare_nr_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_fabrikantens_vare_beskrivelse' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_fabrikantens_vare_beskrivelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_standardiseret_vare_nr' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_standardiseret_vare_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_standardiseret_vare_nr_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_standardiseret_vare_nr_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_standardiseret_vare_beskrivelse' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_standardiseret_vare_beskrivelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_vare_klassifikation' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_vare_klassifikation] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_vare_klassifikation_type' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_vare_klassifikation_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_afgifts_kategori' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_afgifts_kategori] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND name = N'item_afgifts_kategori_procent' AND max_length = -1)
    ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [item_afgifts_kategori_procent] NVARCHAR(255) NULL;
GO
