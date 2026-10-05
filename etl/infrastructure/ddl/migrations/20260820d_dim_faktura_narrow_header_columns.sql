-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- dim.Faktura: narrow header-derived business columns from NVARCHAR(MAX) to sized
-- NVARCHAR (paired with tables/erp/02_dims.sql).
--
-- These columns originate from arc.mysupply_statistic_invoice_header (h.*), which
-- was narrowed in 20260703_narrow_mysupply_invoice_nvarchar.sql. They flow 1:1
-- through pre_stg_Fakturalinje_mysupply -> stg.dim_Faktura_mysupply -> the temp
-- source table, so the values already fit; only the dim columns were still MAX,
-- forcing potential off-row LOB storage and making the MERGE change-detection
-- (EXCEPT) compare LOBs. Widths mirror the arc source (mostly 255; kunde_reference_id
-- is 512).
--
-- Columns kept as NVARCHAR(MAX) on purpose (NOT narrowed here):
--   juridisk_kunde_reference_id  -- arc source stays MAX (observed max ~5364 chars)
--   gebyr_rabat_* / gebyr_tillaeg_*  -- '#'-delimited STRING_AGG composites, unbounded arity
--
-- The matching CREATE TABLE in tables/erp/02_dims.sql already declares the sized
-- types, so on a fresh install this migration is a no-op. On an existing database
-- it narrows in place.
--
-- NOTE: narrowing NVARCHAR(MAX) is a size-of-data operation -- on a populated
-- table SQL Server rewrites and logs every row, so run this in a maintenance
-- window, or rebuild dim.Faktura from empty (the full-rebuild path recreates it
-- from the sized DDL). None of these columns take part in a key or index, so a
-- plain ALTER COLUMN is sufficient. Each ALTER is guarded to run only while the
-- column is still NVARCHAR(MAX), so the script is idempotent; a missing
-- table/column is skipped via the sys.columns guard. SET XACT_ABORT ON ensures
-- that if any value would be truncated the ALTER fails atomically rather than
-- silently dropping data.

SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'regnings_modtager_id' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [regnings_modtager_id] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'regnings_modtager_id_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [regnings_modtager_id_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'koebs_ordrenummer' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [koebs_ordrenummer] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'salgs_ordrenummer' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [salgs_ordrenummer] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_identification' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_identification] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_identification_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_identification_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'dimension_konto_streng' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [dimension_konto_streng] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kundenavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kundenavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_vejnavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_vejnavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_hus_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_hus_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_bynavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_bynavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_post_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_post_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_landekode' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_landekode] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_id' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_id] NVARCHAR(512) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_navn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_navn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_telefon_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_telefon_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_fax_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_fax_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_e_mail' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_e_mail] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kunde_reference_rolle' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kunde_reference_rolle] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_identification' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_identification] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_identification_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_identification_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_dimension_konto_streng' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_dimension_konto_streng] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kundenavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kundenavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_vejnavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_vejnavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_hus_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_hus_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_bynavn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_bynavn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_post_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_post_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_landekode' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_landekode] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_reference_navn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_reference_navn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_reference_telefon_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_reference_telefon_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_reference_fax_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_reference_fax_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_reference_e_mail' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_reference_e_mail] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'juridisk_kunde_reference_rolle' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [juridisk_kunde_reference_rolle] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_identifikation' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_identifikation] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_identifikation_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_identifikation_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_navn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_navn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_adresse_id_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_adresse_id_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_vejnavn_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_vejnavn_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_hus_nr_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_hus_nr_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_bynavn_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_bynavn_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_post_nr_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_post_nr_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_land_kode_vareafsendelse' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_land_kode_vareafsendelse] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_cvr_nummer' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_cvr_nummer] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_cvr_nummer_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_cvr_nummer_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_reference_idenrifikation' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_reference_idenrifikation] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_reference_navn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_reference_navn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_reference_telefon_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_reference_telefon_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_reference_fax_nr' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_reference_fax_nr] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'faktura_udsteder_reference_e_mail' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [faktura_udsteder_reference_e_mail] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kort_arts_kode' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kort_arts_kode] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'betalings_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [betalings_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'betalings_id' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [betalings_id] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kreditnummer' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kreditnummer] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kontonummer' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kontonummer] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kontotype' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kontotype] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'afdelings_id' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [afdelings_id] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'bank_id' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [bank_id] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'bank_navn' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [bank_navn] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'told_kurs' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [told_kurs] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'told_kurs_type' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [told_kurs_type] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kontantrabatsats' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kontantrabatsats] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'strafrentesats' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [strafrentesats] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'kontant_rabat_dato' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [kontant_rabat_dato] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'start_rente_dato' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [start_rente_dato] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'zero_rated_afgifts_kategori' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [zero_rated_afgifts_kategori] NVARCHAR(255) NULL;
GO
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = N'zero_rated_afgifts_kategori_procent' AND max_length = -1)
    ALTER TABLE [dim].[Faktura] ALTER COLUMN [zero_rated_afgifts_kategori_procent] NVARCHAR(255) NULL;
GO
