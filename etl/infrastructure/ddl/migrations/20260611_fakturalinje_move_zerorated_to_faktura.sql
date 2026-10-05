-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Move the zero-rated VAT measures off fct.Fakturalinje and onto fct.Faktura.
--
-- ZeroRated_MomsGrundlag / ZeroRated_MomsBeloeb originate from
-- statistic_invoice_header (ZERO-RATED_TaxAmounts_*) and are repeated verbatim
-- on every line of the same invoice — the same N× overcount fixed for the other
-- header measures in 20260611_fakturalinje_remove_header_measures.sql. They were
-- initially kept on the line "for cross-period coverage", but fct.Faktura is
-- built from the same pre_stg_fakturalinje_mysupply table and covers the same
-- invoice population, so nothing is lost by moving them to header grain.

-- ── Remove zero-rated measures from fct.Fakturalinje ─────────────────────────

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'ZeroRated_MomsGrundlag')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [ZeroRated_MomsGrundlag];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_ZeroRated_MomsGrundlag')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_ZeroRated_MomsGrundlag];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'ZeroRated_MomsBeloeb')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [ZeroRated_MomsBeloeb];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_ZeroRated_MomsBeloeb')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_ZeroRated_MomsBeloeb];

-- ── Add zero-rated measures to fct.Faktura ───────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'ZeroRated_MomsGrundlag')
    ALTER TABLE [fct].[Faktura] ADD [ZeroRated_MomsGrundlag] DECIMAL(18, 4) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_ZeroRated_MomsGrundlag')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_ZeroRated_MomsGrundlag] NVARCHAR(10) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'ZeroRated_MomsBeloeb')
    ALTER TABLE [fct].[Faktura] ADD [ZeroRated_MomsBeloeb] DECIMAL(18, 4) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_ZeroRated_MomsBeloeb')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_ZeroRated_MomsBeloeb] NVARCHAR(10) NULL;
