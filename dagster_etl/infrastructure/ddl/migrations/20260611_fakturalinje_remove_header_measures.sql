-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Move header-level measures off fct.Fakturalinje and onto fct.Faktura.
--
-- These columns all originate from statistic_invoice_header and are repeated
-- verbatim on every line, causing N× overcount when SUM'd. Confirmed by
-- investigation: overcount factor = line_count exactly across all 828k
-- invoices in 2025.
-- SUM(LinjeBeløb) already equals FakturaLinjeTotal, so no information is lost.

-- ── Remove header measures from fct.Fakturalinje ─────────────────────────────

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'LinjeTotal')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [LinjeTotal];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_LinjeTotal')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_LinjeTotal];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_FakturaTotal')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_FakturaTotal];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'VAT_AfgiftsKategori')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [VAT_AfgiftsKategori];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_MomsGrundlag')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_MomsGrundlag];

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'Valutakode_MomsBeloeb')
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [Valutakode_MomsBeloeb];

-- ── Add header measures to fct.Faktura ───────────────────────────────────────

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'VAT_AfgiftsKategori')
    ALTER TABLE [fct].[Faktura] ADD [VAT_AfgiftsKategori] NVARCHAR(50) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_MomsGrundlag')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_MomsGrundlag] NVARCHAR(10) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_MomsBeloeb')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_MomsBeloeb] NVARCHAR(10) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_LinjeTotal')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_LinjeTotal] NVARCHAR(10) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'Valutakode_FakturaTotal')
    ALTER TABLE [fct].[Faktura] ADD [Valutakode_FakturaTotal] NVARCHAR(10) NULL;
