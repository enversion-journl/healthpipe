-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Add summed allowance/charge measures to fct.Faktura and fct.Fakturalinje.
--
-- The *_allowance_charge tables are 1:n per header/line; the detail values live
-- as '#'-concatenated text on dim.Faktura/dim.Fakturalinje. These columns add the
-- numeric, additive aggregate at fact grain. GebyrKategoriIndikering separates
-- charges from discounts (true/NULL = gebyr, false = rabat) — a single naive SUM
-- would add discounts to fees, so the two are split into separate measures.

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'GebyrBeloeb')
    ALTER TABLE [fct].[Faktura] ADD [GebyrBeloeb] DECIMAL(18, 4) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'RabatBeloeb')
    ALTER TABLE [fct].[Faktura] ADD [RabatBeloeb] DECIMAL(18, 4) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'GebyrBeloeb')
    ALTER TABLE [fct].[Fakturalinje] ADD [GebyrBeloeb] DECIMAL(18, 4) NULL;

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'RabatBeloeb')
    ALTER TABLE [fct].[Fakturalinje] ADD [RabatBeloeb] DECIMAL(18, 4) NULL;
