-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop the fact-to-fact FKs on the invoice facts (finding 2.2).
--
--   fct.Faktura.DW_EK_Postering    -- copied from the newest postering (fact->fact)
--   fct.Faktura.DW_EK_Kontoplan    -- TOP-1 fct.Postering subquery on FakturaNr
--   fct.Fakturalinje.DW_EK_Kontoplan -- same TOP-1 subquery, by invoice (not line)
--
-- The account genuinely lives at the posting grain (one FinanskontoId per
-- posting), so an invoice is many-valued over accounts. Storing a single
-- inherited value on the invoice facts silently dropped all-but-one account on
-- multi-account invoices, and the stored value diverged from vw_fct (which
-- resolves the account from the newest posting via ref.Faktura_To_Postering).
-- The link is now exposed only through vw_fct.Faktura / vw_fct.Fakturalinje
-- (DW_EK_Kontoplan_Postering); account-weighted measures aggregate fct.Postering.
--
-- fct.Postering keeps its own DW_EK_Postering surrogate and DW_EK_Kontoplan —
-- those are the legitimate posting-grain account/posting keys and are untouched.
--
-- Applied to BOTH staging and DSO (each tracks meta.migration_history).
-- Safe to re-run: IF EXISTS guards; auto-named DEFAULT constraints dropped first.
-- ============================================================================

DECLARE @sql NVARCHAR(MAX);

-- ---- fct.Faktura.DW_EK_Postering ----
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Faktura] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = 'DW_EK_Postering';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Postering')
BEGIN
    ALTER TABLE [fct].[Faktura] DROP COLUMN [DW_EK_Postering];
    PRINT 'Dropped DW_EK_Postering from fct.Faktura';
END
GO

-- ---- fct.Faktura.DW_EK_Kontoplan ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Faktura] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = 'DW_EK_Kontoplan';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Kontoplan')
BEGIN
    ALTER TABLE [fct].[Faktura] DROP COLUMN [DW_EK_Kontoplan];
    PRINT 'Dropped DW_EK_Kontoplan from fct.Faktura';
END
GO

-- ---- fct.Fakturalinje.DW_EK_Kontoplan ----
DECLARE @sql NVARCHAR(MAX);
SET @sql = NULL;
SELECT @sql = 'ALTER TABLE [fct].[Fakturalinje] DROP CONSTRAINT [' + dc.name + '];'
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = 'DW_EK_Kontoplan';
IF @sql IS NOT NULL EXEC sp_executesql @sql;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Kontoplan')
BEGIN
    ALTER TABLE [fct].[Fakturalinje] DROP COLUMN [DW_EK_Kontoplan];
    PRINT 'Dropped DW_EK_Kontoplan from fct.Fakturalinje';
END
GO

-- ---- Drop the FakturaNr index that only served the removed TOP-1 subqueries ----
IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_FakturaNr')
BEGIN
    DROP INDEX [IX_Fct_Postering_FakturaNr] ON [fct].[Postering];
    PRINT 'Dropped IX_Fct_Postering_FakturaNr (correlated-subquery support index)';
END
GO
