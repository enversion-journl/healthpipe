-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Manual correction to the 20260708 naming overhaul (rename_03_arc_ext):
-- the overhaul renamed arc.kmd_uddata_* columns that are SOURCE data, not DWH
-- housekeeping. The archive layer mirrors the KMD UdData source 1:1 — ext is
-- dropped and recreated by every extract with the source's column names, and
-- the archive merge uses one shared column list for both sides, so arc columns
-- must keep the source spelling (DW_ArchiveID, DW_ValidFrom, ...). Only
-- DWH-added columns keep our convention (dw_timestamp_insert,
-- dw_timestamp_update, computed_max_audit) — those renames are NOT reverted.
-- Without this, the KMD archive merge/fast-load fails on the ext side with
-- "Invalid column name 'dw_archive_id'" (non-case-only renames cannot bind).
-- computed_max_audit is dropped and re-added because its definition references
-- the renamed audit columns (sp_rename is blocked by the persisted dependency).

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_invoice' AND i.name = 'IX_arc_kmd_uddata_invoice_computed_max_audit')
    DROP INDEX [IX_arc_kmd_uddata_invoice_computed_max_audit] ON [arc].[kmd_uddata_invoice];
GO

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_spend_analysis_lines' AND i.name = 'IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit')
    DROP INDEX [IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit] ON [arc].[kmd_uddata_spend_analysis_lines];
GO

IF EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_unspsc' AND i.name = 'IX_arc_kmd_uddata_unspsc_computed_max_audit')
    DROP INDEX [IX_arc_kmd_uddata_unspsc_computed_max_audit] ON [arc].[kmd_uddata_unspsc];
GO

IF OBJECT_ID('arc.kmd_uddata_invoice', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_invoice', 'computed_max_audit') IS NOT NULL
    ALTER TABLE [arc].[kmd_uddata_invoice] DROP COLUMN [computed_max_audit];
GO

IF OBJECT_ID('arc.kmd_uddata_spend_analysis_lines', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_spend_analysis_lines', 'computed_max_audit') IS NOT NULL
    ALTER TABLE [arc].[kmd_uddata_spend_analysis_lines] DROP COLUMN [computed_max_audit];
GO

IF OBJECT_ID('arc.kmd_uddata_unspsc', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_unspsc', 'computed_max_audit') IS NOT NULL
    ALTER TABLE [arc].[kmd_uddata_unspsc] DROP COLUMN [computed_max_audit];
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_archive_id') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ArchiveID')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_archive_id', 'DW_ArchiveID', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_audit', 'DW_Audit', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Insert')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_audit_insert', 'DW_Audit_Insert', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Update')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_audit_update', 'DW_Audit_Update', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_Invoice') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_EK_Invoice')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_ek_Invoice', 'DW_EK_Invoice', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_current') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsCurrent')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_is_current', 'DW_IsCurrent', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Invoice') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Invoice')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_sk_Invoice', 'DW_SK_Invoice', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_valid_from') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ValidFrom')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_valid_from', 'DW_ValidFrom', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_valid_to') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_invoice' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ValidTo')
    EXEC sp_rename 'arc.kmd_uddata_invoice.dw_valid_to', 'DW_ValidTo', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_archive_id') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ArchiveID')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_archive_id', 'DW_ArchiveID', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_audit', 'DW_Audit', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Insert')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_audit_insert', 'DW_Audit_Insert', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Update')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_audit_update', 'DW_Audit_Update', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_SpendAnalysisLine') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_EK_SpendAnalysisLine')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_ek_SpendAnalysisLine', 'DW_EK_SpendAnalysisLine', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_AllowanceChargeLine') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_AllowanceChargeLine')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_AllowanceChargeLine', 'DW_SK_AllowanceChargeLine', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_AppendixNumber') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_AppendixNumber')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_AppendixNumber', 'DW_SK_AppendixNumber', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Compliance') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Compliance')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Compliance', 'DW_SK_Compliance', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_actual_delivery') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_ActualDelivery')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_actual_delivery', 'DW_SK_Date_ActualDelivery', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_issue') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_Issue')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_issue', 'DW_SK_Date_Issue', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_order_issue') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_OrderIssue')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_order_issue', 'DW_SK_Date_OrderIssue', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_payment_due') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_PaymentDue')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_payment_due', 'DW_SK_Date_PaymentDue', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_posting') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_Posting')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_posting', 'DW_SK_Date_Posting', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Date_spend_analysis') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Date_SpendAnalysis')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Date_spend_analysis', 'DW_SK_Date_SpendAnalysis', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Economy') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Economy')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Economy', 'DW_SK_Economy', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_EconomyCustom') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_EconomyCustom')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_EconomyCustom', 'DW_SK_EconomyCustom', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_EconomyLine') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_EconomyLine')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_EconomyLine', 'DW_SK_EconomyLine', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_EtradeObligation') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_EtradeObligation')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_EtradeObligation', 'DW_SK_EtradeObligation', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_ExchangeRates') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_ExchangeRates')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_ExchangeRates', 'DW_SK_ExchangeRates', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_ExpenceType') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_ExpenceType')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_ExpenceType', 'DW_SK_ExpenceType', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_GeoData') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_GeoData')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_GeoData', 'DW_SK_GeoData', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Invoice') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Invoice')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Invoice', 'DW_SK_Invoice', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_InvoiceLine') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_InvoiceLine')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_InvoiceLine', 'DW_SK_InvoiceLine', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_InvoiceType') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_InvoiceType')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_InvoiceType', 'DW_SK_InvoiceType', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_LocalPurchase') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_LocalPurchase')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_LocalPurchase', 'DW_SK_LocalPurchase', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_NACE') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_NACE')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_NACE', 'DW_SK_NACE', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Online') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Online')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Online', 'DW_SK_Online', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_OrderType') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_OrderType')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_OrderType', 'DW_SK_OrderType', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Organic') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Organic')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Organic', 'DW_SK_Organic', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Organization') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Organization')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Organization', 'DW_SK_Organization', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_PostingText') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_PostingText')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_PostingText', 'DW_SK_PostingText', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_PriceAgreement') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_PriceAgreement')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_PriceAgreement', 'DW_SK_PriceAgreement', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_ProcurementAgreement') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_ProcurementAgreement')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_ProcurementAgreement', 'DW_SK_ProcurementAgreement', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_ProcurementType') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_ProcurementType')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_ProcurementType', 'DW_SK_ProcurementType', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_SpendAnalysisLineItem') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_SpendAnalysisLineItem')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_SpendAnalysisLineItem', 'DW_SK_SpendAnalysisLineItem', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_Supplier') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_Supplier')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_Supplier', 'DW_SK_Supplier', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_TenderGroup') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_TenderGroup')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_TenderGroup', 'DW_SK_TenderGroup', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_UNSPSC') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_UNSPSC')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_sk_UNSPSC', 'DW_SK_UNSPSC', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_valid_date') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_spend_analysis_lines' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ValidDate')
    EXEC sp_rename 'arc.kmd_uddata_spend_analysis_lines.dw_valid_date', 'DW_ValidDate', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_archive_id') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ArchiveID')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_archive_id', 'DW_ArchiveID', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_audit', 'DW_Audit', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_insert') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Insert')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_audit_insert', 'DW_Audit_Insert', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_audit_update') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_Audit_Update')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_audit_update', 'DW_Audit_Update', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_ek_UNSPSC') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_EK_UNSPSC')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_ek_UNSPSC', 'DW_EK_UNSPSC', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_is_current') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_IsCurrent')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_is_current', 'DW_IsCurrent', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_sk_UNSPSC') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_SK_UNSPSC')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_sk_UNSPSC', 'DW_SK_UNSPSC', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_valid_from') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ValidFrom')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_valid_from', 'DW_ValidFrom', 'COLUMN';
GO

IF EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'dw_valid_to') AND NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id WHERE t.name = 'kmd_uddata_unspsc' AND SCHEMA_NAME(t.schema_id) = 'arc' AND c.name COLLATE Latin1_General_100_CS_AS = 'DW_ValidTo')
    EXEC sp_rename 'arc.kmd_uddata_unspsc.dw_valid_to', 'DW_ValidTo', 'COLUMN';
GO

IF OBJECT_ID('arc.kmd_uddata_invoice', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_invoice', 'computed_max_audit') IS NULL
    EXEC('ALTER TABLE [arc].[kmd_uddata_invoice] ADD [computed_max_audit] AS (case when [DW_Audit_Insert] IS NULL AND [DW_Audit_Update] IS NULL AND [DW_Audit] IS NULL then NULL when [DW_Audit_Insert]>=coalesce([DW_Audit_Update],[DW_Audit_Insert]) AND [DW_Audit_Insert]>=coalesce([DW_Audit],[DW_Audit_Insert]) then [DW_Audit_Insert] when [DW_Audit_Update]>=coalesce([DW_Audit],[DW_Audit_Update]) then [DW_Audit_Update] else [DW_Audit] end) PERSISTED');
GO

IF OBJECT_ID('arc.kmd_uddata_spend_analysis_lines', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_spend_analysis_lines', 'computed_max_audit') IS NULL
    EXEC('ALTER TABLE [arc].[kmd_uddata_spend_analysis_lines] ADD [computed_max_audit] AS (case when [DW_Audit_Insert] IS NULL AND [DW_Audit_Update] IS NULL AND [DW_Audit] IS NULL then NULL when [DW_Audit_Insert]>=coalesce([DW_Audit_Update],[DW_Audit_Insert]) AND [DW_Audit_Insert]>=coalesce([DW_Audit],[DW_Audit_Insert]) then [DW_Audit_Insert] when [DW_Audit_Update]>=coalesce([DW_Audit],[DW_Audit_Update]) then [DW_Audit_Update] else [DW_Audit] end) PERSISTED');
GO

IF OBJECT_ID('arc.kmd_uddata_unspsc', 'U') IS NOT NULL AND COL_LENGTH('arc.kmd_uddata_unspsc', 'computed_max_audit') IS NULL
    EXEC('ALTER TABLE [arc].[kmd_uddata_unspsc] ADD [computed_max_audit] AS (case when [DW_Audit_Insert] IS NULL AND [DW_Audit_Update] IS NULL AND [DW_Audit] IS NULL then NULL when [DW_Audit_Insert]>=coalesce([DW_Audit_Update],[DW_Audit_Insert]) AND [DW_Audit_Insert]>=coalesce([DW_Audit],[DW_Audit_Insert]) then [DW_Audit_Insert] when [DW_Audit_Update]>=coalesce([DW_Audit],[DW_Audit_Update]) then [DW_Audit_Update] else [DW_Audit] end) PERSISTED');
GO

IF OBJECT_ID('arc.kmd_uddata_invoice', 'U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_invoice' AND i.name = 'IX_arc_kmd_uddata_invoice_computed_max_audit')
    EXEC('CREATE INDEX [IX_arc_kmd_uddata_invoice_computed_max_audit] ON [arc].[kmd_uddata_invoice] ([computed_max_audit] DESC)');
GO

IF OBJECT_ID('arc.kmd_uddata_spend_analysis_lines', 'U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_spend_analysis_lines' AND i.name = 'IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit')
    EXEC('CREATE INDEX [IX_arc_kmd_uddata_spend_analysis_lines_computed_max_audit] ON [arc].[kmd_uddata_spend_analysis_lines] ([computed_max_audit] DESC)');
GO

IF OBJECT_ID('arc.kmd_uddata_unspsc', 'U') IS NOT NULL AND NOT EXISTS (SELECT 1 FROM sys.indexes i JOIN sys.tables t ON t.object_id = i.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'arc' AND t.name = 'kmd_uddata_unspsc' AND i.name = 'IX_arc_kmd_uddata_unspsc_computed_max_audit')
    EXEC('CREATE INDEX [IX_arc_kmd_uddata_unspsc_computed_max_audit] ON [arc].[kmd_uddata_unspsc] ([computed_max_audit] DESC)');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ArchiveID'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_archive_id''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_audit''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Insert'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_audit_insert''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Update'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_audit_update''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_EK_Invoice'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_ek_Invoice''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_IsCurrent'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_is_current''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Invoice'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_sk_Invoice''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ValidFrom'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_valid_from''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ValidTo'' WHERE subject = ''arc.kmd_uddata_invoice'' AND name_suffix = ''dw_valid_to''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ArchiveID'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_archive_id''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_audit''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Insert'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_audit_insert''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Update'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_audit_update''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_EK_SpendAnalysisLine'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_ek_SpendAnalysisLine''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_AllowanceChargeLine'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_AllowanceChargeLine''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_AppendixNumber'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_AppendixNumber''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Compliance'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Compliance''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_ActualDelivery'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_actual_delivery''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_Issue'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_issue''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_OrderIssue'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_order_issue''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_PaymentDue'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_payment_due''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_Posting'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_posting''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Date_SpendAnalysis'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Date_spend_analysis''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Economy'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Economy''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_EconomyCustom'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_EconomyCustom''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_EconomyLine'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_EconomyLine''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_EtradeObligation'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_EtradeObligation''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_ExchangeRates'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_ExchangeRates''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_ExpenceType'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_ExpenceType''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_GeoData'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_GeoData''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Invoice'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Invoice''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_InvoiceLine'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_InvoiceLine''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_InvoiceType'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_InvoiceType''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_LocalPurchase'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_LocalPurchase''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_NACE'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_NACE''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Online'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Online''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_OrderType'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_OrderType''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Organic'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Organic''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Organization'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Organization''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_PostingText'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_PostingText''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_PriceAgreement'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_PriceAgreement''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_ProcurementAgreement'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_ProcurementAgreement''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_ProcurementType'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_ProcurementType''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_SpendAnalysisLineItem'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_SpendAnalysisLineItem''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_Supplier'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_Supplier''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_TenderGroup'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_TenderGroup''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_UNSPSC'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_sk_UNSPSC''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ValidDate'' WHERE subject = ''arc.kmd_uddata_spend_analysis_lines'' AND name_suffix = ''dw_valid_date''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ArchiveID'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_archive_id''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_audit''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Insert'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_audit_insert''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_Audit_Update'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_audit_update''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_EK_UNSPSC'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_ek_UNSPSC''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_IsCurrent'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_is_current''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_SK_UNSPSC'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_sk_UNSPSC''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ValidFrom'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_valid_from''');
GO

IF OBJECT_ID('dqa.Results', 'U') IS NOT NULL
    EXEC('UPDATE dqa.Results SET name_suffix = ''DW_ValidTo'' WHERE subject = ''arc.kmd_uddata_unspsc'' AND name_suffix = ''dw_valid_to''');
GO
