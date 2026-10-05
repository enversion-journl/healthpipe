-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- Migration: rename KMD UdData ext/arc tables from postfix to source-prefixed form
-- (e.g. arc.unspsc_kmd_uddata -> arc.kmd_uddata_unspsc), matching MySupply/ILS/SydAX.
-- Also migrates dimension.Invoice archive from etl_watermark_state to meta.dfm_state.
--
-- Drop old-named arc + ext tables and clear dfm_state / etl_watermark checkpoints;
-- domain DDL (tables/kmd_uddata/00_archive.sql, which runs AFTER migrations)
-- recreates them under the new names, and the next extract full-loads them.
-- Tracked once via meta.migration_history; on a fresh database every guard below
-- is a no-op (the old names never existed).

SET NOCOUNT ON;

IF OBJECT_ID(N'[arc].[invoice_kmd_uddata]', 'U') IS NOT NULL DROP TABLE [arc].[invoice_kmd_uddata];
IF OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
    DROP TABLE [arc].[spend_analysis_lines_kmd_uddata];
IF OBJECT_ID(N'[arc].[unspsc_kmd_uddata]', 'U') IS NOT NULL DROP TABLE [arc].[unspsc_kmd_uddata];
GO

IF OBJECT_ID(N'[ext].[invoice_kmd_uddata]', 'U') IS NOT NULL DROP TABLE [ext].[invoice_kmd_uddata];
IF OBJECT_ID(N'[ext].[spend_analysis_lines_kmd_uddata]', 'U') IS NOT NULL
    DROP TABLE [ext].[spend_analysis_lines_kmd_uddata];
IF OBJECT_ID(N'[ext].[unspsc_kmd_uddata]', 'U') IS NOT NULL DROP TABLE [ext].[unspsc_kmd_uddata];
GO

-- Legacy shadow names from the DFM migration (20260528) if still present.
IF OBJECT_ID(N'[arc].[invoice_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [arc].[invoice_kmd_uddata_dfm_new];
IF OBJECT_ID(N'[arc].[spend_analysis_lines_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [arc].[spend_analysis_lines_kmd_uddata_dfm_new];
IF OBJECT_ID(N'[arc].[unspsc_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [arc].[unspsc_kmd_uddata_dfm_new];
GO

IF OBJECT_ID(N'[ext].[invoice_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [ext].[invoice_kmd_uddata_dfm_new];
IF OBJECT_ID(N'[ext].[spend_analysis_lines_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [ext].[spend_analysis_lines_kmd_uddata_dfm_new];
IF OBJECT_ID(N'[ext].[unspsc_kmd_uddata_dfm_new]', 'U') IS NOT NULL
    DROP TABLE [ext].[unspsc_kmd_uddata_dfm_new];
GO

DELETE FROM [meta].[dfm_state]
WHERE [table_name] IN (
        'invoice_kmd_uddata',
        'spend_analysis_lines_kmd_uddata',
        'unspsc_kmd_uddata',
        'kmd_uddata_invoice',
        'kmd_uddata_spend_analysis_lines',
        'kmd_uddata_unspsc'
    )
   OR [table_name] IN (
        'invoice_kmd_uddata_flow_start',
        'spend_analysis_lines_kmd_uddata_flow_start',
        'unspsc_kmd_uddata_flow_start'
    );
GO

DELETE FROM [meta].[etl_watermark_state]
WHERE [entity_type] = 'archive'
  AND [schema_name] = 'arc'
  AND [table_name] IN (
        'invoice_kmd_uddata',
        'spend_analysis_lines_kmd_uddata',
        'unspsc_kmd_uddata',
        'kmd_uddata_invoice',
        'kmd_uddata_spend_analysis_lines',
        'kmd_uddata_unspsc'
    );
GO
