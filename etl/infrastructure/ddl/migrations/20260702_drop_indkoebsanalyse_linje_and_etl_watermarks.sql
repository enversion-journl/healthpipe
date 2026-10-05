-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: decommission the IndkoebsanalyseLinje (KMD SpendAnalysisLines)
-- pipeline and the legacy ETL watermark tables.
--
-- The fct.IndkoebsanalyseLinje pipeline (staging -> map -> fct -> DSO sync) was
-- the last consumer of core.etl_watermark_manager.EtlWatermarkManager. With the
-- pipeline removed, both the fact/map/view objects and the watermark state/log
-- tables (meta.etl_watermark_state / meta.etl_watermark_log) become orphans.
--
-- The matching DDL definitions have been removed from:
--   infrastructure/ddl/tables/erp/01_maps.sql   (map.IndkoebsanalyseLinje)
--   infrastructure/ddl/tables/erp/03_facts.sql  (fct.IndkoebsanalyseLinje)
--   infrastructure/ddl/views/erp/02_vw_fct.sql  (vw_fct.IndkoebsanalyseLinje)
--   infrastructure/ddl/tables/meta/02_etl_watermarks.sql (deleted)
--
-- This migration runs against both the staging and DSO databases (both execute
-- the shared ddl/migrations directory). Each object is guarded, so on a database
-- where a given object never existed (e.g. the watermark tables on DSO) the
-- statement is a no-op. Idempotent + tracked once via meta.migration_history.

SET NOCOUNT ON;

-- 1. Drop the view first (it depends on the fact table).
IF OBJECT_ID(N'[vw_fct].[IndkoebsanalyseLinje]', 'V') IS NOT NULL
    DROP VIEW [vw_fct].[IndkoebsanalyseLinje];
GO

-- 2. Drop the fact table.
IF OBJECT_ID(N'[fct].[IndkoebsanalyseLinje]', 'U') IS NOT NULL
    DROP TABLE [fct].[IndkoebsanalyseLinje];
GO

-- 3. Drop the map table.
IF OBJECT_ID(N'[map].[IndkoebsanalyseLinje]', 'U') IS NOT NULL
    DROP TABLE [map].[IndkoebsanalyseLinje];
GO

-- 4. Drop the legacy ETL watermark tables (staging only; no-op on DSO).
IF OBJECT_ID(N'[meta].[etl_watermark_log]', 'U') IS NOT NULL
    DROP TABLE [meta].[etl_watermark_log];
GO

IF OBJECT_ID(N'[meta].[etl_watermark_state]', 'U') IS NOT NULL
    DROP TABLE [meta].[etl_watermark_state];
GO
