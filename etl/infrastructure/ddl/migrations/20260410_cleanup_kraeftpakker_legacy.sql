-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Cleanup script: Remove Kraeftpakker legacy objects from DSO QA database
-- Run this manually against BOTH the DSO QA and Staging QA databases.
--
-- Order matters: drop views first (they depend on tables), then facts,
-- then dims, then maps.
-- ============================================================================

-- ============================================================================
-- 1. Drop Kraeftpakker views (DSO only)
-- ============================================================================

-- Security views
IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'BorgerRB_Kraeftpakker' AND SCHEMA_NAME(v.schema_id) = 'sec')
    DROP VIEW [sec].[BorgerRB_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Borger_Kraeftpakker' AND SCHEMA_NAME(v.schema_id) = 'vw_dim')
    DROP VIEW [vw_dim].[Borger_Kraeftpakker];

-- Fact views
IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Indikator' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Indikator];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Markoerforloeb_IndikatorStatus' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Markoerforloeb_IndikatorStatus];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'MarkoerforloebObsliste_Kraeftpakker' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[MarkoerforloebObsliste_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'MarkoerforloebDetail_Kraeftpakker' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[MarkoerforloebDetail_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Markoerforloeb_Kraeftpakker' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Markoerforloeb_Kraeftpakker];

-- ============================================================================
-- 2. Drop Kraeftpakker fact tables
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'MarkoerforloebObsliste_Kraeftpakker' AND SCHEMA_NAME(t.schema_id) = 'fct')
    DROP TABLE [fct].[MarkoerforloebObsliste_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'MarkoerforloebDetail_Kraeftpakker' AND SCHEMA_NAME(t.schema_id) = 'fct')
    DROP TABLE [fct].[MarkoerforloebDetail_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoerforloeb_Kraeftpakker' AND SCHEMA_NAME(t.schema_id) = 'fct')
    DROP TABLE [fct].[Markoerforloeb_Kraeftpakker];

-- ============================================================================
-- 3. Drop Kraeftpakker dimension tables
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'IndikatorEgenskaber' AND SCHEMA_NAME(t.schema_id) = 'dim')
    DROP TABLE [dim].[IndikatorEgenskaber];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Fejltype' AND SCHEMA_NAME(t.schema_id) = 'dim')
    DROP TABLE [dim].[Fejltype];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Standardforloebstid' AND SCHEMA_NAME(t.schema_id) = 'dim')
    DROP TABLE [dim].[Standardforloebstid];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoer' AND SCHEMA_NAME(t.schema_id) = 'dim')
    DROP TABLE [dim].[Markoer];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoergruppe' AND SCHEMA_NAME(t.schema_id) = 'dim')
    DROP TABLE [dim].[Markoergruppe];

-- ============================================================================
-- 4. Drop Kraeftpakker map tables
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoerforloeb_Kraeftpakker' AND SCHEMA_NAME(t.schema_id) = 'map')
    DROP TABLE [map].[Markoerforloeb_Kraeftpakker];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Standardforloebstid' AND SCHEMA_NAME(t.schema_id) = 'map')
    DROP TABLE [map].[Standardforloebstid];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoer' AND SCHEMA_NAME(t.schema_id) = 'map')
    DROP TABLE [map].[Markoer];

IF EXISTS (SELECT 1 FROM sys.tables t WHERE t.name = 'Markoergruppe' AND SCHEMA_NAME(t.schema_id) = 'map')
    DROP TABLE [map].[Markoergruppe];

-- ============================================================================
-- 5. Drop other orphaned views (not Kraeftpakker, but not in codebase)
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'DemoTorsdag' AND SCHEMA_NAME(v.schema_id) = 'vw_dim')
    DROP VIEW [vw_dim].[DemoTorsdag];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Organization_SOR_Flat' AND SCHEMA_NAME(v.schema_id) = 'vw_dim')
    DROP VIEW [vw_dim].[Organization_SOR_Flat];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Organization_SOR_Flat_RSD' AND SCHEMA_NAME(v.schema_id) = 'vw_dim')
    DROP VIEW [vw_dim].[Organization_SOR_Flat_RSD];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Organization_SOR_RSD' AND SCHEMA_NAME(v.schema_id) = 'vw_dim')
    DROP VIEW [vw_dim].[Organization_SOR_RSD];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Fakturalinjer' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Fakturalinjer];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Markoerforloeb_Kraeftpakker_Demo' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Markoerforloeb_Kraeftpakker_Demo];

IF EXISTS (SELECT 1 FROM sys.views v WHERE v.name = 'Vareaftaler' AND SCHEMA_NAME(v.schema_id) = 'vw_fct')
    DROP VIEW [vw_fct].[Vareaftaler];

PRINT 'Kraeftpakker cleanup complete.';
