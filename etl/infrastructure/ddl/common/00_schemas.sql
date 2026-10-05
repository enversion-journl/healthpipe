-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Common Schemas for Data Warehouse
-- Creates all required schemas for both Staging (DSI/DSA) and DSO databases
-- ============================================================================

-- ext: Extract schema (1:1 raw data from source systems, created/dropped by ETL)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ext')
BEGIN
    EXEC('CREATE SCHEMA [ext]');
END
GO

-- stg: Staging schema (empty - tables created/dropped by daily ETL)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA [stg]');
END
GO

-- map: Mapping tables (persistent - source of truth for DW_EK assignment)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'map')
BEGIN
    EXEC('CREATE SCHEMA [map]');
END
GO

-- dim: Dimension tables (persistent)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dim')
BEGIN
    EXEC('CREATE SCHEMA [dim]');
END
GO

-- fct: Fact tables (persistent)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fct')
BEGIN
    EXEC('CREATE SCHEMA [fct]');
END
GO

-- ref: Reference tables (persistent - seed data and lookup tables)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ref')
BEGIN
    EXEC('CREATE SCHEMA [ref]');
END
GO

-- bridge: Bridge tables for many-to-many relationships
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bridge')
BEGIN
    EXEC('CREATE SCHEMA [bridge]');
END
GO

-- sec: Security schema (tables, functions, procedures)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sec')
BEGIN
    EXEC('CREATE SCHEMA [sec]');
END
GO

-- meta: Metadata schema (sync watermarks, operational state)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')
BEGIN
    EXEC('CREATE SCHEMA [meta]');
END
GO

-- vw_dim: Secured dimension views (user-facing)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'vw_dim')
BEGIN
    EXEC('CREATE SCHEMA [vw_dim]');
END
GO

-- vw_fct: Secured fact views (user-facing)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'vw_fct')
BEGIN
    EXEC('CREATE SCHEMA [vw_fct]');
END
GO

-- vw_bridge: Secured bridge views (user-facing)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'vw_bridge')
BEGIN
    EXEC('CREATE SCHEMA [vw_bridge]');
END
GO

-- fhir: FHIR flat tables from SQL on FHIR ViewDefinition materialization
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'fhir')
BEGIN
    EXEC('CREATE SCHEMA [fhir]');
END
GO

-- arc: Archive tables (persistent source snapshots for delta processing)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'arc')
BEGIN
    EXEC('CREATE SCHEMA [arc]');
END
GO

-- vw_fhir: Pass-through reporting views over fhir.* (DSO presentation layer)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'vw_fhir')
BEGIN
    EXEC('CREATE SCHEMA [vw_fhir]');
END
GO

-- codesystem: FHIR CodeSystem tables (persistent, MERGE updates)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'codesystem')
BEGIN
    EXEC('CREATE SCHEMA [codesystem]');
END
GO

-- conceptmap: FHIR ConceptMap tables (persistent, MERGE updates)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'conceptmap')
BEGIN
    EXEC('CREATE SCHEMA [conceptmap]');
END
GO

-- dm: Datamart — færdigopløste udtræk hvor kildens egne værdier står side om
-- side med de opslåede DW-nøgler (dw_ek_Borger, dw_ek_Organisation) og med
-- relationer til FHIR-laget. Det er dette lag konsumenter læser; arc forbliver
-- 1:1 med kildefilerne, så afledte kolonner ikke bliver stale ved næste MERGE.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dm')
BEGIN
    EXEC('CREATE SCHEMA [dm]');
END
GO
