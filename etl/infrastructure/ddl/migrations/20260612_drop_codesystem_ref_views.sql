-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Cleanup: drop the staging ref.* CodeSystem views.
--
-- The DSO codesystem sync now reads directly from the staging codesystem.*
-- tables (source_schema/source_table on SyncTableConfig), so the bridging
-- views from 20260528_sks_codesystem_ref_views.sql are obsolete — codesystem
-- data lives only in the codesystem schema on staging.
--
-- Guarded on type 'V' so the ref.* TABLES of the same names on DSO are
-- never touched.
-- ============================================================================

IF OBJECT_ID(N'[ref].[SKS_Concept]', 'V') IS NOT NULL
    DROP VIEW [ref].[SKS_Concept];
GO

IF OBJECT_ID(N'[ref].[SKS_Properties]', 'V') IS NOT NULL
    DROP VIEW [ref].[SKS_Properties];
GO

IF OBJECT_ID(N'[ref].[SKS_Hierarchy]', 'V') IS NOT NULL
    DROP VIEW [ref].[SKS_Hierarchy];
GO

IF OBJECT_ID(N'[ref].[SOR_Concept]', 'V') IS NOT NULL
    DROP VIEW [ref].[SOR_Concept];
GO

IF OBJECT_ID(N'[ref].[SOR_Properties]', 'V') IS NOT NULL
    DROP VIEW [ref].[SOR_Properties];
GO

IF OBJECT_ID(N'[ref].[SOR_Hierarchy]', 'V') IS NOT NULL
    DROP VIEW [ref].[SOR_Hierarchy];
GO
