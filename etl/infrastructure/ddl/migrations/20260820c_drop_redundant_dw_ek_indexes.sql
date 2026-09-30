-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Drop redundant UNIQUE NONCLUSTERED indexes that duplicate their table's own
-- clustered primary key on the same single column (paired with tables/erp/*.sql):
--
--   dim.Faktura   IX_Dim_Faktura_DW_EK   == PK_Dim_Faktura   ([dw_ek_Faktura])
--   map.Postering IX_Map_Postering_DW_EK == PK_Map_Postering ([dw_ek_Postering])
--
-- In both cases the clustered PK already serves every seek and the MERGE join, so
-- the nonclustered copy is a second B-tree maintained on every insert with zero
-- read benefit. (This mirrors the earlier IX_Dim_Fakturalinje_DW_EK drop.)
--
-- map.Postering is the odd map table: unlike its siblings it clusters the PK on
-- dw_ek_Postering rather than clean_code_Postering, so IX_Map_Postering_DW_EK is
-- the duplicate while IX_Map_Postering_CleanCode remains the useful lookup and is
-- kept.
--
-- The base DDL (tables/erp/01_maps.sql, tables/erp/02_dims.sql) no longer creates
-- these indexes, so on a fresh install this migration is a no-op. On an existing
-- database it drops them. Guarded on index existence, so re-runs are no-ops.
-- Runs after the 20260410 / 20260603 migrations that (re)created them, so the
-- date ordering guarantees the drop wins.

IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
                 AND name = N'IX_Dim_Faktura_DW_EK')
    DROP INDEX [IX_Dim_Faktura_DW_EK] ON [dim].[Faktura];
GO

IF OBJECT_ID(N'[map].[Postering]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes
               WHERE object_id = OBJECT_ID(N'[map].[Postering]')
                 AND name = N'IX_Map_Postering_DW_EK')
    DROP INDEX [IX_Map_Postering_DW_EK] ON [map].[Postering];
GO
