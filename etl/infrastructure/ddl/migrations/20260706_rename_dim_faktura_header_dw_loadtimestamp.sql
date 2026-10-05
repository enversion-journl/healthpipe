-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename dim.Faktura.Header_DW_LoadTimestamp -> Header_DW_Timestamp_Update
--
-- Standardises on the source-derived audit column name DW_Timestamp_Update and
-- drops the legacy DW_LoadTimestamp alias throughout the ERP DSA chain. The
-- staging layer (SELECT ... INTO stg.*) regenerates with the new name on every
-- run; this migration renames the only PERSISTENT column that carried the old
-- name (added by 20260604_dim_faktura_header_identity). The faktura<->postering
-- bridge (ref.Faktura_To_Postering) and DimUpd_faktura reference the new name.
--
-- NOTE: Header_DW_Timestamp_Update stays distinct from the row-audit
-- DW_Timestamp_Update column already on dim.Faktura (different prefix).
--
-- Safe to re-run: guarded on old column present + new column absent, so it is
-- a no-op on a fresh DB (02_dims.sql already creates the new name) and on an
-- already-migrated database.
-- ============================================================================

IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'Header_DW_LoadTimestamp'
   )
   AND NOT EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[dim].[Faktura]') AND name = 'Header_DW_Timestamp_Update'
   )
BEGIN
    EXEC sp_rename '[dim].[Faktura].[Header_DW_LoadTimestamp]', 'Header_DW_Timestamp_Update', 'COLUMN';
    PRINT 'Renamed dim.Faktura.Header_DW_LoadTimestamp -> Header_DW_Timestamp_Update';
END
GO
