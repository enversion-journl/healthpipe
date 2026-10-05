-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Drop legacy vw_dim.*Egenskaber views after transaktionsdim remodel
--
-- vw_dim.FakturaEgenskaber and vw_dim.VareudleveringEgenskaber were retired
-- when dim.Faktura and dim.Vareudlevering became transaction dimensions.
-- View DDL in 01_vw_dim.sql no longer creates these names; this migration
-- cleans up environments where the old views still exist.
--
-- Idempotent: guarded by OBJECT_ID checks.
-- ============================================================================

IF OBJECT_ID(N'[vw_dim].[FakturaEgenskaber]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[FakturaEgenskaber];
GO

IF OBJECT_ID(N'[vw_dim].[VareudleveringEgenskaber]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[VareudleveringEgenskaber];
GO
