-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop sec.BeskytPersonoplysning, sec.BeskytDimBorger and sec.HarAdgang.
-- The datamart security templates (DimBorger view and datamart view) do not
-- call them, and the base DDL no longer creates them. The infrastructure
-- assets only run CREATE OR ALTER, so without this migration the functions
-- stay in existing databases.
--
-- Idempotent: DROP FUNCTION IF EXISTS. Runs on both staging and DSO.
-- ============================================================================

DROP FUNCTION IF EXISTS [sec].[BeskytPersonoplysning];
GO

DROP FUNCTION IF EXISTS [sec].[BeskytDimBorger];
GO

DROP FUNCTION IF EXISTS [sec].[HarAdgang];
GO
