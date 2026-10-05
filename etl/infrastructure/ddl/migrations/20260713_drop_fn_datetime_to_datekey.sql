-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Drop dbo.fn_DateTime_To_DateKey
--
-- The scalar UDF was not inlined in the large DSA staging statements
-- (plan showed NonParallelPlanReason="TSQLUserDefinedFunctionsNotParallelizable")
-- and forced serial plans. All call sites now use the inline expression
-- CAST(CONVERT(CHAR(8), <datetime2 expr>, 112) AS INT).
-- Dependency check (sys.sql_expression_dependencies + Query Store) confirmed
-- no remaining consumers on staging or DSO.
--
-- Idempotent: safe to re-run.
-- ============================================================================

DROP FUNCTION IF EXISTS dbo.fn_DateTime_To_DateKey;
