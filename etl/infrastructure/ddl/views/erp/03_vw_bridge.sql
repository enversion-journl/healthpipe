-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- vw_bridge views (DSO, user-facing). Pass-through — ingen PII i bridge.*.
-- ============================================================================

-- vw_bridge.LeverandoervareILSVare
CREATE OR ALTER VIEW [vw_bridge].[LeverandoervareILSVare]
AS
SELECT *
FROM [bridge].[LeverandoervareILSVare];
GO
