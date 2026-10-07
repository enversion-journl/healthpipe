-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG presentation views: group->code bridges (vw_bridge.*)
--
-- Append-only membership, surrogate keys only (CPR-free) — plain pass-through.
-- A consumer answers "activity for a given SKS procedure code" by filtering
-- vw_dim.DRGProcedure on procedure_kode, joining vw_bridge.DRGProcedureGruppe
-- to get the group keys, and joining vw_fct.DRGKontakt on
-- dw_ek_DRGProcedureGruppe. The (-1,-1[,-1]) seed row is the empty-group
-- sentinel and is kept so the empty-group join resolves.
-- ============================================================================

CREATE OR ALTER VIEW [vw_bridge].[DRGDiagnoseGruppe] AS
SELECT * FROM [bridge].[DRGDiagnoseGruppe];
GO

CREATE OR ALTER VIEW [vw_bridge].[DRGProcedureGruppe] AS
SELECT * FROM [bridge].[DRGProcedureGruppe];
GO
