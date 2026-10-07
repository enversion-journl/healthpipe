-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG presentation views: code dimensions (vw_dim.*)
--
-- The code dimensions are CPR-free and hold only SKS codes + surrogate keys,
-- so they are plain pass-through views. They are append-only (no dw_is_deleted).
--
-- dim.DRGKontaktEgenskaber is NOT here: it carries the delivery's demografi
-- (alder, koen, bopaelskommune) and its DPA-secured view is owned by the
-- separate security-views repo, not this presentation layer.
-- ============================================================================

CREATE OR ALTER VIEW [vw_dim].[DRGKontaktKlassifikation] AS
SELECT * FROM [dim].[DRGKontaktKlassifikation];
GO

CREATE OR ALTER VIEW [vw_dim].[DRGDiagnose] AS
SELECT * FROM [dim].[DRGDiagnose];
GO

CREATE OR ALTER VIEW [vw_dim].[DRGProcedure] AS
SELECT * FROM [dim].[DRGProcedure];
GO
