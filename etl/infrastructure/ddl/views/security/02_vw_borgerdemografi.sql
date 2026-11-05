-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Demographic combinations only: no citizen keys or person-level rows.
CREATE OR ALTER VIEW [vw_dim].[BorgerDemografi]
AS
SELECT
    dw_ek_BorgerDemografi,
    region_id, regionnavn, kommune_id, kommunenavn,
    postnr, postdistrikt, sogn_id, sognenavn, koen_navn,
    foedselsaar, foedselsmaaned,
    dw_timestamp_insert, dw_timestamp_update
FROM [dim].[BorgerDemografi];
GO
