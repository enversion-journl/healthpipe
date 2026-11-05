-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20261027_fhir_encounter_child_backfill_marker_delete
--
-- Sletter efterslæbets markør (meta.DFMState: fhir_server /
-- Encounter_ChildBackfill). 20260929_fhir_encounter_child_delta_reset stemplede
-- den DONE, og det flagstyrede efterslæb, der læste den, er fjernet fra
-- fhir_server_encounter_dfm. Child-vandmærket Encounter_ChildDelta røres ikke.
--
-- Idempotent: en gentagen DELETE på et tomt sæt er en no-op. Samme
-- migrationsmappe kører mod både staging og DSO, og 20260911 kan have indsat
-- rækken på begge, så filen sletter den på begge. OBJECT_ID-gaten gør filen til
-- en no-op på en database uden tabellen.
-- ============================================================================

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[DFMState]
    WHERE [scope] = 'staging'
      AND [target_type] = 'fhir_resource'
      AND [schema_name] = 'fhir_server'
      AND [table_name] = 'Encounter_ChildBackfill';
END
GO
