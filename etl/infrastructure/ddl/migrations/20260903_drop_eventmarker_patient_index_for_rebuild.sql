-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop IX_arc_ifdb_eventmarker_patient_id so the archive DDL rebuilds it.
--
-- The index gained [dw_is_deleted] in its INCLUDE list: the CarePlan per-patient
-- fetch filters on that column, not on the source [DELETED] column that was
-- covered before, so every seek had to return to the clustered index just to
-- evaluate the predicate.
--
-- The table DDL guards on the index NAME (IF NOT EXISTS ... sys.indexes), so a
-- database that already has the old definition keeps it forever. Dropping it
-- here lets staging_infra_ifdb_archive recreate it with the new INCLUDE — that
-- asset depends on staging_migrations, so the order is guaranteed.
--
-- On a fresh database apply_migrations() baselines this file without running it,
-- which is correct: the DDL creates the index right the first time.
--
-- DSO has no arc schema; the OBJECT_ID guard makes this a no-op there.
-- ============================================================================

IF OBJECT_ID(N'[arc].[ifdb_eventmarker]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.indexes
       WHERE name = 'IX_arc_ifdb_eventmarker_patient_id'
         AND object_id = OBJECT_ID(N'[arc].[ifdb_eventmarker]')
   )
BEGIN
    DROP INDEX [IX_arc_ifdb_eventmarker_patient_id] ON [arc].[ifdb_eventmarker];
END
GO
