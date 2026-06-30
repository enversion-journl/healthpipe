-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: reset SydAX arc dfm_state checkpoints after the per-table delta
-- strategy change (full load / RdwTs / Oprettelsestidspunkt / GREATEST), so the
-- old checkpoints (built from COALESCE([Modified],[CreatedOn])) don't apply a
-- wrong delta window against the new delta columns. Deleting the rows makes the
-- next extract a full first-run pull; the arc merge then re-seeds the checkpoint
-- (flow_start for the delta tables; full-load tables ignore it).
-- Idempotent + tracked once.

IF OBJECT_ID(N'[meta].[dfm_state]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[dfm_state]
    WHERE [scope] = 'staging'
      AND [schema_name] = 'arc'
      AND [table_name] IN (
            'sydax_afdeling',
            'sydax_art',
            'sydax_dranst',
            'sydax_finanskladde',
            'sydax_finanskladdelinje',
            'sydax_finanskonto',
            'sydax_finanskonto_omkostningssted',
            'sydax_finanspostering',
            'sydax_finanspostering_kreditortrans_relation',
            'sydax_finanstransaktionstype',
            'sydax_funktion',
            'sydax_gruppering',
            'sydax_gruppering_autoriseret',
            'sydax_hovedkonto',
            'sydax_kreditor',
            'sydax_kreditortransaktion',
            'sydax_omkostningssted_hierarki'
      );
END
GO
