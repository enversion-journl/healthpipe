-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20261030_dqa_rename_reconciliation_check_types
--
-- Renames two DQA check types in the stored results and the remediation queue,
-- so each name states the hop it reconciles:
--   archive_reconciliation -> source_archive_reconciliation  (source vs arc.*)
--   source_reconciliation  -> archive_fhir_reconciliation    (arc.* vs fhir.*)
--
-- dqa.v_latest and dqa_summary take the newest row per check_type, so a row
-- left under an old name would stay as a separate "latest" result forever.
--
-- dqa.RemediationQueue is unique on (subject, reference_column, business_key).
-- Where the renamed check has already captured a key under its new name, that
-- row is kept and the old-name duplicate is deleted before the rename. The
-- index treats NULL keys (overflow markers) as equal, so the match does too.
--
-- Idempotent: a rerun finds no rows under the old names. The dqa tables exist
-- only on staging; the OBJECT_ID gates make the file a no-op on DSO.
-- ============================================================================

IF OBJECT_ID(N'[dqa].[Results]', 'U') IS NOT NULL
BEGIN
    UPDATE [dqa].[Results]
    SET [check_type] = N'source_archive_reconciliation'
    WHERE [check_type] = N'archive_reconciliation';

    UPDATE [dqa].[Results]
    SET [check_type] = N'archive_fhir_reconciliation'
    WHERE [check_type] = N'source_reconciliation';
END
GO

IF OBJECT_ID(N'[dqa].[RemediationQueue]', 'U') IS NOT NULL
BEGIN
    DELETE q
    FROM [dqa].[RemediationQueue] q
    WHERE q.[reference_column] = N'archive_reconciliation'
      AND EXISTS (
          SELECT 1 FROM [dqa].[RemediationQueue] r
          WHERE r.[subject] = q.[subject]
            AND (r.[business_key] = q.[business_key]
                 OR (r.[business_key] IS NULL AND q.[business_key] IS NULL))
            AND r.[reference_column] = N'source_archive_reconciliation'
      );

    UPDATE [dqa].[RemediationQueue]
    SET [reference_column] = N'source_archive_reconciliation'
    WHERE [reference_column] = N'archive_reconciliation';

    DELETE q
    FROM [dqa].[RemediationQueue] q
    WHERE q.[reference_column] = N'source_reconciliation'
      AND EXISTS (
          SELECT 1 FROM [dqa].[RemediationQueue] r
          WHERE r.[subject] = q.[subject]
            AND (r.[business_key] = q.[business_key]
                 OR (r.[business_key] IS NULL AND q.[business_key] IS NULL))
            AND r.[reference_column] = N'archive_fhir_reconciliation'
      );

    UPDATE [dqa].[RemediationQueue]
    SET [reference_column] = N'archive_fhir_reconciliation'
    WHERE [reference_column] = N'source_reconciliation';
END
GO
