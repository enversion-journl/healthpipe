-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: fhir.Condition -> one row per condition, diagnosis code only
--
-- WHY: condition_view flattened `forEach: code.coding`, so a Condition with N
-- codings became N rows in [fhir].[Condition] sharing condition_id. SKS
-- supplementary codes (tillaegskoder) — many are ATC medication codes (e.g.
-- MB01AC06 acetylsalicylsyre) attached to a drug-allergy diagnosis — landed as
-- standalone rows indistinguishable from real diagnoses, and the composite PK
-- (condition_id, code, code_system) tripped the resource_id uniqueness DQA
-- check (~40k false-positive "violations").
--
-- AFTER: [fhir].[Condition] is one row per condition (PK: condition_id) carrying
-- ONLY the primary diagnosis coding. Going forward condition_view selects it via
-- code.coding.where(userSelected = true) (the builder marks the diagnosis coding
-- userSelected=true), so no supplementary/medication code can appear here.
-- Medication/allergen codes are NOT projected to the DWH; they remain on the
-- Aidbox Condition resource for a future AllergyIntolerance model.
--
-- ROLLOUT NOTE: the userSelected marker only exists on RE-POSTED Condition
-- resources. Re-materialise fhir_diagnose_*/fhir_pas_diagnose_* (re-post) and
-- then sof_condition_* so the view populates from the marked coding. This
-- migration performs the one-time historical cleanup of the existing flat table
-- (below) so the DWH is already correct before the re-post completes; it prefers
-- the SKS 'D...' diagnosis code when collapsing legacy rows.
--
-- Idempotent: safe to re-run on staging and DSO. On a fresh DB (01_tables.sql
-- already creates the end-state), every guarded step is a no-op.
--
-- Rollback (manual; no automatic down-migration for FHIR DDL in this repo):
--     ALTER TABLE [fhir].[Condition] DROP CONSTRAINT [PK_FHIR_Condition];
--     ALTER TABLE [fhir].[Condition] ALTER COLUMN [code] NVARCHAR(255) NOT NULL;
--     ALTER TABLE [fhir].[Condition] ALTER COLUMN [code_system] NVARCHAR(500) NOT NULL;
--     ALTER TABLE [fhir].[Condition] ADD CONSTRAINT [PK_FHIR_Condition]
--         PRIMARY KEY CLUSTERED ([condition_id], [code], [code_system]);
--   (then re-materialise sof_condition_* to repopulate the fan-out)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1) Drop the OLD composite PK (condition_id, code, code_system) if present.
--    Guard on key-column count so re-runs (single-column PK) skip this.
-- ---------------------------------------------------------------------------
IF EXISTS (
    SELECT 1
    FROM sys.key_constraints kc
    WHERE kc.name = N'PK_FHIR_Condition'
      AND kc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]')
      AND (
          SELECT COUNT(*) FROM sys.index_columns ic
          WHERE ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
      ) > 1
)
BEGIN
    ALTER TABLE [fhir].[Condition] DROP CONSTRAINT [PK_FHIR_Condition];
END
GO

-- ---------------------------------------------------------------------------
-- 2) Collapse to one row per condition_id, keeping the diagnosis coding.
--    Prefer an SKS 'D...' diagnosis code over supplementary codes so the
--    historical primary is picked deterministically; the authoritative value is
--    refreshed on the next sof_condition_* run (userSelected). No-op once unique.
--    The dropped rows are the supplementary/medication codes — intentionally not
--    retained in the DWH.
-- ---------------------------------------------------------------------------
;WITH ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            PARTITION BY [condition_id]
            ORDER BY CASE WHEN [code] LIKE N'D%' THEN 0 ELSE 1 END, [code], [code_system]
        ) AS rn
    FROM [fhir].[Condition]
)
DELETE FROM ranked WHERE rn > 1;
GO

-- ---------------------------------------------------------------------------
-- 3) Drop the inline DEFAULT (N'') constraints on code/code_system so the
--    columns can become NULLable (primary coding may be absent). Names are
--    auto-generated, so resolve them from metadata.
-- ---------------------------------------------------------------------------
DECLARE @df_code sysname = (
    SELECT dc.name FROM sys.default_constraints dc
    JOIN sys.columns col ON col.object_id = dc.parent_object_id AND col.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND col.name = N'code'
);
IF @df_code IS NOT NULL
    EXEC('ALTER TABLE [fhir].[Condition] DROP CONSTRAINT [' + @df_code + ']');

DECLARE @df_sys sysname = (
    SELECT dc.name FROM sys.default_constraints dc
    JOIN sys.columns col ON col.object_id = dc.parent_object_id AND col.column_id = dc.parent_column_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND col.name = N'code_system'
);
IF @df_sys IS NOT NULL
    EXEC('ALTER TABLE [fhir].[Condition] DROP CONSTRAINT [' + @df_sys + ']');
GO

-- ---------------------------------------------------------------------------
-- 4) Make code/code_system NULLable (end-state matches 01_tables.sql).
-- ---------------------------------------------------------------------------
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND name = N'code' AND is_nullable = 0
)
    ALTER TABLE [fhir].[Condition] ALTER COLUMN [code] NVARCHAR(255) NULL;
GO
IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fhir].[Condition]') AND name = N'code_system' AND is_nullable = 0
)
    ALTER TABLE [fhir].[Condition] ALTER COLUMN [code_system] NVARCHAR(500) NULL;
GO

-- ---------------------------------------------------------------------------
-- 5) Add the new single-column PK (condition_id) if the table has no PK.
-- ---------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT 1 FROM sys.key_constraints
    WHERE parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND type = 'PK'
)
BEGIN
    ALTER TABLE [fhir].[Condition]
        ADD CONSTRAINT [PK_FHIR_Condition] PRIMARY KEY CLUSTERED ([condition_id]);
END
GO
