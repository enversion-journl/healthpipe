-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- fhir.Patient_Address -- period columns become DATETIME2(0)
--
-- WHY: address_period_start / _end were NVARCHAR(50) holding the raw FHIR
-- dateTime, while every consumer keys on an instant. String -> datetime2(0) is
-- many-to-one ('' vs '1900-01-01', timezone offsets, sub-second precision,
-- format variants), which made the key both ambiguous AND unstable:
--
--   * ambiguous -- ref collapsed colliding rows to one while dim.Borger joined
--     back on a TRY_CAST and matched BOTH, producing two source rows for one
--     dw_sk_Borger -> PK_Dim_Borger violation.
--   * unstable -- a citizen going from "no address" ('') to "has an address"
--     ('1900-01-01') CHANGED the key, so the MERGE inserted a second row and
--     stranded the first. The SoF MERGE has no delete branch, so it stayed.
--
-- Storing the normalised instant directly removes both: one value per period, a
-- stable key, and no cast in any join. It also retires the '' sentinel, which only
-- ever existed because a string primary key cannot hold NULL. FHIR fidelity is not
-- lost -- Aidbox holds the versioned resource.
--
-- This is the ONLY migration needed for the change. An earlier attempt on this
-- branch carried a derived address_period_start_ts column alongside the raw string
-- and keyed on that, across four additional migrations. Those were scaffolding for
-- an additive rollout, and every one of their effects is either discarded by the
-- drop below (the derived column, its helper and unique indexes, an orphan-row
-- cleanup) or already declared in the base DDL (dw_is_deleted INT NOT NULL DEFAULT
-- 0). They were removed rather than merged, so environments that never ran them
-- reach the same end state from this file alone.
--
-- HOW: fhir.Patient_Address is a pure projection of the FHIR server, so the
-- simplest correct migration is to drop it and let staging_infra_fhir recreate
-- it from the base DDL (staging_migrations runs first, see the asset's deps),
-- then reload with sof_patient_address_dfm in full_load mode. No in-place type
-- change, no data migration, and -- importantly -- no risk of a SQL-side CAST
-- disagreeing with normalize_fhir_datetime, which truncates sub-seconds where
-- T-SQL rounds and resolves offsets to Europe/Copenhagen.
--
-- RUNS ON BOTH DATABASES. staging_migrations and dso_migrations execute the same
-- migrations/ directory, and the drop is wanted on both: DSO keeps its own
-- fhir.Patient_Address built from this same base DDL, and dso_sync refuses to run
-- while the two schemas differ ("Schema mismatch detected"). Order per database:
-- migrations -> infra (recreates the table) -> load/sync.
--
-- AFTER THIS MIGRATION THE TABLE IS EMPTY until sof_patient_address_dfm has run.
-- Run it before ref_patient_address_borger / dim_Borger, otherwise ref will
-- soft-delete every version it can no longer see. On DSO the watermark sync
-- refills the table on its own: the reload gives every staging row a fresh
-- dw_timestamp_update, so all of them are newer than any existing watermark.
--
-- The DFM checkpoints are reset in the same migration, deliberately: dropping the
-- table without resetting them would leave an empty table plus a checkpoint dated
-- today, so an ordinary delta run would reload almost nothing and the emptiness
-- would look like a successful run. The two must happen together or not at all.
-- This file resets them to '1900-01-01', which still takes the delta path
-- (full_load is partition_start is None). The follow-up
-- 20260813a_borger_checkpoint_null_and_protection_flag.sql resets the same
-- checkpoints — plus fhir/Patient — to NULL so the first scheduled run is a
-- true full_load and address_protection backfills for dormant citizens.
--
-- All three checkpoints in the chain are reset, not just the SoF one:
--   fhir.Patient_Address          -- the table is empty; delta would skip it
--   ref.Patient_Address_To_Borger -- its delta reads fhir.Patient_Address
--     last_updated, which is meta.lastUpdated from the resource and therefore
--     UNCHANGED by the reload. With today's checkpoint ref would find no changed
--     patients and quietly no-op, leaving its rows derived from the old table.
--   dim.Borger                    -- its delta reads ref's timestamps, which only
--     bump where a row actually differs, so a stale checkpoint could leave dim
--     holding rows from the failed runs.
--
-- The stranded rows need no separate cleanup: they go with the table, and the
-- stranding mechanism itself -- a key that changed when a citizen's address status
-- flipped -- is what this migration removes.
--
-- Idempotent: guarded on the column still being a character type.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1
       FROM sys.columns c
       INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
       WHERE c.object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
         AND c.name = N'address_period_start'
         AND t.name IN (N'nvarchar', N'varchar', N'nchar', N'char')
   )
BEGIN
    DECLARE @rows BIGINT = (SELECT COUNT_BIG(*) FROM [fhir].[Patient_Address]);

    DROP TABLE [fhir].[Patient_Address];

    PRINT CONCAT(
        'Dropped fhir.Patient_Address (', @rows, ' row(s)) so it can be recreated ',
        'with DATETIME2(0) period columns. staging_infra_fhir recreates it; then ',
        'run sof_patient_address_dfm BEFORE ref/dim.'
    );
END
GO

-- ----------------------------------------------------------------------------
-- Reset the DFM checkpoints for the whole Borger chain, so a plain delta run
-- rebuilds from scratch. See the header for why all three, not just the SoF one.
--
-- Via meta.usp_dfm_reset rather than an UPDATE against meta.DFMState, per
-- docs/implementation/dfm_operations_guide.md section 5. The procedure also sets
-- last_mode = 'reset' (an UPDATE would leave the row claiming 'delta'), writes an
-- audit row to meta.DFMRunLog with checkpoint_before/after and the reason, and
-- keeps the schema -> target_type mapping in one place instead of hardcoding
-- 'fhir_table' / 'reference' / 'dimension' here, where it could drift from
-- SCHEMA_TO_TARGET_TYPE and the procedure's own CASE.
--
-- This file runs on BOTH databases (staging_migrations and dso_migrations share
-- the migrations/ directory), and the drop is wanted on both -- DSO keeps its own
-- fhir.Patient_Address from the same base DDL, and the sync refuses to run while
-- the two schemas differ. The checkpoint reset, however, is a staging concern:
-- DFM state is written and read against staging only. Each reset is therefore
-- guarded on the checkpoint row already existing, which makes it a no-op on DSO
-- rather than MERGE-inserting rows nothing reads and writing misleading entries
-- into DSO's meta.DFMRunLog. "Reset an existing checkpoint", not "invent one".
--
-- Fails loudly if there ARE checkpoints to reset but the procedure is missing:
-- silently skipping the reset would leave an empty table next to a checkpoint
-- dated today, which is the one outcome this migration exists to prevent.
-- ----------------------------------------------------------------------------
-- Everything touching meta.DFMState / meta.usp_dfm_reset goes through dynamic SQL.
-- DSO has no DFM tables at all, and a static reference to a missing object risks a
-- compile-time "Invalid object name" for the whole batch even when the guarding IF
-- is false -- deferred name resolution is reliable for stored procedures, not for
-- ad-hoc batches. sp_executesql defers the reference until the string is actually
-- run.
-- Guard on the objects alone -- OBJECT_ID is safe on any database. Reaching this
-- state means someone dropped the procedure but kept the state table, which the
-- reset cannot silently skip.
IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50011,
        N'meta.DFMState exists but meta.usp_dfm_reset does not - materialise staging_infra_dfm before this migration. Skipping the checkpoint reset would leave an empty fhir.Patient_Address with a current checkpoint.',
        1;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'No meta.DFMState on this database (expected on DSO) - no checkpoints to reset.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''fhir.Patient_Address rebuilt with DATETIME2(0) period columns; full reload of the Borger chain required.'';
        DECLARE @reset INT = 0;

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''fhir'' AND [table_name] = ''Patient_Address'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''fhir'', @table_name = ''Patient_Address'',
                 @new_value = ''1900-01-01'', @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''ref'' AND [table_name] = ''Patient_Address_To_Borger'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''ref'', @table_name = ''Patient_Address_To_Borger'',
                 @new_value = ''1900-01-01'', @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [schema_name] = ''dim'' AND [table_name] = ''Borger'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''dim'', @table_name = ''Borger'',
                 @new_value = ''1900-01-01'', @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        PRINT CONCAT(''Reset '', @reset, '' DFM checkpoint(s) for the Borger chain.'');';
GO
