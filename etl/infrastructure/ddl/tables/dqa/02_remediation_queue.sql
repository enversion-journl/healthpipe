-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- DQA remediation queue: distinct business keys of rows whose FHIR references
-- dangle (referential_integrity violations), to be fixed by REPLAYING the child
-- through the DFM "ids" mode.
--
-- A non-null dangling FK means the parent was deleted after the child was posted
-- (e.g. duplicate cleanup); re-running the child's transform re-resolves the
-- business key to the surviving parent. The per-entity feeder views (in
-- views/dqa/02_v_replay.sql) expose the pending business keys under the archive's
-- id column name, so a DFM ids-mode run can point id_filter_table at them.
--
-- Written by dqa/remediation/capture.py whenever a referential_integrity check
-- fails on a subject with a ReplaySpec. Owned by the staging_infra_dqa
-- infrastructure asset. Idempotent.
-- =============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dqa')
    EXEC ('CREATE SCHEMA [dqa]');
GO

IF OBJECT_ID(N'[dqa].[RemediationQueue]', N'U') IS NULL
    CREATE TABLE [dqa].[RemediationQueue] (
        [queue_id]             BIGINT IDENTITY(1,1) PRIMARY KEY,
        [detected_at]          DATETIME2(0)  NOT NULL CONSTRAINT [DF_RemediationQueue_detected_at] DEFAULT SYSUTCDATETIME(),
        [run_id]               NVARCHAR(64)  NULL,
        [environment]          NVARCHAR(128) NULL,
        [track]                NVARCHAR(16)  NOT NULL,
        [subject]              NVARCHAR(128) NOT NULL,  -- e.g. fhir_encounter
        [resource_type]        NVARCHAR(128) NOT NULL,  -- e.g. Encounter
        [reference_column]     NVARCHAR(128) NOT NULL,  -- e.g. service_provider_id
        [target]               NVARCHAR(256) NULL,      -- e.g. fhir.Organization.organization_id
        [business_key]         NVARCHAR(255) NULL,      -- OBJECT_ID/PATIENT_ID (NULL = overflow marker)
        [replay_entity]        NVARCHAR(128) NOT NULL,  -- arc base table, e.g. ifdb_encounter
        [replay_filter_column] NVARCHAR(128) NOT NULL,  -- DFM id_filter_column, e.g. OBJECT_ID
        [status]               NVARCHAR(24)  NOT NULL CONSTRAINT [DF_RemediationQueue_status] DEFAULT 'pending',  -- pending|resolved|unresolvable|overflow
        [attempts]             INT           NOT NULL CONSTRAINT [DF_RemediationQueue_attempts] DEFAULT 0,
        [last_attempt_at]      DATETIME2(0)  NULL,
        [note]                 NVARCHAR(512) NULL
    );
GO

-- Widen [environment] on pre-existing tables (feature-branch postfixes exceed 32
-- chars; dqa_environment() truncates to 128 to match). COL_LENGTH is bytes:
-- NVARCHAR(32) -> 64, NVARCHAR(128) -> 256. No index covers the column. Idempotent.
IF COL_LENGTH(N'[dqa].[RemediationQueue]', 'environment') < 256
    ALTER TABLE [dqa].[RemediationQueue] ALTER COLUMN [environment] NVARCHAR(128) NULL;
GO

-- One open row per (subject, reference_column, business_key). The MERGE in
-- capture.py relies on this to reopen a resolved key that dangles again rather
-- than inserting a duplicate.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UX_dqa_remediation_key'
      AND object_id = OBJECT_ID(N'[dqa].[RemediationQueue]')
)
    CREATE UNIQUE NONCLUSTERED INDEX [UX_dqa_remediation_key]
        ON [dqa].[RemediationQueue] ([subject], [reference_column], [business_key]);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_dqa_remediation_pending'
      AND object_id = OBJECT_ID(N'[dqa].[RemediationQueue]')
)
    CREATE NONCLUSTERED INDEX [IX_dqa_remediation_pending]
        ON [dqa].[RemediationQueue] ([replay_entity], [status]) INCLUDE ([business_key]);
GO
