-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- EPJI practitioner identifier archive: arc.epji_practitioner_identifier
--
-- One row per (PRACTITIONER_PK, SYSTEM).
-- 1:1 copy of the EPJI table, loaded from full snapshots (ext.epji_practitioner_identifier). The MERGE
-- runs with detect_deletes = true: a row absent from the latest snapshot is
-- soft-deleted (dw_is_deleted = 1) and un-deleted if it reappears.
--
-- Keyed on (PRACTITIONER_PK, SYSTEM), not PK: EPJ rewrites every identifier row
-- of an account with a new PK each time the account is saved. PK is kept as an
-- ordinary column holding the current source PK. The pair is unique in the
-- source but not enforced there, so a duplicate fails the MERGE on the primary
-- key. SYSTEM is NVARCHAR(200) to keep the clustered key within 900 bytes; the
-- source has four systems of at most 32 characters.
--
-- Every column is NVARCHAR, because the extract stringifies every Oracle value.
--
-- Scope: STAGING only. This file lives under sources/epji/, executed only by
-- staging_infra_source_epji, so DSO never runs it.
--
-- Contains CPR (VALUE). Never log field values from this table.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[epji_practitioner_identifier]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[epji_practitioner_identifier] (
        [PK]                   NVARCHAR(50)  NOT NULL,
        [PRACTITIONER_PK]      NVARCHAR(50)  NOT NULL,
        [SYSTEM]               NVARCHAR(200) NOT NULL,
        [VALUE]                NVARCHAR(500) NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_arc_epji_practitioner_identifier_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_epji_practitioner_identifier] PRIMARY KEY CLUSTERED ([PRACTITIONER_PK], [SYSTEM])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epji_practitioner_identifier_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[epji_practitioner_identifier]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epji_practitioner_identifier_dw_timestamp_update]
        ON [arc].[epji_practitioner_identifier] ([dw_timestamp_update] DESC);
END
GO
