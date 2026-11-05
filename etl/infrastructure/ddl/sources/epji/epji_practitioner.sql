-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- EPJI practitioner archive: arc.epji_practitioner
--
-- One row per EPJ account (PRACTITIONER.PK).
-- 1:1 copy of the EPJI table, loaded from full snapshots (ext.epji_practitioner). The MERGE
-- runs with detect_deletes = true: a row absent from the latest snapshot is
-- soft-deleted (dw_is_deleted = 1) and un-deleted if it reappears.
-- Merge key is the source PK.
--
-- A row is an EPJ account, not a person: one person can hold several accounts.
-- PRACTITIONER.STATE 1 = active, 0 = inactive. An inactive account is still in
-- the source, so it is NOT soft-deleted here.
--
-- Every column is NVARCHAR, because the extract stringifies every Oracle value.
-- EXTERNAL_ID keeps the source casing. The database default collation is case
-- insensitive, so a join that must tell case variants apart needs an explicit
-- COLLATE ..._CS_AS.
--
-- Scope: STAGING only. This file lives under sources/epji/, executed only by
-- staging_infra_source_epji, so DSO never runs it.
--
-- Contains names (NAME, GIVEN_NAME, FAMILY_NAME). Never log field values from
-- this table.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[epji_practitioner]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[epji_practitioner] (
        [PK]                   NVARCHAR(50)  NOT NULL,
        [VERSION]              NVARCHAR(50)  NULL,
        [LAST_UPDATE_DATE]     NVARCHAR(50)  NULL,
        [EXTERNAL_ID]          NVARCHAR(300) NULL,
        [STATE]                NVARCHAR(50)  NULL,
        [RELATED_TO_GUID]      NVARCHAR(40)  NULL,
        [NAME]                 NVARCHAR(300) NULL,
        [GIVEN_NAME]           NVARCHAR(100) NULL,
        [FAMILY_NAME]          NVARCHAR(200) NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_arc_epji_practitioner_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_epji_practitioner] PRIMARY KEY CLUSTERED ([PK])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epji_practitioner_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[epji_practitioner]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epji_practitioner_dw_timestamp_update]
        ON [arc].[epji_practitioner] ([dw_timestamp_update] DESC);
END
GO
