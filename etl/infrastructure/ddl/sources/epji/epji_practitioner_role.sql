-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- EPJI practitioner role archive: arc.epji_practitioner_role
--
-- Columna role catalogue, no person relation. One row per source PK.
-- 1:1 copy of the EPJI table, loaded from full snapshots (ext.epji_practitioner_role). The MERGE
-- runs with detect_deletes = true: a row absent from the latest snapshot is
-- soft-deleted (dw_is_deleted = 1) and un-deleted if it reappears.
-- Merge key is the source PK.
--
-- Every column is NVARCHAR, because the extract stringifies every Oracle value.
--
-- Scope: STAGING only. This file lives under sources/epji/, executed only by
-- staging_infra_source_epji, so DSO never runs it.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[epji_practitioner_role]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[epji_practitioner_role] (
        [PK]                   NVARCHAR(50)  NOT NULL,
        [VERSION]              NVARCHAR(50)  NULL,
        [LAST_UPDATE_DATE]     NVARCHAR(50)  NULL,
        [CODE_PK]              NVARCHAR(50)  NULL,
        [CODE]                 NVARCHAR(64)  NULL,
        [SYSTEM]               NVARCHAR(500) NULL,
        [DISPLAY]              NVARCHAR(64)  NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_arc_epji_practitioner_role_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_epji_practitioner_role] PRIMARY KEY CLUSTERED ([PK])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_epji_practitioner_role_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[epji_practitioner_role]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_epji_practitioner_role_dw_timestamp_update]
        ON [arc].[epji_practitioner_role] ([dw_timestamp_update] DESC);
END
GO
