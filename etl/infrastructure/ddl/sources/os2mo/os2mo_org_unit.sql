-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- OS2mo organisation archive: arc.os2mo_org_unit
--
-- Persistent archive of what Magenta's OS2mo actually holds (ext.os2mo_org_unit).
-- This is the RETURN leg of the OS2mo integration: we publish the region's
-- SOR organisation to OS2mo, and read the result — including
-- whatever Magenta enrich it with — back into the warehouse.
--
-- Grain: one row per org unit VERSION — (uuid, validity_from). The uuid is
-- assigned by OS2mo, not by us; user_key carries our SOR code, which is what
-- makes a row joinable to codesystem.SOR*. It is stored cleaned of the Excel
-- wrapper (="<code>") the phase-1 units carry in OS2mo; user_key_raw keeps the
-- value exactly as OS2mo holds it.
--
-- Every read is a genuine full snapshot of current OS2mo state, so the MERGE
-- runs with detect_deletes = true — the same choice as arc.epji_location, and
-- the opposite of arc.sor_register (a cumulative version file). A unit absent
-- from the latest snapshot is soft-deleted (dw_is_deleted = 1) rather than
-- dropped, and reappears automatically if OS2mo returns it again. The read keeps
-- one version per unit, so a unit's earlier version is soft-deleted too when
-- OS2mo gives it a new one: dw_is_deleted = 1 means "not the version OS2mo
-- holds now", not only "removed from OS2mo".
--
-- Downstream consumers read only the live rows:
--   WHERE dw_is_deleted = 0
--
-- Scope: STAGING only. Its own folder means only staging_infra_source_os2mo
-- executes it (same pattern as sources/epji/ and sources/ifdb/),
-- so no DB_NAME guard is needed: DSO never runs this file.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[os2mo_org_unit]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[os2mo_org_unit] (
        [uuid]                 NVARCHAR(50)  NOT NULL,
        [validity_from]        NVARCHAR(50)  NOT NULL,
        [validity_to]          NVARCHAR(50)  NULL,
        [user_key]             NVARCHAR(100) NULL,
        [user_key_raw]         NVARCHAR(100) NULL,
        [name]                 NVARCHAR(500) NULL,
        [parent_uuid]          NVARCHAR(50)  NULL,
        [parent_user_key]      NVARCHAR(100) NULL,
        [org_unit_type_uuid]   NVARCHAR(50)  NULL,
        [org_unit_type_name]   NVARCHAR(500) NULL,
        [dw_is_deleted]        BIT           NOT NULL
            CONSTRAINT [DF_arc_os2mo_org_unit_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_os2mo_org_unit] PRIMARY KEY CLUSTERED ([uuid], [validity_from])
    );

    -- user_key is how a row is matched back to the SOR code we published.
    CREATE NONCLUSTERED INDEX [IX_arc_os2mo_org_unit_user_key]
        ON [arc].[os2mo_org_unit] ([user_key])
        INCLUDE ([uuid], [name], [dw_is_deleted]);
END
GO
