-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Codesystem delivery sets for OS2mo: export.CodesystemToOs2mo
--
-- The codes built for delivery to Magenta's OS2mo, per codesystem, as they
-- stand right now. Written by the os2mo_<codesystem>_export assets from the same
-- rows that are rendered into the uploaded CSV file. Whether that file was
-- delivered is in meta.DFMRunLog (schema_name 'os2mo').
--
-- Grain: one row per (codesystem, user_key). The columns carry the same names
-- as the file's — OS2mo's own vocabulary — so a query reads like the file.
--
-- REPLACED EVERY RUN, per codesystem — DELETE + INSERT, no history. A delivery
-- set is state, not an archive. The run history lives in meta.DFMRunLog under
-- schema_name 'export' (building the set) and 'os2mo' (uploading the file).
--
-- Columns are typed rather than stringly: the table exists to be queried.
-- created / updated are the warehouse's timestamps for the code — see
-- utils/os2mo/codesystem_export.py for exactly what moves updated.
-- mappings is a JSON array of {code, system, relationship} — the pre-mapped
-- codes the file carries as map_code_N/map_system_N/map_relation_N.
--
-- Scope: STAGING only. Its own folder means only staging_infra_export executes
-- it, so no DB_NAME guard is needed: DSO never runs this file.
-- ============================================================================

-- The export schema is created here rather than in common/00_schemas.sql, the
-- same way tables/dqa/01_results.sql owns the dqa schema.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'export')
    EXEC (N'CREATE SCHEMA [export]');
GO

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[export].[CodesystemToOs2mo]') AND type = 'U'
)
BEGIN
    CREATE TABLE [export].[CodesystemToOs2mo] (
        [codesystem]          NVARCHAR(50)  NOT NULL,
        [user_key]            NVARCHAR(100) NOT NULL,
        [parent_user_key]     NVARCHAR(100) NULL,
        [validity_from]       DATE          NULL,
        [validity_to]         DATE          NULL,
        [name]                NVARCHAR(500) NULL,
        [org_unit_type]       NVARCHAR(500) NULL,
        [active]              BIT           NULL,
        [deleted]             BIT           NULL,
        [created]             DATETIME2(0)  NULL,
        [updated]             DATETIME2(0)  NULL,
        [mappings]            NVARCHAR(MAX) NULL,
        [dw_timestamp_insert] DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_export_CodesystemToOs2mo] PRIMARY KEY CLUSTERED ([codesystem], [user_key])
    );
END
GO
