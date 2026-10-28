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
-- Grain: one row per (codesystem, kode). The columns carry the file's agreed
-- names (kode/kode_parent/gyldig_fra/gyldig_til/navn/type/niveau/aktiv), so a
-- query reads like the file — except the three that would collide with this
-- warehouse's own audit-column names, which carry a _codesystem postfix.
-- dw_timestamp_insert, without that postfix, is this table's own build stamp.
--
-- REPLACED EVERY RUN, per codesystem — DELETE + INSERT, no history. A delivery
-- set is state, not an archive. The run history lives in meta.DFMRunLog under
-- schema_name 'export' (building the set) and 'os2mo' (uploading the file).
--
-- Columns are typed rather than stringly: the table exists to be queried.
-- dw_timestamp_insert_codesystem / dw_timestamp_update_codesystem are the
-- warehouse's timestamps for the code — see utils/os2mo/codesystem_export.py
-- for exactly what moves the latter.
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
        [codesystem]                     NVARCHAR(50)   NOT NULL,
        [kode]                           NVARCHAR(120)  NOT NULL,
        [kode_parent]                    NVARCHAR(120)  NULL,
        [gyldig_fra]                     DATE           NULL,
        [gyldig_til]                     DATE           NULL,
        [navn]                           NVARCHAR(500)  NULL,
        [type]                           NVARCHAR(500)  NULL,
        [niveau]                         INT            NOT NULL,
        [aktiv]                          BIT            NULL,
        [dw_is_deleted_codesystem]       BIT            NULL,
        [dw_timestamp_insert_codesystem] DATETIME2(0)   NULL,
        [dw_timestamp_update_codesystem] DATETIME2(0)   NULL,
        [dw_timestamp_insert]            DATETIME2(0)   NOT NULL,
        CONSTRAINT [PK_export_CodesystemToOs2mo] PRIMARY KEY CLUSTERED ([codesystem], [kode])
    );
END
GO
