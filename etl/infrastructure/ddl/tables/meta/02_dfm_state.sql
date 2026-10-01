-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DFM State
-- Used by DfmManager in core.dfm_manager
--
--   meta.DFMState — current checkpoint per (scope, target_type, schema, table).
--                     Only checkpoint-owner schemas (arc, dim, fct, fhir,
--                     fhir_server, dfm, ref) have entries here.
--
-- checkpoint_value is NVARCHAR(50) so it can hold a stringified int
-- (audit-MAX delta) or an ISO datetime (timestamp delta).
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'DFMState'
)
BEGIN
    CREATE TABLE [meta].[DFMState] (
        [scope]             NVARCHAR(50)   NOT NULL
            CONSTRAINT [DF_DFMState_scope] DEFAULT ('staging'),
        [target_type]       NVARCHAR(50)   NOT NULL,
        [schema_name]       NVARCHAR(128)  NOT NULL,
        [table_name]        NVARCHAR(128)  NOT NULL,
        [checkpoint_value]  NVARCHAR(50)   NULL,
        [last_mode]         NVARCHAR(50)   NULL,
        [updated_at]        DATETIME2      NOT NULL
            CONSTRAINT [DF_DFMState_updated_at] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT [PK_DFMState]
            PRIMARY KEY CLUSTERED ([scope], [target_type], [schema_name], [table_name])
    );
END
GO
