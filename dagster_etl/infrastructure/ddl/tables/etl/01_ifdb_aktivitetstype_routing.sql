-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- IFDB Aktivitetstype Routing
-- Observation allowlist: explicit route for which IFDB AKTIVITETSRESULTAT rows
-- may produce FHIR Observation. Missing/non-approved rows default to
-- QuestionnaireResponse in runtime.
-- Only rows with route_status='active' resolve operationally. review/inactive
-- routes are curation metadata and never route at runtime (they fall through
-- to the QuestionnaireResponse fallback).
-- Seeded from the repo-controlled YAML via staging_infra_etl_routing_seed.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'ifdb_aktivitetstype_routing'
)
BEGIN
    CREATE TABLE [meta].[ifdb_aktivitetstype_routing] (
        [aktivitetstype_kode]   NVARCHAR(255)   NOT NULL,
        [target_resource]       NVARCHAR(32)    NOT NULL,
        [observation_category]  NVARCHAR(64)    NULL,
        [route_status]          NVARCHAR(32)    NOT NULL
            CONSTRAINT [DF_ifdb_routing_status] DEFAULT ('review'),
        [rationale]             NVARCHAR(MAX)   NULL,
        [decided_by]            NVARCHAR(255)   NULL,
        [decided_at]            DATETIME2       NULL,
        [active_from]           DATE            NOT NULL,
        [active_to]             DATE            NULL,
        [DW_Timestamp_Insert]   DATETIME2       NOT NULL
            CONSTRAINT [DF_ifdb_routing_insert] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        [DW_Timestamp_Update]   DATETIME2       NULL,

        CONSTRAINT [PK_ifdb_aktivitetstype_routing]
            PRIMARY KEY CLUSTERED ([aktivitetstype_kode], [active_from]),

        CONSTRAINT [CK_ifdb_routing_target_resource]
            CHECK ([target_resource] IN ('Observation', 'QuestionnaireResponse')),

        CONSTRAINT [CK_ifdb_routing_route_status]
            CHECK ([route_status] IN ('active', 'inactive', 'review'))
    );
END
GO

-- ============================================================================
-- IFDB Aktivitetsresultat Routing Decision Audit
-- One row per source AKTIVITETSRESULTAT when the dispatcher records the actual
-- operational route used for that row.  The Observation allowlist remains the
-- preferred rule source; this table exposes whether a concrete row used the
-- allowlist or defaulted to QR so downstream views can stay transparent.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'ifdb_aktivitetsresultat_routing_decision'
)
BEGIN
    CREATE TABLE [meta].[ifdb_aktivitetsresultat_routing_decision] (
        [ifdb_aktivitet_id]     NVARCHAR(255) NOT NULL,
        [aktivitetstype_kode]   NVARCHAR(255) NULL,
        [target_resource]       NVARCHAR(32)  NOT NULL,
        [observation_category]  NVARCHAR(64)  NULL,
        [routing_source]        NVARCHAR(50)  NOT NULL,
        [routing_confidence]    NVARCHAR(32)  NULL,
        [routing_reason]        NVARCHAR(MAX) NULL,
        [decided_at]            DATETIME2     NOT NULL
            CONSTRAINT [DF_ifdb_route_decision_decided_at] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        [DW_Timestamp_Insert]   DATETIME2     NOT NULL
            CONSTRAINT [DF_ifdb_route_decision_insert] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        [DW_Timestamp_Update]   DATETIME2     NULL,

        CONSTRAINT [PK_ifdb_aktivitetsresultat_routing_decision]
            PRIMARY KEY CLUSTERED ([ifdb_aktivitet_id]),

        CONSTRAINT [CK_ifdb_route_decision_target_resource]
            CHECK ([target_resource] IN ('Observation', 'QuestionnaireResponse', 'Procedure')),

        CONSTRAINT [CK_ifdb_route_decision_source]
            CHECK ([routing_source] IN ('routing_table', 'xml_fallback', 'procedure_code', 'manual'))
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[meta].[ifdb_aktivitetsresultat_routing_decision]')
      AND name = 'IX_ifdb_route_decision_target_source'
)
AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[meta].[ifdb_aktivitetsresultat_routing_decision]')
      AND type = 'U'
)
BEGIN
    EXEC('
        CREATE NONCLUSTERED INDEX [IX_ifdb_route_decision_target_source]
        ON [meta].[ifdb_aktivitetsresultat_routing_decision]
            ([target_resource], [routing_source])
        INCLUDE ([ifdb_aktivitet_id], [aktivitetstype_kode], [routing_confidence])
    ');
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[meta].[ifdb_aktivitetstype_routing]')
      AND name = 'IX_ifdb_routing_status_target'
)
AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[meta].[ifdb_aktivitetstype_routing]')
      AND type = 'U'
)
BEGIN
    EXEC('
        CREATE NONCLUSTERED INDEX [IX_ifdb_routing_status_target]
        ON [meta].[ifdb_aktivitetstype_routing]
            ([route_status], [target_resource])
        INCLUDE ([aktivitetstype_kode], [active_from], [active_to])
    ');
END
GO
