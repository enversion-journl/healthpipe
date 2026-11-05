-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: KMD UdData (UNSPSC + SpendAnalysisLines)
--
-- 1:1 reflection of the KMD Indkøbsanalyse source views.
-- Delta-loaded via audit-MAX watermark; UPSERT-merged on the entity key.
-- See DSI/mssql/archive/kmd_uddata_archive_configs.py
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.kmd_uddata_unspsc  (1:1 from KMD dimension.UNSPSC)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'kmd_uddata_unspsc'
)
BEGIN
    CREATE TABLE [arc].[kmd_uddata_unspsc] (
        -- Source columns (1:1 from KMD dimension.UNSPSC)
        [DW_SK_UNSPSC]                  INT             NOT NULL,
        [DW_EK_UNSPSC]                  INT             NOT NULL,
        [UNSPSCClassID]                 NVARCHAR(100)   NULL,
        [UNSPSCClassDescription]        NVARCHAR(400)   NULL,
        [UNSPSCLevel]                   SMALLINT        NULL,
        [TenderCategoryKey]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev1ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev1Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev1Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev2ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev2Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev2Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev3ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev3Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev3Label]          NVARCHAR(502)   NULL,
        [UNSPSCClassLev4ID]             NVARCHAR(100)   NULL,
        [UNSPSCClassLev4Description]    NVARCHAR(400)   NULL,
        [UNSPSCClassLev4Label]          NVARCHAR(502)   NULL,
        [DW_ArchiveID]                  BIGINT          NULL,
        [DW_ValidFrom]                  DATETIME2       NULL,
        [DW_ValidTo]                    DATETIME2       NULL,
        [DW_IsCurrent]                  BIT             NULL,
        [DW_Audit_Insert]               INT             NULL,
        [DW_Audit_Update]               INT             NULL,
        [DW_Audit]                      INT             NULL,

        -- DWH audit columns
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,

        -- Persisted MAX of the three audit columns; backs the watermark lookup.
        -- Inline CASE: PERSISTED computed columns cannot reference UDFs; native
        -- GREATEST() is unavailable on SQL Server 2019 anyway.
        [computed_max_audit] AS (
            CASE
                WHEN [DW_Audit_Insert] IS NULL
                 AND [DW_Audit_Update] IS NULL
                 AND [DW_Audit] IS NULL
                    THEN NULL
                WHEN [DW_Audit_Insert] >= COALESCE([DW_Audit_Update], [DW_Audit_Insert])
                 AND [DW_Audit_Insert] >= COALESCE([DW_Audit], [DW_Audit_Insert])
                    THEN [DW_Audit_Insert]
                WHEN [DW_Audit_Update] >= COALESCE([DW_Audit], [DW_Audit_Update])
                    THEN [DW_Audit_Update]
                ELSE [DW_Audit]
            END
        ) PERSISTED,

        -- Source is SCD Type 2: DW_SK_UNSPSC is unique per row (one per version),
        -- DW_EK_UNSPSC is the entity key and may repeat across versions.
        CONSTRAINT [PK_arc_kmd_uddata_unspsc]
            PRIMARY KEY CLUSTERED ([DW_SK_UNSPSC])
    );
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_unspsc]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_unspsc_computed_max_audit'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_unspsc]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_unspsc_computed_max_audit]
        ON [arc].[kmd_uddata_unspsc] ([computed_max_audit] DESC);
END
GO

IF OBJECT_ID(N'[arc].[kmd_uddata_unspsc]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_kmd_uddata_unspsc_dw_ek_unspsc'
          AND object_id = OBJECT_ID(N'[arc].[kmd_uddata_unspsc]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_kmd_uddata_unspsc_dw_ek_unspsc]
        ON [arc].[kmd_uddata_unspsc] ([DW_EK_UNSPSC]);
END
GO
