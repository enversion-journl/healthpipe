-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_referralinformation  (1:1 from IFDB.REFERRALINFORMATION — 14 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_referralinformation'
)
BEGIN
    CREATE TABLE [arc].[ifdb_referralinformation] (
        [OBJECT_ID]             NVARCHAR(50)    NOT NULL,
        [REFERRAL_ORG_CODE]     NVARCHAR(512)   NULL,
        [REFERRAL_ORG_NAME]     NVARCHAR(512)   NULL,
        [REFERRAL_ORG_ORGCODE]  NVARCHAR(512)   NULL,
        [REFERRAL_ORG_SORCODE]  NVARCHAR(512)   NULL,
        [PRACTITIONER_NUMBER]   NVARCHAR(100)   NULL,
        [PRACTITIONER_NAME]     NVARCHAR(512)   NULL,
        [REFERRAL_TYPE_CODE]    NVARCHAR(512)   NULL,
        [REFERRAL_TYPE_TEXT]    NVARCHAR(512)   NULL,
        [REFERRAL_REASON_CODE]  NVARCHAR(512)   NULL,
        [REFERRAL_REASON_TEXT]  NVARCHAR(1750)  NULL,
        [FREECHOICE_CODE]       NVARCHAR(512)   NULL,
        [FREECHOICE_TEXT]       NVARCHAR(512)   NULL,
        [REFERRAL_DATE]         NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)    NOT NULL,
        [dw_is_deleted]         BIT             NOT NULL CONSTRAINT [DF_ifdb_referralinformation_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_referralinformation_ifdb] PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_referralinformation_ifdb_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_referralinformation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_referralinformation_ifdb_DW_Timestamp_Update]
        ON [arc].[ifdb_referralinformation] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_referralinformation_ifdb_REFERRAL_DATE'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_referralinformation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_referralinformation_ifdb_REFERRAL_DATE]
        ON [arc].[ifdb_referralinformation] ([REFERRAL_DATE]);
END
GO
