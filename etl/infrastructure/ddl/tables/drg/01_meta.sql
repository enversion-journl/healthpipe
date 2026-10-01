-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.DRGLeverance — which delivery the DRG archive currently reflects.
--
-- One row per (source table, takstsystem, delivery). The direction guard in
-- DSI/csv_files/drg_archive.py refuses to merge an older delivery over a
-- newer one (per takstsystem).
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'DRGLeverance'
)
BEGIN
    CREATE TABLE [meta].[DRGLeverance] (
        [kildetabel]           NVARCHAR(60)  NOT NULL,
        [takstsystem]          NVARCHAR(3)   NOT NULL,
        [leverance_dato]       DATE          NOT NULL,
        [lpropdateringsdato]   NVARCHAR(10)  NULL,
        [indlaesningstype]     NVARCHAR(20)  NOT NULL,
        [raekker_indsat]       INT           NULL,
        [raekker_opdateret]    INT           NULL,
        [raekker_soft_slettet] INT           NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_DRGLeverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_DRGLeverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        CONSTRAINT [PK_drg_meta_leverance] PRIMARY KEY CLUSTERED (
            [kildetabel], [takstsystem], [leverance_dato]
        )
    );
END
GO
