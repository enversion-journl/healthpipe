-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.metadata_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_metadata'
)
BEGIN
    CREATE TABLE [arc].[mysupply_metadata] (
        [id]                              INT             NOT NULL,
        [Guid]                            NVARCHAR(MAX)   NULL,
        [data_unit_identity_unique_name]  NVARCHAR(MAX)   NULL,
        [RowNumber]                       NVARCHAR(MAX)   NULL,
        [Type]                            NVARCHAR(MAX)   NULL,
        [FileName]                        NVARCHAR(MAX)   NULL,
        [ReceivedId]                      NVARCHAR(MAX)   NULL,
        [DestinationEP]                   NVARCHAR(MAX)   NULL,
        [SystemNavn]                      NVARCHAR(MAX)   NULL,
        [OrgUnit]                         NVARCHAR(MAX)   NULL,
        [ReceiveTime]                     NVARCHAR(MAX)   NULL,
        [ReceiveTimeFormat]               NVARCHAR(MAX)   NULL,
        [Kilde]                           NVARCHAR(MAX)   NULL,
        [FakturaTotal]                    NVARCHAR(MAX)   NULL,
        [OrdreRef]                        NVARCHAR(MAX)   NULL,
        [LeverandorCVR]                   NVARCHAR(MAX)   NULL,
        [SaelgerMomsNummer]               NVARCHAR(MAX)   NULL,
        [FirmaNavn]                       NVARCHAR(MAX)   NULL,
        [Adresse]                         NVARCHAR(MAX)   NULL,
        [Postnummer]                      NVARCHAR(MAX)   NULL,
        [By]                              NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_metadata]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_metadata_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_metadata]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_metadata_dw_timestamp_update]
        ON [arc].[mysupply_metadata] ([dw_timestamp_update] DESC);
END
GO
