-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ifdb_sengeenhed_normerede  (1:1 from IFDB.SENGEENHED_NORMEREDE)
--
-- Structurally identical to _disponible. Kept as its own table because the two
-- are independent period sets in the source; the FHIR transform distinguishes
-- them by Observation.code, not by column.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_sengeenhed_normerede'
)
BEGIN
    CREATE TABLE [arc].[ifdb_sengeenhed_normerede] (
        [SENGEENHED_ID]                  NVARCHAR(50)    NOT NULL,
        [FRA]                            NVARCHAR(100)   NOT NULL,
        [TIL]                            NVARCHAR(100)   NULL,
        [ANTAL]                          NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_sengeenhed_normerede_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_sengeenhed_normerede]
            PRIMARY KEY CLUSTERED ([SENGEENHED_ID], [FRA])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_sengeenhed_normerede_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_sengeenhed_normerede]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_sengeenhed_normerede_dw_timestamp_update]
        ON [arc].[ifdb_sengeenhed_normerede] ([dw_timestamp_update] DESC);
END
GO
