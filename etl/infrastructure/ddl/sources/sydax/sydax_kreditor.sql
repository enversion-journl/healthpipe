-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_kreditor  (1:1 from SydAX [Kreditor].[Kreditor])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_kreditor'
)
BEGIN
    CREATE TABLE [arc].[sydax_kreditor] (
        -- Merge keys
        [Kildesystem]                  VARCHAR(10)    NOT NULL,
        [Kreditornummer]               VARCHAR(30)    NOT NULL,

        -- Source columns (1:1 from [Kreditor].[Kreditor], minus merge keys)
        [KreditorId]                   BIGINT         NOT NULL,
        [Kreditornavn]                 VARCHAR(100)   NULL,
        [Soegenavn]                    VARCHAR(100)   NULL,
        [JuridiskEnhed]                VARCHAR(30)    NULL,
        [ErBlokeret]                   BIT            NULL,
        [Oprettelsestidspunkt]         DATETIME2(7)   NULL,
        [OprettetAf]                   VARCHAR(10)    NULL,
        [SidstAendretTidspunkt]        DATETIME2(7)   NULL,
        [SidstAendretAf]               VARCHAR(10)    NULL,
        [ref_VendtableId]              BIGINT         NULL,
        [RunId]                        INT            NULL,
        [StartDateTime]                DATETIME2(0)   NOT NULL,
        [EndDateTime]                  DATETIME2(0)   NULL,
        [RdwTs]                        DATETIME2(7)   NULL,
        [DokumentId]                   BIGINT         NULL,
        [DataAreaId]                   NVARCHAR(4)    NULL,
        [CreatedOn]                    DATETIME       NULL,
        [Modified]                     DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]          DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]          DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_kreditor] PRIMARY KEY CLUSTERED ([Kildesystem], [KreditorId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditor_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditor_Modified]
        ON [arc].[sydax_kreditor] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_kreditor_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_kreditor]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_kreditor_CreatedOn]
        ON [arc].[sydax_kreditor] ([CreatedOn] DESC);
END
GO
