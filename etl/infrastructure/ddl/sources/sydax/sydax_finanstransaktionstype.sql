-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_finanstransaktionstype  (1:1 from SydAX [Finans].[Finanstransaktionstype])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanstransaktionstype'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanstransaktionstype] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [Transaktionstypenummer]   INT            NOT NULL,

        -- Source columns (1:1 from [Finans].[Finanstransaktionstype], minus merge keys)
        [FinanstransaktionstypeId] BIGINT         NOT NULL,
        [Transaktionstypetekst]    VARCHAR(1000)  NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanstransaktionstype] PRIMARY KEY CLUSTERED ([Kildesystem], [FinanstransaktionstypeId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanstransaktionstype_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanstransaktionstype]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanstransaktionstype_Modified]
        ON [arc].[sydax_finanstransaktionstype] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanstransaktionstype_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanstransaktionstype]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanstransaktionstype_CreatedOn]
        ON [arc].[sydax_finanstransaktionstype] ([CreatedOn] DESC);
END
GO
