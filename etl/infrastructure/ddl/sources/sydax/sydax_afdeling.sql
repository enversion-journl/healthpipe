-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_afdeling  (1:1 from SydAX [Finans].[Afdeling])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_afdeling'
)
BEGIN
    CREATE TABLE [arc].[sydax_afdeling] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [Afdelingsnummer]                      VARCHAR(20)    NOT NULL,

        -- Source columns (1:1 from [Finans].[Afdeling], minus merge keys)
        [AfdelingId]                           BIGINT         NULL,
        [Afdelingstekst]                       VARCHAR(1000)  NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [E1]                                   VARCHAR(4)     NULL,
        [O1]                                   VARCHAR(4)     NULL,
        [ref_RECID]                            BIGINT         NULL,
        [ref_DirPartyTableId]                  BIGINT         NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_afdeling] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_afdeling_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_afdeling]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_afdeling_Modified]
        ON [arc].[sydax_afdeling] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_afdeling_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_afdeling]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_afdeling_CreatedOn]
        ON [arc].[sydax_afdeling] ([CreatedOn] DESC);
END
GO
