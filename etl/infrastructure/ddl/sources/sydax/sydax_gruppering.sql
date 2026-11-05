-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_gruppering  (1:1 from SydAX [Finans].[gruppering])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_gruppering'
)
BEGIN
    CREATE TABLE [arc].[sydax_gruppering] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[gruppering], minus merge keys)
        [GrupperingId]                         BIGINT         NULL,
        [Grupperingsnummer]                    VARCHAR(20)    NULL,
        [Grupperingstekst]                     VARCHAR(60)    NULL,
        [GrupperingDimension]                  VARCHAR(60)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [Ejerforhold]                          VARCHAR(4)     NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_gruppering] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_Modified]
        ON [arc].[sydax_gruppering] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_gruppering_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_gruppering]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_gruppering_CreatedOn]
        ON [arc].[sydax_gruppering] ([CreatedOn] DESC);
END
GO
