-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_art  (1:1 from SydAX [Finans].[art])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_art'
)
BEGIN
    CREATE TABLE [arc].[sydax_art] (
        -- Merge keys
        [Kildesystem]                          VARCHAR(10)    NOT NULL,
        [RECID]                                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[art], minus merge keys)
        [ArtId]                                BIGINT         NULL,
        [Artsnummer]                           VARCHAR(20)    NULL,
        [Artstekst]                            VARCHAR(60)    NULL,
        [AktivFra]                             DATE           NULL,
        [AktivTil]                             DATE           NULL,
        [Suspenderet]                          INT            NULL,
        [Ejer_id]                              BIGINT         NULL,
        [TilladIkkeManuelAngivelse]            INT            NULL,
        [A1]                                   VARCHAR(4)     NULL,
        [A2]                                   VARCHAR(4)     NULL,
        [Momsretning]                          INT            NULL,
        [RunId]                                INT            NULL,
        [StartDateTime]                        DATETIME2(0)   NOT NULL,
        [EndDateTime]                          DATETIME2(0)   NULL,
        [RdwTs]                                DATETIME2(7)   NULL,
        [ref_DimensionAttributeValueId]        BIGINT         NULL,
        [ArtDimension]                         VARCHAR(60)    NULL,
        [CreatedOn]                            DATETIME       NULL,
        [Modified]                             DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                  DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                  DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_art] PRIMARY KEY CLUSTERED ([Kildesystem], [RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_art_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_art]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_art_Modified]
        ON [arc].[sydax_art] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_art_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_art]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_art_CreatedOn]
        ON [arc].[sydax_art] ([CreatedOn] DESC);
END
GO
