-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.sydax_funktion  (1:1 from SydAX [Finans].[Funktion])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_funktion'
)
BEGIN
    CREATE TABLE [arc].[sydax_funktion] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [ref_RECID]                BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[Funktion], minus merge keys)
        [FunktionId]               BIGINT         NULL,
        [F1]                       VARCHAR(4)     NULL,
        [F2]                       VARCHAR(4)     NULL,
        [F3]                       VARCHAR(4)     NULL,
        [FunktionTekst]            VARCHAR(360)   NULL,
        [GyldigFra]                DATE           NULL,
        [GyldigTil]                DATE           NULL,
        [Autoriseret]              INT            NULL,
        [ref_LedgerTextId]         BIGINT         NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_funktion] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_RECID], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_funktion_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_funktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_funktion_Modified]
        ON [arc].[sydax_funktion] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_funktion_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_funktion]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_funktion_CreatedOn]
        ON [arc].[sydax_funktion] ([CreatedOn] DESC);
END
GO
