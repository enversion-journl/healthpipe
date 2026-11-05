-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.omkostningssted_hierarki_sydax  (1:1 from SydAX [Finans].[OmkostningsstedHierarki])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_omkostningssted_hierarki'
)
BEGIN
    CREATE TABLE [arc].[sydax_omkostningssted_hierarki] (
        -- Merge keys
        [Kildesystem]              VARCHAR(10)    NOT NULL,
        [max_OmkostningsstedId]    BIGINT         NULL,

        -- Source columns (1:1 from [Finans].[OmkostningsstedHierarki], minus merge keys)
        [OmkostningsstedHierarkiId] BIGINT        NOT NULL,
        [jeId1]                    BIGINT         NULL,
        [veId2]                    BIGINT         NULL,
        [afId2]                    BIGINT         NULL,
        [veId3]                    BIGINT         NULL,
        [afId3]                    BIGINT         NULL,
        [osId3]                    BIGINT         NULL,
        [veId4]                    BIGINT         NULL,
        [afId4]                    BIGINT         NULL,
        [osId4]                    BIGINT         NULL,
        [veId5]                    BIGINT         NULL,
        [afId5]                    BIGINT         NULL,
        [osId5]                    BIGINT         NULL,
        [veId6]                    BIGINT         NULL,
        [afId6]                    BIGINT         NULL,
        [osId6]                    BIGINT         NULL,
        [veId7]                    BIGINT         NULL,
        [afId7]                    BIGINT         NULL,
        [osId7]                    BIGINT         NULL,
        [veId8]                    BIGINT         NULL,
        [afId8]                    BIGINT         NULL,
        [osId8]                    BIGINT         NULL,
        [veId9]                    BIGINT         NULL,
        [afId9]                    BIGINT         NULL,
        [osId9]                    BIGINT         NULL,
        [veId10]                   BIGINT         NULL,
        [afId10]                   BIGINT         NULL,
        [osId10]                   BIGINT         NULL,
        [veId11]                   BIGINT         NULL,
        [afId11]                   BIGINT         NULL,
        [osId11]                   BIGINT         NULL,
        [veId12]                   BIGINT         NULL,
        [afId12]                   BIGINT         NULL,
        [osId12]                   BIGINT         NULL,
        [veId13]                   BIGINT         NULL,
        [afId13]                   BIGINT         NULL,
        [osId13]                   BIGINT         NULL,
        [veId14]                   BIGINT         NULL,
        [afId14]                   BIGINT         NULL,
        [osId14]                   BIGINT         NULL,
        [veId15]                   BIGINT         NULL,
        [afId15]                   BIGINT         NULL,
        [osId15]                   BIGINT         NULL,
        [veId16]                   BIGINT         NULL,
        [afId16]                   BIGINT         NULL,
        [osId16]                   BIGINT         NULL,
        [veId17]                   BIGINT         NULL,
        [afId17]                   BIGINT         NULL,
        [osId17]                   BIGINT         NULL,
        [veId18]                   BIGINT         NULL,
        [afId18]                   BIGINT         NULL,
        [osId18]                   BIGINT         NULL,
        [veId19]                   BIGINT         NULL,
        [afId19]                   BIGINT         NULL,
        [osId19]                   BIGINT         NULL,
        [veId20]                   BIGINT         NULL,
        [afId20]                   BIGINT         NULL,
        [osId20]                   BIGINT         NULL,
        [max_VirksomhedsenhedId]   BIGINT         NULL,
        [max_AfdelingId]           BIGINT         NULL,
        [nummer1]                  VARCHAR(100)   NULL,
        [nummer2]                  VARCHAR(100)   NULL,
        [nummer3]                  VARCHAR(100)   NULL,
        [nummer4]                  VARCHAR(100)   NULL,
        [nummer5]                  VARCHAR(100)   NULL,
        [nummer6]                  VARCHAR(100)   NULL,
        [nummer7]                  VARCHAR(100)   NULL,
        [nummer8]                  VARCHAR(100)   NULL,
        [nummer9]                  VARCHAR(100)   NULL,
        [nummer10]                 VARCHAR(100)   NULL,
        [nummer11]                 VARCHAR(100)   NULL,
        [nummer12]                 VARCHAR(100)   NULL,
        [nummer13]                 VARCHAR(100)   NULL,
        [nummer14]                 VARCHAR(100)   NULL,
        [nummer15]                 VARCHAR(100)   NULL,
        [nummer16]                 VARCHAR(100)   NULL,
        [nummer17]                 VARCHAR(100)   NULL,
        [nummer18]                 VARCHAR(100)   NULL,
        [nummer19]                 VARCHAR(100)   NULL,
        [nummer20]                 VARCHAR(100)   NULL,
        [tekst1]                   VARCHAR(1000)  NULL,
        [tekst2]                   VARCHAR(1000)  NULL,
        [tekst3]                   VARCHAR(1000)  NULL,
        [tekst4]                   VARCHAR(1000)  NULL,
        [tekst5]                   VARCHAR(1000)  NULL,
        [tekst6]                   VARCHAR(1000)  NULL,
        [tekst7]                   VARCHAR(1000)  NULL,
        [tekst8]                   VARCHAR(1000)  NULL,
        [tekst9]                   VARCHAR(1000)  NULL,
        [tekst10]                  VARCHAR(1000)  NULL,
        [tekst11]                  VARCHAR(1000)  NULL,
        [tekst12]                  VARCHAR(1000)  NULL,
        [tekst13]                  VARCHAR(1000)  NULL,
        [tekst14]                  VARCHAR(1000)  NULL,
        [tekst15]                  VARCHAR(1000)  NULL,
        [tekst16]                  VARCHAR(1000)  NULL,
        [tekst17]                  VARCHAR(1000)  NULL,
        [tekst18]                  VARCHAR(1000)  NULL,
        [tekst19]                  VARCHAR(1000)  NULL,
        [tekst20]                  VARCHAR(1000)  NULL,
        [Aktiv]                    INT            NULL,
        [HierarkiId]               BIGINT         NULL,
        [RunId]                    INT            NULL,
        [StartDateTime]            DATETIME2(0)   NOT NULL,
        [EndDateTime]              DATETIME2(0)   NULL,
        [RdwTs]                    DATETIME2(7)   NULL,
        [Slettet]                  INT            NULL,
        [CreatedOn]                DATETIME       NULL,
        [Modified]                 DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]      DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_omkostningssted_hierarki] PRIMARY KEY CLUSTERED ([Kildesystem], [OmkostningsstedHierarkiId], [StartDateTime])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_omkostningssted_hierarki_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_omkostningssted_hierarki]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_omkostningssted_hierarki_Modified]
        ON [arc].[sydax_omkostningssted_hierarki] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_omkostningssted_hierarki_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_omkostningssted_hierarki]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_omkostningssted_hierarki_CreatedOn]
        ON [arc].[sydax_omkostningssted_hierarki] ([CreatedOn] DESC);
END
GO
