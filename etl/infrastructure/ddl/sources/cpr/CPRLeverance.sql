-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.CPRLeverance — one row per (archive table, delivery)
--
-- The delivery's start record (production date, previous production date, task
-- number) and counters. The watermark is the newest delivery with all tables
-- archived; the chain check compares the next file's previous production date with
-- it. A rerun of the same delivery updates the row. Control table, not an archive.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'CPRLeverance'
)
BEGIN
    CREATE TABLE [meta].[CPRLeverance] (
        -- No schema prefix: 'cpr_001_person' is both the ext and the arc table.
        [kildetabel]              NVARCHAR(60)  NOT NULL,
        -- The production date of the start record (PRODDTO).
        [leverance_dato]          DATE          NOT NULL,
        -- 208110 = base extract, 208109 = daily change extract.
        [opgavenr]                NVARCHAR(10)  NOT NULL,
        -- PRODDTOFORRIG of the start record; NULL for the first base extract.
        [forrige_produktionsdato] DATE          NULL,
        [filnavn]                 NVARCHAR(100) NOT NULL,
        -- The counter of the end record and the number of records read.
        [records_i_fil]           BIGINT        NULL,
        [records_indlaest]        BIGINT        NULL,
        -- 'grundudtraek' or 'aendring'.
        [indlaesningstype]        NVARCHAR(20)  NOT NULL,
        -- Days without a change extract after a base extract (blind window).
        [blindt_vindue_fra]       DATE          NULL,
        [blindt_vindue_til]       DATE          NULL,
        [raekker_indsat]          BIGINT        NULL,
        [raekker_opdateret]       BIGINT        NULL,
        [raekker_soft_slettet]    BIGINT        NULL,
        -- Set by arc_cpr when the table is archived for the delivery.
        [arkiveret_tidspunkt]     DATETIME2(0)  NULL,
        [dw_timestamp_insert]     DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_CPRLeverance_dw_timestamp_insert] DEFAULT (CAST(SYSUTCDATETIME() AS datetime2(0))),
        [dw_timestamp_update]     DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_CPRLeverance_dw_timestamp_update] DEFAULT (CAST(SYSUTCDATETIME() AS datetime2(0))),
        CONSTRAINT [PK_cpr_leverance]
            PRIMARY KEY CLUSTERED ([kildetabel], [leverance_dato])
    );

    -- The chain check looks up the latest delivery across tables.
    CREATE NONCLUSTERED INDEX [IX_CPRLeverance_dato]
        ON [meta].[CPRLeverance] ([leverance_dato] DESC)
        INCLUDE ([opgavenr], [forrige_produktionsdato]);
END
GO
