-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.UdredningsretLeverance — which delivery the archive reflects.
--
-- One row per (kildetabel, leverance_tidsstempel from the filename). A rerun
-- of the same delivery updates the row; rows accumulate, so the history can
-- answer "which deliveries have we loaded, and what did they do". It is an
-- audit log — the "may I skip this file" decision lives in meta.DFMState
-- (('dfm', 'udredningsret_leverance_1083'), see core/dfm_delivery.py).
--
-- The delivery-direction guard in DSI/csv_files/udredningsret_archive.py
-- reads MAX(lpropdateringsdato) (the source's own date for the content, from
-- LEVERANCEINFO) from here and refuses to merge an older delivery on top of
-- a newer one. The column is NVARCHAR(10) and compared as a STRING; that is
-- chronological only because the extract refuses any LPROPDATERINGSDATO that
-- is not ISO YYYY-MM-DD (review F14). helbredsforloeb_genfindelse is the
-- share of the archive's live helbredsforloeb keys recovered in the delivery
-- (the B3 guard).
--
-- A control table, not an archive table: it mirrors no source file and takes
-- no part in the MERGE. Hence meta, not arc.
-- ============================================================================

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'UdredningsretLeverance'
)
BEGIN
    CREATE TABLE [meta].[UdredningsretLeverance] (
        [kildetabel]                  NVARCHAR(60)  NOT NULL,
        [leverance_tidsstempel]       DATETIME2(0)  NOT NULL,
        [kadence]                     NVARCHAR(1)   NOT NULL,
        [leverance_dato]              DATE          NOT NULL,
        [lpropdateringsdato]          NVARCHAR(10)  NULL,  -- ISO YYYY-MM-DD, enforced by the extract
        [indlaesningstype]            NVARCHAR(20)  NOT NULL,
        [raekker_ext]                 INT           NULL,
        [raekker_kilde]               INT           NULL,
        [raekker_indsat]              INT           NULL,
        [raekker_opdateret]           INT           NULL,
        [raekker_soft_slettet]        INT           NULL,
        [helbredsforloeb_genfindelse] DECIMAL(7,6)  NULL,
        [dw_timestamp_insert]         DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_UdredningsretLeverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        [dw_timestamp_update]         DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_UdredningsretLeverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        CONSTRAINT [PK_udredningsret_meta_leverance] PRIMARY KEY CLUSTERED (
            [kildetabel], [leverance_tidsstempel]
        )
    );
END
GO
