-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- LPR3 source-file delivery audit. Delivery selection itself uses meta.DFMState.
-- Counts are BIGINT: a full delivery exceeds the INT range across tables.
-- A source's archived status is written only after its archive merge succeeds.
-- The final gate requires every expected source of every archive table; a
-- missing member must never advance the checkpoint for the entire delivery.

IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'LPR3Leverance'
)
BEGIN
    CREATE TABLE [meta].[LPR3Leverance] (
        [kildetabel]                NVARCHAR(80) NOT NULL,
        [leverance_tidsstempel]     DATETIME2(0) NOT NULL,
        [run_id]                   NVARCHAR(64) NOT NULL,
        [kadence]                  NCHAR(1) NOT NULL,
        [leverance_dato]           DATE NOT NULL,
        [arc_tabel]                NVARCHAR(100) NOT NULL,
        [indlaesningstype]         NVARCHAR(20) NOT NULL,
        [merge_metode]             NVARCHAR(10) NULL,
        [status]                   NVARCHAR(20) NOT NULL,
        [raekker_ext]              BIGINT NOT NULL,
        [raekker_kilde]            BIGINT NOT NULL,
        [bytes_kilde]              BIGINT NOT NULL,
        [raekker_arc]              BIGINT NULL,
        [raekker_indsat]           BIGINT NULL,
        [raekker_opdateret]        BIGINT NULL,
        [raekker_soft_slettet]     BIGINT NULL,
        [surrogat_id_opdateret]    BIGINT NULL,
        [noegle_fallback_raekker]   BIGINT NULL,
        [referencer_uoploest]      BIGINT NULL,
        [audit_insert_min]         BIGINT NULL,
        [audit_insert_distinct]    BIGINT NOT NULL,
        [genindlaesning_alarm]     BIT NOT NULL,
        [sekunder]                 DECIMAL(18,3) NULL,
        [dw_timestamp_insert]      DATETIME2(0) NOT NULL
            CONSTRAINT [DF_LPR3Leverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        [dw_timestamp_update]      DATETIME2(0) NOT NULL
            CONSTRAINT [DF_LPR3Leverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS DATETIME2(0))),
        CONSTRAINT [PK_lpr3_meta_leverance] PRIMARY KEY CLUSTERED (
            [kildetabel], [leverance_tidsstempel]
        ),
        CONSTRAINT [CK_LPR3Leverance_status] CHECK ([status] IN ('extracted','archived')),
        CONSTRAINT [CK_LPR3Leverance_indlaesningstype] CHECK ([indlaesningstype] IN ('full','delta')),
        CONSTRAINT [CK_LPR3Leverance_merge_metode] CHECK ([merge_metode] IN ('bulk','merge')),
        CONSTRAINT [CK_LPR3Leverance_kadence] CHECK ([kadence] IN ('U','M'))
    );
END
GO
