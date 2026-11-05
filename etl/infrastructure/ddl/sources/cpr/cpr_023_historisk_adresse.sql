-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_023_historisk_adresse  (record type 023B, 132 characters)
-- Historical addresses with periods. The B variant, with address UUID. Up to 121 per person as measured.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_023_historisk_adresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_023_historisk_adresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [annkor]               NVARCHAR(1)   NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [convn]                NVARCHAR(34)  NULL,
        [tilflydto]            NVARCHAR(12)  NOT NULL,
        [tilflydto_umrk]       NVARCHAR(1)   NULL,
        [fraflydto]            NVARCHAR(12)  NOT NULL,
        [fraflydto_umrk]       NVARCHAR(1)   NULL,
        [adresse_uuid]         NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_023_historisk_adresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        -- No primary key: the source does not identify its rows
        -- uniquely here. Measured on the deliveries, even the widest
        -- combination of source columns was not unique. The rows are
        -- therefore replaced entirely for the persons a delivery
        -- contains, and the index exists to make that replacement fast.
        INDEX [IX_arc_cpr_023_historisk_adresse_person] CLUSTERED ([pnr], [tilflydto], [fraflydto])
    );
END
GO
