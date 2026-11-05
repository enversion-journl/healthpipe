-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_002_adresse  (record type 002B, 382 characters)
-- Current address of residence. The delivery uses the B variant, with street name and address UUID.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_002_adresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_002_adresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [convn]                NVARCHAR(34)  NULL,
        [tilflydto]            NVARCHAR(12)  NULL,
        [tilflydto_umrk]       NVARCHAR(1)   NULL,
        [tilflykomdto]         NVARCHAR(12)  NULL,
        [tilflykomdt_umrk]     NVARCHAR(1)   NULL,
        [fraflykomkod]         NVARCHAR(4)   NULL,
        [fraflykomdto]         NVARCHAR(12)  NULL,
        [fraflykomdt_umrk]     NVARCHAR(1)   NULL,
        [start_mynkod_adrtxt]  NVARCHAR(4)   NULL,
        [adr1_supladr]         NVARCHAR(34)  NULL,
        [adr2_supladr]         NVARCHAR(34)  NULL,
        [adr3_supladr]         NVARCHAR(34)  NULL,
        [adr4_supladr]         NVARCHAR(34)  NULL,
        [adr5_supladr]         NVARCHAR(34)  NULL,
        [start_dt_adrtxt]      NVARCHAR(10)  NULL,
        [slet_dt_adrtxt]       NVARCHAR(10)  NULL,
        [vejnvn]               NVARCHAR(40)  NULL,
        [adresse_uuid]         NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_002_adresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_002_adresse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
