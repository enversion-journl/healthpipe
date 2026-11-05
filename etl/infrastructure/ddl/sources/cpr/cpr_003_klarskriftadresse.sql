-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_003_klarskriftadresse  (record type 003, 249 characters)
-- Addressing name and plain-text address: postal code, city name, standard address.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_003_klarskriftadresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_003_klarskriftadresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [adrnvn]               NVARCHAR(34)  NULL,
        [conavn]               NVARCHAR(34)  NULL,
        [lokalitet]            NVARCHAR(34)  NULL,
        [standardadr]          NVARCHAR(34)  NULL,
        [bynavn]               NVARCHAR(34)  NULL,
        [postnr]               NVARCHAR(4)   NULL,
        [postdisttxt]          NVARCHAR(20)  NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [vejadrnvn]            NVARCHAR(20)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_003_klarskriftadresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_003_klarskriftadresse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
