-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.cpr_996_data_fremadrettet  (record type 996, 15 characters)
-- Whether the person is delivered going forward: in the set of persons and/or by subscription.
-- leverance_dato is the latest delivery the person was in.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_996_data_fremadrettet]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_996_data_fremadrettet] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [personkreds]          NVARCHAR(1)   NULL,
        [abonnement]           NVARCHAR(1)   NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_996_data_fremadrettet_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_996_data_fremadrettet] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
