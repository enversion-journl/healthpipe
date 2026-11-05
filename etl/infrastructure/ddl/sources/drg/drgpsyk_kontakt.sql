-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.drgpsyk_kontakt  (DRGPSYK: Psyk_kontakt)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[drgpsyk_kontakt]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[drgpsyk_kontakt] (
        [takstsystem]                NVARCHAR(3)     NOT NULL,
        [forloeb_noegle]             NVARCHAR(36)    NULL,
        [DW_EK_Helbredsforloeb]      NVARCHAR(500)   NULL,
        [CPRnummer]                  NVARCHAR(500)   NULL,
        [INDTIDSPUNKT_KONTAKT]       NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_KONTAKT]        NVARCHAR(500)   NULL,
        [UDTIDSPUNKT_KONTAKT_KONSTR] NVARCHAR(500)   NULL,
        [KONTAKTTYPE]                NVARCHAR(500)   NULL,
        [PRIORITET]                  NVARCHAR(500)   NULL,
        [KONTAKT_ENHED]              NVARCHAR(500)   NULL,
        [KONTAKT_ENHEDSNAVN]         NVARCHAR(510)   NULL,
        [SPECIALE]                   NVARCHAR(500)   NULL,
        [SYGEHUS]                    NVARCHAR(500)   NULL,
        [SYGEHUSNAVN]                NVARCHAR(510)   NULL,
        [SYGEHUSTYPE]                NVARCHAR(500)   NULL,
        [SYGEHUS_REGION]             NVARCHAR(500)   NULL,
        [BO_KOM]                     NVARCHAR(500)   NULL,
        [BO_REGION]                  NVARCHAR(500)   NULL,
        [BO_KOM_SOURCE]              NVARCHAR(500)   NULL,
        [ALDER_AAR_IND]              NVARCHAR(500)   NULL,
        [ALDER_AAR_UD]               NVARCHAR(500)   NULL,
        [KOEN]                       NVARCHAR(500)   NULL,
        [KONTAKTTYPE_Forloeb]        NVARCHAR(500)   NULL,
        [KONTAKT_AFSLUTTET]          NVARCHAR(500)   NULL,
        [KONTAKT_VAERDI]             NVARCHAR(36)    NOT NULL,
        [Koerselsdato]               NVARCHAR(500)   NULL,
        [DIA01]                      NVARCHAR(500)   NULL,
        [DIA02]                      NVARCHAR(500)   NULL,
        [DIA03]                      NVARCHAR(500)   NULL,
        [DIA04]                      NVARCHAR(500)   NULL,
        [DIA05]                      NVARCHAR(500)   NULL,
        [PROC01]                     NVARCHAR(500)   NULL,
        [PROC02]                     NVARCHAR(500)   NULL,
        [PROC03]                     NVARCHAR(500)   NULL,
        [PROC04]                     NVARCHAR(500)   NULL,
        [PROC05]                     NVARCHAR(500)   NULL,
        [leverance_dato]             DATE            NOT NULL,
        [dw_is_deleted]              BIT             NOT NULL CONSTRAINT [DF_drgpsyk_kontakt_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]        DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]        DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_drgpsyk_kontakt] PRIMARY KEY CLUSTERED (
            [takstsystem], [KONTAKT_VAERDI]
        )
    );

    CREATE NONCLUSTERED INDEX [IX_arc_drgpsyk_kontakt_DW_Timestamp_Update]
        ON [arc].[drgpsyk_kontakt] ([dw_timestamp_update] DESC);
END
GO
