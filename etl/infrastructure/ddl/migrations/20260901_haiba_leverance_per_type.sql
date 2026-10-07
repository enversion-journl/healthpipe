-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260901_haiba_leverance_per_type
--
-- meta.HAIBALeverance går fra pr. (kildetabel, leverance_dato) til pr.
-- (kildetabel, hai_type, leverance_dato): en ny hai_type-kolonne i PK'en, så
-- arkivet kan registrere leverancen pr. HAI-type. Uden det kunne
-- per-type-friskhed og cases<->agg-takt-gaten kun læse arc.leverance_dato, som
-- kun rykker når en rækkes værdier ændrer sig — og melder derfor falsk "stale"
-- eller står inert grønt på en levende, næsten append-only feed (MR !60, F2).
--
-- Registret er ren bogføring, der genopbygges ved hver kørsel (upsert pr.
-- leverance pr. type), så den gamle tabel droppes og genskabes med det nye
-- skema frem for en in-place PK-ombygning. Den gamle per-tabel-historik (uden
-- hai_type) passer ikke det nye grain og har ingen værdi at migrere.
--
-- Idempotent: DROP kun når tabellen findes UDEN hai_type-kolonnen; CREATE kun
-- når tabellen mangler. En allerede migreret DB rammer ingen af delene.
-- ============================================================================

IF OBJECT_ID(N'[meta].[HAIBALeverance]', N'U') IS NOT NULL
   AND COL_LENGTH(N'meta.HAIBALeverance', 'hai_type') IS NULL
BEGIN
    DROP TABLE [meta].[HAIBALeverance];
END
GO

IF OBJECT_ID(N'[meta].[HAIBALeverance]', N'U') IS NULL
BEGIN
    CREATE TABLE [meta].[HAIBALeverance] (
        [kildetabel]            NVARCHAR(50)  NOT NULL,
        [hai_type]              NVARCHAR(50)  NOT NULL,
        [leverance_dato]        DATE          NOT NULL,
        [indlaesningstype]      NVARCHAR(20)  NOT NULL,
        [raekker_indsat]        BIGINT        NULL,
        [raekker_opdateret]     BIGINT        NULL,
        [raekker_soft_slettet]  BIGINT        NULL,
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_HAIBALeverance_dw_timestamp_insert] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_HAIBALeverance_dw_timestamp_update] DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT [PK_haiba_leverance]
            PRIMARY KEY CLUSTERED ([kildetabel], [hai_type], [leverance_dato])
    );
END
GO
