-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- meta.CPRAbonnementKandidat
--
-- Personal numbers known from fhir.Patient or arc.ifdb_patient that the CPR
-- extract does not deliver: candidates for subscription. Built by
-- cpr_abonnement_kandidater; the enrolment itself sets tilmeldt_dato.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'meta' AND TABLE_NAME = 'CPRAbonnementKandidat'
)
BEGIN
    CREATE TABLE [meta].[CPRAbonnementKandidat] (
        [pnr]                     NVARCHAR(10)  NOT NULL,
        -- 'fhir', 'arkiv' (arc.ifdb_patient) or 'begge'.
        [kilde]                   NVARCHAR(10)  NOT NULL,
        [foerste_gang_set]        DATETIME2(0)  NOT NULL,
        [sidste_gang_set]         DATETIME2(0)  NOT NULL,
        -- Set by the enrolment; NULL is a proposal.
        [tilmeldt_dato]           DATE          NULL,
        -- The date the person was later seen in the extract.
        [set_i_udtraek_dato]      DATE          NULL,
        [dw_timestamp_insert]     DATETIME2(0)  NOT NULL CONSTRAINT [DF_CPRAbonnementKandidat_ins] DEFAULT (SYSUTCDATETIME()),
        [dw_timestamp_update]     DATETIME2(0)  NOT NULL CONSTRAINT [DF_CPRAbonnementKandidat_upd] DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT [PK_meta_CPRAbonnementKandidat] PRIMARY KEY CLUSTERED ([pnr])
    );

    CREATE NONCLUSTERED INDEX [IX_meta_CPRAbonnementKandidat_tilmeldt]
        ON [meta].[CPRAbonnementKandidat] ([tilmeldt_dato])
        INCLUDE ([kilde], [foerste_gang_set]);
END
GO
