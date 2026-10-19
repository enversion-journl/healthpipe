-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Specialerne paa en aktivitetsudfoerelse skal kunne laeses af aftageren.
--
-- Vaerdierne hentes, arkiveres og laegges paa Observation-ressourcen som
-- extension, men blev aldrig skrevet ud som kolonner. De var dermed
-- utilgaengelige for aftageren, selv om de laa i modellen.
--
-- En udfoerelse kan have flere specialer, saa de faar eget grain frem for
-- kolonner paa Observation. Vaerdierne ligger allerede paa ressourcen, saa der
-- kraeves ingen genpostering; kun ny ViewDefinition og en projektion.

IF OBJECT_ID(N'[fhir].[Observation_Speciale]', 'U') IS NULL
BEGIN
    CREATE TABLE [fhir].[Observation_Speciale] (
        [observation_id] NVARCHAR(255) NOT NULL,
        [speciale_kode] NVARCHAR(512) NOT NULL,
        [speciale_kodetekst] NVARCHAR(512) NULL,
        [speciale_hospital_kode] NVARCHAR(512) NULL,
        [speciale_hospital_navn] NVARCHAR(512) NULL,
        [speciale_hospital_orgkode] NVARCHAR(512) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Observation_Speciale_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Observation_Speciale]
            PRIMARY KEY CLUSTERED ([observation_id], [speciale_kode])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ObservationSpeciale_SpecialeKode]
        ON [fhir].[Observation_Speciale] ([speciale_kode] ASC) ON [PRIMARY];
END
GO
