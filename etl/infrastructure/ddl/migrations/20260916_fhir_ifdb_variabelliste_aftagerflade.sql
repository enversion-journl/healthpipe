-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: aftagerfladen for IFDB-variabellisterne (henvisninger, procedurer, diagnoser).
-- Én samlet migration for hele leverancen; intet af den er kørt på noget miljø før.
-- Alle trin er idempotente (COL_LENGTH / OBJECT_ID / IS NULL-værn).
--
-- Trin:
--   1. fhir.Procedure: 15 nye kolonner + indeks på performer_organization_id
--   2. fhir.ServiceRequest: 8 nye henvisningskolonner
--   3. fhir.Procedure_SupplementaryCode (foer omdoebning: _tillaegskode): idx
--   4. fhir.Condition_henvisning: ny børnetabel
--   5. fhir.Questionnaire_item: fjern død kolonne required
--   6. fhir.ServiceRequest: engangsudfyldning af source_table på historiske rækker

-- ---------------------------------------------------------------------------
-- Trin 1: 20260915_fhir_procedure_kildefelter.sql
-- ---------------------------------------------------------------------------
-- Migration: projicér de ADMINISTRATIVE_PROCEDURE-felter, der allerede ligger på Procedure-ressourcen
-- (procedureSourceMeta, procedureLocationMeta, sourceCreatedAt/ModifiedAt). Kun kolonner med
-- høj udfyldning i kilden. Udførende enhed/stamafdeling lander som opløste Organization-
-- referencer og klinikeren fra performer; kræver ny ViewDefinition, genpostering af
-- administrative procedurer og RIS-ydelser samt SOF/DSO-genkørsel.

IF COL_LENGTH('fhir.Procedure', 'status_text') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [status_text] NVARCHAR(500) NULL;
IF COL_LENGTH('fhir.Procedure', 'location_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_code] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'location_name') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_name] NVARCHAR(500) NULL;
IF COL_LENGTH('fhir.Procedure', 'location_sorcode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [location_sorcode] NVARCHAR(50) NULL;
IF COL_LENGTH('fhir.Procedure', 'performer_organization_id') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [performer_organization_id] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_organization_id') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_organization_id] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'udfoert_enhed_sorcode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [udfoert_enhed_sorcode] NVARCHAR(50) NULL;
IF COL_LENGTH('fhir.Procedure', 'stamafdeling_sorcode') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [stamafdeling_sorcode] NVARCHAR(50) NULL;
IF COL_LENGTH('fhir.Procedure', 'practitioner_id') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [practitioner_id] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'practitioner_display') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [practitioner_display] NVARCHAR(500) NULL;
IF COL_LENGTH('fhir.Procedure', 'practitioner_role_code') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [practitioner_role_code] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.Procedure', 'practitioner_role_display') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [practitioner_role_display] NVARCHAR(500) NULL;
IF COL_LENGTH('fhir.Procedure', 'epji_created_at') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [epji_created_at] DATETIME2 NULL;
IF COL_LENGTH('fhir.Procedure', 'epji_modified_at') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [epji_modified_at] DATETIME2 NULL;
IF COL_LENGTH('fhir.Procedure', 'ifdb_modified_at') IS NULL
    ALTER TABLE [fhir].[Procedure] ADD [ifdb_modified_at] NVARCHAR(50) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_Procedure_PerformerOrganizationId' AND object_id = OBJECT_ID('fhir.Procedure'))
    CREATE NONCLUSTERED INDEX [IX_FHIR_Procedure_PerformerOrganizationId]
        ON [fhir].[Procedure] ([performer_organization_id] ASC) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY];
GO

-- ---------------------------------------------------------------------------
-- Trin 2: 20260915_fhir_servicerequest_henvisningsfelter.sql
-- ---------------------------------------------------------------------------
-- Migration: projicér henvisningsfelter, der allerede ligger i henvisningSourceMeta-extensionen
-- (rå status/prioritet, henvisningsmåde, formål, modtaget, afsendelsesdato). afsendelsesdato
-- kræver re-post af henvisninger (ny sub-extension); resten kun SOF/DSO-genkørsel.

IF COL_LENGTH('fhir.ServiceRequest', 'status_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [status_kode] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'status_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [status_kodetekst] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'prioritet_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [prioritet_kode] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'henvisningsmaade_kode') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvisningsmaade_kode] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'henvisningsmaade_kodetekst') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [henvisningsmaade_kodetekst] NVARCHAR(255) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'referral_purpose') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [referral_purpose] NVARCHAR(500) NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'modtaget') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [modtaget] DATETIME2 NULL;
IF COL_LENGTH('fhir.ServiceRequest', 'afsendelsesdato') IS NULL
    ALTER TABLE [fhir].[ServiceRequest] ADD [afsendelsesdato] DATETIME2 NULL;
GO

-- ---------------------------------------------------------------------------
-- Trin 3: 20260915_fhir_procedure_tillaegskode_idx.sql
-- ---------------------------------------------------------------------------
-- Migration: rækkefølge (IDX) på tillægskoder. Byggeren lægger kildens IDX på Coding som
-- codingSequence-extension; kræver re-post af administrative procedurer.

-- Tabellen omdoebes til Procedure_SupplementaryCode af
-- 20260909_fhir_tillaegskode_to_supplementary_code.sql, som sorterer FOER denne
-- fil. COL_LENGTH paa en tabel, der ikke findes, giver NULL — en ren
-- COL_LENGTH-vagt ville derfor lade ALTER TABLE koere mod et navn, der er vaek,
-- og vaelte foerste udrulning til et miljoe, hvor omdoebningen er koert.
-- Vagten ser derfor paa tabellen foerst, og begge navne haandteres, saa filen er
-- korrekt uanset om omdoebningen har vaeret der endnu.
IF OBJECT_ID(N'[fhir].[Procedure_SupplementaryCode]', 'U') IS NOT NULL
   AND COL_LENGTH('fhir.Procedure_SupplementaryCode', 'idx') IS NULL
    ALTER TABLE [fhir].[Procedure_SupplementaryCode] ADD [idx] INT NULL;
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH('fhir.Procedure_tillaegskode', 'idx') IS NULL
    ALTER TABLE [fhir].[Procedure_tillaegskode] ADD [idx] INT NULL;
GO

-- ---------------------------------------------------------------------------
-- Trin 4: 20260915_fhir_condition_henvisning_table.sql
-- ---------------------------------------------------------------------------
-- Migration: Create fhir.Condition_henvisning child table
-- WHY: PAS_DIAGNOSE_HENVISNING_REF was only carried as an extension on the Condition
-- resource. Own grain because a diagnosis can reference several referrals. Requires SoF
-- extract of condition_henvisning_view; no re-post (extension already present).

IF OBJECT_ID(N'[fhir].[Condition_henvisning]', 'U') IS NULL
BEGIN
    CREATE TABLE [fhir].[Condition_henvisning] (
        [condition_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [henvisning_id] NVARCHAR(255) NOT NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        -- Soft-delete flag (0 = live, 1 = deleted). Warehouse-managed; se soesterbordet for tillaegskoder.
        [dw_is_deleted] INT NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_is_deleted] DEFAULT 0,
        [dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update] DATETIME2 NOT NULL CONSTRAINT [DF_Condition_henvisning_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_FHIR_Condition_henvisning]
            PRIMARY KEY CLUSTERED ([condition_id], [henvisning_id])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionHenvisning_HenvisningId]
        ON [fhir].[Condition_henvisning] ([henvisning_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionHenvisning_LastUpdated]
        ON [fhir].[Condition_henvisning] ([last_updated] ASC) ON [PRIMARY];
END
GO

-- ---------------------------------------------------------------------------
-- Trin 5: 20260915_fhir_questionnaire_item_drop_required.sql
-- ---------------------------------------------------------------------------
-- Migration: drop fhir.Questionnaire_item.required
-- WHY: byggeren sætter aldrig Questionnaire.item.required (0 af 66.160 items i Aidbox),
-- så kolonnen har været NULL siden maj. Fjernes frem for at stå som en død kolonne.

IF COL_LENGTH('fhir.Questionnaire_item', 'required') IS NOT NULL
    ALTER TABLE [fhir].[Questionnaire_item] DROP COLUMN [required];
GO

-- ---------------------------------------------------------------------------
-- Trin 6: 20260916_fhir_servicerequest_source_table_backfill.sql
-- ---------------------------------------------------------------------------
-- Engangsudfyldning af historiske ServiceRequest-kilder på staging og DSO.
-- Den løbende SOF-indlæsning afleder samme værdi før MERGE. Eksisterende
-- kildeangivelser og ukendte identifier-systemer bevares.

IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
BEGIN
    UPDATE sr
    SET
        [source_table] = CASE sr.[identifier_system]
            WHEN N'urn:source:ifdb:henvisning' THEN N'HENVISNING'
            WHEN N'urn:source:ifdb:ris_ydelse' THEN N'RIS_YDELSE'
            WHEN N'urn:source:ifdb:rehabilitation_plan' THEN N'REHABILITATION_PLAN'
        END,
        [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
    FROM [fhir].[ServiceRequest] AS sr
    WHERE
        sr.[source_table] IS NULL
        AND sr.[identifier_system] IN (
            N'urn:source:ifdb:henvisning',
            N'urn:source:ifdb:ris_ydelse',
            N'urn:source:ifdb:rehabilitation_plan'
        );
END
GO
