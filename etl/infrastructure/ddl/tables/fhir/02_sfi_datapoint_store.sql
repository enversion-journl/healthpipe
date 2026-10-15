-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.SFI_datapoint_store
-- Materialiseret aftagertabel for fhir.SFI_datapoint (fyldes fra fhir.SFI_datapoint_build
-- af dso_refresh_sfi_datapoint_*). Udrulles på staging og DSO; fyldes kun på DSO.
--
-- Clustered columnstore på månedspartitioner (occurred_at_dt, RANGE RIGHT, 2021-10 ..
-- 2035-12; NULL og alt før lander i partition 1). Aftagerne aggregerer over perioder,
-- elementkoder og modeller — det læses fra columnstoren i batch-tilstand med segment-
-- eliminering på partitionen. Kun to B-træer: den unikke datapoint-nøgle og patientopslag.
-- Målt på 25 mio. rækker: godt halv størrelse af rowstore med fire brede indeks, flad
-- indsættelsestid, punktopslag pr. patient via B-træet.
--
-- Backfill-vinduer indlæses i arbejdstabellen nedenfor (heap), får columnstore og B-træer
-- bygget over vinduets egne rækker og skiftes ind med ALTER TABLE ... SWITCH
-- (DSO/load/sfi_store_partitioning.py). Slettede rækker og åbne deltastore-rækkegrupper
-- ryddes af refreshen med REORGANIZE efter hver kørsel.
--
-- Regler, der ikke må brydes:
--   * Alle B-træer er alignede med partitionsskemaet (krav på en columnstore og for SWITCH)
--     og har occurred_at_dt som sidste nøgle.
--   * Id kommer fra sekvensen (ikke IDENTITY), så store og arbejdstabel deler nummerrække.
--   * Ingen fuldtekst: søgning i elementtekster ligger i codesystem-søgedokumentet.
--   * Indeksdefinitionerne spejles 1:1 af sfi_store_partitioning (kontrakttest).
--
-- Hvis det ikke performer: en aftagerforespørgsel, der seeker (én kontakt, ét aktivitets-
-- id), får sit eget smalle, alignede B-træ; en scanning uden segment-eliminering får
-- columnstoren sorteret på kolonnen (CREATE CLUSTERED COLUMNSTORE INDEX ... ORDER (...)
-- WITH (DROP_EXISTING = ON)). Nye grænser efter 2035: ALTER PARTITION SCHEME ... NEXT
-- USED [PRIMARY]; ALTER PARTITION FUNCTION ... () SPLIT RANGE (...) på den tomme sidste.
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_SFI_datapoint_store_Maaned')
BEGIN
    CREATE PARTITION FUNCTION [pf_SFI_datapoint_store_Maaned] (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES (
        N'2021-10-01T00:00:00', N'2021-11-01T00:00:00', N'2021-12-01T00:00:00', N'2022-01-01T00:00:00',
        N'2022-02-01T00:00:00', N'2022-03-01T00:00:00', N'2022-04-01T00:00:00', N'2022-05-01T00:00:00',
        N'2022-06-01T00:00:00', N'2022-07-01T00:00:00', N'2022-08-01T00:00:00', N'2022-09-01T00:00:00',
        N'2022-10-01T00:00:00', N'2022-11-01T00:00:00', N'2022-12-01T00:00:00', N'2023-01-01T00:00:00',
        N'2023-02-01T00:00:00', N'2023-03-01T00:00:00', N'2023-04-01T00:00:00', N'2023-05-01T00:00:00',
        N'2023-06-01T00:00:00', N'2023-07-01T00:00:00', N'2023-08-01T00:00:00', N'2023-09-01T00:00:00',
        N'2023-10-01T00:00:00', N'2023-11-01T00:00:00', N'2023-12-01T00:00:00', N'2024-01-01T00:00:00',
        N'2024-02-01T00:00:00', N'2024-03-01T00:00:00', N'2024-04-01T00:00:00', N'2024-05-01T00:00:00',
        N'2024-06-01T00:00:00', N'2024-07-01T00:00:00', N'2024-08-01T00:00:00', N'2024-09-01T00:00:00',
        N'2024-10-01T00:00:00', N'2024-11-01T00:00:00', N'2024-12-01T00:00:00', N'2025-01-01T00:00:00',
        N'2025-02-01T00:00:00', N'2025-03-01T00:00:00', N'2025-04-01T00:00:00', N'2025-05-01T00:00:00',
        N'2025-06-01T00:00:00', N'2025-07-01T00:00:00', N'2025-08-01T00:00:00', N'2025-09-01T00:00:00',
        N'2025-10-01T00:00:00', N'2025-11-01T00:00:00', N'2025-12-01T00:00:00', N'2026-01-01T00:00:00',
        N'2026-02-01T00:00:00', N'2026-03-01T00:00:00', N'2026-04-01T00:00:00', N'2026-05-01T00:00:00',
        N'2026-06-01T00:00:00', N'2026-07-01T00:00:00', N'2026-08-01T00:00:00', N'2026-09-01T00:00:00',
        N'2026-10-01T00:00:00', N'2026-11-01T00:00:00', N'2026-12-01T00:00:00', N'2027-01-01T00:00:00',
        N'2027-02-01T00:00:00', N'2027-03-01T00:00:00', N'2027-04-01T00:00:00', N'2027-05-01T00:00:00',
        N'2027-06-01T00:00:00', N'2027-07-01T00:00:00', N'2027-08-01T00:00:00', N'2027-09-01T00:00:00',
        N'2027-10-01T00:00:00', N'2027-11-01T00:00:00', N'2027-12-01T00:00:00', N'2028-01-01T00:00:00',
        N'2028-02-01T00:00:00', N'2028-03-01T00:00:00', N'2028-04-01T00:00:00', N'2028-05-01T00:00:00',
        N'2028-06-01T00:00:00', N'2028-07-01T00:00:00', N'2028-08-01T00:00:00', N'2028-09-01T00:00:00',
        N'2028-10-01T00:00:00', N'2028-11-01T00:00:00', N'2028-12-01T00:00:00', N'2029-01-01T00:00:00',
        N'2029-02-01T00:00:00', N'2029-03-01T00:00:00', N'2029-04-01T00:00:00', N'2029-05-01T00:00:00',
        N'2029-06-01T00:00:00', N'2029-07-01T00:00:00', N'2029-08-01T00:00:00', N'2029-09-01T00:00:00',
        N'2029-10-01T00:00:00', N'2029-11-01T00:00:00', N'2029-12-01T00:00:00', N'2030-01-01T00:00:00',
        N'2030-02-01T00:00:00', N'2030-03-01T00:00:00', N'2030-04-01T00:00:00', N'2030-05-01T00:00:00',
        N'2030-06-01T00:00:00', N'2030-07-01T00:00:00', N'2030-08-01T00:00:00', N'2030-09-01T00:00:00',
        N'2030-10-01T00:00:00', N'2030-11-01T00:00:00', N'2030-12-01T00:00:00', N'2031-01-01T00:00:00',
        N'2031-02-01T00:00:00', N'2031-03-01T00:00:00', N'2031-04-01T00:00:00', N'2031-05-01T00:00:00',
        N'2031-06-01T00:00:00', N'2031-07-01T00:00:00', N'2031-08-01T00:00:00', N'2031-09-01T00:00:00',
        N'2031-10-01T00:00:00', N'2031-11-01T00:00:00', N'2031-12-01T00:00:00', N'2032-01-01T00:00:00',
        N'2032-02-01T00:00:00', N'2032-03-01T00:00:00', N'2032-04-01T00:00:00', N'2032-05-01T00:00:00',
        N'2032-06-01T00:00:00', N'2032-07-01T00:00:00', N'2032-08-01T00:00:00', N'2032-09-01T00:00:00',
        N'2032-10-01T00:00:00', N'2032-11-01T00:00:00', N'2032-12-01T00:00:00', N'2033-01-01T00:00:00',
        N'2033-02-01T00:00:00', N'2033-03-01T00:00:00', N'2033-04-01T00:00:00', N'2033-05-01T00:00:00',
        N'2033-06-01T00:00:00', N'2033-07-01T00:00:00', N'2033-08-01T00:00:00', N'2033-09-01T00:00:00',
        N'2033-10-01T00:00:00', N'2033-11-01T00:00:00', N'2033-12-01T00:00:00', N'2034-01-01T00:00:00',
        N'2034-02-01T00:00:00', N'2034-03-01T00:00:00', N'2034-04-01T00:00:00', N'2034-05-01T00:00:00',
        N'2034-06-01T00:00:00', N'2034-07-01T00:00:00', N'2034-08-01T00:00:00', N'2034-09-01T00:00:00',
        N'2034-10-01T00:00:00', N'2034-11-01T00:00:00', N'2034-12-01T00:00:00', N'2035-01-01T00:00:00',
        N'2035-02-01T00:00:00', N'2035-03-01T00:00:00', N'2035-04-01T00:00:00', N'2035-05-01T00:00:00',
        N'2035-06-01T00:00:00', N'2035-07-01T00:00:00', N'2035-08-01T00:00:00', N'2035-09-01T00:00:00',
        N'2035-10-01T00:00:00', N'2035-11-01T00:00:00', N'2035-12-01T00:00:00'
    );
    PRINT 'Oprettede partitionsfunktion pf_SFI_datapoint_store_Maaned';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_SFI_datapoint_store_Maaned')
BEGIN
    CREATE PARTITION SCHEME [ps_SFI_datapoint_store_Maaned]
        AS PARTITION [pf_SFI_datapoint_store_Maaned] ALL TO ([PRIMARY]);
    PRINT 'Oprettede partitionsskema ps_SFI_datapoint_store_Maaned';
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.sequences
    WHERE name = N'seq_SFI_datapoint_store' AND schema_id = SCHEMA_ID(N'fhir')
)
BEGIN
    CREATE SEQUENCE [fhir].[seq_SFI_datapoint_store]
        AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 10000;
    PRINT 'Oprettede sekvens fhir.seq_SFI_datapoint_store';
END
GO

IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[SFI_datapoint_store] (
        [sfi_datapoint_id] BIGINT NOT NULL
            CONSTRAINT [DF_FHIR_SFI_datapoint_store_sfi_datapoint_id]
            DEFAULT (NEXT VALUE FOR [fhir].[seq_SFI_datapoint_store]),
        [datapoint_key] NVARCHAR(64) NOT NULL,
        [fhir_resource_id] NVARCHAR(255) NOT NULL,
        [source_resource_id] NVARCHAR(255) NOT NULL,
        [source_link_id] NVARCHAR(255) NULL,
        [source_item_key] NVARCHAR(100) NULL,
        [source_answer_index] NVARCHAR(10) NULL,
        [source_last_updated] DATETIME2 NULL,
        [ifdb_aktivitet_id] NVARCHAR(255) NULL,
        [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [practitioner_display] NVARCHAR(500) NULL,
        [source_resource] NVARCHAR(50) NULL,
        [occurred_at] NVARCHAR(50) NULL,
        [occurred_at_dt] DATETIME2(0) NULL,
        [source_model_name] NVARCHAR(500) NULL,
        [source_model_version] NVARCHAR(100) NULL,
        [source_element_id] NVARCHAR(500) NULL,
        [source_element_name] NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path] NVARCHAR(1000) NULL,
        [source_hierarchy_path] NVARCHAR(1000) NULL,
        [source_group_path] NVARCHAR(1000) NULL,
        [source_repetition_path] NVARCHAR(1000) NULL,
        [klynge_niveau] NVARCHAR(100) NULL,
        [repetition_niveau] NVARCHAR(100) NULL,
        [raekke_type] NVARCHAR(100) NULL,
        [klynge_type] NVARCHAR(100) NULL,
        [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
        [element_code] NVARCHAR(450) NULL,
        [element_display] NVARCHAR(500) NULL,
        [value_type] NVARCHAR(50) NULL,
        [has_value] BIT NULL,
        [value_int] NVARCHAR(50) NULL,
        [value_dec] NVARCHAR(50) NULL,
        [value_text] NVARCHAR(MAX) NULL,
        [value_datetime] NVARCHAR(50) NULL,
        [value_bool] NVARCHAR(10) NULL,
        [value_quantity] NVARCHAR(100) NULL,
        [value_unit] NVARCHAR(100) NULL,
        [value_coding_code] NVARCHAR(255) NULL,
        [value_coding_display] NVARCHAR(500) NULL,
        [element_nummer] NVARCHAR(50) NULL,
        [klynge_id] NVARCHAR(50) NULL,
        [klynge_id_hierarki] NVARCHAR(500) NULL,
        [klynge_parent_id] NVARCHAR(50) NULL,
        [repetition_id] NVARCHAR(50) NULL,
        [element_kode] NVARCHAR(255) NULL,
        [element_original_kode] NVARCHAR(255) NULL,
        [element_administrativ_enhed_id] NVARCHAR(255) NULL,
        [klynge_navne] NVARCHAR(MAX) NULL,
        [klynge_ledetekster] NVARCHAR(MAX) NULL,
        [klynge_ledetekst] NVARCHAR(500) NULL,
        [repetition_id_hierarki] NVARCHAR(500) NULL,
        [opdateret_tid] NVARCHAR(50) NULL,
        [xml_status] INT NULL,
        [xml_status_text] NVARCHAR(255) NULL,
        [refreshed_at] DATETIME2(0) NOT NULL
    ) ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);

    -- Columnstore og B-træer spejles 1:1 af DSO/load/sfi_store_partitioning (kontrakttest):
    -- arbejdstabellen får de samme bygget lige før et SWITCH.
    CREATE CLUSTERED COLUMNSTORE INDEX [CCI_FHIR_SFI_datapoint_store]
        ON [fhir].[SFI_datapoint_store]
        ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);

    CREATE UNIQUE NONCLUSTERED INDEX [UX_FHIR_SFI_datapoint_store_DatapointKey]
        ON [fhir].[SFI_datapoint_store] ([datapoint_key] ASC, [occurred_at_dt] ASC)
        WITH (DATA_COMPRESSION = PAGE, STATISTICS_INCREMENTAL = ON)
        ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);

    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt]
        ON [fhir].[SFI_datapoint_store] ([patient_id] ASC, [occurred_at_dt] ASC)
        WITH (DATA_COMPRESSION = PAGE, STATISTICS_INCREMENTAL = ON)
        ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);

    PRINT 'Oprettede fhir.SFI_datapoint_store (månedspartitioneret columnstore)';
END
GO

-- ============================================================================
-- fhir.SFI_datapoint_store_backfill
-- Arbejdstabel til SWITCH-indlæsning af backfill-vinduer: identiske kolonner, samme
-- partitionsskema og samme sekvens som storen. Den er en heap under indlæsning;
-- columnstore og B-træer bygges af refreshen umiddelbart før skiftet og droppes igen
-- bagefter, så tabellen normalt står tom og uden indeks. Rækker heri er aldrig synlige.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[SFI_datapoint_store_backfill] (
        [sfi_datapoint_id] BIGINT NOT NULL
            CONSTRAINT [DF_FHIR_SFI_datapoint_store_backfill_sfi_datapoint_id]
            DEFAULT (NEXT VALUE FOR [fhir].[seq_SFI_datapoint_store]),
        [datapoint_key] NVARCHAR(64) NOT NULL,
        [fhir_resource_id] NVARCHAR(255) NOT NULL,
        [source_resource_id] NVARCHAR(255) NOT NULL,
        [source_link_id] NVARCHAR(255) NULL,
        [source_item_key] NVARCHAR(100) NULL,
        [source_answer_index] NVARCHAR(10) NULL,
        [source_last_updated] DATETIME2 NULL,
        [ifdb_aktivitet_id] NVARCHAR(255) NULL,
        [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [practitioner_display] NVARCHAR(500) NULL,
        [source_resource] NVARCHAR(50) NULL,
        [occurred_at] NVARCHAR(50) NULL,
        [occurred_at_dt] DATETIME2(0) NULL,
        [source_model_name] NVARCHAR(500) NULL,
        [source_model_version] NVARCHAR(100) NULL,
        [source_element_id] NVARCHAR(500) NULL,
        [source_element_name] NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path] NVARCHAR(1000) NULL,
        [source_hierarchy_path] NVARCHAR(1000) NULL,
        [source_group_path] NVARCHAR(1000) NULL,
        [source_repetition_path] NVARCHAR(1000) NULL,
        [klynge_niveau] NVARCHAR(100) NULL,
        [repetition_niveau] NVARCHAR(100) NULL,
        [raekke_type] NVARCHAR(100) NULL,
        [klynge_type] NVARCHAR(100) NULL,
        [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
        [element_code] NVARCHAR(450) NULL,
        [element_display] NVARCHAR(500) NULL,
        [value_type] NVARCHAR(50) NULL,
        [has_value] BIT NULL,
        [value_int] NVARCHAR(50) NULL,
        [value_dec] NVARCHAR(50) NULL,
        [value_text] NVARCHAR(MAX) NULL,
        [value_datetime] NVARCHAR(50) NULL,
        [value_bool] NVARCHAR(10) NULL,
        [value_quantity] NVARCHAR(100) NULL,
        [value_unit] NVARCHAR(100) NULL,
        [value_coding_code] NVARCHAR(255) NULL,
        [value_coding_display] NVARCHAR(500) NULL,
        [element_nummer] NVARCHAR(50) NULL,
        [klynge_id] NVARCHAR(50) NULL,
        [klynge_id_hierarki] NVARCHAR(500) NULL,
        [klynge_parent_id] NVARCHAR(50) NULL,
        [repetition_id] NVARCHAR(50) NULL,
        [element_kode] NVARCHAR(255) NULL,
        [element_original_kode] NVARCHAR(255) NULL,
        [element_administrativ_enhed_id] NVARCHAR(255) NULL,
        [klynge_navne] NVARCHAR(MAX) NULL,
        [klynge_ledetekster] NVARCHAR(MAX) NULL,
        [klynge_ledetekst] NVARCHAR(500) NULL,
        [repetition_id_hierarki] NVARCHAR(500) NULL,
        [opdateret_tid] NVARCHAR(50) NULL,
        [xml_status] INT NULL,
        [xml_status_text] NVARCHAR(255) NULL,
        [refreshed_at] DATETIME2(0) NOT NULL
    ) ON [ps_SFI_datapoint_store_Maaned]([occurred_at_dt]);

    PRINT 'Oprettede fhir.SFI_datapoint_store_backfill (arbejdstabel til SWITCH)';
END
GO

-- ============================================================================
-- fhir.SFI_datapoint_store_efterslaeb
-- Holdetabel til efterslæbere ved SWITCH-indlæsning: de få rækker, der allerede ligger i en
-- partition, et vindue skal skiftes ind i, flyttes hertil før skiftet og lægges tilbage
-- bagefter, hvis deres ressource ikke blev genbygget. Heap, samme kolonner som storen;
-- normalt tom. Står der rækker efter en afbrudt kørsel, lægger næste kørsel dem tilbage.
-- ============================================================================
IF NOT EXISTS (
    SELECT *
    FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store_efterslaeb]') AND type = 'U'
)
BEGIN
    CREATE TABLE [fhir].[SFI_datapoint_store_efterslaeb] (
        [sfi_datapoint_id] BIGINT NOT NULL
            CONSTRAINT [DF_FHIR_SFI_datapoint_store_efterslaeb_sfi_datapoint_id]
            DEFAULT (NEXT VALUE FOR [fhir].[seq_SFI_datapoint_store]),
        [datapoint_key] NVARCHAR(64) NOT NULL,
        [fhir_resource_id] NVARCHAR(255) NOT NULL,
        [source_resource_id] NVARCHAR(255) NOT NULL,
        [source_link_id] NVARCHAR(255) NULL,
        [source_item_key] NVARCHAR(100) NULL,
        [source_answer_index] NVARCHAR(10) NULL,
        [source_last_updated] DATETIME2 NULL,
        [ifdb_aktivitet_id] NVARCHAR(255) NULL,
        [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL,
        [patient_id] NVARCHAR(255) NULL,
        [encounter_id] NVARCHAR(255) NULL,
        [organization_id] NVARCHAR(255) NULL,
        [practitioner_id] NVARCHAR(255) NULL,
        [practitioner_display] NVARCHAR(500) NULL,
        [source_resource] NVARCHAR(50) NULL,
        [occurred_at] NVARCHAR(50) NULL,
        [occurred_at_dt] DATETIME2(0) NULL,
        [source_model_name] NVARCHAR(500) NULL,
        [source_model_version] NVARCHAR(100) NULL,
        [source_element_id] NVARCHAR(500) NULL,
        [source_element_name] NVARCHAR(500) NULL,
        [source_element_label] NVARCHAR(500) NULL,
        [sfi_path] NVARCHAR(1000) NULL,
        [source_hierarchy_path] NVARCHAR(1000) NULL,
        [source_group_path] NVARCHAR(1000) NULL,
        [source_repetition_path] NVARCHAR(1000) NULL,
        [klynge_niveau] NVARCHAR(100) NULL,
        [repetition_niveau] NVARCHAR(100) NULL,
        [raekke_type] NVARCHAR(100) NULL,
        [klynge_type] NVARCHAR(100) NULL,
        [klynge_valg_ledetekst] NVARCHAR(MAX) NULL,
        [element_code] NVARCHAR(450) NULL,
        [element_display] NVARCHAR(500) NULL,
        [value_type] NVARCHAR(50) NULL,
        [has_value] BIT NULL,
        [value_int] NVARCHAR(50) NULL,
        [value_dec] NVARCHAR(50) NULL,
        [value_text] NVARCHAR(MAX) NULL,
        [value_datetime] NVARCHAR(50) NULL,
        [value_bool] NVARCHAR(10) NULL,
        [value_quantity] NVARCHAR(100) NULL,
        [value_unit] NVARCHAR(100) NULL,
        [value_coding_code] NVARCHAR(255) NULL,
        [value_coding_display] NVARCHAR(500) NULL,
        [element_nummer] NVARCHAR(50) NULL,
        [klynge_id] NVARCHAR(50) NULL,
        [klynge_id_hierarki] NVARCHAR(500) NULL,
        [klynge_parent_id] NVARCHAR(50) NULL,
        [repetition_id] NVARCHAR(50) NULL,
        [element_kode] NVARCHAR(255) NULL,
        [element_original_kode] NVARCHAR(255) NULL,
        [element_administrativ_enhed_id] NVARCHAR(255) NULL,
        [klynge_navne] NVARCHAR(MAX) NULL,
        [klynge_ledetekster] NVARCHAR(MAX) NULL,
        [klynge_ledetekst] NVARCHAR(500) NULL,
        [repetition_id_hierarki] NVARCHAR(500) NULL,
        [opdateret_tid] NVARCHAR(50) NULL,
        [xml_status] INT NULL,
        [xml_status_text] NVARCHAR(255) NULL,
        [refreshed_at] DATETIME2(0) NOT NULL
    ) ON [PRIMARY];

    PRINT 'Oprettede fhir.SFI_datapoint_store_efterslaeb (holdetabel til efterslæbere)';
END
GO
