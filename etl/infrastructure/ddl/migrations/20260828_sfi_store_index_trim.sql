-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.SFI_datapoint_store: fjern overlappende indeksdækning
--
-- Tre snit, spejlet i tables/fhir/02_sfi_datapoint_store.sql:
--
-- 1. IX_..._PatientOccurredDt genopbygges med occurred_at i INCLUDE.
-- 2. IX_..._PatientDate droppes. Det er en nær-dublet af PatientOccurredDt:
--    samme patient_id som førstenøgle, samme INCLUDE-liste, og forskellen er
--    kun streng- mod typet dato som andennøgle. Efter trin 1 er occurred_at
--    dækket af PatientOccurredDt som residual-prædikat i stedet for som
--    seek-nøgle; kun ORDER BY på strengformen mister sin sorteringsfrie sti.
-- 3. IX_..._ElementPatientDate genskabes UDEN value_text i INCLUDE.
--    value_text er NVARCHAR(MAX), så INCLUDE'n kopierer LOB-data ind i
--    indekset og betaler det ved hvert insert. Elementkode-udtræk der
--    SELECTer value_text laver i stedet key lookups for de matchende rækker.
--
-- Rækkefølgen af trin 1 og 2 er bevidst: dækningen flyttes til
-- PatientOccurredDt, FØR dubletten droppes. Batches kører uafhængigt, så en
-- kørsel der afbrydes mellem dem må ikke kunne efterlade tabellen uden
-- dækning for strengdato-filtrerede patientudtræk.
--
-- Trin 1 og 3 genopbygger store indeks. På en udgave uden online rebuild
-- låses tabellen i det vindue, og en samtidig refresh af storet venter på
-- låsen — migrationen bør derfor køre uden for et refresh-vindue.
--
-- Idempotent og selvreparerende: vogterne er formuleret som fravær af den
-- ØNSKEDE form, ikke som tilstedeværelse af den gamle. Et indeks der mangler
-- helt opfylder derfor også vogteren og bliver genskabt i stedet for sprunget
-- over. Det er nødvendigt, fordi trin 2 fjerner den anden patientdækning, og
-- fordi genkørsel er reparationsvejen: filnavnet registreres først i
-- meta.MigrationHistory efter en fejlfri kørsel.
--
-- Trin 1 og 3 er desuden vogtet på tabellens eksistens, som de øvrige
-- store-migrationer: mangler storet, springes de over, så en database uden
-- tabellen ikke fejler på CREATE INDEX og spærrer infra-jobbet — basis-DDL'en
-- opretter tabellen med den ønskede indeksform.
-- ============================================================================

-- Trin 1: PatientOccurredDt skal have occurred_at i INCLUDE. Vogteren er sand
-- både for den gamle form og for et indeks der mangler helt. Tabelvogteren
-- holder trinnet ude på en database, hvor storet endnu ikke findes — der
-- giver basis-DDL'en formen.
IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND NOT EXISTS (
    SELECT 1
    FROM sys.index_columns ic
    JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
      AND i.name = N'IX_FHIR_SFI_datapoint_store_PatientOccurredDt'
      AND c.name = N'occurred_at'
      AND ic.is_included_column = 1
)
BEGIN
    DROP INDEX IF EXISTS [IX_FHIR_SFI_datapoint_store_PatientOccurredDt]
        ON [fhir].[SFI_datapoint_store];
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_PatientOccurredDt]
        ON [fhir].[SFI_datapoint_store] ([patient_id] ASC, [occurred_at_dt] ASC)
        INCLUDE ([element_code], [element_display], [source_model_name], [source_resource], [occurred_at])
        ON [PRIMARY];
    PRINT 'Genskabte IX_FHIR_SFI_datapoint_store_PatientOccurredDt med occurred_at i INCLUDE';
END
GO

-- Trin 2: PatientDate droppes — dækningen er flyttet til PatientOccurredDt i
-- trin 1.
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
      AND name = N'IX_FHIR_SFI_datapoint_store_PatientDate'
)
BEGIN
    DROP INDEX [IX_FHIR_SFI_datapoint_store_PatientDate]
        ON [fhir].[SFI_datapoint_store];
    PRINT 'Droppede IX_FHIR_SFI_datapoint_store_PatientDate (dækket af PatientOccurredDt)';
END
GO

-- Trin 3: ElementPatientDate må ikke bære value_text i INCLUDE. Første led
-- fanger et indeks der mangler helt, andet led den gamle form. Tabelvogteren
-- som i trin 1.
IF OBJECT_ID(N'[fhir].[SFI_datapoint_store]', 'U') IS NOT NULL
    AND (NOT EXISTS (
    SELECT 1 FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
      AND i.name = N'IX_FHIR_SFI_datapoint_store_ElementPatientDate'
) OR EXISTS (
    SELECT 1
    FROM sys.index_columns ic
    JOIN sys.indexes i ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')
      AND i.name = N'IX_FHIR_SFI_datapoint_store_ElementPatientDate'
      AND c.name = N'value_text'
      AND ic.is_included_column = 1
))
BEGIN
    DROP INDEX IF EXISTS [IX_FHIR_SFI_datapoint_store_ElementPatientDate]
        ON [fhir].[SFI_datapoint_store];
    CREATE NONCLUSTERED INDEX [IX_FHIR_SFI_datapoint_store_ElementPatientDate]
        ON [fhir].[SFI_datapoint_store] (
            [element_code] ASC,
            [occurred_at_dt] ASC,
            [patient_id] ASC
        )
        INCLUDE (
            [occurred_at],
            [element_display],
            [source_model_name],
            [organization_id],
            [value_type],
            [has_value],
            [value_int],
            [value_dec],
            [value_quantity],
            [value_unit]
        )
        ON [PRIMARY];
    PRINT 'Genskabte IX_FHIR_SFI_datapoint_store_ElementPatientDate uden value_text i INCLUDE';
END
GO
