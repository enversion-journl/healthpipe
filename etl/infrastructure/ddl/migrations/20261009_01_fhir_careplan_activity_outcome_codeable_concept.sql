-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.CarePlan_Activity: markoerkode/markoer_display ->
-- outcome_codeable_concept_code, outcome_codeable_concept_system,
-- outcome_codeable_concept_display.
--
-- Kolonnerne projicerer standard-elementet CarePlan.activity.outcomeCodeableConcept
-- og navngives efter elementet i snake_case. outcome_codeable_concept_system er
-- ny: kodesystemet (SKS) ligger allerede på ressourcen, men blev ikke projiceret.
-- Ingen indeks eller constraints dækker kolonnerne, så kun kolonnerne omdøbes.
--
-- BACKFILL: eksisterende rækker får systemet SKS, hvor koden er udfyldt. Det er
-- præcis, hvad SoF-projektionen skriver: CarePlan-byggeren sætter
-- system=SYSTEM_SKS på hver outcome-coding, og viewet filtrerer på kategorien
-- alle_markoerer. En full_load af sof_careplan_activity_dfm ville give samme
-- resultat, men markerer hele tabellen slettet, merger alle rækker tilbage og
-- sætter dw_timestamp_update, så DSO-synken flytter hele tabellen igen.
-- dw_timestamp_update røres derfor ikke: migrationen kører på staging og DSO
-- hver for sig.
--
-- Løkken går i nøgleintervaller på den clustered PK, så hver række læses én
-- gang, og hver runde committes for sig. execute_sql kører med autocommit slået
-- fra, og driveren åbner selv en transaktion (@@TRANCOUNT = 1). En
-- BEGIN/COMMIT-blok ville blive indlejret i den og ikke committe noget. Derfor
-- afslutter løkken driverens transaktion med en bar COMMIT. Bagefter er hver
-- UPDATE sin egen transaktion. Loggen skal kun bære én runde ad gangen, og en
-- afbrudt backfill mister højst den igangværende runde. Ved genkørsel springer
-- prædikatet "system IS NULL" de færdige rækker over.
--
-- RÆKKEFØLGE: det daglige DFM SoF-job PUT'er den nye ViewDefinition
-- (fhir_sof_infrastructure), men kører ikke migrationerne. Kører det før
-- full_infrastructure_setup, fejler sof_careplan_activity_dfm på de ukendte
-- kolonnenavne, indtil migrationen er kørt. Der skrives ingen forkerte data.
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', N'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'markoerkode') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'outcome_codeable_concept_code') IS NULL
    EXEC sp_rename N'fhir.CarePlan_Activity.markoerkode', N'outcome_codeable_concept_code', N'COLUMN';
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', N'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'markoer_display') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'outcome_codeable_concept_display') IS NULL
    EXEC sp_rename N'fhir.CarePlan_Activity.markoer_display', N'outcome_codeable_concept_display', N'COLUMN';
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', N'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'outcome_codeable_concept_system') IS NULL
    ALTER TABLE [fhir].[CarePlan_Activity] ADD [outcome_codeable_concept_system] NVARCHAR(500) NULL;
GO

IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', N'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'outcome_codeable_concept_code') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[CarePlan_Activity]', N'outcome_codeable_concept_system') IS NOT NULL
BEGIN
    DECLARE @last NVARCHAR(255) = N'';
    DECLARE @next NVARCHAR(255);

    WHILE 1 = 1
    BEGIN
        SET @next = NULL;
        SELECT @next = MAX(batch.[ifdb_eventmarker_id])
        FROM (
            SELECT TOP (100000) [ifdb_eventmarker_id]
            FROM [fhir].[CarePlan_Activity]
            WHERE [ifdb_eventmarker_id] > @last
            ORDER BY [ifdb_eventmarker_id]
        ) AS batch;

        IF @next IS NULL BREAK;

        EXEC sp_executesql
            N'UPDATE [fhir].[CarePlan_Activity]
              SET [outcome_codeable_concept_system] = N''urn:oid:1.2.208.176.2.4''
              WHERE [ifdb_eventmarker_id] > @last
                AND [ifdb_eventmarker_id] <= @next
                AND [outcome_codeable_concept_code] IS NOT NULL
                AND [outcome_codeable_concept_system] IS NULL;',
            N'@last NVARCHAR(255), @next NVARCHAR(255)',
            @last = @last,
            @next = @next;

        WHILE @@TRANCOUNT > 0 COMMIT;

        SET @last = @next;
    END
END
GO

-- vw_fhir.CarePlan_Activity er SELECT * og skal følge tabellens kolonner med det samme.
IF OBJECT_ID(N'[vw_fhir].[CarePlan_Activity]', N'V') IS NOT NULL
    EXEC sys.sp_refreshview N'[vw_fhir].[CarePlan_Activity]';
GO
