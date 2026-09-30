-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- fhir.Patient.dw_is_deleted: NULL -> 0, og herefter NOT NULL DEFAULT 0
--
-- Fundet under AB#27834. fhir.Patient havde 1.991.600 raekker med 0 og 898 med
-- NULL. De 898 er praecis de stub-patienter AB#27833 oprettede: SQL-on-FHIR-
-- mergens INSERT naevner ikke kolonnen, og uden default lander den som NULL.
--
-- Det er en faelde, ikke kosmetik. Et helt almindeligt `WHERE dw_is_deleted = 0`
-- dropper lydloest netop de borgere, AB#27833 blev lavet for at tilfoeje. I dag
-- reddes de kun af, at ref.PatientToBorger ikke filtrerer paa kolonnen.
--
-- Samme moenster er allerede loest paa fhir.Patient_Address og
-- fhir.Organization (NOT NULL DEFAULT 0) — se kommentaren i
-- tables/fhir/01_tables.sql, der netop advarer mod den nullable variant.
--
-- BEMAERK: fhir.Encounter_Location (19.891.446 NULL) og fhir.Location (3.660
-- NULL) har det samme problem i vaerre grad — der betyder NULL "levende", og
-- `= 0` returnerer ingenting overhovedet. De roeres IKKE her: de er faelles
-- FHIR-flader, og en NOT NULL-aendring paa 20 mio. raekker hoerer til en egen
-- opgave med sit eget aftryk. Konsumenter skal indtil da bruge
-- ISNULL(dw_is_deleted, 0) = 0 — det goer DSA/SQL/dm/haiba_kontakt.py.
-- ============================================================================

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
BEGIN
    UPDATE [fhir].[Patient] SET [dw_is_deleted] = 0 WHERE [dw_is_deleted] IS NULL;
END
GO

IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
                 AND name = N'dw_is_deleted' AND is_nullable = 1)
BEGIN
    ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_is_deleted] BIT NOT NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints
                   WHERE name = N'DF_FHIR_Patient_DwIsDeleted')
    BEGIN
        ALTER TABLE [fhir].[Patient]
            ADD CONSTRAINT [DF_FHIR_Patient_DwIsDeleted] DEFAULT 0 FOR [dw_is_deleted];
    END
END
GO
