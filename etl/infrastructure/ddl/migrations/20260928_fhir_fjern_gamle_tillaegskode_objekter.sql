-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Fjern de gamle tillægskode-tabeller og -views, som omdøbningen efterlod.
--
-- Omdøbningen er hegnet af «AND OBJECT_ID('...SupplementaryCode') IS NULL» og
-- nægter derfor at omdøbe oven i en tabel, der allerede findes. Grund-DDL'en
-- havde allerede oprettet de nye tabeller, så omdøbningen sprang over, og de
-- gamle blev stående ved siden af. De står på den rækkemængde, de havde ved
-- omdøbningen, og opdateres ikke; checkpointet fulgte med til *_SupplementaryCode.
--
-- Ingen kode læser de gamle objekter, og hverken tabellerne eller viewene står
-- i den kanoniske DDL under tables/ og views/, så droppet holder. En gammel
-- tabel droppes kun, når efterfølgeren har mindst lige så mange rækker; ellers
-- stopper migrationen, for så er den gamle tabel den eneste fulde kopi.

IF OBJECT_ID(N'[vw_fhir].[Condition_tillaegskode]', N'V') IS NOT NULL
    DROP VIEW [vw_fhir].[Condition_tillaegskode];
GO

IF OBJECT_ID(N'[vw_fhir].[Procedure_tillaegskode]', N'V') IS NOT NULL
    DROP VIEW [vw_fhir].[Procedure_tillaegskode];
GO

IF OBJECT_ID(N'[vw_fhir].[ServiceRequest_tillaegskode]', N'V') IS NOT NULL
    DROP VIEW [vw_fhir].[ServiceRequest_tillaegskode];
GO

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Condition_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    IF (SELECT COUNT_BIG(*) FROM [fhir].[Condition_SupplementaryCode])
       < (SELECT COUNT_BIG(*) FROM [fhir].[Condition_tillaegskode])
        THROW 50028,
            N'fhir.Condition_SupplementaryCode har færre rækker end fhir.Condition_tillaegskode - genopbyg efterfølgeren, før den gamle tabel droppes.',
            1;
    DROP TABLE [fhir].[Condition_tillaegskode];
END
GO

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[Procedure_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    IF (SELECT COUNT_BIG(*) FROM [fhir].[Procedure_SupplementaryCode])
       < (SELECT COUNT_BIG(*) FROM [fhir].[Procedure_tillaegskode])
        THROW 50028,
            N'fhir.Procedure_SupplementaryCode har færre rækker end fhir.Procedure_tillaegskode - genopbyg efterfølgeren, før den gamle tabel droppes.',
            1;
    DROP TABLE [fhir].[Procedure_tillaegskode];
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[fhir].[ServiceRequest_SupplementaryCode]', N'U') IS NOT NULL
BEGIN
    IF (SELECT COUNT_BIG(*) FROM [fhir].[ServiceRequest_SupplementaryCode])
       < (SELECT COUNT_BIG(*) FROM [fhir].[ServiceRequest_tillaegskode])
        THROW 50028,
            N'fhir.ServiceRequest_SupplementaryCode har færre rækker end fhir.ServiceRequest_tillaegskode - genopbyg efterfølgeren, før den gamle tabel droppes.',
            1;
    DROP TABLE [fhir].[ServiceRequest_tillaegskode];
END
GO
