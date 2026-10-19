-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Fjern kapacitetstypens kodesystem-tabeller
--
--   codesystem.SengeenhedKapacitetConcept
--   codesystem.SengeenhedKapacitetProperties
--   codesystem.SengeenhedKapacitetHierarchy
--
-- Kapacitetstypen (disponible | normerede) er et lukket saet paa to vaerdier,
-- defineret i kode frem for udtrukket. Repoets oevrige kodesystemer er
-- udtrukne kataloger med mange koder, et planlagt job og en aftagerflade;
-- dette havde ingen af delene, og de to koder er deres egen displaytekst.
-- Koden bliver, hvor den hoerer hjemme: paa ressourcen og i
-- fhir.WardUnit_Capacity (capacity_type + capacity_type_system) -- samme
-- behandling som kategorien bed-capacity paa den samme ressource.
--
-- Tabellerne findes kun paa development (oprettet 17-09-2026); QA og prod har
-- dem aldrig faaet. Guarded pr. tabel, saa en gentagelse og en database uden
-- dem er en no-op. Base-DDL'en opretter dem ikke laengere, saa der er intet,
-- der bygger dem op igen.
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[codesystem].[SengeenhedKapacitetHierarchy]', N'U') IS NOT NULL
BEGIN
    DROP TABLE [codesystem].[SengeenhedKapacitetHierarchy];
    PRINT 'Droppede codesystem.SengeenhedKapacitetHierarchy';
END
GO

IF OBJECT_ID(N'[codesystem].[SengeenhedKapacitetProperties]', N'U') IS NOT NULL
BEGIN
    DROP TABLE [codesystem].[SengeenhedKapacitetProperties];
    PRINT 'Droppede codesystem.SengeenhedKapacitetProperties';
END
GO

IF OBJECT_ID(N'[codesystem].[SengeenhedKapacitetConcept]', N'U') IS NOT NULL
BEGIN
    DROP TABLE [codesystem].[SengeenhedKapacitetConcept];
    PRINT 'Droppede codesystem.SengeenhedKapacitetConcept';
END
GO
