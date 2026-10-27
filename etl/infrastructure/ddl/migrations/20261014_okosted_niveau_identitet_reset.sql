-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Okosted re-keyes til (nummer, niveau)-identitet. Tabeller med ny
-- form DROPpes (DDL-assets genskaber dem med nye bredder/kolonner, inkl.
-- dim.Okosted's UNKNOWN-raekke); tabeller med uaendret form TRUNCATEs. Alt
-- genopbygges af en fuld omkoersel (cs_okosted -> cm -> stg/map/dim -> facts
-- -> DSO-sync). DFM-state for de tre fact-kaeders pre_stg-checkpoints
-- nulstilles ogsaa: pre_stg_Postering/Budget/Finanskladdelinje_sydax
-- delta-filtrerer paa MIN af deres nedstroems dim/fct-checkpoints
-- (meta.DFMState), og uden reset ville en almindelig (ikke full_load) koersel
-- kun MERGE'e delta-vinduet ind i de lige truncate'de fact-tabeller. Idempotent
-- + tracked once pr. database.

IF OBJECT_ID(N'[codesystem].[OkostedConcept]', 'U') IS NOT NULL
    DROP TABLE [codesystem].[OkostedConcept];
GO
IF OBJECT_ID(N'[codesystem].[OkostedProperties]', 'U') IS NOT NULL
    DROP TABLE [codesystem].[OkostedProperties];
GO
IF OBJECT_ID(N'[codesystem].[OkostedHierarchy]', 'U') IS NOT NULL
    DROP TABLE [codesystem].[OkostedHierarchy];
GO
IF OBJECT_ID(N'[map].[Okosted]', 'U') IS NOT NULL
    DROP TABLE [map].[Okosted];
GO
IF OBJECT_ID(N'[dim].[Okosted]', 'U') IS NOT NULL
    DROP TABLE [dim].[Okosted];
GO
IF OBJECT_ID(N'[conceptmap].[OkostedToSOR]', 'U') IS NOT NULL
    DROP TABLE [conceptmap].[OkostedToSOR];
GO
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
    TRUNCATE TABLE [fct].[Postering];
GO
IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL
    TRUNCATE TABLE [fct].[Budget];
GO
IF OBJECT_ID(N'[fct].[Finanskladdelinje]', 'U') IS NOT NULL
    TRUNCATE TABLE [fct].[Finanskladdelinje];
GO
IF OBJECT_ID(N'[export].[CodesystemToOs2mo]', 'U') IS NOT NULL
    DELETE FROM [export].[CodesystemToOs2mo] WHERE [codesystem] = 'Okosted';
GO

-- pre_stg_Postering_sydax reads MIN(dim.Postering, fct.Postering); pre_stg_Budget_sydax
-- reads MIN(dim.BudgetEgenskaber, fct.Budget); pre_stg_Finanskladdelinje_sydax reads
-- MIN(dim.Finanskladdelinje, fct.Finanskladdelinje, dim.Finanskladde). Clearing all
-- seven rows forces the next (non-full_load) run of each chain to read the archives
-- fully again, matching the just-truncated fact tables.
IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
BEGIN
    DELETE FROM [meta].[DFMState]
    WHERE [scope] = 'staging'
      AND (
            ([target_type] = 'dimension' AND [schema_name] = 'dim'
             AND [table_name] IN ('Postering', 'BudgetEgenskaber', 'Finanskladdelinje', 'Finanskladde'))
         OR ([target_type] = 'fact'      AND [schema_name] = 'fct'
             AND [table_name] IN ('Postering', 'Budget', 'Finanskladdelinje'))
      );
END
GO
