-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Create fhir.Procedure_tillaegskode child table
-- WHY: Supplementary procedure codes in own grain. Requires re-post of Procedure
-- assets and SoF extract of procedure_tillaegskode_view.

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NULL
BEGIN
    CREATE TABLE [fhir].[Procedure_tillaegskode] (
        [procedure_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [tillaegskode_kode] NVARCHAR(255) NOT NULL,
        [tillaegskode_system] NVARCHAR(500) NOT NULL,
        [tillaegskode_display] NVARCHAR(500) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NULL,
        [dw_timestamp_update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Procedure_tillaegskode]
            PRIMARY KEY CLUSTERED ([procedure_id], [tillaegskode_kode], [tillaegskode_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ProcedureTillaegskode_ProcedureId]
        ON [fhir].[Procedure_tillaegskode] ([procedure_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ProcedureTillaegskode_LastUpdated]
        ON [fhir].[Procedure_tillaegskode] ([last_updated] ASC) ON [PRIMARY];
END
GO
