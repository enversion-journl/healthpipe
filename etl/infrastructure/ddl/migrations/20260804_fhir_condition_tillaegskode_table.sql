-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Create fhir.Condition_tillaegskode child table
-- WHY: Tillægskoder in own grain (not inline on parent Condition). Requires
-- re-post of Condition assets and SoF extract of condition_tillaegskode_view.

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NULL
BEGIN
    CREATE TABLE [fhir].[Condition_tillaegskode] (
        [condition_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [tillaegskode_kode] NVARCHAR(255) NOT NULL,
        [tillaegskode_system] NVARCHAR(500) NOT NULL,
        [tillaegskode_display] NVARCHAR(500) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NULL,
        [dw_timestamp_update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_Condition_tillaegskode]
            PRIMARY KEY CLUSTERED ([condition_id], [tillaegskode_kode], [tillaegskode_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionTillaegskode_ConditionId]
        ON [fhir].[Condition_tillaegskode] ([condition_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ConditionTillaegskode_LastUpdated]
        ON [fhir].[Condition_tillaegskode] ([last_updated] ASC) ON [PRIMARY];
END
GO
