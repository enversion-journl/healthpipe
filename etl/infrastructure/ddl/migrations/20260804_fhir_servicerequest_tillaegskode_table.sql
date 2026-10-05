-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: Create fhir.ServiceRequest_tillaegskode child table
-- WHY: RIS orderDetail tillægskoder in own grain. Requires re-post of
-- ServiceRequest assets and SoF extract of servicerequest_tillaegskode_view.

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NULL
BEGIN
    CREATE TABLE [fhir].[ServiceRequest_tillaegskode] (
        [service_request_id] NVARCHAR(255) NOT NULL,
        [source_identifier_value] NVARCHAR(255) NULL,
        [tillaegskode_kode] NVARCHAR(255) NOT NULL,
        [tillaegskode_system] NVARCHAR(500) NOT NULL,
        [tillaegskode_display] NVARCHAR(500) NULL,
        [last_updated] DATETIME2 NULL,
        [created_at] DATETIME2 NULL,
        [dw_timestamp_insert] DATETIME2 NULL,
        [dw_timestamp_update] DATETIME2 NULL,
        CONSTRAINT [PK_FHIR_ServiceRequest_tillaegskode]
            PRIMARY KEY CLUSTERED ([service_request_id], [tillaegskode_kode], [tillaegskode_system])
    ) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequestTillaegskode_SrId]
        ON [fhir].[ServiceRequest_tillaegskode] ([service_request_id] ASC) ON [PRIMARY];

    CREATE NONCLUSTERED INDEX [IX_FHIR_ServiceRequestTillaegskode_LastUpdated]
        ON [fhir].[ServiceRequest_tillaegskode] ([last_updated] ASC) ON [PRIMARY];
END
GO
