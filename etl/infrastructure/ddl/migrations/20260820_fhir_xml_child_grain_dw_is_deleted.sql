-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- Migration: dw_is_deleted on XML/SOF child-grain tables.
--
-- Procedure/Condition/ServiceRequest tillægskode and QuestionnaireResponse_item
-- explode nested JSON. MERGE without parent-scoped reconcile accumulates rows
-- that left the parent resource. child_grain_reconcile (same as Patient_Address)
-- sets this flag; MERGE clears it on match. NOT NULL DEFAULT 0 so
-- `WHERE dw_is_deleted = 0` cannot drop live rows. SoF views do not project it.
-- Idempotent; safe on staging and DSO.
-- =============================================================================

IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Procedure_tillaegskode]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Procedure_tillaegskode]
        ADD [dw_is_deleted] INT NOT NULL
            CONSTRAINT [DF_Procedure_tillaegskode_dw_is_deleted] DEFAULT 0;
END
GO

IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[Condition_tillaegskode]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition_tillaegskode]
        ADD [dw_is_deleted] INT NOT NULL
            CONSTRAINT [DF_Condition_tillaegskode_dw_is_deleted] DEFAULT 0;
END
GO

IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[ServiceRequest_tillaegskode]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[ServiceRequest_tillaegskode]
        ADD [dw_is_deleted] INT NOT NULL
            CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_is_deleted] DEFAULT 0;
END
GO

IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[fhir].[QuestionnaireResponse_item]', 'dw_is_deleted') IS NULL
BEGIN
    ALTER TABLE [fhir].[QuestionnaireResponse_item]
        ADD [dw_is_deleted] INT NOT NULL
            CONSTRAINT [DF_QuestionnaireResponse_item_dw_is_deleted] DEFAULT 0;
END
GO
