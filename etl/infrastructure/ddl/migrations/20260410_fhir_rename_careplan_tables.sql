-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Migration: Rename CarePlanAlleMarkoerer -> CarePlan
--            Rename CarePlanActivityAlleMarkoerer -> CarePlanActivity
--
-- Idempotent: only runs if old name exists and new name does not.
-- Safe to re-run after the rename has been applied.
-- ============================================================================

-- ---------- CarePlanAlleMarkoerer -> CarePlan ----------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlanAlleMarkoerer' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlan' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.CarePlanAlleMarkoerer', 'CarePlan';

    -- Rename PK constraint
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_FHIR_CarePlanAlleMarkoerer')
        EXEC sp_rename 'fhir.PK_FHIR_CarePlanAlleMarkoerer', 'PK_FHIR_CarePlan', 'OBJECT';

    -- Rename indexes
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanAlleMarkoerer_PatientId' AND object_id = OBJECT_ID('fhir.CarePlan'))
        EXEC sp_rename 'fhir.CarePlan.IX_FHIR_CarePlanAlleMarkoerer_PatientId', 'IX_FHIR_CarePlan_PatientId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanAlleMarkoerer_LastUpdated' AND object_id = OBJECT_ID('fhir.CarePlan'))
        EXEC sp_rename 'fhir.CarePlan.IX_FHIR_CarePlanAlleMarkoerer_LastUpdated', 'IX_FHIR_CarePlan_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.CarePlanAlleMarkoerer -> fhir.CarePlan (table + PK + indexes)';
END
GO

-- ---------- CarePlanActivityAlleMarkoerer -> CarePlanActivity ----------
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlanActivityAlleMarkoerer' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlanActivity' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.CarePlanActivityAlleMarkoerer', 'CarePlanActivity';

    -- Rename PK constraint
    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_FHIR_CarePlanActivityAlleMarkoerer')
        EXEC sp_rename 'fhir.PK_FHIR_CarePlanActivityAlleMarkoerer', 'PK_FHIR_CarePlanActivity', 'OBJECT';

    -- Rename indexes
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivityAlleMarkoerer_CarePlanId' AND object_id = OBJECT_ID('fhir.CarePlanActivity'))
        EXEC sp_rename 'fhir.CarePlanActivity.IX_FHIR_CarePlanActivityAlleMarkoerer_CarePlanId', 'IX_FHIR_CarePlanActivity_CarePlanId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivityAlleMarkoerer_PatientId' AND object_id = OBJECT_ID('fhir.CarePlanActivity'))
        EXEC sp_rename 'fhir.CarePlanActivity.IX_FHIR_CarePlanActivityAlleMarkoerer_PatientId', 'IX_FHIR_CarePlanActivity_PatientId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivityAlleMarkoerer_LastUpdated' AND object_id = OBJECT_ID('fhir.CarePlanActivity'))
        EXEC sp_rename 'fhir.CarePlanActivity.IX_FHIR_CarePlanActivityAlleMarkoerer_LastUpdated', 'IX_FHIR_CarePlanActivity_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.CarePlanActivityAlleMarkoerer -> fhir.CarePlanActivity (table + PK + indexes)';
END
GO
