-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename fhir.CarePlanActivity -> fhir.CarePlan_Activity
-- Follows Parent_Child underscore convention (Thread #55219).
-- Pattern matches 20260410_fhir_rename_careplan_tables.sql.
-- ============================================================================

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlanActivity' AND SCHEMA_NAME(schema_id) = 'fhir')
   AND NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CarePlan_Activity' AND SCHEMA_NAME(schema_id) = 'fhir')
BEGIN
    EXEC sp_rename 'fhir.CarePlanActivity', 'CarePlan_Activity';

    IF EXISTS (SELECT 1 FROM sys.key_constraints WHERE name = 'PK_FHIR_CarePlanActivity')
        EXEC sp_rename 'fhir.PK_FHIR_CarePlanActivity', 'PK_FHIR_CarePlan_Activity', 'OBJECT';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivity_CarePlanId' AND object_id = OBJECT_ID('fhir.CarePlan_Activity'))
        EXEC sp_rename 'fhir.CarePlan_Activity.IX_FHIR_CarePlanActivity_CarePlanId', 'IX_FHIR_CarePlan_Activity_CarePlanId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivity_PatientId' AND object_id = OBJECT_ID('fhir.CarePlan_Activity'))
        EXEC sp_rename 'fhir.CarePlan_Activity.IX_FHIR_CarePlanActivity_PatientId', 'IX_FHIR_CarePlan_Activity_PatientId', 'INDEX';

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FHIR_CarePlanActivity_LastUpdated' AND object_id = OBJECT_ID('fhir.CarePlan_Activity'))
        EXEC sp_rename 'fhir.CarePlan_Activity.IX_FHIR_CarePlanActivity_LastUpdated', 'IX_FHIR_CarePlan_Activity_LastUpdated', 'INDEX';

    PRINT 'Renamed fhir.CarePlanActivity -> fhir.CarePlan_Activity (table + PK + indexes)';
END

-- Also update the view
IF OBJECT_ID(N'[vw_fhir].[CarePlanActivity]', 'V') IS NOT NULL
BEGIN
    DROP VIEW [vw_fhir].[CarePlanActivity];
    PRINT 'Dropped old view vw_fhir.CarePlanActivity (will be recreated as vw_fhir.CarePlan_Activity)';
END
GO
