-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- DQA: per-entity replay feeder views over dqa.RemediationQueue -- the distinct
-- PENDING business keys under the archive's id column name, so a DFM ids-mode run
-- can point id_filter_table straight at them (see the operations guide).
--
-- Depends on dqa.RemediationQueue (tables/dqa/02_remediation_queue.sql); applied
-- after it by the staging_infra_dqa infrastructure asset. Idempotent.
-- =============================================================================

CREATE OR ALTER VIEW [dqa].[v_replay_ifdb_encounter] AS
    SELECT DISTINCT [business_key] AS [OBJECT_ID]
    FROM [dqa].[RemediationQueue]
    WHERE [replay_entity] = 'ifdb_encounter' AND [status] = 'pending' AND [business_key] IS NOT NULL;
GO

CREATE OR ALTER VIEW [dqa].[v_replay_ifdb_episode_of_care] AS
    SELECT DISTINCT [business_key] AS [OBJECT_ID]
    FROM [dqa].[RemediationQueue]
    WHERE [replay_entity] = 'ifdb_episode_of_care' AND [status] = 'pending' AND [business_key] IS NOT NULL;
GO

CREATE OR ALTER VIEW [dqa].[v_replay_ifdb_patient] AS
    SELECT DISTINCT [business_key] AS [PATIENT_ID]
    FROM [dqa].[RemediationQueue]
    WHERE [replay_entity] = 'ifdb_patient' AND [status] = 'pending' AND [business_key] IS NOT NULL;
GO
