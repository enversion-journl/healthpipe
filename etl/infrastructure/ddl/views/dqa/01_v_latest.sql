-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- DQA: newest row per (environment, subject, check_type, name_suffix) -- the
-- current state of every check. Backs dashboards and the dqa_summary rollup.
--
-- Depends on dqa.Results (tables/dqa/01_results.sql); applied after it by the
-- staging_infra_dqa infrastructure asset. Idempotent (CREATE OR ALTER).
-- =============================================================================

CREATE OR ALTER VIEW [dqa].[v_latest] AS
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY [environment], [subject], [check_type], ISNULL([name_suffix], '')
               ORDER BY [checked_at] DESC, [result_id] DESC
           ) AS rn
    FROM [dqa].[Results]
)
SELECT result_id, run_id, checked_at, environment, track, subject, check_type,
       name_suffix, severity, cadence, passed, checked_count, violation_count,
       examples_json, detail_json
FROM ranked
WHERE rn = 1;
GO
