-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*
 * Migrate ERP watermark state from meta.etl_watermark_state to meta.dfm_state.
 *
 * The ERP-related DFM assets (ILS and KMD UdData) have been converted from
 * EtlWatermarkManager to DfmManager. This script copies their last successful
 * checkpoint values so the new DFM assets continue delta loading from where
 * the watermark system left off — avoiding an expensive full extract
 * (especially critical for spend_analysis_lines_kmd_uddata from Synapse).
 *
 * Safe for re-runs: uses WHEN NOT MATCHED only — if a DFM checkpoint already
 * exists (because DFM assets have already run), it is NOT overwritten.
 *
 * Run ONCE before the first execution of the new DFM-based ERP assets in an
 * environment that has NOT run the _dfm_new shadow pipeline.
 *
 * Full-load tables (lgrbeh_ils, afdlgrbeh_ils) do not have checkpoints and
 * are excluded — they always extract everything regardless.
 */

-- ============================================================================
-- Archive layer checkpoints (arc.*)
-- ============================================================================

-- arc.unspsc_ils (ILS, delta via sysdato, datetime checkpoint)
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'  AS [scope],
        'archive'  AS [target_type],
        'arc'      AS [schema_name],
        'unspsc_ils' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'archive'
      AND w.[schema_name] = 'arc'
      AND w.[table_name]  = 'unspsc_ils'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- arc.vare_ils (ILS, delta via sysdato, datetime checkpoint)
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'  AS [scope],
        'archive'  AS [target_type],
        'arc'      AS [schema_name],
        'vare_ils' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'archive'
      AND w.[schema_name] = 'arc'
      AND w.[table_name]  = 'vare_ils'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- arc.unspsc_kmd_uddata (KMD UdData, delta via audit-MAX, int checkpoint)
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'  AS [scope],
        'archive'  AS [target_type],
        'arc'      AS [schema_name],
        'unspsc_kmd_uddata' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'archive'
      AND w.[schema_name] = 'arc'
      AND w.[table_name]  = 'unspsc_kmd_uddata'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- arc.spend_analysis_lines_kmd_uddata (KMD UdData / Synapse, delta via audit-MAX, int checkpoint)
-- CRITICAL: without this migration, a NULL checkpoint triggers full extract from Synapse (timeout risk)
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'  AS [scope],
        'archive'  AS [target_type],
        'arc'      AS [schema_name],
        'spend_analysis_lines_kmd_uddata' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'archive'
      AND w.[schema_name] = 'arc'
      AND w.[table_name]  = 'spend_analysis_lines_kmd_uddata'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- ============================================================================
-- Dimension layer checkpoints (dim.*)
-- ============================================================================

-- dim.UNSPSC
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'dimension' AS [target_type],
        'dim'       AS [schema_name],
        'UNSPSC'    AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'dimension'
      AND w.[schema_name] = 'dim'
      AND w.[table_name]  = 'UNSPSC'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- dim.LagerbeholdningEgenskaber
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'dimension' AS [target_type],
        'dim'       AS [schema_name],
        'LagerbeholdningEgenskaber' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'dimension'
      AND w.[schema_name] = 'dim'
      AND w.[table_name]  = 'LagerbeholdningEgenskaber'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- dim.AfdelingLagerbeholdningEgenskaber
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'dimension' AS [target_type],
        'dim'       AS [schema_name],
        'AfdelingLagerbeholdningEgenskaber' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'dimension'
      AND w.[schema_name] = 'dim'
      AND w.[table_name]  = 'AfdelingLagerbeholdningEgenskaber'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- ============================================================================
-- Fact layer checkpoints (fct.*)
-- ============================================================================

-- fct.Lagerbeholdning
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging' AS [scope],
        'fact'    AS [target_type],
        'fct'     AS [schema_name],
        'Lagerbeholdning' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'fact'
      AND w.[schema_name] = 'fct'
      AND w.[table_name]  = 'Lagerbeholdning'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

-- fct.AfdelingLagerbeholdning
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging' AS [scope],
        'fact'    AS [target_type],
        'fct'     AS [schema_name],
        'AfdelingLagerbeholdning' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'fact'
      AND w.[schema_name] = 'fct'
      AND w.[table_name]  = 'AfdelingLagerbeholdning'
) AS src
ON  tgt.[scope]       = src.[scope]
AND tgt.[target_type] = src.[target_type]
AND tgt.[schema_name] = src.[schema_name]
AND tgt.[table_name]  = src.[table_name]
WHEN NOT MATCHED THEN
    INSERT ([scope], [target_type], [schema_name], [table_name],
            [checkpoint_value], [last_mode])
    VALUES (src.[scope], src.[target_type], src.[schema_name], src.[table_name],
            src.[checkpoint_value], 'delta');

PRINT 'ERP watermark → DFM checkpoint migration complete (9 entries).';
PRINT 'Excluded: lgrbeh_ils, afdlgrbeh_ils (full_load, no checkpoint needed).';
