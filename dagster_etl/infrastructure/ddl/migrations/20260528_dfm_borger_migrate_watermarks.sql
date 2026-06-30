-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*
 * Migrate Borger watermark state from meta.etl_watermark_state to meta.dfm_state.
 *
 * The 3 Borger-related assets (ref.Patient_Borger, ref.Patient_Address_Borger,
 * dim.Borger) have been converted from EtlWatermarkManager to DfmManager.
 * This script copies their last successful checkpoint values so the new DFM
 * assets continue delta loading from where the watermark system left off.
 *
 * Run ONCE before the first execution of the new DFM-based Borger assets.
 */

-- ref.Patient_Borger
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'reference' AS [target_type],
        'ref'       AS [schema_name],
        'Patient_Borger' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'ref'
      AND w.[schema_name] = 'ref'
      AND w.[table_name]  = 'Patient_Borger'
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

-- ref.Patient_Address_Borger
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'reference' AS [target_type],
        'ref'       AS [schema_name],
        'Patient_Address_Borger' AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'ref'
      AND w.[schema_name] = 'ref'
      AND w.[table_name]  = 'Patient_Address_Borger'
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

-- dim.Borger
MERGE [meta].[dfm_state] AS tgt
USING (
    SELECT
        'staging'   AS [scope],
        'dimension' AS [target_type],
        'dim'       AS [schema_name],
        'Borger'    AS [table_name],
        w.[watermark_value] AS [checkpoint_value]
    FROM [meta].[etl_watermark_state] w
    WHERE w.[entity_type] = 'dim'
      AND w.[schema_name] = 'dim'
      AND w.[table_name]  = 'Borger'
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

PRINT 'Borger watermark → DFM checkpoint migration complete.';
