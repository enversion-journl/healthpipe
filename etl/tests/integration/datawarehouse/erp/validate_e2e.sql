-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*
 * End-to-End Validation Script for ERP ETL Pipeline
 *
 * Purpose: Validate referential integrity and data quality after running full pipeline
 *
 * Run after:
 * 1. dagster job execute -j erp_infrastructure_setup
 * 2. dagster job execute -j erp_daily_etl
 *
 * Expected result: All checks should return 0 (no orphaned records)
 */
-- ============================================================================
-- 1. REFERENTIAL INTEGRITY: Dimensions → Maps
-- ============================================================================
PRINT '===== 1. REFERENTIAL INTEGRITY: Dimensions → Maps =====';
-- Check dim.Kreditor → map.Kreditor
SELECT 'dim.Kreditor' AS dimension_table,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM dim.Kreditor d
WHERE d.dw_ek_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.dw_ek_Kreditor = d.dw_ek_Kreditor
    );
-- Check dim.Leverandoervare → map.Leverandoervare
SELECT 'dim.Leverandoervare' AS dimension_table,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM dim.Leverandoervare d
WHERE d.dw_ek_Leverandoervare <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Leverandoervare m
        WHERE m.dw_ek_Leverandoervare = d.dw_ek_Leverandoervare
    );
-- Check dim.Kontoplan → map.Kontoplan
SELECT 'dim.Kontoplan' AS dimension_table,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM dim.Kontoplan d
WHERE d.dw_ek_Kontoplan <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kontoplan m
        WHERE m.dw_ek_Kontoplan = d.dw_ek_Kontoplan
    );
-- ============================================================================
-- 2. REFERENTIAL INTEGRITY: Facts → Maps (Clean Architecture!)
-- ============================================================================
PRINT '===== 2. REFERENTIAL INTEGRITY: Facts → Maps (Clean Architecture) =====';
-- Check fct.Fakturalinje → map.Kreditor
SELECT 'fct.Fakturalinje → map.Kreditor' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje f
WHERE f.dw_ek_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.dw_ek_Kreditor = f.dw_ek_Kreditor
    );
-- Check fct.Fakturalinje → map.Leverandoervare
SELECT 'fct.Fakturalinje → map.Leverandoervare' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje f
WHERE f.dw_ek_Leverandoervare <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Leverandoervare m
        WHERE m.dw_ek_Leverandoervare = f.dw_ek_Leverandoervare
    );
-- Check fct.Postering → map.Kreditor
SELECT 'fct.Postering → map.Kreditor' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering f
WHERE f.dw_ek_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.dw_ek_Kreditor = f.dw_ek_Kreditor
    );
-- Check fct.Postering → map.Kontoplan
SELECT 'fct.Postering → map.Kontoplan' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering f
WHERE f.dw_ek_Kontoplan <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kontoplan m
        WHERE m.dw_ek_Kontoplan = f.dw_ek_Kontoplan
    );
-- ============================================================================
-- 3. DATA QUALITY: UNKNOWN Usage
-- ============================================================================
PRINT '===== 3. DATA QUALITY: UNKNOWN Usage (Diagnostic) =====';
-- UNKNOWN dimension rows should exist
SELECT 'dim.Kreditor UNKNOWN row' AS check_item,
    COUNT(*) AS count,
    CASE
        WHEN COUNT(*) = 1 THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END AS status
FROM dim.Kreditor
WHERE dw_ek_Kreditor = -1;
SELECT 'dim.Leverandoervare UNKNOWN row' AS check_item,
    COUNT(*) AS count,
    CASE
        WHEN COUNT(*) = 1 THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END AS status
FROM dim.Leverandoervare
WHERE dw_ek_Leverandoervare = -1;
SELECT 'dim.Kontoplan UNKNOWN row' AS check_item,
    COUNT(*) AS count,
    CASE
        WHEN COUNT(*) = 1 THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END AS status
FROM dim.Kontoplan
WHERE dw_ek_Kontoplan = -1;
-- Count UNKNOWN usage in facts (for diagnostics - these are OK!)
SELECT 'fct.Fakturalinje UNKNOWN Kreditor' AS metric,
    COUNT(*) AS count,
    CAST(
        COUNT(*) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM fct.Fakturalinje
            ),
            0
        ) AS DECIMAL(5, 2)
    ) AS percentage
FROM fct.Fakturalinje
WHERE dw_ek_Kreditor = -1;
SELECT 'fct.Fakturalinje UNKNOWN Leverandoervare' AS metric,
    COUNT(*) AS count,
    CAST(
        COUNT(*) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM fct.Fakturalinje
            ),
            0
        ) AS DECIMAL(5, 2)
    ) AS percentage
FROM fct.Fakturalinje
WHERE dw_ek_Leverandoervare = -1;
SELECT 'fct.Postering UNKNOWN Kreditor' AS metric,
    COUNT(*) AS count,
    CAST(
        COUNT(*) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM fct.Postering
            ),
            0
        ) AS DECIMAL(5, 2)
    ) AS percentage
FROM fct.Postering
WHERE dw_ek_Kreditor = -1;
SELECT 'fct.Postering UNKNOWN Kontoplan' AS metric,
    COUNT(*) AS count,
    CAST(
        COUNT(*) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM fct.Postering
            ),
            0
        ) AS DECIMAL(5, 2)
    ) AS percentage
FROM fct.Postering
WHERE dw_ek_Kontoplan = -1;
-- ============================================================================
-- 4. DATA QUALITY: NULL Checks
-- ============================================================================
PRINT '===== 4. DATA QUALITY: NULL Checks =====';
-- FK columns should NEVER be NULL (use -1 for UNKNOWN instead)
SELECT 'fct.Fakturalinje NULL Kreditor FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje
WHERE dw_ek_Kreditor IS NULL;
SELECT 'fct.Fakturalinje NULL Leverandoervare FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje
WHERE dw_ek_Leverandoervare IS NULL;
SELECT 'fct.Postering NULL Kreditor FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering
WHERE dw_ek_Kreditor IS NULL;
SELECT 'fct.Postering NULL Kontoplan FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering
WHERE dw_ek_Kontoplan IS NULL;
-- ============================================================================
-- 5. DATA QUALITY: Grain Uniqueness
-- ============================================================================
PRINT '===== 5. DATA QUALITY: Grain Uniqueness =====';
-- Check for duplicate grains
SELECT 'fct.Fakturalinje duplicates' AS check_item,
    COUNT(*) AS duplicate_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM (
        SELECT statistic_invoice_line_id,
            statistic_invoice_header_id,
            COUNT(*) AS cnt
        FROM fct.Fakturalinje
        GROUP BY statistic_invoice_line_id,
            statistic_invoice_header_id
        HAVING COUNT(*) > 1
    ) dups;
SELECT 'fct.Postering duplicates' AS check_item,
    COUNT(*) AS duplicate_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM (
        SELECT ref_ledgertrans_id,
            COUNT(*) AS cnt
        FROM fct.Postering
        GROUP BY ref_ledgertrans_id
        HAVING COUNT(*) > 1
    ) dups;
-- ============================================================================
-- 6. DATA VOLUME: Record Counts
-- ============================================================================
PRINT '===== 6. DATA VOLUME: Record Counts (Diagnostic) =====';
SELECT 'map.Kreditor' AS table_name,
    COUNT(*) AS row_count
FROM map.Kreditor;
SELECT 'map.Leverandoervare' AS table_name,
    COUNT(*) AS row_count
FROM map.Leverandoervare;
SELECT 'map.Kontoplan' AS table_name,
    COUNT(*) AS row_count
FROM map.Kontoplan;
SELECT 'dim.Kreditor' AS table_name,
    COUNT(*) AS row_count
FROM dim.Kreditor;
SELECT 'dim.Leverandoervare' AS table_name,
    COUNT(*) AS row_count
FROM dim.Leverandoervare;
SELECT 'dim.Kontoplan' AS table_name,
    COUNT(*) AS row_count
FROM dim.Kontoplan;
SELECT 'fct.Fakturalinje' AS table_name,
    COUNT(*) AS row_count
FROM fct.Fakturalinje;
SELECT 'fct.Postering' AS table_name,
    COUNT(*) AS row_count
FROM fct.Postering;
-- ============================================================================
-- 7. DATA FLOW: Staging → Map → Dim → Fact
-- ============================================================================
PRINT '===== 7. DATA FLOW: Coverage Analysis (Diagnostic) =====';
-- What % of staging Kreditor made it to map?
SELECT 'Staging Kreditor → Map coverage' AS metric,
    COUNT(DISTINCT s.kreditor_id) AS staging_count,
    (
        SELECT COUNT(*)
        FROM map.Kreditor
    ) AS map_count,
    CAST(
        (
            SELECT COUNT(*)
            FROM map.Kreditor
        ) * 100.0 / NULLIF(COUNT(DISTINCT s.kreditor_id), 0) AS DECIMAL(5, 2)
    ) AS coverage_pct
FROM stg.sydax_kreditor s;
-- What % of map entries have dimension attributes?
SELECT 'Map Kreditor → Dim enrichment' AS metric,
    (
        SELECT COUNT(*)
        FROM map.Kreditor
        WHERE dw_ek_Kreditor <> -1
    ) AS map_count,
    (
        SELECT COUNT(*)
        FROM dim.Kreditor
        WHERE dw_ek_Kreditor <> -1
    ) AS dim_count,
    CAST(
        (
            SELECT COUNT(*)
            FROM dim.Kreditor
            WHERE dw_ek_Kreditor <> -1
        ) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM map.Kreditor
                WHERE dw_ek_Kreditor <> -1
            ),
            0
        ) AS DECIMAL(5, 2)
    ) AS coverage_pct;
-- ============================================================================
-- 8. SUMMARY
-- ============================================================================
PRINT '===== 8. VALIDATION SUMMARY =====';
PRINT '';
PRINT 'Expected Results:';
PRINT '- All referential integrity checks should return 0 orphaned records (✅ PASS)';
PRINT '- All NULL checks should return 0 null count (✅ PASS)';
PRINT '- All grain uniqueness checks should return 0 duplicates (✅ PASS)';
PRINT '- UNKNOWN usage is OK and expected for missing FK matches';
PRINT '';
PRINT 'If any checks FAIL:';
PRINT '1. Review asset logs for FK matching diagnostics';
PRINT '2. Check seed data in source systems';
PRINT '3. Verify clean code generation in staging layer';
PRINT '4. Review COALESCE logic in dimension assets';
PRINT '';
PRINT 'Script complete. Review all checks above.';
