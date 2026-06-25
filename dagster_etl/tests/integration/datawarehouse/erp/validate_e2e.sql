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
WHERE d.DW_EK_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.DW_EK_Kreditor = d.DW_EK_Kreditor
    );
-- Check dim.Vare → map.Vare
SELECT 'dim.Vare' AS dimension_table,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM dim.Vare d
WHERE d.DW_EK_Vare <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Vare m
        WHERE m.DW_EK_Vare = d.DW_EK_Vare
    );
-- Check dim.Kontoplan → map.Kontoplan
SELECT 'dim.Kontoplan' AS dimension_table,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM dim.Kontoplan d
WHERE d.DW_EK_Kontoplan <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kontoplan m
        WHERE m.DW_EK_Kontoplan = d.DW_EK_Kontoplan
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
WHERE f.DW_EK_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.DW_EK_Kreditor = f.DW_EK_Kreditor
    );
-- Check fct.Fakturalinje → map.Vare
SELECT 'fct.Fakturalinje → map.Vare' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje f
WHERE f.DW_EK_Vare <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Vare m
        WHERE m.DW_EK_Vare = f.DW_EK_Vare
    );
-- Check fct.Postering → map.Kreditor
SELECT 'fct.Postering → map.Kreditor' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering f
WHERE f.DW_EK_Kreditor <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kreditor m
        WHERE m.DW_EK_Kreditor = f.DW_EK_Kreditor
    );
-- Check fct.Postering → map.Kontoplan
SELECT 'fct.Postering → map.Kontoplan' AS relationship,
    COUNT(*) AS orphaned_records,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering f
WHERE f.DW_EK_Kontoplan <> -1 -- Exclude UNKNOWN
    AND NOT EXISTS (
        SELECT 1
        FROM map.Kontoplan m
        WHERE m.DW_EK_Kontoplan = f.DW_EK_Kontoplan
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
WHERE DW_EK_Kreditor = -1;
SELECT 'dim.Vare UNKNOWN row' AS check_item,
    COUNT(*) AS count,
    CASE
        WHEN COUNT(*) = 1 THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END AS status
FROM dim.Vare
WHERE DW_EK_Vare = -1;
SELECT 'dim.Kontoplan UNKNOWN row' AS check_item,
    COUNT(*) AS count,
    CASE
        WHEN COUNT(*) = 1 THEN '✅ EXISTS'
        ELSE '❌ MISSING'
    END AS status
FROM dim.Kontoplan
WHERE DW_EK_Kontoplan = -1;
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
WHERE DW_EK_Kreditor = -1;
SELECT 'fct.Fakturalinje UNKNOWN Vare' AS metric,
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
WHERE DW_EK_Vare = -1;
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
WHERE DW_EK_Kreditor = -1;
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
WHERE DW_EK_Kontoplan = -1;
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
WHERE DW_EK_Kreditor IS NULL;
SELECT 'fct.Fakturalinje NULL Vare FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Fakturalinje
WHERE DW_EK_Vare IS NULL;
SELECT 'fct.Postering NULL Kreditor FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering
WHERE DW_EK_Kreditor IS NULL;
SELECT 'fct.Postering NULL Kontoplan FK' AS check_item,
    COUNT(*) AS null_count,
    CASE
        WHEN COUNT(*) = 0 THEN '✅ PASS'
        ELSE '❌ FAIL'
    END AS status
FROM fct.Postering
WHERE DW_EK_Kontoplan IS NULL;
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
        SELECT ref_LedgertransId,
            COUNT(*) AS cnt
        FROM fct.Postering
        GROUP BY ref_LedgertransId
        HAVING COUNT(*) > 1
    ) dups;
-- ============================================================================
-- 6. DATA VOLUME: Record Counts
-- ============================================================================
PRINT '===== 6. DATA VOLUME: Record Counts (Diagnostic) =====';
SELECT 'map.Kreditor' AS table_name,
    COUNT(*) AS row_count
FROM map.Kreditor;
SELECT 'map.Vare' AS table_name,
    COUNT(*) AS row_count
FROM map.Vare;
SELECT 'map.Kontoplan' AS table_name,
    COUNT(*) AS row_count
FROM map.Kontoplan;
SELECT 'dim.Kreditor' AS table_name,
    COUNT(*) AS row_count
FROM dim.Kreditor;
SELECT 'dim.Vare' AS table_name,
    COUNT(*) AS row_count
FROM dim.Vare;
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
    COUNT(DISTINCT s.KreditorId) AS staging_count,
    (
        SELECT COUNT(*)
        FROM map.Kreditor
    ) AS map_count,
    CAST(
        (
            SELECT COUNT(*)
            FROM map.Kreditor
        ) * 100.0 / NULLIF(COUNT(DISTINCT s.KreditorId), 0) AS DECIMAL(5, 2)
    ) AS coverage_pct
FROM stg.kreditor_sydax s;
-- What % of map entries have dimension attributes?
SELECT 'Map Kreditor → Dim enrichment' AS metric,
    (
        SELECT COUNT(*)
        FROM map.Kreditor
        WHERE DW_EK_Kreditor <> -1
    ) AS map_count,
    (
        SELECT COUNT(*)
        FROM dim.Kreditor
        WHERE DW_EK_Kreditor <> -1
    ) AS dim_count,
    CAST(
        (
            SELECT COUNT(*)
            FROM dim.Kreditor
            WHERE DW_EK_Kreditor <> -1
        ) * 100.0 / NULLIF(
            (
                SELECT COUNT(*)
                FROM map.Kreditor
                WHERE DW_EK_Kreditor <> -1
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
