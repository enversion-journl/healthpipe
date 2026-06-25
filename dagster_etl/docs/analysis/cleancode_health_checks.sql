-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- CleanCode health & conformance checks (ERP pipeline)
-- ============================================================================
-- Run section-by-section against the QA *staging* DB. Each section is
-- independent. Purpose: verify every CleanCode resolves and conforms the way
-- we expect — i.e. the same real-world entity yields ONE surrogate key, codes
-- are well-formed, and fact FKs actually resolve.
--
-- Background: the faktura<->postering bridge exposed a creditor-conformance bug
-- where MySupply ('CVR_DK########') and SydAX/ILS ('CVR_########') produced two
-- surrogate keys for the same creditor. These queries generalise that check to
-- every CleanCode so we catch the whole class of problem.
-- ============================================================================


-- ============================================================================
-- SECTION 1 — SPLIT-SURROGATE DETECTOR (the DK-class bug, generalised)
-- ----------------------------------------------------------------------------
-- A map's PK is the raw CleanCode, so it is unique *as a string*. But two codes
-- that differ only by case / whitespace describe the SAME entity yet get
-- DIFFERENT surrogate keys. This finds every such collision across all maps.
-- norm = UPPER + trim + strip-spaces. n_surrogates > 1 => a conformance bug.
-- Expectation: zero rows. Any row is a split surrogate to investigate.
-- ============================================================================
;WITH split AS (
    SELECT 'Kontoplan'     AS entity, UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Kontoplan,    ' ','')))) AS norm, CleanCode_Kontoplan     AS cc FROM [map].[Kontoplan]
    UNION ALL SELECT 'Kreditor',     UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Kreditor,     ' ','')))), CleanCode_Kreditor      FROM [map].[Kreditor]
    UNION ALL SELECT 'Vare',         UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Vare,         ' ','')))), CleanCode_Vare          FROM [map].[Vare]
    UNION ALL SELECT 'Faktura',      UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Faktura,      ' ','')))), CleanCode_Faktura       FROM [map].[Faktura]
    UNION ALL SELECT 'Fakturalinje', UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Fakturalinje, ' ','')))), CleanCode_Fakturalinje  FROM [map].[Fakturalinje]
    UNION ALL SELECT 'Postering',    UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Postering,    ' ','')))), CleanCode_Postering     FROM [map].[Postering]
    UNION ALL SELECT 'Aftale',       UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Aftale,       ' ','')))), CleanCode_Aftale        FROM [map].[Aftale]
    UNION ALL SELECT 'Vareudlevering',UPPER(LTRIM(RTRIM(REPLACE(CleanCode_Vareudlevering,' ','')))),CleanCode_Vareudlevering FROM [map].[Vareudlevering]
)
SELECT entity, norm,
       COUNT(DISTINCT cc)            AS n_surrogates,
       MIN(cc)                       AS sample_code_1,
       MAX(cc)                       AS sample_code_2
FROM split
GROUP BY entity, norm
HAVING COUNT(DISTINCT cc) > 1
ORDER BY n_surrogates DESC, entity;


-- ============================================================================
-- SECTION 2 — CREDITOR DK-PREFIX REGRESSION GUARD
-- ----------------------------------------------------------------------------
-- After the DK-strip fix + reload, no CVR cleancode should keep a 'DK' prefix,
-- and the same CVR digits must not appear both with and without 'DK'.
-- Expectation: dk_prefixed_rows = 0 ; split-by-DK returns no rows.
-- ============================================================================
-- 2a) Any CVR cleancode still carrying a 'DK' prefix:
SELECT SUM(CASE WHEN CleanCode_Kreditor LIKE 'CVR[_]DK[0-9]%' THEN 1 ELSE 0 END) AS dk_prefixed_rows,
       COUNT(*) AS total_kreditor
FROM [map].[Kreditor];

-- 2b) Same CVR digits present both with and without 'DK' (split surrogate):
;WITH k AS (
    SELECT CleanCode_Kreditor AS cc,
           CASE WHEN CleanCode_Kreditor LIKE 'CVR[_]DK%'
                THEN 'CVR_' + SUBSTRING(CleanCode_Kreditor, 7, 100)
                ELSE CleanCode_Kreditor END AS cvr_norm
    FROM [map].[Kreditor]
)
SELECT cvr_norm, COUNT(DISTINCT cc) AS n_surrogates, MIN(cc) AS code_1, MAX(cc) AS code_2
FROM k
GROUP BY cvr_norm
HAVING COUNT(DISTINCT cc) > 1
ORDER BY n_surrogates DESC;


-- ============================================================================
-- SECTION 3 — CLEANCODE FORMAT / HYGIENE AUDIT (per map)
-- ----------------------------------------------------------------------------
-- Row counts, NULL/empty, stray whitespace, lowercase, and prefix (text before
-- the first '_'). Eyeball that prefixes match expectations
-- (CVR / KONTOPLAN / FAKTURA / AFTALE / LEVVARE / etc.) and null/space = 0.
-- ============================================================================
SELECT entity, total, null_or_empty, has_ws, has_lower
FROM (
    SELECT 'Kontoplan' AS entity, COUNT(*) total,
           SUM(CASE WHEN CleanCode_Kontoplan IS NULL OR LTRIM(RTRIM(CleanCode_Kontoplan))='' THEN 1 ELSE 0 END) null_or_empty,
           SUM(CASE WHEN CleanCode_Kontoplan <> LTRIM(RTRIM(CleanCode_Kontoplan)) THEN 1 ELSE 0 END) has_ws,
           SUM(CASE WHEN CleanCode_Kontoplan COLLATE Latin1_General_CS_AS <> UPPER(CleanCode_Kontoplan) COLLATE Latin1_General_CS_AS THEN 1 ELSE 0 END) has_lower
    FROM [map].[Kontoplan]
    UNION ALL SELECT 'Kreditor', COUNT(*),
           SUM(CASE WHEN CleanCode_Kreditor IS NULL OR LTRIM(RTRIM(CleanCode_Kreditor))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Kreditor <> LTRIM(RTRIM(CleanCode_Kreditor)) THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Kreditor COLLATE Latin1_General_CS_AS <> UPPER(CleanCode_Kreditor) COLLATE Latin1_General_CS_AS THEN 1 ELSE 0 END)
    FROM [map].[Kreditor]
    UNION ALL SELECT 'Vare', COUNT(*),
           SUM(CASE WHEN CleanCode_Vare IS NULL OR LTRIM(RTRIM(CleanCode_Vare))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Vare <> LTRIM(RTRIM(CleanCode_Vare)) THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Vare COLLATE Latin1_General_CS_AS <> UPPER(CleanCode_Vare) COLLATE Latin1_General_CS_AS THEN 1 ELSE 0 END)
    FROM [map].[Vare]
    UNION ALL SELECT 'Faktura', COUNT(*),
           SUM(CASE WHEN CleanCode_Faktura IS NULL OR LTRIM(RTRIM(CleanCode_Faktura))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Faktura <> LTRIM(RTRIM(CleanCode_Faktura)) THEN 1 ELSE 0 END),
           0   -- Faktura header key is mixed-case by design ('CleanCode_..')
    FROM [map].[Faktura]
    UNION ALL SELECT 'Fakturalinje', COUNT(*),
           SUM(CASE WHEN CleanCode_Fakturalinje IS NULL OR LTRIM(RTRIM(CleanCode_Fakturalinje))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Fakturalinje <> LTRIM(RTRIM(CleanCode_Fakturalinje)) THEN 1 ELSE 0 END),
           0
    FROM [map].[Fakturalinje]
    UNION ALL SELECT 'Postering', COUNT(*),
           SUM(CASE WHEN CleanCode_Postering IS NULL OR LTRIM(RTRIM(CleanCode_Postering))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Postering <> LTRIM(RTRIM(CleanCode_Postering)) THEN 1 ELSE 0 END),
           0
    FROM [map].[Postering]
    UNION ALL SELECT 'Aftale', COUNT(*),
           SUM(CASE WHEN CleanCode_Aftale IS NULL OR LTRIM(RTRIM(CleanCode_Aftale))='' THEN 1 ELSE 0 END),
           SUM(CASE WHEN CleanCode_Aftale <> LTRIM(RTRIM(CleanCode_Aftale)) THEN 1 ELSE 0 END),
           0
    FROM [map].[Aftale]
) s
ORDER BY entity;

-- Prefix distribution (run per map; example for Kreditor — swap table/column):
SELECT LEFT(CleanCode_Kreditor, CHARINDEX('_', CleanCode_Kreditor + '_')) AS prefix,
       COUNT(*) AS n
FROM [map].[Kreditor]
GROUP BY LEFT(CleanCode_Kreditor, CHARINDEX('_', CleanCode_Kreditor + '_'))
ORDER BY n DESC;


-- ============================================================================
-- SECTION 4 — FACT FK RESOLUTION RATES (orphans = unresolved CleanCodes)
-- ----------------------------------------------------------------------------
-- For each fact FK, how many rows are stuck at -1 (UNKNOWN). A high share means
-- the source CleanCode did not resolve to its map (format/conformance gap).
-- ============================================================================
SELECT 'fct.Postering' AS fact, COUNT(*) AS total,
       SUM(CASE WHEN DW_EK_Kreditor        = -1 THEN 1 ELSE 0 END) AS miss_Kreditor,
       SUM(CASE WHEN DW_EK_Kontoplan       = -1 THEN 1 ELSE 0 END) AS miss_Kontoplan,
       SUM(CASE WHEN DW_EK_Faktura         = -1 THEN 1 ELSE 0 END) AS miss_Faktura,
       SUM(CASE WHEN DW_EK_Okosted         = -1 THEN 1 ELSE 0 END) AS miss_Okosted,
       SUM(CASE WHEN DW_EK_Finanskladdelinje = -1 THEN 1 ELSE 0 END) AS miss_Finanskladdelinje
FROM [fct].[Postering];

SELECT 'fct.Faktura' AS fact, COUNT(*) AS total,
       SUM(CASE WHEN DW_EK_Kreditor          = -1 THEN 1 ELSE 0 END) AS miss_Kreditor,
       SUM(CASE WHEN DW_EK_Kontoplan         = -1 THEN 1 ELSE 0 END) AS miss_Kontoplan,
       SUM(CASE WHEN DW_EK_EAN_Regningsmodtager = -1 THEN 1 ELSE 0 END) AS miss_EAN,
       SUM(CASE WHEN DW_EK_Postering         = -1 THEN 1 ELSE 0 END) AS miss_Postering   -- bridge apply-pass
FROM [fct].[Faktura];

SELECT 'fct.Fakturalinje' AS fact, COUNT(*) AS total,
       SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS miss_Kreditor,
       SUM(CASE WHEN DW_EK_Vare     = -1 THEN 1 ELSE 0 END) AS miss_Vare,
       SUM(CASE WHEN DW_EK_Faktura  = -1 THEN 1 ELSE 0 END) AS miss_Faktura,
       SUM(CASE WHEN DW_EK_Kontoplan = -1 THEN 1 ELSE 0 END) AS miss_Kontoplan
FROM [fct].[Fakturalinje];


-- ============================================================================
-- SECTION 5 — FAKTURA <-> POSTERING MATCH DECOMPOSITION
-- ----------------------------------------------------------------------------
-- Isolates whether the bridge match is limited by the invoice number or by the
-- creditor surrogate. After the DK fix + reload, match_norm_kreditor should be
-- close to match_norm_only (the residual gap = genuine issuer<>creditor cases).
-- ============================================================================
;WITH f AS (
    SELECT DISTINCT f.DW_EK_Kreditor,
           UPPER(LTRIM(RTRIM(REPLACE(f.FakturaNr,' ','')))) AS norm
    FROM [fct].[Faktura] f
    WHERE f.DW_EK_Faktura <> -1 AND f.DW_EK_Kreditor <> -1
      AND LTRIM(RTRIM(REPLACE(f.FakturaNr,' ',''))) <> ''
),
p AS (
    SELECT DISTINCT p.DW_EK_Kreditor,
           UPPER(LTRIM(RTRIM(REPLACE(p.FakturaNr,' ','')))) AS norm
    FROM [fct].[Postering] p
    WHERE p.DW_EK_Postering <> -1 AND p.DW_EK_Kreditor <> -1
      AND LTRIM(RTRIM(REPLACE(p.FakturaNr,' ',''))) <> ''
)
SELECT
    (SELECT COUNT(*) FROM f) AS faktura_keys,
    (SELECT COUNT(*) FROM p) AS postering_keys,
    (SELECT COUNT(*) FROM f JOIN p ON p.DW_EK_Kreditor=f.DW_EK_Kreditor AND p.norm=f.norm) AS match_kreditor_and_invoice,
    (SELECT COUNT(*) FROM f JOIN p ON p.norm=f.norm)                                        AS match_invoice_only;


-- ============================================================================
-- SECTION 6 — CREDITOR MISMATCH DRILL-DOWN (run if Section 5 still gaps)
-- ----------------------------------------------------------------------------
-- For invoice numbers that match but resolve to different creditors, show the
-- two CleanCode_Kreditor values. After the DK fix these should be genuine
-- issuer<>creditor cases (different CVR digits), not formatting.
-- ============================================================================
;WITH f AS (
    SELECT DISTINCT f.DW_EK_Kreditor, kf.CleanCode_Kreditor AS cc,
           UPPER(LTRIM(RTRIM(REPLACE(f.FakturaNr,' ','')))) AS norm
    FROM [fct].[Faktura] f JOIN [map].[Kreditor] kf ON kf.DW_EK_Kreditor=f.DW_EK_Kreditor
    WHERE f.DW_EK_Faktura<>-1 AND f.DW_EK_Kreditor<>-1 AND LTRIM(RTRIM(REPLACE(f.FakturaNr,' ','')))<>''
),
p AS (
    SELECT DISTINCT p.DW_EK_Kreditor, kp.CleanCode_Kreditor AS cc,
           UPPER(LTRIM(RTRIM(REPLACE(p.FakturaNr,' ','')))) AS norm
    FROM [fct].[Postering] p JOIN [map].[Kreditor] kp ON kp.DW_EK_Kreditor=p.DW_EK_Kreditor
    WHERE p.DW_EK_Postering<>-1 AND p.DW_EK_Kreditor<>-1 AND LTRIM(RTRIM(REPLACE(p.FakturaNr,' ','')))<>''
)
SELECT TOP 50 f.cc AS faktura_cc, p.cc AS postering_cc, COUNT(*) AS invoice_matches
FROM f JOIN p ON p.norm = f.norm AND p.DW_EK_Kreditor <> f.DW_EK_Kreditor
GROUP BY f.cc, p.cc
ORDER BY COUNT(*) DESC;
