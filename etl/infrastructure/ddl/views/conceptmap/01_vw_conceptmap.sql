-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Staging: conceptmap.vw_* views over conceptmap.* (filter invalid DW_EK)
-- Same columns as the DSO conceptmap.* tables for linked-server MERGE validation.
-- Each view guarantees ONE row per DW_EK join column:
--   1. Excludes 'unmatched'
--   2. ROW_NUMBER picks the best per DW_EK (equal > equivalent > narrower > inexact, newest wins ties)
-- Staging-only (the linked-server sync source).
-- ============================================================================

CREATE OR ALTER VIEW [conceptmap].[vw_rekvirent_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_Rekvirent], -1) AS [dw_ek_Rekvirent],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_Rekvirent]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[RekvirentToSOR]
    WHERE [dw_ek_Rekvirent] IS NOT NULL AND [dw_ek_Rekvirent] <> -1
      AND [dw_ek_Organisation_target] IS NOT NULL AND [dw_ek_Organisation_target] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_Rekvirent], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_lager_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_Lager], -1) AS [dw_ek_Lager],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_Lager]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[LagerToSOR]
    WHERE [dw_ek_Lager] IS NOT NULL AND [dw_ek_Lager] <> -1
      AND [dw_ek_Organisation_target] IS NOT NULL AND [dw_ek_Organisation_target] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_Lager], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_ean_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_EAN], -1) AS [dw_ek_EAN],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_EAN]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[EANToSOR]
    WHERE [dw_ek_EAN] IS NOT NULL AND [dw_ek_EAN] <> -1
      AND [dw_ek_Organisation_target] IS NOT NULL AND [dw_ek_Organisation_target] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_EAN], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_okosted_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_Okosted], -1) AS [dw_ek_Okosted],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_Okosted]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[OkostedToSOR]
    WHERE [dw_ek_Okosted] IS NOT NULL AND [dw_ek_Okosted] <> -1
      AND [dw_ek_Organisation_target] IS NOT NULL AND [dw_ek_Organisation_target] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_Okosted], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_shak_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_SHAK], -1) AS [dw_ek_SHAK],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_SHAK]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[SHAKToSOR]
    -- Keep every SHAK code: ~56% map to CLOSED SOR entities absent from the
    -- daily ref.Organisation, so dw_ek_Organisation_target stays unresolved
    -- (-1). We intentionally do NOT filter on it here (unlike the other
    -- *-to-SOR views) — the authoritative SOR code + name are still carried.
    WHERE [dw_ek_SHAK] IS NOT NULL AND [dw_ek_SHAK] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_SHAK], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_shakalike_to_sor] AS
WITH ranked AS (
    SELECT
        [source_code], [source_display], [relationship], [target_code], [target_display],
        ISNULL([dw_ek_SHAKAlike], -1) AS [dw_ek_SHAKAlike],
        ISNULL([dw_ek_Organisation_target], -1) AS [dw_ek_Organisation_target],
        [dw_timestamp_insert], [dw_timestamp_update],
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek_SHAKAlike]
            ORDER BY CASE [relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [dw_timestamp_update] DESC
        ) AS _rn
    FROM [conceptmap].[SHAKAlikeToSOR]
    -- Keep mappings whose SOR target is closed/absent from ref.Organisation
    -- (dw_ek_Organisation_target unresolved). Source key must resolve.
    WHERE [dw_ek_SHAKAlike] IS NOT NULL AND [dw_ek_SHAKAlike] <> -1
      AND [relationship] <> 'unmatched'
)
SELECT [source_code], [source_display], [relationship], [target_code], [target_display],
       [dw_ek_SHAKAlike], [dw_ek_Organisation_target], [dw_timestamp_insert], [dw_timestamp_update]
FROM ranked WHERE _rn = 1;
GO
