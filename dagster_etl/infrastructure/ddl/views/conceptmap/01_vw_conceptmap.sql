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
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        ISNULL([DW_EK_Rekvirent], -1) AS [DW_EK_Rekvirent],
        ISNULL([DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update],
        ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_Rekvirent]
            ORDER BY CASE [Relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
    FROM [conceptmap].[RekvirentToSOR]
    WHERE [DW_EK_Rekvirent] IS NOT NULL AND [DW_EK_Rekvirent] <> -1
      AND [DW_EK_Organisation_Target] IS NOT NULL AND [DW_EK_Organisation_Target] <> -1
      AND [Relationship] <> 'unmatched'
)
SELECT [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
       [DW_EK_Rekvirent], [DW_EK_Organisation_Target], [DW_Timestamp_Insert], [DW_Timestamp_Update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_lager_to_sor] AS
WITH ranked AS (
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        ISNULL([DW_EK_Lager], -1) AS [DW_EK_Lager],
        ISNULL([DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update],
        ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_Lager]
            ORDER BY CASE [Relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
    FROM [conceptmap].[LagerToSOR]
    WHERE [DW_EK_Lager] IS NOT NULL AND [DW_EK_Lager] <> -1
      AND [DW_EK_Organisation_Target] IS NOT NULL AND [DW_EK_Organisation_Target] <> -1
      AND [Relationship] <> 'unmatched'
)
SELECT [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
       [DW_EK_Lager], [DW_EK_Organisation_Target], [DW_Timestamp_Insert], [DW_Timestamp_Update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_ean_to_sor] AS
WITH ranked AS (
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        ISNULL([DW_EK_EAN], -1) AS [DW_EK_EAN],
        ISNULL([DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update],
        ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_EAN]
            ORDER BY CASE [Relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
    FROM [conceptmap].[EANToSOR]
    WHERE [DW_EK_EAN] IS NOT NULL AND [DW_EK_EAN] <> -1
      AND [DW_EK_Organisation_Target] IS NOT NULL AND [DW_EK_Organisation_Target] <> -1
      AND [Relationship] <> 'unmatched'
)
SELECT [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
       [DW_EK_EAN], [DW_EK_Organisation_Target], [DW_Timestamp_Insert], [DW_Timestamp_Update]
FROM ranked WHERE _rn = 1;
GO

CREATE OR ALTER VIEW [conceptmap].[vw_okosted_to_sor] AS
WITH ranked AS (
    SELECT
        [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
        ISNULL([DW_EK_Okosted], -1) AS [DW_EK_Okosted],
        ISNULL([DW_EK_Organisation_Target], -1) AS [DW_EK_Organisation_Target],
        [DW_Timestamp_Insert], [DW_Timestamp_Update],
        ROW_NUMBER() OVER (
            PARTITION BY [DW_EK_Okosted]
            ORDER BY CASE [Relationship]
                        WHEN 'equal' THEN 1 WHEN 'equivalent' THEN 2
                        WHEN 'narrower' THEN 3 WHEN 'inexact' THEN 4 ELSE 99 END,
                     [DW_Timestamp_Update] DESC
        ) AS _rn
    FROM [conceptmap].[OkostedToSOR]
    WHERE [DW_EK_Okosted] IS NOT NULL AND [DW_EK_Okosted] <> -1
      AND [DW_EK_Organisation_Target] IS NOT NULL AND [DW_EK_Organisation_Target] <> -1
      AND [Relationship] <> 'unmatched'
)
SELECT [SourceCode], [SourceDisplay], [Relationship], [TargetCode], [TargetDisplay],
       [DW_EK_Okosted], [DW_EK_Organisation_Target], [DW_Timestamp_Insert], [DW_Timestamp_Update]
FROM ranked WHERE _rn = 1;
GO
