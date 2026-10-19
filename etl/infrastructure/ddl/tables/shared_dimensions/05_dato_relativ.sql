-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Dato: relative offsets and "afsluttet" flags
--
-- Runs on every infrastructure execution (staging + DSO) and is also reused by
-- the daily dim_Dato asset to keep time-relative columns current.
-- ============================================================================

IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NULL
    RETURN;
GO

SET DATEFIRST 1;

DECLARE @today DATE = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Central European Standard Time' AS DATE);
DECLARE @today_year INT = YEAR(@today);
DECLARE @today_quarter INT = DATEPART(QUARTER, @today);
DECLARE @today_month INT = MONTH(@today);
DECLARE @today_month_start DATE = DATEFROMPARTS(@today_year, @today_month, 1);
DECLARE @today_week INT = DATEPART(ISOWK, @today);
DECLARE @today_week_year INT = CASE
    WHEN DATEPART(DAYOFYEAR, @today) > 350 AND @today_week = 1 THEN @today_year + 1
    WHEN DATEPART(DAYOFYEAR, @today) < 10 AND @today_week >= 52 THEN @today_year - 1
    ELSE @today_year
END;
DECLARE @today_week_start DATE = DATEADD(DAY, 1 - DATEPART(WEEKDAY, @today), @today);

UPDATE d
SET
    [aar_afsluttet] = CASE WHEN d.[aar] < @today_year THEN 1 ELSE 0 END,
    [kvartal_afsluttet] = CASE
        WHEN d.[kvartal_aar_id] < (@today_year * 100 + @today_quarter) THEN 1
        ELSE 0
    END,
    [maaned_afsluttet] = CASE
        WHEN d.[maaned_aar_id] < (@today_year * 100 + @today_month) THEN 1
        ELSE 0
    END,
    [uge_afsluttet] = CASE
        WHEN d.[uge_aar_id] < (@today_week_year * 100 + @today_week) THEN 1
        ELSE 0
    END,
    -- Rækkens dato minus dags dato: passerede perioder negative, fremtidige
    -- positive (fx aar_relativ = -1 i fjor, 0 i år, +1 næste år).
    [aar_relativ] = CAST(d.[aar] - @today_year AS SMALLINT),
    [kvartal_relativ] = CAST(
        (d.[aar] * 4 + d.[kvartal_id]) - (@today_year * 4 + @today_quarter) AS SMALLINT
    ),
    [maaned_relativ] = CAST(DATEDIFF(MONTH, @today_month_start, d.[maaned_startdato]) AS SMALLINT),
    [uge_relativ] = CAST(DATEDIFF(WEEK, @today_week_start, d.[uge_startdato]) AS SMALLINT),
    [dag_relativ] = CAST(DATEDIFF(DAY, @today, d.[dato]) AS SMALLINT)
FROM [dim].[Dato] d
WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;
GO
