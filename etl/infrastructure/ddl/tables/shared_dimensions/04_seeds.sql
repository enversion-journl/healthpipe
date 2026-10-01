-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Shared Dimensions: Seed Data
-- Populates dim.Dato with calendar data from 1970-01-01 to 2050-12-31
-- Includes Danish holiday flags (moveable Easter-based + fixed holidays)
--
-- Adapted from historical PopulateDato.sql
-- Populates static date attributes on calendar rows (dw_ek_Dato > 0).
-- Relative/afsluttet attributes are maintained by 05_dato_relativ.sql.
-- ============================================================================

SET NOCOUNT ON;
SET DATEFIRST 1;    -- Monday = 1 (European standard)
SET LANGUAGE Danish; -- Danish month/day names from DATENAME()

-- Only populate if table is empty (idempotent — skip if already seeded)
IF NOT EXISTS (SELECT 1 FROM dim.Dato WHERE dw_ek_Dato > 0)
BEGIN

-- ========================================================================
-- Step 1: Insert one row per date with basic keys (set-based via CTE)
-- ========================================================================
;WITH DateCTE AS (
    SELECT CAST('1970-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(day, 1, d) FROM DateCTE WHERE d < '2050-12-31'
)
INSERT INTO dim.[Dato] (
    [dw_ek_Dato],
    [dato]
)
SELECT
    YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d),
    d
FROM DateCTE
OPTION (MAXRECURSION 0);

-- ========================================================================
-- Step 2: Year attributes
-- ========================================================================
UPDATE d
SET [aar] = CAST(YEAR([dato]) AS SMALLINT),
    [aar_startdato] = DATEFROMPARTS(YEAR([dato]), 1, 1),
    [aar_slutdato] = DATEFROMPARTS(YEAR([dato]), 12, 31)
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

UPDATE d
SET [aar_antal_dage]  = (
    SELECT COUNT(*) FROM dim.[Dato] d2 WHERE d.[aar] = d2.[aar] AND d2.[dw_ek_Dato] > 0
)
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

-- ========================================================================
-- Step 3: Quarter attributes
-- ========================================================================
UPDATE dim.[Dato]
SET [kvartal_id]          = CAST(DATEPART(QUARTER, [dato]) AS TINYINT),
    [kvartal]             = N'K' + CAST(DATEPART(QUARTER, [dato]) AS NVARCHAR(1)),
    [kvartals_startdato]  = CAST(DATEADD(QUARTER, DATEDIFF(QUARTER, 0, [dato]), 0) AS DATE),
    [kvartals_slutdato]   = CAST(
        DATEADD(DAY, -1, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, [dato]) + 1, 0)) AS DATE
    ),
    [kvartal_aar_id]      = YEAR([dato]) * 100 + DATEPART(QUARTER, [dato]),
    [kvartal_aar]         = N'K' + CAST(DATEPART(QUARTER, [dato]) AS NVARCHAR(1)) + N' '
        + CAST(YEAR([dato]) AS NVARCHAR(4))
WHERE [dw_ek_Dato] > 0;

UPDATE d
SET [kvartal_antal_dage]  = CAST(
    (SELECT COUNT(*) FROM dim.[Dato] d2 WHERE d.[kvartal_aar_id] = d2.[kvartal_aar_id] AND d2.[dw_ek_Dato] > 0)
    AS TINYINT
)
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

-- ========================================================================
-- Step 4: Month attributes
-- ========================================================================
UPDATE dim.[Dato]
SET [maaned_id]          = CAST(DATEPART(MONTH, [dato]) AS TINYINT),
    [maaned_lang]        = DATENAME(MONTH, [dato]),
    [maaned]             = LEFT(DATENAME(MONTH, [dato]), 3),
    [maaned_startdato]   = DATEFROMPARTS(YEAR([dato]), MONTH([dato]), 1),
    [maaned_slutdato]    = EOMONTH([dato]),
    [maaned_aar_id]      = YEAR([dato]) * 100 + MONTH([dato]),
    [maaned_aar]         = LEFT(DATENAME(MONTH, [dato]), 3) + N' ' + CAST(YEAR([dato]) AS NVARCHAR(4))
WHERE [dw_ek_Dato] > 0;

UPDATE d
SET [maaned_antal_dage]  = CAST(
    (SELECT COUNT(*) FROM dim.[Dato] d2 WHERE d.[maaned_aar_id] = d2.[maaned_aar_id] AND d2.[dw_ek_Dato] > 0)
    AS TINYINT
)
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

-- ========================================================================
-- Step 5: Day and weekday attributes
-- ========================================================================
UPDATE dim.[Dato]
SET [dag_aar]        = CAST(DATEPART(dy, [dato]) AS SMALLINT),
    [dag_maaned]     = CAST(DAY([dato]) AS TINYINT),
    [dag_uge]        = CAST(DATEPART(dw, [dato]) AS TINYINT),
    [dag_kvartal]    = CAST(
        DATEDIFF(DAY, DATEADD(QUARTER, DATEDIFF(QUARTER, 0, [dato]), 0), [dato]) + 1 AS TINYINT
    ),
    [ugedag_lang]    = DATENAME(dw, [dato]),
    [ugedag]         = LEFT(DATENAME(dw, [dato]), 3)
WHERE [dw_ek_Dato] > 0;

-- ========================================================================
-- Step 6: Week attributes (ISO 8601 — handles year-boundary edge cases)
-- ========================================================================
UPDATE d
SET d.[uge_aar_id] = CASE
                    WHEN [dag_aar] > 350 AND DATEPART(ISOWK, [dato]) = 1 THEN [aar] + 1
                    WHEN [dag_aar] < 10  AND DATEPART(ISOWK, [dato]) >= 52 THEN [aar] - 1
                    ELSE [aar]
                END * 100 + DATEPART(ISOWK, [dato])
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

UPDATE d
SET [uge_aar] = N'U' + RIGHT(N'00' + CAST(DATEPART(ISOWK, [dato]) AS NVARCHAR(2)), 2)
    + N' ' + CAST([uge_aar_id] / 100 AS NVARCHAR(4)),
    [uge_id]      = CAST(DATEPART(ISOWK, [dato]) AS TINYINT),
    [uge]         = N'U' + CAST(DATEPART(ISOWK, [dato]) AS NVARCHAR),
    [uge_startdato] = DATEADD(DAY, 1 - DATEPART(WEEKDAY, [dato]), [dato]),
    [uge_slutdato] = DATEADD(DAY, 7 - DATEPART(WEEKDAY, [dato]), [dato])
FROM dim.[Dato] d
WHERE d.[dw_ek_Dato] > 0;

-- ========================================================================
-- Step 7: Weekday/weekend flags + holiday defaults
-- ========================================================================
UPDATE dim.[Dato]
SET [er_hverdag]        = CASE WHEN [dag_uge] >= 1 AND [dag_uge] <= 5 THEN 1 ELSE 0 END,
    [hverdag_weekend]  = CASE WHEN [dag_uge] BETWEEN 1 AND 5 THEN N'Hverdag' ELSE N'Weekend' END,
    [er_helligdag]     = 0,
    [helligdag]        = N'Ikke en helligdag'
WHERE [dw_ek_Dato] > 0;

-- ========================================================================
-- Step 8: Danish moveable holidays (Easter-based, 1950-2099)
-- ========================================================================
IF OBJECT_ID('tempdb..#Helligdage') IS NOT NULL DROP TABLE #Helligdage;

SELECT Aar, [Palmesøndag], [Skærtorsdag], Langfredag, [Påskedag], Paaskedag2, [Store Bededag], [Kristi Himmelfartsdag], Pinsedag, Pinsedag2
INTO #Helligdage
FROM ( VALUES
 (1950,'2. april','6. april','7. april','9. april','10. april','5. maj','18. maj','28. maj','29. maj' )
,(1951,'18. marts','22. marts','23. marts','25. marts','26. marts','20. april','3. maj','13. maj','14. maj' )
,(1952,'6. april','10. april','11. april','13. april','14. april','9. maj','22. maj','1. juni','2. juni' )
,(1953,'29. marts','2. april','3. april','5. april','6. april','1. maj','14. maj','24. maj','25. maj' )
,(1954,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(1955,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(1956,'25. marts','29. marts','30. marts','1. april','2. april','27. april','10. maj','20. maj','21. maj' )
,(1957,'14. april','18. april','19. april','21. april','22. april','17. maj','30. maj','9. juni','10. juni' )
,(1958,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(1959,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(1960,'10. april','14. april','15. april','17. april','18. april','13. maj','26. maj','5. juni','6. juni' )
,(1961,'26. marts','30. marts','31. marts','2. april','3. april','28. april','11. maj','21. maj','22. maj' )
,(1962,'15. april','19. april','20. april','22. april','23. april','18. maj','31. maj','10. juni','11. juni' )
,(1963,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(1964,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(1965,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(1966,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(1967,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(1968,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(1969,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(1970,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(1971,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(1972,'26. marts','30. marts','31. marts','2. april','3. april','28. april','11. maj','21. maj','22. maj' )
,(1973,'15. april','19. april','20. april','22. april','23. april','18. maj','31. maj','10. juni','11. juni' )
,(1974,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(1975,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(1976,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(1977,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(1978,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(1979,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(1980,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(1981,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(1982,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(1983,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(1984,'15. april','19. april','20. april','22. april','23. april','18. maj','31. maj','10. juni','11. juni' )
,(1985,'31. marts','4. april','5. april','7. april','8. april','3. maj','16. maj','26. maj','27. maj' )
,(1986,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(1987,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(1988,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(1989,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(1990,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(1991,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(1992,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(1993,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(1994,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(1995,'9. april','13. april','14. april','16. april','17. april','12. maj','25. maj','4. juni','5. juni' )
,(1996,'31. marts','4. april','5. april','7. april','8. april','3. maj','16. maj','26. maj','27. maj' )
,(1997,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(1998,'5. april','9. april','10. april','12. april','13. april','8. maj','21. maj','31. maj','1. juni' )
,(1999,'28. marts','1. april','2. april','4. april','5. april','30. april','13. maj','23. maj','24. maj' )
,(2000,'16. april','20. april','21. april','23. april','24. april','19. maj','1. juni','11. juni','12. juni' )
,(2001,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(2002,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(2003,'13. april','17. april','18. april','20. april','21. april','16. maj','29. maj','8. juni','9. juni' )
,(2004,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(2005,'20. marts','24. marts','25. marts','27. marts','28. marts','22. april','5. maj','15. maj','16. maj' )
,(2006,'9. april','13. april','14. april','16. april','17. april','12. maj','25. maj','4. juni','5. juni' )
,(2007,'1. april','5. april','6. april','8. april','9. april','4. maj','17. maj','27. maj','28. maj' )
,(2008,'16. marts','20. marts','21. marts','23. marts','24. marts','18. april','1. maj','11. maj','12. maj' )
,(2009,'5. april','9. april','10. april','12. april','13. april','8. maj','21. maj','31. maj','1. juni' )
,(2010,'28. marts','1. april','2. april','4. april','5. april','30. april','13. maj','23. maj','24. maj' )
,(2011,'17. april','21. april','22. april','24. april','25. april','20. maj','2. juni','12. juni','13. juni' )
,(2012,'1. april','5. april','6. april','8. april','9. april','4. maj','17. maj','27. maj','28. maj' )
,(2013,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(2014,'13. april','17. april','18. april','20. april','21. april','16. maj','29. maj','8. juni','9. juni' )
,(2015,'29. marts','2. april','3. april','5. april','6. april','1. maj','14. maj','24. maj','25. maj' )
,(2016,'20. marts','24. marts','25. marts','27. marts','28. marts','22. april','5. maj','15. maj','16. maj' )
,(2017,'9. april','13. april','14. april','16. april','17. april','12. maj','25. maj','4. juni','5. juni' )
,(2018,'25. marts','29. marts','30. marts','1. april','2. april','27. april','10. maj','20. maj','21. maj' )
,(2019,'14. april','18. april','19. april','21. april','22. april','17. maj','30. maj','9. juni','10. juni' )
,(2020,'5. april','9. april','10. april','12. april','13. april','8. maj','21. maj','31. maj','1. juni' )
,(2021,'28. marts','1. april','2. april','4. april','5. april','30. april','13. maj','23. maj','24. maj' )
,(2022,'10. april','14. april','15. april','17. april','18. april','13. maj','26. maj','5. juni','6. juni' )
,(2023,'2. april','6. april','7. april','9. april','10. april','5. maj','18. maj','28. maj','29. maj' )
,(2024,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(2025,'13. april','17. april','18. april','20. april','21. april','16. maj','29. maj','8. juni','9. juni' )
,(2026,'29. marts','2. april','3. april','5. april','6. april','1. maj','14. maj','24. maj','25. maj' )
,(2027,'21. marts','25. marts','26. marts','28. marts','29. marts','23. april','6. maj','16. maj','17. maj' )
,(2028,'9. april','13. april','14. april','16. april','17. april','12. maj','25. maj','4. juni','5. juni' )
,(2029,'25. marts','29. marts','30. marts','1. april','2. april','27. april','10. maj','20. maj','21. maj' )
,(2030,'14. april','18. april','19. april','21. april','22. april','17. maj','30. maj','9. juni','10. juni' )
,(2031,'6. april','10. april','11. april','13. april','14. april','9. maj','22. maj','1. juni','2. juni' )
,(2032,'21. marts','25. marts','26. marts','28. marts','29. marts','23. april','6. maj','16. maj','17. maj' )
,(2033,'10. april','14. april','15. april','17. april','18. april','13. maj','26. maj','5. juni','6. juni' )
,(2034,'2. april','6. april','7. april','9. april','10. april','5. maj','18. maj','28. maj','29. maj' )
,(2035,'18. marts','22. marts','23. marts','25. marts','26. marts','20. april','3. maj','13. maj','14. maj' )
,(2036,'6. april','10. april','11. april','13. april','14. april','9. maj','22. maj','1. juni','2. juni' )
,(2037,'29. marts','2. april','3. april','5. april','6. april','1. maj','14. maj','24. maj','25. maj' )
,(2038,'18. april','22. april','23. april','25. april','26. april','21. maj','3. juni','13. juni','14. juni' )
,(2039,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(2040,'25. marts','29. marts','30. marts','1. april','2. april','27. april','10. maj','20. maj','21. maj' )
,(2041,'14. april','18. april','19. april','21. april','22. april','17. maj','30. maj','9. juni','10. juni' )
,(2042,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(2043,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(2044,'10. april','14. april','15. april','17. april','18. april','13. maj','26. maj','5. juni','6. juni' )
,(2045,'2. april','6. april','7. april','9. april','10. april','5. maj','18. maj','28. maj','29. maj' )
,(2046,'18. marts','22. marts','23. marts','25. marts','26. marts','20. april','3. maj','13. maj','14. maj' )
,(2047,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(2048,'29. marts','2. april','3. april','5. april','6. april','1. maj','14. maj','24. maj','25. maj' )
,(2049,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(2050,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(2051,'26. marts','30. marts','31. marts','2. april','3. april','28. april','11. maj','21. maj','22. maj' )
,(2052,'14. april','18. april','19. april','21. april','22. april','17. maj','30. maj','9. juni','10. juni' )
,(2053,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(2054,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(2055,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(2056,'26. marts','30. marts','31. marts','2. april','3. april','28. april','11. maj','21. maj','22. maj' )
,(2057,'15. april','19. april','20. april','22. april','23. april','18. maj','31. maj','10. juni','11. juni' )
,(2058,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(2059,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(2060,'11. april','15. april','16. april','18. april','19. april','14. maj','27. maj','6. juni','7. juni' )
,(2061,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(2062,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(2063,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(2064,'30. marts','3. april','4. april','6. april','7. april','2. maj','15. maj','25. maj','26. maj' )
,(2065,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
,(2066,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(2067,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(2068,'15. april','19. april','20. april','22. april','23. april','18. maj','31. maj','10. juni','11. juni' )
,(2069,'7. april','11. april','12. april','14. april','15. april','10. maj','23. maj','2. juni','3. juni' )
,(2070,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(2071,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(2072,'3. april','7. april','8. april','10. april','11. april','6. maj','19. maj','29. maj','30. maj' )
,(2073,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(2074,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(2075,'31. marts','4. april','5. april','7. april','8. april','3. maj','16. maj','26. maj','27. maj' )
,(2076,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(2077,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(2078,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(2079,'16. april','20. april','21. april','23. april','24. april','19. maj','1. juni','11. juni','12. juni' )
,(2080,'31. marts','4. april','5. april','7. april','8. april','3. maj','16. maj','26. maj','27. maj' )
,(2081,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(2082,'12. april','16. april','17. april','19. april','20. april','15. maj','28. maj','7. juni','8. juni' )
,(2083,'28. marts','1. april','2. april','4. april','5. april','30. april','13. maj','23. maj','24. maj' )
,(2084,'19. marts','23. marts','24. marts','26. marts','27. marts','21. april','4. maj','14. maj','15. maj' )
,(2085,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(2086,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(2087,'13. april','17. april','18. april','20. april','21. april','16. maj','29. maj','8. juni','9. juni' )
,(2088,'4. april','8. april','9. april','11. april','12. april','7. maj','20. maj','30. maj','31. maj' )
,(2089,'27. marts','31. marts','1. april','3. april','4. april','29. april','12. maj','22. maj','23. maj' )
,(2090,'9. april','13. april','14. april','16. april','17. april','12. maj','25. maj','4. juni','5. juni' )
,(2091,'1. april','5. april','6. april','8. april','9. april','4. maj','17. maj','27. maj','28. maj' )
,(2092,'23. marts','27. marts','28. marts','30. marts','31. marts','25. april','8. maj','18. maj','19. maj' )
,(2093,'5. april','9. april','10. april','12. april','13. april','8. maj','21. maj','31. maj','1. juni' )
,(2094,'28. marts','1. april','2. april','4. april','5. april','30. april','13. maj','23. maj','24. maj' )
,(2095,'17. april','21. april','22. april','24. april','25. april','20. maj','2. juni','12. juni','13. juni' )
,(2096,'8. april','12. april','13. april','15. april','16. april','11. maj','24. maj','3. juni','4. juni' )
,(2097,'24. marts','28. marts','29. marts','31. marts','1. april','26. april','9. maj','19. maj','20. maj' )
,(2098,'13. april','17. april','18. april','20. april','21. april','16. maj','29. maj','8. juni','9. juni' )
,(2099,'5. april','9. april','10. april','12. april','13. april','8. maj','21. maj','31. maj','1. juni' )
) T (Aar, [Palmesøndag], [Skærtorsdag], Langfredag, [Påskedag], Paaskedag2, [Store Bededag], [Kristi Himmelfartsdag], Pinsedag, Pinsedag2);

-- Update moveable holidays via UNPIVOT join
-- Note: Palmesoendag is NOT a public holiday at Region Syddanmark
UPDATE dim.[Dato]
SET [er_helligdag] = 1,
    [helligdag] = h.Hellig
FROM dim.[Dato] d
INNER JOIN (
    SELECT
      DW_EK_Date = CAST(CONVERT(VARCHAR(8), TRY_PARSE(Dag+' '+CAST(Aar AS VARCHAR(4)) AS DATETIME USING 'da-DK'), 112) AS INT),
      Hellig
    FROM #Helligdage
    UNPIVOT (
      Dag
      FOR Hellig IN ([Skærtorsdag],[Langfredag],[Påskedag],[Paaskedag2],[Store Bededag],[Kristi Himmelfartsdag],[Pinsedag],[Pinsedag2])
    ) pvt
) h ON d.[dw_ek_Dato] = h.DW_EK_Date;

-- Rename internal aliases to display names
UPDATE dim.[Dato] SET [helligdag] = N'2. Påskedag' WHERE [helligdag] = 'Paaskedag2';
UPDATE dim.[Dato] SET [helligdag] = N'2. Pinsedag' WHERE [helligdag] = 'Pinsedag2';

-- ========================================================================
-- Step 9: Fixed holidays (same date every year)
-- ========================================================================
UPDATE dim.[Dato]
SET [er_helligdag] = 1, [helligdag] = N'Nytårsdag'
WHERE [maaned_id] = 1 AND [dag_maaned] = 1;

UPDATE dim.[Dato]
SET [er_helligdag] = 1, [helligdag] = N'Juledag'
WHERE [maaned_id] = 12 AND [dag_maaned] = 25;

UPDATE dim.[Dato]
SET [er_helligdag] = 1, [helligdag] = N'2. Juledag'
WHERE [maaned_id] = 12 AND [dag_maaned] = 26;

-- ========================================================================
-- Step 10: Beredskabshelligdage
-- ========================================================================
UPDATE dim.[Dato]
SET [er_beredskabshelligdag] = CASE
        WHEN [er_helligdag] = 1 OR ([maaned_id] = 12 AND [dag_maaned] IN (24, 31))
            THEN 1
        ELSE 0
    END,
    [beredskabshelligdag] = CASE
        WHEN [er_helligdag] = 1
            THEN [helligdag]
        WHEN [maaned_id] = 12 AND [dag_maaned] = 24
            THEN N'Juleaften'
        WHEN [maaned_id] = 12 AND [dag_maaned] = 31
            THEN N'Nytårsaften'
        ELSE N'Ikke en beredskabshelligdag'
    END
WHERE [dw_ek_Dato] > 0;

-- ========================================================================
-- Rebuild clustered index after bulk insert
-- ========================================================================
ALTER INDEX [PK_Dim_Dato] ON dim.[Dato] REBUILD;

END -- IF NOT EXISTS guard
GO
