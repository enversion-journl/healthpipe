-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Dato: indholdsrettelser efter indholdstjek fra DOL.
-- Retter tre grupper af indholdsfejl paa allerede udrullede
-- databaser:
--
--   1) maaned/maaned_lang/maaned_aar/ugedag/ugedag_lang faar stort
--      begyndelsesbogstav (dansk DATENAME() er lowercase).
--   2) er_helligdag/helligdag genberegnes fra bunden: officielle danske
--      helligdage med navne efter DOL-listen, Store Bededag kun til og med
--      2023 (afskaffet), og teksten "Ikke en helligdag" paa oevrige dage.
--   3) er_beredskabshelligdag/beredskabshelligdag = officielle helligdage +
--      Juleaftensdag (24/12) + Nytaarsaftensdag (31/12); fallback-teksten er
--      ligeledes "Ikke en helligdag".
--
-- Relativ-kolonnernes fortegn (raekkens dato - dags dato) rettes ikke her --
-- 05_dato_relativ.sql genberegner dem ved hver infrastrukturkoersel.
--
-- tables/-DDL'en (04_seeds.sql) er rettet tilsvarende, saa friske databaser
-- seedes korrekt og faar denne migration baselinet.
--
-- Sentinelraekkerne (-1/-2/99999998/99999999) beroeres ikke -- alt er
-- afgraenset til kalenderraekkerne. Safe to re-run: rene genberegninger.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1) Stort begyndelsesbogstav i navnekolonner
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 1;
    SET LANGUAGE Danish;

    UPDATE dim.[Dato]
    SET [maaned_lang] = UPPER(LEFT(DATENAME(MONTH, [dato]), 1)) + SUBSTRING(DATENAME(MONTH, [dato]), 2, 48),
        [maaned]      = UPPER(LEFT(DATENAME(MONTH, [dato]), 1)) + SUBSTRING(DATENAME(MONTH, [dato]), 2, 2),
        [maaned_aar]  = UPPER(LEFT(DATENAME(MONTH, [dato]), 1)) + SUBSTRING(DATENAME(MONTH, [dato]), 2, 2)
            + N' ' + CAST(YEAR([dato]) AS NVARCHAR(4)),
        [ugedag_lang] = UPPER(LEFT(DATENAME(dw, [dato]), 1)) + SUBSTRING(DATENAME(dw, [dato]), 2, 48),
        [ugedag]      = UPPER(LEFT(DATENAME(dw, [dato]), 1)) + SUBSTRING(DATENAME(dw, [dato]), 2, 2)
    WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
END
GO

-- --------------------------------------------------------------------------
-- 2) Helligdage: nulstil og genberegn fra bunden
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    UPDATE dim.[Dato]
    SET [er_helligdag] = 0,
        [helligdag]    = N'Ikke en helligdag'
    WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
END
GO

-- Bevaegelige helligdage (paaskebaserede), samme kildetabel som 04_seeds.sql
-- men afgraenset til dim.Dato's kalenderaar 1970-2050.
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
IF OBJECT_ID('tempdb..#Helligdage') IS NOT NULL DROP TABLE #Helligdage;

-- UNPIVOT kræver én type; VALUES giver hver kolonne dens længste dato (varchar(7..9)).
SELECT Aar
     , CAST([Skærtorsdag] AS VARCHAR(20)) AS [Skærtorsdag]
     , CAST([Langfredag] AS VARCHAR(20)) AS [Langfredag]
     , CAST([Påskedag] AS VARCHAR(20)) AS [Påskedag]
     , CAST([Paaskedag2] AS VARCHAR(20)) AS [Paaskedag2]
     , CAST([Store Bededag] AS VARCHAR(20)) AS [Store Bededag]
     , CAST([Kristi Himmelfartsdag] AS VARCHAR(20)) AS [Kristi Himmelfartsdag]
     , CAST([Pinsedag] AS VARCHAR(20)) AS [Pinsedag]
     , CAST([Pinsedag2] AS VARCHAR(20)) AS [Pinsedag2]
INTO #Helligdage
FROM ( VALUES
 (1970,'22. marts','26. marts','27. marts','29. marts','30. marts','24. april','7. maj','17. maj','18. maj' )
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
) T (Aar, [Palmesøndag], [Skærtorsdag], Langfredag, [Påskedag], Paaskedag2, [Store Bededag], [Kristi Himmelfartsdag], Pinsedag, Pinsedag2);

-- Note: Palmesoendag er IKKE en officiel helligdag
UPDATE d
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
    -- Store Bededag blev afskaffet som helligdag fra og med 2024
    WHERE NOT (Hellig = 'Store Bededag' AND Aar > 2023)
) h ON d.[dw_ek_Dato] = h.DW_EK_Date
WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231;

-- Interne aliasser til visningsnavne
UPDATE dim.[Dato] SET [helligdag] = N'2. påskedag'
WHERE [helligdag] = 'Paaskedag2' AND [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
UPDATE dim.[Dato] SET [helligdag] = N'2. pinsedag'
WHERE [helligdag] = 'Pinsedag2' AND [dw_ek_Dato] BETWEEN 19700101 AND 20501231;

-- Faste helligdage
UPDATE dim.[Dato] SET [er_helligdag] = 1, [helligdag] = N'Nytårsdag'
WHERE MONTH([dato]) = 1 AND DAY([dato]) = 1 AND [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
UPDATE dim.[Dato] SET [er_helligdag] = 1, [helligdag] = N'1. juledag'
WHERE MONTH([dato]) = 12 AND DAY([dato]) = 25 AND [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
UPDATE dim.[Dato] SET [er_helligdag] = 1, [helligdag] = N'2. juledag'
WHERE MONTH([dato]) = 12 AND DAY([dato]) = 26 AND [dw_ek_Dato] BETWEEN 19700101 AND 20501231;

DROP TABLE #Helligdage;
END
GO

-- --------------------------------------------------------------------------
-- 3) Beredskabshelligdage: officielle helligdage + 24/12 + 31/12
-- --------------------------------------------------------------------------
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    UPDATE dim.[Dato]
    SET [er_beredskabshelligdag] = CASE
            WHEN [er_helligdag] = 1 OR (MONTH([dato]) = 12 AND DAY([dato]) IN (24, 31))
                THEN 1
            ELSE 0
        END,
        [beredskabshelligdag] = CASE
            WHEN [er_helligdag] = 1
                THEN [helligdag]
            WHEN MONTH([dato]) = 12 AND DAY([dato]) = 24
                THEN N'Juleaftensdag'
            WHEN MONTH([dato]) = 12 AND DAY([dato]) = 31
                THEN N'Nytårsaftensdag'
            ELSE N'Ikke en helligdag'
        END
    WHERE [dw_ek_Dato] BETWEEN 19700101 AND 20501231;
END
GO
