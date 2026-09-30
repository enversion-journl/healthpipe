-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Function: dbo.fn_datetime_to_datekey
-- Beregner en dim.Dato entity-key (DW_EK_Dato, YYYYMMDD som INT) fra en
-- datetime-agtig vaerdi. Genskabt til brug DIREKTE af slutbrugere paa
-- SQL-serveren — bruges IKKE af ETL-pipelinen (der emitter DSA/SQL koden
-- udtrykket inline som en table-expression, jf. utils/sql dato_key()).
--
-- Logikken spejler repoets dato_key(): keys clampes til dim.Dato's seed-range,
-- saa datoer uden for range mapper til en sentinel-raekke i stedet for en
-- forael-loes key. Bounds SKAL matche dim.Dato's seed-range i
-- infrastructure/ddl/tables/shared_dimensions/04_seeds.sql, og sentinel-
-- raekkerne SKAL findes i dim.Dato (02_dims.sql) saa hver key resolver.
--
--   -1        UNKNOWN  — kilden er NULL eller kan ikke parses
--   -2        BEFORE   — gyldig dato tidligere end dim.Dato's range
--   99999998  BLANK    — kun via dato_key() for tomme strenge (denne UDF
--                        tager datetime2, saa blank er ikke relevant her)
--   99999999  AFTER    — gyldig dato senere end dim.Dato's range
--   YYYYMMDD           — in-range
--
-- Performance: funktionen blev tidligere fjernet fordi en scalar-UDF
-- evalueres raekke-vis. WITH SCHEMABINDING goer den inline-egnet (Froid) paa
-- SQL Server 2019+/compat level 150+, saa optimizeren folder udtrykket ind i
-- den kaldende query (set-based). Paa aeldre versioner/compat-niveauer falder
-- den tilbage til raekke-vis eval — undgaa da store scans der kalder den pr.
-- raekke, eller brug udtrykket inline.
--
-- Eksempel: '2024-08-28 13:22' -> 20240828
-- ============================================================================

CREATE OR ALTER FUNCTION dbo.fn_datetime_to_datekey
(
    @dt datetime2(7)
)
RETURNS INT
WITH SCHEMABINDING
AS
BEGIN
    RETURN (
        CASE
            WHEN TRY_CONVERT(datetime2, @dt) IS NULL THEN -1
            WHEN CAST(CONVERT(char(8), TRY_CONVERT(datetime2, @dt), 112) AS int) < 19700101 THEN -2
            WHEN CAST(CONVERT(char(8), TRY_CONVERT(datetime2, @dt), 112) AS int) > 20501231 THEN 99999999
            ELSE CAST(CONVERT(char(8), TRY_CONVERT(datetime2, @dt), 112) AS int)
        END
    );
END;
GO
