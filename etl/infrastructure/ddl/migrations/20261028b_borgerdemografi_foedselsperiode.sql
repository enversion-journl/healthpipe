-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Add birth cohorts without changing existing dimension keys or external references.
-- Each database publishes the new Borger keys after their members exist locally.
IF OBJECT_ID(N'dim.BorgerDemografi', N'U') IS NOT NULL
   AND COL_LENGTH(N'dim.BorgerDemografi', N'foedselsaar') IS NULL
    ALTER TABLE dim.BorgerDemografi ADD foedselsaar SMALLINT NULL;
GO
IF OBJECT_ID(N'dim.BorgerDemografi', N'U') IS NOT NULL
   AND COL_LENGTH(N'dim.BorgerDemografi', N'foedselsmaaned') IS NULL
    ALTER TABLE dim.BorgerDemografi ADD foedselsmaaned TINYINT NULL;
GO
IF OBJECT_ID(N'dim.Borger', N'U') IS NOT NULL
   AND OBJECT_ID(N'dim.BorgerDemografi', N'U') IS NOT NULL
   AND (DB_NAME() LIKE N'%staging%' OR DB_NAME() LIKE N'%dso%')
    EXEC(N'SET XACT_ABORT ON;
SET LOCK_TIMEOUT 30000;
BEGIN TRANSACTION;
DECLARE @lock_result INT, @borger_rows BIGINT, @dimension_rows BIGINT;
EXEC @lock_result = sys.sp_getapplock @Resource=N''dim.BorgerDemografi'',
    @LockMode=''Exclusive'', @LockOwner=''Transaction'', @LockTimeout=30000;
IF @lock_result < 0 THROW 51000, ''Demography writer lock unavailable'', 1;
IF DB_NAME() LIKE N''%staging%'' AND EXISTS (
    SELECT 1 FROM sys.foreign_key_columns WHERE parent_object_id=OBJECT_ID(N''dim.Borger'')
      AND parent_column_id=COLUMNPROPERTY(OBJECT_ID(N''dim.Borger''), N''dw_ek_BorgerDemografi'', ''ColumnId'')
) THROW 51000, ''Borger hash constraint needs an explicit migration'', 1;
SELECT @borger_rows=COUNT_BIG(*) FROM dim.Borger WITH (TABLOCKX, HOLDLOCK);
SELECT @dimension_rows=COUNT_BIG(*) FROM dim.BorgerDemografi WITH (TABLOCKX, HOLDLOCK);

-- Existing keys stay valid with unknown birth cohort; only new members are inserted.
WITH Combinations AS (
    SELECT DISTINCT NULLIF(UPPER(LTRIM(RTRIM(b.[region_id]))), N'''') AS [region_id],
            NULLIF(UPPER(LTRIM(RTRIM(b.[regionnavn]))), N'''') AS [regionnavn],
            NULLIF(UPPER(LTRIM(RTRIM(b.[kommune_id]))), N'''') AS [kommune_id],
            NULLIF(UPPER(LTRIM(RTRIM(b.[kommunenavn]))), N'''') AS [kommunenavn],
            NULLIF(UPPER(LTRIM(RTRIM(b.[postnr]))), N'''') AS [postnr],
            NULLIF(UPPER(LTRIM(RTRIM(b.[postdistrikt]))), N'''') AS [postdistrikt],
            NULLIF(UPPER(LTRIM(RTRIM(b.[sogn_id]))), N'''') AS [sogn_id],
            NULLIF(UPPER(LTRIM(RTRIM(b.[sognenavn]))), N'''') AS [sognenavn],
            NULLIF(UPPER(LTRIM(RTRIM(b.[koen_navn]))), N'''') AS [koen_navn],
            CAST(YEAR(b.[foedselsdato]) AS SMALLINT) AS [foedselsaar],
            CAST(MONTH(b.[foedselsdato]) AS TINYINT) AS [foedselsmaaned]
    FROM dim.Borger b
)
INSERT INTO dim.BorgerDemografi (dw_ek_BorgerDemografi, [region_id], [regionnavn], [kommune_id], [kommunenavn], [postnr], [postdistrikt], [sogn_id], [sognenavn], [koen_navn], [foedselsaar], [foedselsmaaned])
SELECT k.demografi_key, s.[region_id], s.[regionnavn], s.[kommune_id], s.[kommunenavn], s.[postnr], s.[postdistrikt], s.[sogn_id], s.[sognenavn], s.[koen_navn], s.[foedselsaar], s.[foedselsmaaned]
FROM Combinations s
CROSS APPLY (SELECT CASE WHEN s.[foedselsaar] IS NULL AND s.[foedselsmaaned] IS NULL THEN CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[koen_navn]))), N''''), N''|''))), 2) ELSE CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[koen_navn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[foedselsaar]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[foedselsaar]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(s.[foedselsmaaned]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(s.[foedselsmaaned]))), N''''), N''|''))), 2) END AS demografi_key) k
WHERE NOT EXISTS (
    SELECT 1 FROM dim.BorgerDemografi d WHERE d.dw_ek_BorgerDemografi=k.demografi_key
);

IF DB_NAME() LIKE N''%staging%'' AND NOT EXISTS (
    SELECT 1 FROM sys.computed_columns WHERE object_id=OBJECT_ID(N''dim.Borger'')
      AND name=N''dw_ek_BorgerDemografi'' AND definition LIKE N''%foedselsdato%''
)
BEGIN
    ALTER TABLE dim.Borger DROP COLUMN dw_ek_BorgerDemografi;
    ALTER TABLE dim.Borger ADD dw_ek_BorgerDemografi AS (CASE WHEN CONVERT(NVARCHAR(4), YEAR([foedselsdato])) IS NULL AND CONVERT(NVARCHAR(2), MONTH([foedselsdato])) IS NULL THEN CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''), N''|''))), 2) ELSE CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N''''), N''|''))), 2) END) PERSISTED;
END;
IF DB_NAME() LIKE N''%dso%''
    EXEC(N''UPDATE dim.Borger SET dw_ek_BorgerDemografi=CASE WHEN CONVERT(NVARCHAR(4), YEAR([foedselsdato])) IS NULL AND CONVERT(NVARCHAR(2), MONTH([foedselsdato])) IS NULL THEN CONVERT(CHAR(64), HASHBYTES(''''SHA2_256'''', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''''''), N''''|''''))), 2) ELSE CONVERT(CHAR(64), HASHBYTES(''''SHA2_256'''', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N''''''''), N''''|''''))), 2) END WHERE dw_ek_BorgerDemografi IS NULL OR dw_ek_BorgerDemografi <> CASE WHEN CONVERT(NVARCHAR(4), YEAR([foedselsdato])) IS NULL AND CONVERT(NVARCHAR(2), MONTH([foedselsdato])) IS NULL THEN CONVERT(CHAR(64), HASHBYTES(''''SHA2_256'''', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''''''), N''''|''''))), 2) ELSE CONVERT(CHAR(64), HASHBYTES(''''SHA2_256'''', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N''''''''), N''''|''''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N'''''''')), N'''':'''', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N''''''''), N''''|''''))), 2) END;'');
IF EXISTS (
    SELECT 1 FROM dim.Borger b WHERE NOT EXISTS (
        SELECT 1 FROM dim.BorgerDemografi d WHERE d.dw_ek_BorgerDemografi=b.dw_ek_BorgerDemografi
    )
) THROW 51000, ''Demography key coverage incomplete'', 1;
IF DB_NAME() LIKE N''%dso%''
    EXEC(N''CREATE OR ALTER VIEW [vw_dim].[BorgerDemografi]
AS
SELECT
    dw_ek_BorgerDemografi,
    region_id, regionnavn, kommune_id, kommunenavn,
    postnr, postdistrikt, sogn_id, sognenavn, koen_navn,
    foedselsaar, foedselsmaaned,
    dw_timestamp_insert, dw_timestamp_update
FROM [dim].[BorgerDemografi]'');
COMMIT TRANSACTION;
');
GO
