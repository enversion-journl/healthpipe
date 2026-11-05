-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Staging owns the computed key; the writable DSO column is compatible with
-- existing sync deployments, which discover and copy every source column.
IF DB_NAME() LIKE N'%staging%'
   AND OBJECT_ID(N'[dim].[Borger]', N'U') IS NOT NULL
   AND ISNULL(COLUMNPROPERTY(OBJECT_ID(N'dim.Borger'), N'dw_ek_BorgerDemografi', 'IsComputed'), 0) = 0
BEGIN
    ALTER TABLE [dim].[Borger] DROP COLUMN [dw_ek_BorgerDemografi];
    ALTER TABLE [dim].[Borger] ADD [dw_ek_BorgerDemografi] AS (CASE WHEN CONVERT(NVARCHAR(4), YEAR([foedselsdato])) IS NULL AND CONVERT(NVARCHAR(2), MONTH([foedselsdato])) IS NULL THEN CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''), N'|'))), 2) ELSE CONVERT(CHAR(64), HASHBYTES('SHA2_256', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N''), N'|'), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N'')), N':', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N''), N'|'))), 2) END) PERSISTED;
END
GO
IF DB_NAME() LIKE N'%dso%'
   AND OBJECT_ID(N'[dim].[Borger]', N'U') IS NOT NULL
    EXEC(N'UPDATE [dim].[Borger] SET [dw_ek_BorgerDemografi] = CASE WHEN CONVERT(NVARCHAR(4), YEAR([foedselsdato])) IS NULL AND CONVERT(NVARCHAR(2), MONTH([foedselsdato])) IS NULL THEN CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''), N''|''))), 2) ELSE CONVERT(CHAR(64), HASHBYTES(''SHA2_256'', CONCAT(CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([region_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([regionnavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommune_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([kommunenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postnr]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([postdistrikt]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sogn_id]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([sognenavn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM([koen_navn]))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(4), YEAR([foedselsdato]))))), N''''), N''|''), CONCAT(LEN(ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N'''')), N'':'', ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(2), MONTH([foedselsdato]))))), N''''), N''|''))), 2) END
    WHERE [dw_ek_BorgerDemografi] IS NULL;');
GO

-- One row per demographic combination. Its key is the same CHAR(64) SHA-256
-- persisted on dim.Borger. No map, IDENTITY, citizen key or temporal columns.
IF OBJECT_ID(N'[dim].[BorgerDemografi]', N'U') IS NULL
BEGIN
    CREATE TABLE [dim].[BorgerDemografi] (
        [dw_ek_BorgerDemografi] CHAR(64) NOT NULL,
        [region_id] VARCHAR(8) NULL,
        [regionnavn] NVARCHAR(255) NULL,
        [kommune_id] VARCHAR(8) NULL,
        [kommunenavn] NVARCHAR(255) NULL,
        [postnr] VARCHAR(10) NULL,
        [postdistrikt] NVARCHAR(255) NULL,
        [sogn_id] VARCHAR(4) NULL,
        [sognenavn] NVARCHAR(60) NULL,
        [koen_navn] NVARCHAR(20) NULL,
        [foedselsaar] SMALLINT NULL,
        [foedselsmaaned] TINYINT NULL,
        [dw_timestamp_insert] DATETIME2(0) NOT NULL
            CONSTRAINT DF_BorgerDemografi_dw_timestamp_insert DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update] DATETIME2(0) NOT NULL
            CONSTRAINT DF_BorgerDemografi_dw_timestamp_update DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_Dim_BorgerDemografi PRIMARY KEY (dw_ek_BorgerDemografi)
    );
END
GO
-- Unknown member: the hash of nine empty demographic fields.
IF NOT EXISTS (SELECT 1 FROM [dim].[BorgerDemografi]
               WHERE dw_ek_BorgerDemografi = '0C37FCED8DC1F76E2CC47EC38707AF99AEA730FF7DACDEB329D8ED2A7D39924A')
    EXEC(N'INSERT INTO [dim].[BorgerDemografi] (dw_ek_BorgerDemografi)
    VALUES (''0C37FCED8DC1F76E2CC47EC38707AF99AEA730FF7DACDEB329D8ED2A7D39924A'');');
GO
