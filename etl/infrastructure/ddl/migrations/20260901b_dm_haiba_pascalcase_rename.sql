-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260901b_dm_haiba_pascalcase_rename
--
-- HAIBA dm/map-tabellerne (og deres dw_ek_-nøglekolonner) omdøbes til ren
-- PascalCase uden underscores inde i navnet, jf. navngivningsstandarden
-- (MR !60, F7): HAIBA_Agg_Infektion -> HAIBA_AggInfektion,
-- HAIBA_Cases_Infektion_Egenskaber -> HAIBA_CasesInfektionEgenskaber osv.
-- HAIBA_-præfikset (UPPERCASE-akronym) bevares; kun de interne underscores
-- fjernes.
--
-- Omfang: 8 dm-tabeller + 4 map-tabeller + de 4 dw_ek_HAIBA_*-kolonner (som
-- optræder på map-fakta, dm-fakta og dm-egenskaber). De GAMLE vw_dm-views
-- droppes; den opdaterede views/haiba/01_vw_dm.sql genskaber dem under de nye
-- navne ved næste infra-kørsel.
--
-- BEVIDST IKKE omdøbt: interne constraint- og index-navne (PK_/UQ_/DF_/IX_)
-- beholder den gamle stavemåde. Standarden gælder tabelnavne, ikke interne
-- objektnavne; og at lade dem stå betyder at end-state-DDL'en (02_dm.sql,
-- 01_maps.sql) og en frisk database matcher en migreret database præcist —
-- ingen divergens. arc.haiba_*-tabellerne (lowercase) er en anden navneakse
-- og røres ikke.
--
-- Idempotent: hver omdøbning er guarded (kun når det gamle navn findes og det
-- nye ikke gør). map-tabellerne findes kun på staging, dm på både staging og
-- DSO, vw_dm kun på DSO — de manglende er no-ops via guards.
-- ============================================================================

-- ---- Drop gamle vw_dm-views (genskabes af 01_vw_dm.sql under nye navne) ----
IF OBJECT_ID(N'[vw_dm].[HAIBA_Agg_Infektion]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Agg_Infektion];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Agg_Infektion_Egenskaber]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Agg_Infektion_Egenskaber];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Agg_Ortopaedi]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Agg_Ortopaedi];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Agg_Ortopaedi_Egenskaber]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Agg_Ortopaedi_Egenskaber];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Cases_Infektion]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Cases_Infektion];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Cases_Infektion_Egenskaber]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Cases_Infektion_Egenskaber];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Cases_SSI]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Cases_SSI];
IF OBJECT_ID(N'[vw_dm].[HAIBA_Cases_SSI_Egenskaber]', N'V') IS NOT NULL DROP VIEW [vw_dm].[HAIBA_Cases_SSI_Egenskaber];
GO

-- ---- Omdøb tabeller (dm + map) ----
DECLARE @tables TABLE (schema_name SYSNAME, old_name SYSNAME, new_name SYSNAME);
INSERT INTO @tables VALUES
    (N'dm',  N'HAIBA_Agg_Infektion',             N'HAIBA_AggInfektion'),
    (N'dm',  N'HAIBA_Agg_Infektion_Egenskaber',  N'HAIBA_AggInfektionEgenskaber'),
    (N'dm',  N'HAIBA_Agg_Ortopaedi',             N'HAIBA_AggOrtopaedi'),
    (N'dm',  N'HAIBA_Agg_Ortopaedi_Egenskaber',  N'HAIBA_AggOrtopaediEgenskaber'),
    (N'dm',  N'HAIBA_Cases_Infektion',           N'HAIBA_CasesInfektion'),
    (N'dm',  N'HAIBA_Cases_Infektion_Egenskaber', N'HAIBA_CasesInfektionEgenskaber'),
    (N'dm',  N'HAIBA_Cases_SSI',                 N'HAIBA_CasesSSI'),
    (N'dm',  N'HAIBA_Cases_SSI_Egenskaber',      N'HAIBA_CasesSSIEgenskaber'),
    (N'map', N'HAIBA_Agg_Infektion',             N'HAIBA_AggInfektion'),
    (N'map', N'HAIBA_Agg_Ortopaedi',             N'HAIBA_AggOrtopaedi'),
    (N'map', N'HAIBA_Cases_Infektion',           N'HAIBA_CasesInfektion'),
    (N'map', N'HAIBA_Cases_SSI',                 N'HAIBA_CasesSSI');

DECLARE @schema SYSNAME, @old SYSNAME, @new SYSNAME, @qualified NVARCHAR(300);
DECLARE tcur CURSOR LOCAL FAST_FORWARD FOR
    SELECT schema_name, old_name, new_name FROM @tables;
OPEN tcur;
FETCH NEXT FROM tcur INTO @schema, @old, @new;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@old), N'U') IS NOT NULL
       AND OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@new), N'U') IS NULL
    BEGIN
        SET @qualified = @schema + N'.' + @old;
        EXEC sp_rename @qualified, @new;
    END
    FETCH NEXT FROM tcur INTO @schema, @old, @new;
END
CLOSE tcur;
DEALLOCATE tcur;
GO

-- ---- Omdøb dw_ek_-nøglekolonnerne (på de nu-omdøbte tabeller) ----
-- Kolonnen bor på map-fakta, dm-fakta og dm-egenskaber (delt nøgle).
DECLARE @cols TABLE (tbl NVARCHAR(300), old_col SYSNAME, new_col SYSNAME);
INSERT INTO @cols VALUES
    (N'map.HAIBA_AggInfektion',           N'dw_ek_HAIBA_Agg_Infektion',  N'dw_ek_HAIBA_AggInfektion'),
    (N'dm.HAIBA_AggInfektion',            N'dw_ek_HAIBA_Agg_Infektion',  N'dw_ek_HAIBA_AggInfektion'),
    (N'dm.HAIBA_AggInfektionEgenskaber',  N'dw_ek_HAIBA_Agg_Infektion',  N'dw_ek_HAIBA_AggInfektion'),
    (N'map.HAIBA_AggOrtopaedi',           N'dw_ek_HAIBA_Agg_Ortopaedi',  N'dw_ek_HAIBA_AggOrtopaedi'),
    (N'dm.HAIBA_AggOrtopaedi',            N'dw_ek_HAIBA_Agg_Ortopaedi',  N'dw_ek_HAIBA_AggOrtopaedi'),
    (N'dm.HAIBA_AggOrtopaediEgenskaber',  N'dw_ek_HAIBA_Agg_Ortopaedi',  N'dw_ek_HAIBA_AggOrtopaedi'),
    (N'map.HAIBA_CasesInfektion',         N'dw_ek_HAIBA_Cases_Infektion', N'dw_ek_HAIBA_CasesInfektion'),
    (N'dm.HAIBA_CasesInfektion',          N'dw_ek_HAIBA_Cases_Infektion', N'dw_ek_HAIBA_CasesInfektion'),
    (N'dm.HAIBA_CasesInfektionEgenskaber', N'dw_ek_HAIBA_Cases_Infektion', N'dw_ek_HAIBA_CasesInfektion'),
    (N'map.HAIBA_CasesSSI',               N'dw_ek_HAIBA_Cases_SSI',      N'dw_ek_HAIBA_CasesSSI'),
    (N'dm.HAIBA_CasesSSI',                N'dw_ek_HAIBA_Cases_SSI',      N'dw_ek_HAIBA_CasesSSI'),
    (N'dm.HAIBA_CasesSSIEgenskaber',      N'dw_ek_HAIBA_Cases_SSI',      N'dw_ek_HAIBA_CasesSSI');

-- @qualified declared here: GO ended the previous batch, so its variables are
-- out of scope in this one.
DECLARE @tbl NVARCHAR(300), @oldc SYSNAME, @newc SYSNAME, @qualified NVARCHAR(400);
DECLARE ccur CURSOR LOCAL FAST_FORWARD FOR SELECT tbl, old_col, new_col FROM @cols;
OPEN ccur;
FETCH NEXT FROM ccur INTO @tbl, @oldc, @newc;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(@tbl, N'U') IS NOT NULL
       AND COL_LENGTH(@tbl, @oldc) IS NOT NULL
       AND COL_LENGTH(@tbl, @newc) IS NULL
    BEGIN
        SET @qualified = @tbl + N'.' + @oldc;
        EXEC sp_rename @qualified, @newc, 'COLUMN';
    END
    FETCH NEXT FROM ccur INTO @tbl, @oldc, @newc;
END
CLOSE ccur;
DEALLOCATE ccur;
GO
