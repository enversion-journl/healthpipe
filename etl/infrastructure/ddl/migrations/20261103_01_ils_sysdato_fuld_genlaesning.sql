-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20261103_01_ils_sysdato_fuld_genlaesning
--
-- De ti ILS-tabeller med delta på sysdato (laftdpris, laftdstam, laftd, udlev,
-- lgrvare, pmdm, indkfak, ordh, unspsc, vare) er hidtil læst med
-- `sysdato > MAX(sysdato)` uden lookback. sysdato har intet klokkeslæt, så en
-- række, der blev ændret efter udtrækket på checkpoint-dagen, beholdt samme
-- sysdato og blev aldrig læst igen, før den blev ændret på ny. Udtrækket læser
-- nu med to dages lookback (DSI/mssql/extract/ils_extract_configs.py), men de
-- ændringer, der allerede er tabt, ligger vilkårligt langt tilbage.
--
-- Migrationen nulstiller arkiv-checkpointet for de ti tabeller. Ext-udtrækket
-- læser det checkpoint (checkpoint_from=("arc", ...)), så næste ordinære
-- kørsel læser hver tabel fuldt fra ILS. Arkivets upsert på prrowid med
-- change detection opdaterer kun de rækker, der reelt afviger - dvs. de tabte
-- ændringer - og sætter derefter checkpointet til MAX(sysdato) igen. DSA
-- henter de opdaterede rækker selv via dw_timestamp_update; ingen manuel
-- full_load, ingen delete/reload.
--
-- Change detection ignorerer pro2created/pro2modified (og pmdm's
-- sidstopdpmdm/sidstopdils), som ellers ville markere ~90 % af de genlæste
-- rækker som ændrede (se _PRO2_STAMPS i ils_archive_configs.py). Den rettelse
-- ligger i samme commit, så genlæsningen bumper kun reelle ændringer.
--
-- Den første kørsel efter deploy bliver markant længere: arc.ils_udlev er ca.
-- 31,8 mio. rækker i prod, og ext-tabellerne holder hele tabellen indtil næste
-- kørsel.
--
-- Idempotent: en gentaget kørsel nulstiller checkpointet igen og udløser blot
-- endnu en fuld genlæsning, som er korrekt men dyr - meta.MigrationHistory
-- forhindrer det.
--
-- Samme migrationsmappe kører mod både staging og DSO, og dso_infra_meta
-- opretter også meta.DFMState og meta.usp_dfm_reset på DSO. Hver nulstilling
-- kræver derfor, at checkpoint-rækken allerede findes: på DSO og på en staging,
-- hvor ILS-arkivet aldrig har kørt, er migrationen en no-op i stedet for at
-- MERGE-indsætte rækker, som intet læser, og skrive vildledende poster i
-- meta.DFMRunLog. Dér er den første kørsel alligevel en fuld indlæsning.
-- ============================================================================
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[meta].[usp_dfm_reset]', 'P') IS NULL
    THROW 50013,
        N'meta.DFMState findes, men meta.usp_dfm_reset gør ikke - materialisér staging_infra_dfm før denne migration. Uden nulstillingen bliver de tabte ILS-ændringer aldrig genlæst.',
        1;
GO

IF OBJECT_ID(N'[meta].[DFMState]', 'U') IS NULL
    PRINT 'Ingen meta.DFMState på denne database - intet checkpoint at nulstille.';
ELSE
    EXEC sp_executesql N'
        DECLARE @reason NVARCHAR(200) = N''[internal ticket]: fuld genlæsning af ILS-sysdato-tabellen for at hente ændringer tabt uden lookback.'';
        DECLARE @reset INT = 0;

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_laftdpris'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_laftdpris'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_laftdstam'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_laftdstam'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_laftd'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_laftd'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_udlev'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_udlev'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_lgrvare'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_lgrvare'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_pmdm'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_pmdm'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_indkfak'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_indkfak'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_ordh'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_ordh'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_unspsc'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_unspsc'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        IF EXISTS (SELECT 1 FROM [meta].[DFMState] WHERE [scope] = ''staging''
                   AND [target_type] = ''archive'' AND [schema_name] = ''arc''
                   AND [table_name] = ''ils_vare'')
        BEGIN
            EXEC [meta].[usp_dfm_reset] @schema_name = ''arc'', @table_name = ''ils_vare'',
                 @new_value = NULL, @reason = @reason, @scope = ''staging'';
            SET @reset = @reset + 1;
        END

        PRINT CONCAT(''Nulstillede '', @reset, '' ILS-arkivcheckpoint(s) til NULL.'');';
GO
