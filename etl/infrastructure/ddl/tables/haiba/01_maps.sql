-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Map: HAIBA (map-skemaet)
--
-- Nøglens fødested. Én map-tabel pr. dm-faktatabel: clean_code er en
-- deterministisk sammensætning af arc-tabellens PK-kolonner ('|'-separeret),
-- og IDENTITY-kolonnen minter dw_ek. Samme mønster som map.Organisation og
-- ERP-domænets map-tabeller.
--
-- Append-only: en clean_code der én gang har fået et dw_ek beholder det for
-- altid — også når rækken soft-slettes i arc. Nøgler genbruges aldrig, og
-- der køres hverken UPDATE eller DELETE mod disse tabeller.
--
-- dw_timestamp_insert er dermed ægte "først set" pr. dw_ek (modsat
-- dm-tabellerne, hvor TRUNCATE + INSERT fornyer det hver kørsel).
-- dw_timestamp_update skrives ALDRIG — minten er INSERT-only, så kolonnen er
-- altid lig insert; den beholdes alene for DW-konventionens skyld.
--
-- Append-only har én udetekterbar fejlmåde: skifter en nøglekomponent FORMAT
-- (SDS ændrer datoskik, SOR-nulpadding, et redigeret schema.key_columns),
-- mintes en hel ny generation nøgler mens rowcounts og paritetsguard forbliver
-- grønne. Derfor tæller rebuild-assettet map_rows_minted pr. kørsel og advarer
-- når minten overstiger REKEY_WARN_FRACTION af kilderækkerne (DSA/SQL/dm/
-- haiba.py) — steady state minter nær nul uden for SDS's januar-vindue.
--
-- SYNCES ALDRIG TIL DSO. Case-tabellernes clean_code indeholder cprnr — det
-- er bevidst: map-laget lever kun i staging, og CPR forlader dermed dm-laget
-- fuldstændigt (fakta og egenskaber bærer kun dw_ek_Borger). GDPR-adgangs-
-- begrænsningen der før lå på dm.HAIBA_Cases_* gælder herefter map + arc.
--
-- clean_code er NVARCHAR(500): komponenterne er kildens nøglekolonner
-- (maks ~174 tegn for agg, ~92 for cases), så en unik nonclustered
-- constraint holder sig under 1700-byte-grænsen.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- map.HAIBA_AggInfektion
-- clean_code = HAI|HAIBA_dato|Rolle_ansvar|RegionKode|SORIdentifier
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[HAIBA_AggInfektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[HAIBA_AggInfektion] (
        [dw_ek_HAIBA_AggInfektion] INT IDENTITY(1, 1) NOT NULL,
        [clean_code]                NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Infektion_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_map_HAIBA_Agg_Infektion]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggInfektion]),
        CONSTRAINT [UQ_map_HAIBA_Agg_Infektion_CleanCode] UNIQUE ([clean_code])
    );
END
GO


-- ----------------------------------------------------------------------------
-- map.HAIBA_AggOrtopaedi
-- clean_code = HAI|HAIBA_dato|Rolle_ansvar|RegionKode|SORIdentifier
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[HAIBA_AggOrtopaedi]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[HAIBA_AggOrtopaedi] (
        [dw_ek_HAIBA_AggOrtopaedi] INT IDENTITY(1, 1) NOT NULL,
        [clean_code]                NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]       DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Agg_Ortopaedi_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_map_HAIBA_Agg_Ortopaedi]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_AggOrtopaedi]),
        CONSTRAINT [UQ_map_HAIBA_Agg_Ortopaedi_CleanCode] UNIQUE ([clean_code])
    );
END
GO


-- ----------------------------------------------------------------------------
-- map.HAIBA_CasesInfektion
-- clean_code = HAI_type|cprnr|HAI_date   (INDEHOLDER CPR — kun staging)
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[HAIBA_CasesInfektion]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[HAIBA_CasesInfektion] (
        [dw_ek_HAIBA_CasesInfektion] INT IDENTITY(1, 1) NOT NULL,
        [clean_code]                  NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert]         DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]         DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_Infektion_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_map_HAIBA_Cases_Infektion]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesInfektion]),
        CONSTRAINT [UQ_map_HAIBA_Cases_Infektion_CleanCode] UNIQUE ([clean_code])
    );
END
GO


-- ----------------------------------------------------------------------------
-- map.HAIBA_CasesSSI
-- clean_code = HAI_type|cprnr|operation_date   (INDEHOLDER CPR — kun staging)
-- ----------------------------------------------------------------------------
IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[map].[HAIBA_CasesSSI]') AND type = 'U'
)
BEGIN
    CREATE TABLE [map].[HAIBA_CasesSSI] (
        [dw_ek_HAIBA_CasesSSI] INT IDENTITY(1, 1) NOT NULL,
        [clean_code]            NVARCHAR(500) NOT NULL,
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_insert] DEFAULT SYSDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL CONSTRAINT [DF_HAIBA_Cases_SSI_dw_timestamp_update] DEFAULT SYSDATETIME(),
        CONSTRAINT [PK_map_HAIBA_Cases_SSI]
            PRIMARY KEY CLUSTERED ([dw_ek_HAIBA_CasesSSI]),
        CONSTRAINT [UQ_map_HAIBA_Cases_SSI_CleanCode] UNIQUE ([clean_code])
    );
END
GO
