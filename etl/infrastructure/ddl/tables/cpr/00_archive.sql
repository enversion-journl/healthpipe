-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: the file extracts of the CPR register (product U12160P base, U12170P change)
--
-- 1:1 mirror of the delivery files, one table per record type. All value columns
-- are NVARCHAR with the field width of the source — the archive does not type,
-- it keeps the text of the source. Typing belongs in the stg layer.
--
-- Column names are the CPR field names in lower case with underscores.
-- The widths come from the position tables of the record descriptions, which
-- also drive the parser (DSI/csv_files/cpr_layouts.py), so the two cannot drift
-- apart. The record type field is left out: it is constant per table, and
-- the table name carries it.
--
-- Two record types are delivered in the B variant. Records marked 002 are 382
-- characters and not the documented 306, and records marked 023 are 132 and not
-- 96. The variants have identical field positions and append street name and
-- address UUID at the end. The file carries no B, so the parser chooses the
-- layout from record type AND record length. The tables below follow the B variant.
--
-- Keys: most record types have one record per person, protection one per
-- type; the two historical types have no unique key and are replaced per person.
--
-- Key columns are NOT NULL and carry '' rather than NULL. SQL Server does not
-- allow NULL in a primary key, and NULL never matches itself in a MERGE, so the
-- row would be reinserted on every run. This applies in particular to the
-- move-out date on the latest historical address, which is empty.
--
-- The change extract is person snapshots: when record type 001 is received for
-- a person, all earlier records for that person are replaced. The archive
-- therefore limits delete detection to the personal numbers the delivery
-- contains, rather than deleting across the table.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- arc.cpr_001_person  (record type 001, 106 characters)
-- Personal details: sex, date of birth, status and current personal number.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_001_person]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_001_person] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [pnrgaeld]              NVARCHAR(10)  NULL,
        [status]                NVARCHAR(2)   NULL,
        [statushaenstart]       NVARCHAR(12)  NULL,
        [statusdto_umrk]        NVARCHAR(1)   NULL,
        [koen]                  NVARCHAR(1)   NULL,
        [foed_dt]               NVARCHAR(10)  NULL,
        [foed_dt_umrk]          NVARCHAR(1)   NULL,
        [start_dt_person]       NVARCHAR(10)  NULL,
        [start_dt_umrk_person]  NVARCHAR(1)   NULL,
        [slut_dt_person]        NVARCHAR(10)  NULL,
        [slut_dt_umrk_person]   NVARCHAR(1)   NULL,
        [stilling]              NVARCHAR(34)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_001_person_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_001_person] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_002_adresse  (record type 002B, 382 characters)
-- Current address of residence. The delivery uses the B variant, with street name and address UUID.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_002_adresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_002_adresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [convn]                NVARCHAR(34)  NULL,
        [tilflydto]            NVARCHAR(12)  NULL,
        [tilflydto_umrk]       NVARCHAR(1)   NULL,
        [tilflykomdto]         NVARCHAR(12)  NULL,
        [tilflykomdt_umrk]     NVARCHAR(1)   NULL,
        [fraflykomkod]         NVARCHAR(4)   NULL,
        [fraflykomdto]         NVARCHAR(12)  NULL,
        [fraflykomdt_umrk]     NVARCHAR(1)   NULL,
        [start_mynkod_adrtxt]  NVARCHAR(4)   NULL,
        [adr1_supladr]         NVARCHAR(34)  NULL,
        [adr2_supladr]         NVARCHAR(34)  NULL,
        [adr3_supladr]         NVARCHAR(34)  NULL,
        [adr4_supladr]         NVARCHAR(34)  NULL,
        [adr5_supladr]         NVARCHAR(34)  NULL,
        [start_dt_adrtxt]      NVARCHAR(10)  NULL,
        [slet_dt_adrtxt]       NVARCHAR(10)  NULL,
        [vejnvn]               NVARCHAR(40)  NULL,
        [adresse_uuid]         NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_002_adresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_002_adresse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_003_klarskriftadresse  (record type 003, 249 characters)
-- Addressing name and plain-text address: postal code, city name, standard address.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_003_klarskriftadresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_003_klarskriftadresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [adrnvn]               NVARCHAR(34)  NULL,
        [conavn]               NVARCHAR(34)  NULL,
        [lokalitet]            NVARCHAR(34)  NULL,
        [standardadr]          NVARCHAR(34)  NULL,
        [bynavn]               NVARCHAR(34)  NULL,
        [postnr]               NVARCHAR(4)   NULL,
        [postdisttxt]          NVARCHAR(20)  NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [vejadrnvn]            NVARCHAR(20)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_003_klarskriftadresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_003_klarskriftadresse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_004_beskyttelse  (record type 004, 37 characters)
-- Current protection. Up to three types per person, hence the type in the key.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_004_beskyttelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_004_beskyttelse] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [beskyttype]            NVARCHAR(4)   NOT NULL,
        [start_dt_beskyttelse]  NVARCHAR(10)  NULL,
        [slet_dt_beskyttelse]   NVARCHAR(10)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_004_beskyttelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_004_beskyttelse] PRIMARY KEY CLUSTERED (
            [pnr], [beskyttype]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_005_udrejse  (record type 005, 200 characters)
-- Emigration details for persons with status 80.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_005_udrejse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_005_udrejse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [udr_landekod]         NVARCHAR(4)   NULL,
        [udrdto]               NVARCHAR(12)  NULL,
        [udrdto_umrk]          NVARCHAR(1)   NULL,
        [udlandadr1]           NVARCHAR(34)  NULL,
        [udlandadr2]           NVARCHAR(34)  NULL,
        [udlandadr3]           NVARCHAR(34)  NULL,
        [udlandadr4]           NVARCHAR(34)  NULL,
        [udlandadr5]           NVARCHAR(34)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_005_udrejse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_005_udrejse] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_008_navn  (record type 008, 193 characters)
-- Current names: first, middle and last name, and addressing name.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_008_navn]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_008_navn] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [fornvn]                NVARCHAR(50)  NULL,
        [fornvn_mrk]            NVARCHAR(1)   NULL,
        [melnvn]                NVARCHAR(40)  NULL,
        [melnvn_mrk]            NVARCHAR(1)   NULL,
        [efternvn]              NVARCHAR(40)  NULL,
        [efternvn_mrk]          NVARCHAR(1)   NULL,
        [nvnhaenstart]          NVARCHAR(12)  NULL,
        [haenstart_umrk_navne]  NVARCHAR(1)   NULL,
        [adrnvn]                NVARCHAR(34)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_008_navn_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_008_navn] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_010_statsborgerskab  (record type 010, 30 characters)
-- Current citizenship as country code.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_010_statsborgerskab]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_010_statsborgerskab] (
        [pnr]                             NVARCHAR(10)  NOT NULL,
        [landekode]                       NVARCHAR(4)   NULL,
        [haenstart_statsborgerskab]       NVARCHAR(12)  NULL,
        [haenstart_umrk_statsborgerskab]  NVARCHAR(1)   NULL,
        [leverance_dato]                  DATE          NOT NULL,
        [dw_is_deleted]                   BIT           NOT NULL CONSTRAINT [DF_cpr_010_statsborgerskab_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]             DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]             DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_010_statsborgerskab] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_012_civilstand  (record type 012, 95 characters)
-- Current marital status, including the personal number of the spouse.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_012_civilstand]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_012_civilstand] (
        [pnr]                        NVARCHAR(10)  NOT NULL,
        [civst]                      NVARCHAR(1)   NULL,
        [aegtepnr]                   NVARCHAR(10)  NULL,
        [aegtefoed_dt]               NVARCHAR(10)  NULL,
        [aegtefoeddt_umrk]           NVARCHAR(1)   NULL,
        [aegtenvn]                   NVARCHAR(34)  NULL,
        [aegtenvn_mrk]               NVARCHAR(1)   NULL,
        [haenstart_civilstand]       NVARCHAR(12)  NULL,
        [haenstart_umrk_civilstand]  NVARCHAR(1)   NULL,
        [sep_henvis_civilstand]      NVARCHAR(12)  NULL,
        [leverance_dato]             DATE          NOT NULL,
        [dw_is_deleted]              BIT           NOT NULL CONSTRAINT [DF_cpr_012_civilstand_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]        DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]        DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_012_civilstand] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_015_foraeldre  (record type 015, 147 characters)
-- Mother and father: personal number, name and date of birth.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_015_foraeldre]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_015_foraeldre] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [mor_dt]               NVARCHAR(10)  NULL,
        [mor_dt_umrk]          NVARCHAR(1)   NULL,
        [pnrmor]               NVARCHAR(10)  NULL,
        [mor_foed_dt]          NVARCHAR(10)  NULL,
        [mor_foed_dt_umrk]     NVARCHAR(1)   NULL,
        [mornvn]               NVARCHAR(34)  NULL,
        [mornvn_mrk]           NVARCHAR(1)   NULL,
        [far_dt]               NVARCHAR(10)  NULL,
        [far_dt_umrk]          NVARCHAR(1)   NULL,
        [pnrfar]               NVARCHAR(10)  NULL,
        [far_foed_dt]          NVARCHAR(10)  NULL,
        [far_foed_dt_umrk]     NVARCHAR(1)   NULL,
        [farnvn]               NVARCHAR(34)  NULL,
        [farnvn_mrk]           NVARCHAR(1)   NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_015_foraeldre_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_015_foraeldre] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_022_historisk_personnummer  (record type 022, 45 characters)
-- Historical personal numbers. The bridge between an old and a current number.
-- pnrgaeld (position 4-13) is the current number and pnr the historical one;
-- the person behind the row is pnrgaeld, and delete detection is limited by it.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_022_historisk_personnummer]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_022_historisk_personnummer] (
        [pnrgaeld]              NVARCHAR(10)  NOT NULL,
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [start_dt_person]       NVARCHAR(10)  NULL,
        [start_dt_umrk_person]  NVARCHAR(1)   NULL,
        [slut_dt_person]        NVARCHAR(10)  NULL,
        [slut_dt_umrk_person]   NVARCHAR(1)   NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_022_historisk_personnummer_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_022_historisk_personnummer] PRIMARY KEY CLUSTERED (
            [pnr], [pnrgaeld]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_023_historisk_adresse  (record type 023B, 132 characters)
-- Historical addresses with periods. The B variant, with address UUID. Up to 121 per person as measured.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_023_historisk_adresse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_023_historisk_adresse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [annkor]               NVARCHAR(1)   NULL,
        [komkod]               NVARCHAR(4)   NULL,
        [vejkod]               NVARCHAR(4)   NULL,
        [husnr]                NVARCHAR(4)   NULL,
        [etage]                NVARCHAR(2)   NULL,
        [sidedoer]             NVARCHAR(4)   NULL,
        [bnr]                  NVARCHAR(4)   NULL,
        [convn]                NVARCHAR(34)  NULL,
        [tilflydto]            NVARCHAR(12)  NOT NULL,
        [tilflydto_umrk]       NVARCHAR(1)   NULL,
        [fraflydto]            NVARCHAR(12)  NOT NULL,
        [fraflydto_umrk]       NVARCHAR(1)   NULL,
        [adresse_uuid]         NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_023_historisk_adresse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        -- No primary key: the source does not identify its rows
        -- uniquely here. Measured on the deliveries, even the widest
        -- combination of source columns was not unique. The rows are
        -- therefore replaced entirely for the persons a delivery
        -- contains, and the index exists to make that replacement fast.
        INDEX [IX_arc_cpr_023_historisk_adresse_person] CLUSTERED ([pnr], [tilflydto], [fraflydto])
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_034_historisk_beskyttelse  (record type 034, 37 characters)
-- Historical protections with periods.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_034_historisk_beskyttelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_034_historisk_beskyttelse] (
        [pnr]                   NVARCHAR(10)  NOT NULL,
        [beskyttype]            NVARCHAR(4)   NOT NULL,
        [start_dt_beskyttelse]  NVARCHAR(10)  NOT NULL,
        [slut_dt_beskyttelse]   NVARCHAR(10)  NULL,
        [leverance_dato]        DATE          NOT NULL,
        [dw_is_deleted]         BIT           NOT NULL CONSTRAINT [DF_cpr_034_historisk_beskyttelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL,
        -- No primary key: the source does not identify its rows
        -- uniquely here. Measured on the deliveries, even the widest
        -- combination of source columns was not unique. The rows are
        -- therefore replaced entirely for the persons a delivery
        -- contains, and the index exists to make that replacement fast.
        INDEX [IX_arc_cpr_034_historisk_beskyttelse_person] CLUSTERED ([pnr], [beskyttype], [start_dt_beskyttelse])
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_037_personid  (record type 037, 49 characters)
-- PersonID: stable identity across changes of personal number.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_037_personid]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_037_personid] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [person_id]            NVARCHAR(36)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_037_personid_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_037_personid] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_038_sogn  (record type 038, 77 characters)
-- Current parish of residence.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_038_sogn]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_038_sogn] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [sognekode]            NVARCHAR(4)   NULL,
        [sognenvn]             NVARCHAR(60)  NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_038_sogn_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_038_sogn] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_099_haendelse  (record type 099, 45 characters)
-- Events affecting the person during the day. A log: new events are
-- appended, nothing is updated or deleted.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_099_haendelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_099_haendelse] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [timestampu]           NVARCHAR(12)  NOT NULL,
        [haendelse]            NVARCHAR(3)   NOT NULL,
        [afledtmrk]            NVARCHAR(2)   NOT NULL,
        [nglkonst]             NVARCHAR(15)  NOT NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_099_haendelse_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_099_haendelse] PRIMARY KEY CLUSTERED (
            [pnr], [timestampu], [haendelse], [afledtmrk], [nglkonst]
        )
    );
END
GO


-- ----------------------------------------------------------------------------
-- arc.cpr_996_data_fremadrettet  (record type 996, 15 characters)
-- Whether the person is delivered going forward: in the set of persons and/or by subscription.
-- leverance_dato is the latest delivery the person was in.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[cpr_996_data_fremadrettet]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[cpr_996_data_fremadrettet] (
        [pnr]                  NVARCHAR(10)  NOT NULL,
        [personkreds]          NVARCHAR(1)   NULL,
        [abonnement]           NVARCHAR(1)   NULL,
        [leverance_dato]       DATE          NOT NULL,
        [dw_is_deleted]        BIT           NOT NULL CONSTRAINT [DF_cpr_996_data_fremadrettet_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL,
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_arc_cpr_996_data_fremadrettet] PRIMARY KEY CLUSTERED (
            [pnr]
        )
    );
END
GO
