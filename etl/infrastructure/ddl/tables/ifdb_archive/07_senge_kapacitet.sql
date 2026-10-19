-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB (senge og kapacitet)
--
-- 1:1 reflection of the IFDB Oracle source tables plus DWH audit columns.
-- All source values land as strings (mirrors the Oracle→MSSQL stringify path);
-- parsing happens in the FHIR transform.
--
-- Load strategy per table, decided on volumes and key uniqueness measured
-- against the live source:
--
-- ifdb_sengeenhed:              delta on MODTIDSPUNKT; UPSERT on OBJEKT_ID.
--                              Soft-delete via SLETTET (source column). The
--                              only table here the source gives a primary key,
--                              a modification timestamp and a delete flag.
-- ifdb_sengeenhed_disponible:   full load + detect_deletes. 1.266 rows; the
--                              source has no delete signal, and a full snapshot
--                              is the only way to retire a removed period.
-- ifdb_sengeenhed_normerede:    full load + detect_deletes. 379 rows, same
--                              reasoning.
-- ifdb_absence:                 full load + detect_deletes. 78.951 rows — small
--                              enough that the snapshot buys delete detection
--                              the source cannot otherwise provide.
-- ifdb_encounter_bed_space_history:
--                              join-delta via ENCOUNTER (1,3 mio. rows is too
--                              large for a nightly snapshot). Price: no delete
--                              detection, same as arc.ifdb_location.
--
-- Merge keys are measured, not assumed: every key below was verified unique
-- against the live source (0 duplicates in all four PK-less tables).
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_sengeenhed  (1:1 from IFDB.SENGEENHED — 6 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_sengeenhed'
)
BEGIN
    CREATE TABLE [arc].[ifdb_sengeenhed] (
        [OBJEKT_ID]                      NVARCHAR(50)    NOT NULL,
        [MODTIDSPUNKT]                   NVARCHAR(100)   NULL,
        [SLETTET]                        NVARCHAR(100)   NULL,
        [ADMENHED_KODE]                  NVARCHAR(512)   NULL,
        [ADMENHED_NAVN]                  NVARCHAR(512)   NULL,
        [ADMENHED_ORGKODE]               NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_sengeenhed_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_sengeenhed]
            PRIMARY KEY CLUSTERED ([OBJEKT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_sengeenhed_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_sengeenhed]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_sengeenhed_dw_timestamp_update]
        ON [arc].[ifdb_sengeenhed] ([dw_timestamp_update] DESC);
END
GO

-- ADMENHED_KODE is the join key for the organisation lookup (the dictionary
-- harvested from arc.ifdb_aktivitetsresultat / arc.ifdb_ris_ydelse), so the
-- Location transform reads the archive by this column.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_sengeenhed_ADMENHED_KODE'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_sengeenhed]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_sengeenhed_ADMENHED_KODE]
        ON [arc].[ifdb_sengeenhed] ([ADMENHED_KODE]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_sengeenhed_disponible  (1:1 from IFDB.SENGEENHED_DISPONIBLE)
--
-- Grain: one row per bed unit per period start. Verified unique in the source
-- (1.266 rows, 1.266 distinct (SENGEENHED_ID, FRA)).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_sengeenhed_disponible'
)
BEGIN
    CREATE TABLE [arc].[ifdb_sengeenhed_disponible] (
        [SENGEENHED_ID]                  NVARCHAR(50)    NOT NULL,
        [FRA]                            NVARCHAR(100)   NOT NULL,
        [TIL]                            NVARCHAR(100)   NULL,
        [ANTAL]                          NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_sengeenhed_disponible_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_sengeenhed_disponible]
            PRIMARY KEY CLUSTERED ([SENGEENHED_ID], [FRA])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_sengeenhed_disponible_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_sengeenhed_disponible]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_sengeenhed_disponible_dw_timestamp_update]
        ON [arc].[ifdb_sengeenhed_disponible] ([dw_timestamp_update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_sengeenhed_normerede  (1:1 from IFDB.SENGEENHED_NORMEREDE)
--
-- Structurally identical to _disponible. Kept as its own table because the two
-- are independent period sets in the source; the FHIR transform distinguishes
-- them by Observation.code, not by column.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_sengeenhed_normerede'
)
BEGIN
    CREATE TABLE [arc].[ifdb_sengeenhed_normerede] (
        [SENGEENHED_ID]                  NVARCHAR(50)    NOT NULL,
        [FRA]                            NVARCHAR(100)   NOT NULL,
        [TIL]                            NVARCHAR(100)   NULL,
        [ANTAL]                          NVARCHAR(100)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_sengeenhed_normerede_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_sengeenhed_normerede]
            PRIMARY KEY CLUSTERED ([SENGEENHED_ID], [FRA])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_sengeenhed_normerede_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_sengeenhed_normerede]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_sengeenhed_normerede_dw_timestamp_update]
        ON [arc].[ifdb_sengeenhed_normerede] ([dw_timestamp_update] DESC);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_encounter_bed_space_history  (1:1 from IFDB.ENCOUNTER_BED_SPACE_HISTORY)
--
-- Grain: one row per encounter per registration start. BED_ID is deliberately
-- NOT part of the key: the source is unique on (OBJECT_ID,
-- PERIOD_START_REGISTRATION) alone (measured: 1.312.474 rows, 1.312.474
-- distinct), because an encounter is never in two beds at the same instant.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_encounter_bed_space_history'
)
BEGIN
    CREATE TABLE [arc].[ifdb_encounter_bed_space_history] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [PERIOD_START_REGISTRATION]      NVARCHAR(100)   NOT NULL,
        [PERIOD_END_REGISTRATION]        NVARCHAR(100)   NULL,
        [BED_ID]                         NVARCHAR(512)   NULL,
        [BED]                            NVARCHAR(512)   NULL,
        [ROOM]                           NVARCHAR(512)   NULL,
        [LOCATION_CODE]                  NVARCHAR(512)   NULL,
        [LOCATION_NAME]                  NVARCHAR(512)   NULL,
        [LOCATION_ORGCODE]               NVARCHAR(512)   NULL,
        [LOCATION_SORCODE]               NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_encounter_bed_space_history_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_encounter_bed_space_history]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START_REGISTRATION])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_ebsh_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter_bed_space_history]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_ebsh_dw_timestamp_update]
        ON [arc].[ifdb_encounter_bed_space_history] ([dw_timestamp_update] DESC);
END
GO

-- The bed Location transform reads distinct BED_ID from this table, and the
-- Encounter transform reads it per encounter batch. Both are covered by the
-- clustered PK on OBJECT_ID; BED_ID needs its own index for the first.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_ebsh_BED_ID'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_encounter_bed_space_history]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_ebsh_BED_ID]
        ON [arc].[ifdb_encounter_bed_space_history] ([BED_ID])
        INCLUDE ([BED], [ROOM], [LOCATION_SORCODE], [PERIOD_START_REGISTRATION]);
END
GO


-- ----------------------------------------------------------------------------
-- arc.ifdb_absence  (1:1 from IFDB.ABSENCE)
--
-- Grain: one row per encounter per absence start. TYPE_CODE is not needed in
-- the key (measured: 78.951 rows, 78.951 distinct on (OBJECT_ID,
-- PERIOD_START), and the same count when TYPE_CODE is added).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_absence'
)
BEGIN
    CREATE TABLE [arc].[ifdb_absence] (
        [OBJECT_ID]                      NVARCHAR(50)    NOT NULL,
        [PERIOD_START]                   NVARCHAR(100)   NOT NULL,
        [PERIOD_END]                     NVARCHAR(100)   NULL,
        [EXPECTED_RETURN]                NVARCHAR(100)   NULL,
        [TYPE_CODE]                      NVARCHAR(512)   NULL,
        [TYPE_TEXT]                      NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]            DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]            DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                  BIT             NOT NULL CONSTRAINT [DF_ifdb_absence_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_absence]
            PRIMARY KEY CLUSTERED ([OBJECT_ID], [PERIOD_START])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_absence_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_absence]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_absence_dw_timestamp_update]
        ON [arc].[ifdb_absence] ([dw_timestamp_update] DESC);
END
GO
