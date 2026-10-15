-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: IFDB Eventmarker (CarePlan "alle markører")
--
-- 1:1 reflection of IFDB.EVENTMARKER plus DWH audit columns. All 17 source
-- columns are archived, including the ones the CarePlan transform does not
-- read, so a later consumer never needs a re-extract to reach them.
--
-- ifdb_eventmarker: delta via IFDB_MODIFICATION_DATETIME (ceiling on
--                   EPJI_MODIFICATION_DATETIME); UPSERT on OBJECT_ID.
--                   Removals arrive as DELETED=1 in the delta, so the merge
--                   does no NOT-MATCHED-BY-SOURCE detection.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.ifdb_eventmarker  (1:1 from IFDB.EVENTMARKER — 17 source columns)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ifdb_eventmarker'
)
BEGIN
    CREATE TABLE [arc].[ifdb_eventmarker] (
        [OBJECT_ID]                    NVARCHAR(50)    NOT NULL,
        [PATIENT_ID]                   NVARCHAR(50)    NULL,
        [EPISODE_OF_CARE_ID]           NVARCHAR(50)    NULL,
        [EVENT_CODE]                   NVARCHAR(512)   NULL,
        [EVENT_TEXT]                   NVARCHAR(100)   NULL,
        [TIME]                         NVARCHAR(100)   NULL,
        [EPJI_CREATED_DATETIME]        NVARCHAR(100)   NULL,
        [EPJI_MODIFICATION_DATETIME]   NVARCHAR(100)   NULL,
        [IFDB_MODIFICATION_DATETIME]   NVARCHAR(100)   NULL,
        [DELETED]                      NVARCHAR(22)    NULL,
        [ADMINISTRATIVE_UNIT_CODE]     NVARCHAR(512)   NULL,
        [ADMINISTRATIVE_UNIT_NAME]     NVARCHAR(512)   NULL,
        [ADMINISTRATIVE_UNIT_ORGCODE]  NVARCHAR(512)   NULL,
        [ADMINISTRATIVE_UNIT_SORCODE]  NVARCHAR(512)   NULL,
        [RESPONSIBLE_STAFF_PERSID]     NVARCHAR(512)   NULL,
        [RESPONSIBLE_STAFF_PERSNAME]   NVARCHAR(512)   NULL,
        [RESPONSIBLE_STAFF_ROLE]       NVARCHAR(512)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]          DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]          DATETIME2(0)    NOT NULL,
        [dw_is_deleted]                BIT             NOT NULL CONSTRAINT [DF_ifdb_eventmarker_dw_is_deleted] DEFAULT (0),

        CONSTRAINT [PK_arc_ifdb_eventmarker]
            PRIMARY KEY CLUSTERED ([OBJECT_ID])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_eventmarker_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_eventmarker]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_eventmarker_dw_timestamp_update]
        ON [arc].[ifdb_eventmarker] ([dw_timestamp_update] DESC);
END
GO

-- The CarePlan transform runs two queries against this table on every cycle:
-- DISTINCT PATIENT_ID over the delta window (served by the index above), then
-- "all markers for this patient batch". The second one is a PATIENT_ID seek
-- returning the whole row, so the covering INCLUDE keeps it off the clustered
-- index. TIME is bracket-quoted everywhere — it is a T-SQL type keyword.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_eventmarker_patient_id'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_eventmarker]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_eventmarker_patient_id]
        ON [arc].[ifdb_eventmarker] ([PATIENT_ID])
        INCLUDE (
            [EPISODE_OF_CARE_ID],
            [EVENT_CODE],
            [EVENT_TEXT],
            [TIME],
            [ADMINISTRATIVE_UNIT_SORCODE],
            [RESPONSIBLE_STAFF_PERSID],
            [DELETED],
            -- The fetch filters on dw_is_deleted, not on the source DELETED
            -- column, so it has to be in the leaf too — without it every seek
            -- goes back to the clustered index just to evaluate the predicate.
            [dw_is_deleted]
        );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ifdb_eventmarker_IFDB_MODIFICATION_DATETIME'
      AND object_id = OBJECT_ID(N'[arc].[ifdb_eventmarker]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ifdb_eventmarker_IFDB_MODIFICATION_DATETIME]
        ON [arc].[ifdb_eventmarker] ([IFDB_MODIFICATION_DATETIME] DESC);
END
GO
