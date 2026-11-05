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
