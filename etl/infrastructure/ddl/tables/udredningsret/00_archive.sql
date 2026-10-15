-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: SDS' datamart for monitoring of the udredningsret
-- (LPR_Udredningsret, [internal ticket], [internal ticket])
--
-- 1:1 mirror of the delivery file UdredningsretMont_1083_<dato>.csv (28
-- columns) PLUS four computed keys in front, because the source's own ids do
-- not identify anything across deliveries (F_ID is renumbered, PK follows,
-- OBJEKTID_FORLOBSMARKOR is reissued for 3.8 %/month, and SDS' surrogate
-- DW_EK_HelbredsForloeb was renumbered in Dec 2021):
--
--   helbredsforloeb_noegle  SHA2_256(CPR | first marker's code | date |
--                           responsible unit | forloebselement UUID)
--   markoer_noegle          SHA2_256(helbredsforloeb key | code | date)
--   loebenummer             ROW_NUMBER over markoer_noegle, ordered by a hash
--                           of the marker's stable content (duplicates)
--   antal_kilderaekker      fully identical source rows collapsed into one
--
-- The keys are named *_noegle like DRG's derived arc columns (*_uuid/*_noegle);
-- clean_code_* is formed in pre_stg (US-3). The source's ids are ALL kept as
-- "as last delivered" attributes. CPR enters the key hashed only; clear-text
-- CPR sits in its own column (never in DSO).
--
-- Every value column is NVARCHAR(500) — the archive does not type, and the
-- width equals ext, so an over-long value can never abort the merge with a
-- truncation error (HAIBA lesson [internal ticket]). The key columns are NOT NULL.
--
-- Merge: ext.udredningsret_mont_noegler (physical source table with the keys,
-- built and dropped again by DSI/csv_files/udredningsret_archive.py) -> arc on
-- (markoer_noegle, loebenummer); detect_deletes over the WHOLE table (the
-- source is one full snapshot from 2018-07). Consistency with
-- DSI/csv_files/udredningsret_datasets.py is enforced by
-- tests/unit/test_udredningsret_datasets_ddl.py.
--
-- Indexes: only what merge and guards use (PK on the merge keys, the
-- helbredsforloeb key for the recovery guard, dw_timestamp_update). An index
-- on DW_EK_HelbredsForloeb is added only once staging (US-3) actually reads it.
-- ============================================================================


-- ----------------------------------------------------------------------------
-- arc.udredningsret_mont  (UdredningsretMont_1083_<dato>.csv)
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[arc].[udredningsret_mont]') AND type = 'U'
)
BEGIN
    CREATE TABLE [arc].[udredningsret_mont] (
        [helbredsforloeb_noegle]       CHAR(64)        NOT NULL,
        [markoer_noegle]               CHAR(64)        NOT NULL,
        [loebenummer]                      INT             NOT NULL,
        [antal_kilderaekker]               INT             NOT NULL,
        [DW_EK_HelbredsForloeb]            NVARCHAR(500)   NULL,
        [OBJEKTID_FORLOBELEMENT]           NVARCHAR(500)   NULL,
        [OBJEKTID_FORLOBSMARKOR]           NVARCHAR(500)   NULL,
        [PK]                               NVARCHAR(500)   NULL,
        [F_ID]                             NVARCHAR(500)   NULL,
        [CPR]                              NVARCHAR(500)   NULL,
        [C_SGHREGION]                      NVARCHAR(500)   NULL,
        [ANSVARLIG_ENHED_FORLOB]           NVARCHAR(500)   NULL,
        [FORLOEBSLABEL]                    NVARCHAR(500)   NULL,
        [C_ADIAG]                          NVARCHAR(500)   NULL,
        [C_ADIAG_KONTAKT]                  NVARCHAR(500)   NULL,
        [ANSVARLIG_ENHED_KONTAKT]          NVARCHAR(500)   NULL,
        [FORLOEBSMARKOER]                  NVARCHAR(500)   NULL,
        [FORLOEBSMARKOER_DATO]             NVARCHAR(500)   NULL,
        [MRK_C]                            NVARCHAR(500)   NULL,
        [H]                                NVARCHAR(500)   NULL,
        [MRK_ST_H]                         NVARCHAR(500)   NULL,
        [MRK_H]                            NVARCHAR(500)   NULL,
        [MRK_ST_L]                         NVARCHAR(500)   NULL,
        [MRK_ST_N]                         NVARCHAR(500)   NULL,
        [MRK_L_N]                          NVARCHAR(500)   NULL,
        [C_BOREGION]                       NVARCHAR(500)   NULL,
        [C_BOREGION_F]                     NVARCHAR(500)   NULL,
        [PSYKIATRI]                        NVARCHAR(500)   NULL,
        [ALDER]                            NVARCHAR(500)   NULL,
        [OPGPER]                           NVARCHAR(500)   NULL,
        [OPGMAANED]                        NVARCHAR(500)   NULL,
        [Speciale_ansvarlig_enhed_kontakt] NVARCHAR(500)   NULL,
        -- The delivery in which this row LAST CHANGED — not the delivery it came
        -- from. leverance_dato is outside change_detect_columns, so it is only
        -- rewritten together with a real content change (otherwise 8.2 M rows
        -- would be rewritten weekly). "Which delivery does the archive reflect"
        -- is answered by meta.UdredningsretLeverance, never by this column.
        [leverance_dato]                   DATE            NOT NULL,
        [dw_is_deleted]                    BIT             NOT NULL CONSTRAINT [DF_udredningsret_mont_dw_is_deleted] DEFAULT (0),
        [dw_timestamp_insert]              DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]              DATETIME2(0)    NOT NULL,
        CONSTRAINT [PK_arc_udredningsret_mont] PRIMARY KEY CLUSTERED (
            [markoer_noegle], [loebenummer]
        )
    );

    -- The recovery guard and staging (US-3) go by the helbredsforloeb key.
    CREATE NONCLUSTERED INDEX [IX_arc_udredningsret_mont_Helbredsforloeb]
        ON [arc].[udredningsret_mont] ([helbredsforloeb_noegle], [dw_is_deleted]);

    CREATE NONCLUSTERED INDEX [IX_arc_udredningsret_mont_DW_Timestamp_Update]
        ON [arc].[udredningsret_mont] ([dw_timestamp_update] DESC);
END
GO
