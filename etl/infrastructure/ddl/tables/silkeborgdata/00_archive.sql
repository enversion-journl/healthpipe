-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: SilkeborgData E26PERS  (løn- og personaledata)
--
-- Source      : E26PERS.TXT on the SilkeborgData delivery share, read through
--               the existing read-only SMB mount:
--                   /mnt/ftproot/LØN/arkiv/E26/<YYYYMMDD>/E26PERS.TXT
--               Semicolon-separated, no header, 41 positional fields, cp865
--               (DOS-Nordic — NOT cp1252; HAIBA on the same mount is cp1252).
--               Loaded by DSI/csv_files/silkeborgdata_e26_pers.py.
--
-- Grain       : one row per (Inst, Tjenestenummer, Udtraeksdato).
--               Udtraeksdato is field 7 INSIDE the file and carries the period
--               the row describes. It is part of the key because the archive
--               accumulates monthly snapshots: the same employment appears once
--               per delivery and every delivery must be kept. Without it a new
--               month would overwrite history.
--               Tjenestenummer identifies the employment, not the person — one
--               CprNr can hold several concurrent employments — so CprNr can
--               never be the key.
--               Inst is in the key because a tjenestenummer is only unique
--               WITHIN an institution. Two institutions reusing the same
--               5-digit number in the same period would otherwise collide on
--               the primary key and abort the load.
--
-- Deletes     : never. detect_deletes=False on the archive merge; ext only ever
--               holds the deliveries just read, so delete detection would
--               soft-delete every earlier month.
--
-- Checkpoint  : dw_leverance_dato carries the DFM checkpoint. The archive merge
--               runs
--                   SELECT CONVERT(CHAR(10), MAX([dw_leverance_dato]), 23)
--               after commit and writes the result to meta.DFMState for
--               arc.silkeborgdata_e26_pers; the extract reads it back as its
--               lower bound. The mechanism only works because Udtraeksdato is
--               in the key, so every new delivery brings new keys and therefore
--               real INSERTs. HAIBA deliberately does NOT infer its delivery
--               this way (its grain excludes the period, so a week without
--               changes writes nothing) — see haiba_archive.py. Do not narrow
--               change_detect_columns here: excluding dw_leverance_dato would
--               stall the checkpoint permanently and silently.
--
-- Naming      : HAIBA and DRG call the equivalent column leverance_dato without
--               the dw_ prefix. The dw_ prefix here is deliberate: the column is
--               not in the file and is not source data, so the prefix keeps the
--               1:1 boundary between the 41 file columns and everything else
--               visible in the column name itself.
--
-- Garantiloen : column 12 holds a STEP (SAS variable garantilon_trin), not an
--               amount, even though the SAS label is only 'Garantiløn'. The
--               amount is GarantiloenBeloeb (column 13).
--
-- Cols 26/27  : CONFIRMED correct, settled after the first full load over
--               1,627,400 rows. Profiling had suspected Ansaettelsesdato (26)
--               and Jubilaeumsdato (27) of being swapped because jubilee dates
--               lay before hire dates. They do — and that is correct:
--               jubil_dato is a jubilee SENIORITY date crediting earlier public
--               service. Position 27 equals position 26 in 59 % of rows (staff
--               with no prior service) and is earlier in a decaying tail. A
--               swap would put that tail on the positive side. Do not swap.
--
-- Sentinels   : the source uses far dates as missing markers, and they are
--               stored AS-IS because arc is 1:1 with the file:
--                   Jubilaeumsdato   9999-12-31  ~117,000 rows
--                   Ansaettelsesdato 2099-12-31 / 0001-01-01
--               They are syntactically valid dates, so parse_sd_date cannot
--               reject them. Anything computing an age, a duration or a date
--               filter downstream MUST exclude them explicitly — a DATEDIFF
--               against 9999-12-31 returns a number, not an error.
--
-- Contains CPR, name and address. Never log field values from this table.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'silkeborgdata_e26_pers'
)
BEGIN
    CREATE TABLE [arc].[silkeborgdata_e26_pers] (
        -- Source columns 1-41, in the file's positional order. This order is
        -- the contract with FILE_COLUMNS in silkeborgdata_e26_pers.py —
        -- never reorder.
        -- NVARCHAR widths match the SAS $w. informat exactly, so an
        -- over-long value fails visibly instead of being truncated.
        [Inst]                          NVARCHAR(2)     NOT NULL,   --  1 inst
        [Tjenestenummer]                NVARCHAR(5)     NOT NULL,   --  2 tjnr
        [CprNr]                         NVARCHAR(10)    NULL,       --  3 cprnr — NEVER numeric, leading zeros
        [Afdelingskode]                 NVARCHAR(4)     NULL,       --  4 afdkode
        [Tjenestekode]                  NVARCHAR(1)     NULL,       --  5 tjkode
        [Stillingskode]                 NVARCHAR(4)     NULL,       --  6 stilkode
        [Udtraeksdato]                  DATE            NOT NULL,   --  7 periode
        [Lederkode]                     NVARCHAR(2)     NULL,       --  8 lederkode
        [Ovk]                           NVARCHAR(5)     NULL,       --  9 ovk
        [Loenklasse]                    NVARCHAR(3)     NULL,       -- 10 lonkl
        [Trin]                          NVARCHAR(2)     NULL,       -- 11 trin
        [Garantiloen]                   NVARCHAR(2)     NULL,       -- 12 garantilon_trin — a STEP, not an amount
        [GarantiloenBeloeb]             DECIMAL(11,2)   NULL,       -- 13 garantilon_kr
        [Afloenning]                    NVARCHAR(1)     NULL,       -- 14 loko
        [Anciennitetsdato]              DATE            NULL,       -- 15 ancien_dato
        [Deltidsbeskaeftigelseskode]    NVARCHAR(1)     NULL,       -- 16 beskkode
        [Loendecimal]                   DECIMAL(7,4)    NULL,       -- 17 londecimal
        [Beskaeftigelsesdecimal]        DECIMAL(7,4)    NULL,       -- 18 beskdecimal
        [TimerIPerioden]                DECIMAL(9,2)    NULL,       -- 19 timer
        [Navn]                          NVARCHAR(40)    NULL,       -- 20 navn
        [Stilling]                      NVARCHAR(24)    NULL,       -- 21 stilling
        [Adresse]                       NVARCHAR(30)    NULL,       -- 22 adresse
        [Postnr]                        NVARCHAR(5)     NULL,       -- 23 postnr
        [Postdistrikt]                  NVARCHAR(24)    NULL,       -- 24 postdistrikt
        [Kommunekode]                   NVARCHAR(3)     NULL,       -- 25 kmkode
        [Ansaettelsesdato]              DATE            NULL,       -- 26 ansaet_dato — confirmed; sentinels 0001-01-01 / 2099-12-31
        [Jubilaeumsdato]                DATE            NULL,       -- 27 jubil_dato  — jubilee SENIORITY, <= 26; sentinel 9999-12-31
        [Statuskode]                    NVARCHAR(1)     NULL,       -- 28 statuskode
        [Statusdato]                    DATE            NULL,       -- 29 statusdato
        [Hensaettelseskode]             NVARCHAR(1)     NULL,       -- 30 hens_kode
        [Udbetalingskode]               NVARCHAR(1)     NULL,       -- 31 udb_kode
        [Aconto]                        DECIMAL(11,2)   NULL,       -- 32 aconto
        [Acontoperiode]                 DATE            NULL,       -- 33 ac_periode
        [Discokode]                     NVARCHAR(6)     NULL,       -- 34 discokode
        [Jobstatuskode]                 NVARCHAR(1)     NULL,       -- 35 jskode
        [Omraadekode]                   NVARCHAR(1)     NULL,       -- 36 omr
        [Pensionskode]                  NVARCHAR(1)     NULL,       -- 37 pens_kode      (no SAS label)
        [Pensionsfaggruppe]             NVARCHAR(3)     NULL,       -- 38 Pens_faggrup   (no SAS label)
        [Refusionskode]                 NVARCHAR(1)     NULL,       -- 39 ref_kode       (no SAS label)
        -- 40 ref_pct. The SAS informat is PERCENT8.2, which does not say
        -- whether the value is a fraction (0,5000 = 50 %) or already a
        -- percentage (50,0000 = 50 %). Stored raw either way; DECIMAL(9,4)
        -- holds both. Settle it before any downstream calculation:
        --   SELECT MIN([Refusionsprocent]), MAX([Refusionsprocent])
        --   FROM [arc].[silkeborgdata_e26_pers]
        --   WHERE [Refusionsprocent] > 0;
        -- A maximum near 1 means fractions; near 100 means percentages.
        [Refusionsprocent]              DECIMAL(9,4)    NULL,
        [Refusionsbeloeb]               DECIMAL(13,2)   NULL,       -- 41 ref_kr         (no SAS label)

        -- Provenance. Not in the file — stamped per row from the delivery
        -- folder the row came from. dw_leverance_dato is the DFM checkpoint
        -- source (see header); without it the checkpoint cannot be derived and
        -- every run would reload the whole history.
        [dw_leverance_dato]             DATE            NOT NULL,
        [dw_leverance_mappe]            NVARCHAR(50)    NOT NULL,

        -- DWH audit columns. Set by execute_archive_merge; no DEFAULT, as in
        -- arc.sor_register.
        [dw_timestamp_insert]           DATETIME2(0)    NOT NULL,
        [dw_timestamp_update]           DATETIME2(0)    NOT NULL,

        CONSTRAINT [PK_arc_silkeborgdata_e26_pers]
            PRIMARY KEY CLUSTERED ([Inst], [Tjenestenummer], [Udtraeksdato])
    );
END
GO

-- Backs checkpoint_query's MAX(dw_leverance_dato). That query runs on every
-- scheduled run, so this index is not optional — without it the checkpoint
-- derivation scans the whole archive daily.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_arc_silkeborgdata_e26_pers_leverance_dato'
      AND object_id = OBJECT_ID(N'[arc].[silkeborgdata_e26_pers]')
)
    CREATE NONCLUSTERED INDEX [IX_arc_silkeborgdata_e26_pers_leverance_dato]
        ON [arc].[silkeborgdata_e26_pers] ([dw_leverance_dato]);
GO

-- Drives period reconciliation, the asset checks and "latest delivery" lookups.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_arc_silkeborgdata_e26_pers_udtraeksdato'
      AND object_id = OBJECT_ID(N'[arc].[silkeborgdata_e26_pers]')
)
    CREATE NONCLUSTERED INDEX [IX_arc_silkeborgdata_e26_pers_udtraeksdato]
        ON [arc].[silkeborgdata_e26_pers] ([Udtraeksdato]);
GO

-- Person lookups across employments: one CprNr can hold several
-- tjenestenumre, and the clustered key starts with Inst.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_arc_silkeborgdata_e26_pers_cprnr'
      AND object_id = OBJECT_ID(N'[arc].[silkeborgdata_e26_pers]')
)
    CREATE NONCLUSTERED INDEX [IX_arc_silkeborgdata_e26_pers_cprnr]
        ON [arc].[silkeborgdata_e26_pers] ([CprNr])
        INCLUDE ([Inst], [Tjenestenummer], [Udtraeksdato]);
GO
