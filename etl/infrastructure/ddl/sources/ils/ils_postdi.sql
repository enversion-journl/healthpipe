-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_postdi  (1:1 from ILS dbo.postdi)
-- Postal code -> town. Lookup table for (land, postdi), which rekv/lgr/lev carry
-- without a town name. Note that (land, postdi) is NOT unique in the source
-- (e.g. (DK, 1172)), so consumers must deduplicate — see stg_dim_Rekvirent.
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_postdi'
)
BEGIN
    CREATE TABLE [arc].[ils_postdi] (
        -- Source columns (1:1 from dbo.postdi)
        [prrowid]      varchar(36)    NOT NULL,
        [land]         varchar(6)     NULL,
        [postdi]       varchar(10)    NULL,
        [navn]         varchar(58)    NULL,
        [sognavn]      varchar(60)    NULL,
        [sw_synk]      bit            NULL,
        [bruger]       varchar(6)     NULL,
        [seqnr]        int            NULL,
        [sysdato]      datetime       NULL,
        [Pro2SrcPDB]   varchar(12)    NULL,
        [pro2created]  datetime       NULL,
        [pro2modified] datetime       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_postdi_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL CONSTRAINT [DF_ils_postdi_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_postdi] PRIMARY KEY CLUSTERED ([prrowid])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_postdi_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[ils_postdi]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_postdi_dw_timestamp_update]
        ON [arc].[ils_postdi] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_postdi_land_postdi'
      AND object_id = OBJECT_ID(N'[arc].[ils_postdi]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_postdi_land_postdi]
        ON [arc].[ils_postdi] ([land], [postdi]) INCLUDE ([navn]);
END
GO
