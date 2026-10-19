-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: create ref.ILSAendringslog.
-- ILS dbo.changelog as-is. PK: prrowid.

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[ILSAendringslog] (
        [prrowid]              VARCHAR(36)   NOT NULL,
        [tabelnavn]            VARCHAR(40)   NULL,
        [noegle]               VARCHAR(30)   NULL,
        [feltnavn]             VARCHAR(30)   NULL,
        [feltlabel]            VARCHAR(60)   NULL,
        [fra]                  VARCHAR(40)   NULL,
        [til]                  VARCHAR(30)   NULL,
        [opret]                BIT           NULL,
        [ajourfoert_af]        VARCHAR(6)    NULL,
        [dato]                 DATE          NULL,
        [tidspunkt]            CHAR(8)       NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_ILSAendringslog_dw_timestamp_insert]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_ILSAendringslog_dw_timestamp_update]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        CONSTRAINT [PK_Ref_ILSAendringslog]
            PRIMARY KEY CLUSTERED ([prrowid] ASC)
    ) ON [PRIMARY];
END
GO

-- Watermark for the DSO sync (mode='watermark' on dw_timestamp_update).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]')
      AND name = 'IX_Ref_ILSAendringslog_DW_Timestamp_Update'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_ILSAendringslog_DW_Timestamp_Update]
        ON [ref].[ILSAendringslog] ([dw_timestamp_update] DESC);
END
GO

-- Point-in-time lookups filter on tabelnavn + noegle (e.g. a vare's basisenhed).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]')
      AND name = 'IX_Ref_ILSAendringslog_tabelnavn_noegle'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[ILSAendringslog]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_ILSAendringslog_tabelnavn_noegle]
        ON [ref].[ILSAendringslog] ([tabelnavn] ASC, [noegle] ASC, [dato] ASC, [tidspunkt] ASC);
END
GO
