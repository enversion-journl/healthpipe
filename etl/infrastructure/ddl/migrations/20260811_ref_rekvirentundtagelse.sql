-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: create ref.RekvirentUndtagelse (AB#29115 PR-B).
-- Exclusion list for reelt forbrug. PK: clean_code_Rekvirent (pad-only).

IF NOT EXISTS (
    SELECT * FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND type = 'U'
)
BEGIN
    CREATE TABLE [ref].[RekvirentUndtagelse] (
        [clean_code_Rekvirent] VARCHAR(100)  NOT NULL,
        [sgh]                  VARCHAR(8)    NOT NULL,
        [rnr]                  VARCHAR(16)   NOT NULL,
        [navn]                 VARCHAR(80)   NULL,
        [navn_manuelt]         VARCHAR(80)   NULL,
        [dw_ek_Rekvirent]      INT           NULL,
        [dw_timestamp_insert]  DATETIME2(0)  NOT NULL
            CONSTRAINT [DF_Ref_RekvirentUndtagelse_DW_Timestamp_Insert]
            DEFAULT CAST(SYSDATETIME() AS datetime2(0)),
        [dw_timestamp_update]  DATETIME2(0)  NOT NULL,
        CONSTRAINT [PK_Ref_RekvirentUndtagelse]
            PRIMARY KEY CLUSTERED ([clean_code_Rekvirent] ASC),
        CONSTRAINT [UQ_Ref_RekvirentUndtagelse_SghRnr]
            UNIQUE ([sgh], [rnr])
    ) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]')
      AND name = 'IX_Ref_RekvirentUndtagelse_DW_EK'
)
   AND EXISTS (
    SELECT 1 FROM sys.objects
    WHERE object_id = OBJECT_ID(N'[ref].[RekvirentUndtagelse]') AND type = 'U'
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Ref_RekvirentUndtagelse_DW_EK]
        ON [ref].[RekvirentUndtagelse] ([dw_ek_Rekvirent] ASC)
        WHERE [dw_ek_Rekvirent] IS NOT NULL;
END
GO
