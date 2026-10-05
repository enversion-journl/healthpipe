-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: fct.AfdelingLagerbeholdning DW_EK_UNSPSC FK via map.UNSPSC (task 28041)

IF OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
   AND COL_LENGTH('fct.AfdelingLagerbeholdning', 'DW_EK_UNSPSC') IS NULL
BEGIN
    ALTER TABLE [fct].[AfdelingLagerbeholdning]
        ADD [DW_EK_UNSPSC] INT NOT NULL
            CONSTRAINT [DF_Fct_AfdelingLagerbeholdning_DW_EK_UNSPSC] DEFAULT (-1);
    PRINT 'Added DW_EK_UNSPSC to fct.AfdelingLagerbeholdning';
END
GO

IF OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
   AND COL_LENGTH('fct.AfdelingLagerbeholdning', 'DW_EK_UNSPSC') IS NOT NULL
   AND OBJECT_ID(N'[dim].[IlsVare]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[UNSPSC]', 'U') IS NOT NULL
BEGIN
    UPDATE f
    SET f.[DW_EK_UNSPSC] = COALESCE(um.[DW_EK_UNSPSC], -1)
    FROM [fct].[AfdelingLagerbeholdning] f
    LEFT JOIN [dim].[IlsVare] di
        ON di.[DW_EK_IlsVare] = f.[DW_EK_IlsVare]
    LEFT JOIN [map].[UNSPSC] um
        ON um.[CleanCode_UNSPSC] = COALESCE(
            NULLIF(LTRIM(RTRIM(di.[UNSPSC])), ''),
            CASE
                WHEN di.[UNSPSC_Intern] LIKE '%.0'
                THEN LEFT(
                    LTRIM(RTRIM(di.[UNSPSC_Intern])),
                    LEN(LTRIM(RTRIM(di.[UNSPSC_Intern]))) - 2
                )
                ELSE NULLIF(LTRIM(RTRIM(di.[UNSPSC_Intern])), '')
            END
        )
    WHERE f.[DW_EK_IlsVare] <> -1;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_Fct_AfdelingLagerbeholdning_UNSPSC'
      AND object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_AfdelingLagerbeholdning_UNSPSC]
        ON [fct].[AfdelingLagerbeholdning] ([DW_EK_UNSPSC] ASC);
    PRINT 'Created IX_Fct_AfdelingLagerbeholdning_UNSPSC';
END
GO
