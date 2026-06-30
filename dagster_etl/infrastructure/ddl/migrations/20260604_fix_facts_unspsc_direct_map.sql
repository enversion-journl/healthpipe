-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: re-backfill fct DW_EK_UNSPSC via map.UNSPSC (not dim.IlsVare.DW_EK_UNSPSC)
-- For databases that already ran the original 28041 backfill before outrigger revert.
-- Skips if dim.IlsVare text columns have been dropped (replaced by DW_EK_UNSPSC FK).

IF COL_LENGTH('fct.Vareudlevering', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
BEGIN
    UPDATE f
    SET f.[DW_EK_UNSPSC] = COALESCE(um.[DW_EK_UNSPSC], -1)
    FROM [fct].[Vareudlevering] f
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
    PRINT CONCAT('Re-backfilled fct.Vareudlevering.DW_EK_UNSPSC: ', @@ROWCOUNT, ' rows');
END
GO

IF COL_LENGTH('fct.Lagerbeholdning', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
BEGIN
    UPDATE f
    SET f.[DW_EK_UNSPSC] = COALESCE(um.[DW_EK_UNSPSC], -1)
    FROM [fct].[Lagerbeholdning] f
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
    PRINT CONCAT('Re-backfilled fct.Lagerbeholdning.DW_EK_UNSPSC: ', @@ROWCOUNT, ' rows');
END
GO

IF COL_LENGTH('fct.AfdelingLagerbeholdning', 'DW_EK_UNSPSC') IS NOT NULL
   AND COL_LENGTH('dim.IlsVare', 'UNSPSC') IS NOT NULL
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
    PRINT CONCAT('Re-backfilled fct.AfdelingLagerbeholdning.DW_EK_UNSPSC: ', @@ROWCOUNT, ' rows');
END
GO
