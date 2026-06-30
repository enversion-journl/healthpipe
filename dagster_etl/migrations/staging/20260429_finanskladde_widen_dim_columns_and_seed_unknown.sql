-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Widen NVARCHAR columns in dim.Finanskladde and dim.Finanskladdelinje so the
-- "UNKNOWN" sentinel value (7 chars) fits, then populate the existing -1 row
-- with N'UNKNOWN' on every NVARCHAR attribute.
--
-- Safe to re-run: max_length guard on ALTER COLUMN; COALESCE-based UPDATE only
-- fills currently NULL values on the -1 row.
-- Apply to: staging database. A matching migration exists under migrations/dso/.
-- Task: #27805
-- ============================================================================

-- ---- dim.Finanskladde: widen DataAreaId / Maskinnummer ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
      AND name = 'DataAreaId'
      AND max_length < 40  -- nvarchar(20) = 40 bytes
)
BEGIN
    ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [DataAreaId] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladde.DataAreaId to NVARCHAR(20)';
END
GO

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
      AND name = 'Maskinnummer'
      AND max_length < 40
)
BEGIN
    ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [Maskinnummer] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladde.Maskinnummer to NVARCHAR(20)';
END
GO

-- ---- dim.Finanskladdelinje: widen Maskinnummer ----

IF EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
      AND name = 'Maskinnummer'
      AND max_length < 40
)
BEGIN
    ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [Maskinnummer] NVARCHAR(20) NULL;
    PRINT 'Widened dim.Finanskladdelinje.Maskinnummer to NVARCHAR(20)';
END
GO

-- ---- dim.Finanskladde: backfill UNKNOWN on the -1 sentinel row ----

IF EXISTS (
    SELECT 1 FROM [dim].[Finanskladde] WHERE [DW_EK_Finanskladde] = -1
)
BEGIN
    UPDATE [dim].[Finanskladde]
    SET [bk_ref_LedgerjournaltableId] = COALESCE([bk_ref_LedgerjournaltableId], N'UNKNOWN'),
        [Kladdenummer]                = COALESCE([Kladdenummer], N'UNKNOWN'),
        [Kladdetekst]                 = COALESCE([Kladdetekst], N'UNKNOWN'),
        [Kladdenavn]                  = COALESCE([Kladdenavn], N'UNKNOWN'),
        [DataAreaId]                  = COALESCE([DataAreaId], N'UNKNOWN'),
        [Maskinnummer]                = COALESCE([Maskinnummer], N'UNKNOWN'),
        [DW_Timestamp_Update]         = CAST(SYSDATETIME() AS DATETIME2)
    WHERE [DW_EK_Finanskladde] = -1
      AND (
            [bk_ref_LedgerjournaltableId] IS NULL
         OR [Kladdenummer] IS NULL
         OR [Kladdetekst] IS NULL
         OR [Kladdenavn] IS NULL
         OR [DataAreaId] IS NULL
         OR [Maskinnummer] IS NULL
      );
    PRINT 'Seeded UNKNOWN values on dim.Finanskladde -1 sentinel row';
END
GO

-- ---- dim.Finanskladdelinje: backfill UNKNOWN on the -1 sentinel row ----

IF EXISTS (
    SELECT 1 FROM [dim].[Finanskladdelinje] WHERE [DW_EK_Finanskladdelinje] = -1
)
BEGIN
    UPDATE [dim].[Finanskladdelinje]
    SET [bk_ref_LedgertransId]    = COALESCE([bk_ref_LedgertransId], N'UNKNOWN'),
        [Beskrivelse]              = COALESCE([Beskrivelse], N'UNKNOWN'),
        [Ekspeditionsnummer]       = COALESCE([Ekspeditionsnummer], N'UNKNOWN'),
        [Transaktionstypetekst]    = COALESCE([Transaktionstypetekst], N'UNKNOWN'),
        [Maskinnummer]             = COALESCE([Maskinnummer], N'UNKNOWN'),
        [DW_Timestamp_Update]      = CAST(SYSDATETIME() AS DATETIME2)
    WHERE [DW_EK_Finanskladdelinje] = -1
      AND (
            [bk_ref_LedgertransId] IS NULL
         OR [Beskrivelse] IS NULL
         OR [Ekspeditionsnummer] IS NULL
         OR [Transaktionstypetekst] IS NULL
         OR [Maskinnummer] IS NULL
      );
    PRINT 'Seeded UNKNOWN values on dim.Finanskladdelinje -1 sentinel row';
END
GO
