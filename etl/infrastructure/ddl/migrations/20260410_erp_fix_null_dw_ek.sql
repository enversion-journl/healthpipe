-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: One-Time Data Migrations
-- These fix historical data issues and are safe to re-run (idempotent).
-- ============================================================================

-- Migration: Fix NULL DW_EK values in fct.Fakturalinje (set to -1 UNKNOWN)
IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
        AND
    type = 'U'
) BEGIN
UPDATE [fct].[Fakturalinje]
SET
    [DW_EK_Kreditor] = -1
WHERE
    [DW_EK_Kreditor] IS NULL;

UPDATE [fct].[Fakturalinje]
SET
    [DW_EK_Vare] = -1
WHERE
    [DW_EK_Vare] IS NULL;

UPDATE [fct].[Fakturalinje]
SET
    [DW_EK_Faktura] = -1
WHERE
    [DW_EK_Faktura] IS NULL;

UPDATE [fct].[Fakturalinje]
SET
    [DW_EK_Kontoplan] = -1
WHERE
    [DW_EK_Kontoplan] IS NULL;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organization')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Organization] = -1 WHERE [DW_EK_Organization] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Organisation')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Organisation] = -1 WHERE [DW_EK_Organisation] IS NULL');

IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_EAN_Regningsmodtager')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_EAN_Regningsmodtager] = -1 WHERE [DW_EK_EAN_Regningsmodtager] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Faktura')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Faktura] = -1 WHERE [DW_EK_Date_Faktura] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Faktura')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Faktura] = -1 WHERE [DW_EK_Dato_Faktura] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Ordre')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Ordre] = -1 WHERE [DW_EK_Date_Ordre] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Ordre')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Ordre] = -1 WHERE [DW_EK_Dato_Ordre] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Betalings')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Betalings] = -1 WHERE [DW_EK_Date_Betalings] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Betaling')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Betaling] = -1 WHERE [DW_EK_Dato_Betaling] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Modtagelses')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Modtagelses] = -1 WHERE [DW_EK_Date_Modtagelses] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Dato_Modtagelse')
    EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Dato_Modtagelse] = -1 WHERE [DW_EK_Dato_Modtagelse] IS NULL');

PRINT 'Fixed NULL DW_EK values in fct.Fakturalinje (set to -1)';

END
GO

-- Migration: Fix NULL DW_EK values in fct.Postering (set to -1 UNKNOWN)
IF EXISTS (
    SELECT *
    FROM sys.objects
    WHERE
        object_id = OBJECT_ID(N'[fct].[Postering]')
        AND
    type = 'U'
) BEGIN
UPDATE [fct].[Postering]
SET
    [DW_EK_Kreditor] = -1
WHERE
    [DW_EK_Kreditor] IS NULL;

UPDATE [fct].[Postering]
SET
    [DW_EK_Kontoplan] = -1
WHERE
    [DW_EK_Kontoplan] IS NULL;

UPDATE [fct].[Postering]
SET
    [DW_EK_Faktura] = -1
WHERE
    [DW_EK_Faktura] IS NULL;

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Godkendelses')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Godkendelses] = -1 WHERE [DW_EK_Date_Godkendelses] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Godkendelse')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Godkendelse] = -1 WHERE [DW_EK_Dato_Godkendelse] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Lukke')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Lukke] = -1 WHERE [DW_EK_Date_Lukke] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Lukket')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Lukket] = -1 WHERE [DW_EK_Dato_Lukket] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_TransOprettet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_TransOprettet] = -1 WHERE [DW_EK_Date_TransOprettet] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_TransOprettet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_TransOprettet] = -1 WHERE [DW_EK_Dato_TransOprettet] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Forfalds')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Forfalds] = -1 WHERE [DW_EK_Date_Forfalds] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Forfald')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Forfald] = -1 WHERE [DW_EK_Dato_Forfald] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_JournalFaktura')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_JournalFaktura] = -1 WHERE [DW_EK_Date_JournalFaktura] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_JournalFaktura')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_JournalFaktura] = -1 WHERE [DW_EK_Dato_JournalFaktura] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_SidstUdlignet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_SidstUdlignet] = -1 WHERE [DW_EK_Date_SidstUdlignet] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_SidstUdlignet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_SidstUdlignet] = -1 WHERE [DW_EK_Dato_SidstUdlignet] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Transaktions')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Transaktions] = -1 WHERE [DW_EK_Date_Transaktions] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Transaktion')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Transaktion] = -1 WHERE [DW_EK_Dato_Transaktion] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Bogfoerings')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_Bogfoerings] = -1 WHERE [DW_EK_Date_Bogfoerings] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_Bogfoering')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_Bogfoering] = -1 WHERE [DW_EK_Dato_Bogfoering] IS NULL');

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_PostOprettet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Date_PostOprettet] = -1 WHERE [DW_EK_Date_PostOprettet] IS NULL');
ELSE IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Dato_PostOprettet')
    EXEC(N'UPDATE [fct].[Postering] SET [DW_EK_Dato_PostOprettet] = -1 WHERE [DW_EK_Dato_PostOprettet] IS NULL');

PRINT 'Fixed NULL DW_EK values in fct.Postering (set to -1)';

END
GO
