-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename DW_EK_Date_* columns to DW_EK_Dato_* (Danish naming convention)
-- with suffix corrections (e.g. Betalings -> Betaling, Godkendelses -> Godkendelse).
--
-- Applies to: fct.Fakturalinje, fct.Postering, fct.Faktura
-- Safe to re-run: IF EXISTS guards on old column names.
-- Task: #27717
-- ============================================================================

-- ---- fct.Fakturalinje (4 columns) ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Faktura')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Date_Faktura]', 'DW_EK_Dato_Faktura', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Faktura -> DW_EK_Dato_Faktura on fct.Fakturalinje';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Ordre')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Date_Ordre]', 'DW_EK_Dato_Ordre', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Ordre -> DW_EK_Dato_Ordre on fct.Fakturalinje';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Betalings')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Date_Betalings]', 'DW_EK_Dato_Betaling', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Betalings -> DW_EK_Dato_Betaling on fct.Fakturalinje';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND name = 'DW_EK_Date_Modtagelses')
BEGIN
    EXEC sp_rename '[fct].[Fakturalinje].[DW_EK_Date_Modtagelses]', 'DW_EK_Dato_Modtagelse', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Modtagelses -> DW_EK_Dato_Modtagelse on fct.Fakturalinje';
END
GO

-- ---- fct.Postering (9 columns) ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Godkendelses')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_Godkendelses]', 'DW_EK_Dato_Godkendelse', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Godkendelses -> DW_EK_Dato_Godkendelse on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Lukke')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_Lukke]', 'DW_EK_Dato_Lukket', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Lukke -> DW_EK_Dato_Lukket on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_TransOprettet')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_TransOprettet]', 'DW_EK_Dato_TransOprettet', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_TransOprettet -> DW_EK_Dato_TransOprettet on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Forfalds')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_Forfalds]', 'DW_EK_Dato_Forfald', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Forfalds -> DW_EK_Dato_Forfald on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_JournalFaktura')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_JournalFaktura]', 'DW_EK_Dato_JournalFaktura', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_JournalFaktura -> DW_EK_Dato_JournalFaktura on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_SidstUdlignet')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_SidstUdlignet]', 'DW_EK_Dato_SidstUdlignet', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_SidstUdlignet -> DW_EK_Dato_SidstUdlignet on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Transaktions')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_Transaktions]', 'DW_EK_Dato_Transaktion', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Transaktions -> DW_EK_Dato_Transaktion on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_Bogfoerings')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_Bogfoerings]', 'DW_EK_Dato_Bogfoering', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Bogfoerings -> DW_EK_Dato_Bogfoering on fct.Postering';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Date_PostOprettet')
BEGIN
    EXEC sp_rename '[fct].[Postering].[DW_EK_Date_PostOprettet]', 'DW_EK_Dato_PostOprettet', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_PostOprettet -> DW_EK_Dato_PostOprettet on fct.Postering';
END
GO

-- ---- fct.Faktura (3 columns) ----

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Date_Faktura')
BEGIN
    EXEC sp_rename '[fct].[Faktura].[DW_EK_Date_Faktura]', 'DW_EK_Dato_Faktura', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Faktura -> DW_EK_Dato_Faktura on fct.Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Date_Ordre')
BEGIN
    EXEC sp_rename '[fct].[Faktura].[DW_EK_Date_Ordre]', 'DW_EK_Dato_Ordre', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Ordre -> DW_EK_Dato_Ordre on fct.Faktura';
END
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]') AND name = 'DW_EK_Date_Betalings')
BEGIN
    EXEC sp_rename '[fct].[Faktura].[DW_EK_Date_Betalings]', 'DW_EK_Dato_Betaling', 'COLUMN';
    PRINT 'Renamed DW_EK_Date_Betalings -> DW_EK_Dato_Betaling on fct.Faktura';
END
GO
