-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Add DW_EK_Dato_Oprettet and DW_EK_Dato_Aendret to dim.Finanskladde (DSO)
-- These date keys were previously on fct.Finanskladde (now removed) and belong on the dimension.

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = 'DW_EK_Dato_Oprettet')
BEGIN
    ALTER TABLE [dim].[Finanskladde] ADD [DW_EK_Dato_Oprettet] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Dato_Oprettet column to dim.Finanskladde';
END
GO

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND name = 'DW_EK_Dato_Aendret')
BEGIN
    ALTER TABLE [dim].[Finanskladde] ADD [DW_EK_Dato_Aendret] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Dato_Aendret column to dim.Finanskladde';
END
GO
