-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Add DW_EK_Finanskladdelinje to fct.Postering
-- Links each financial posting to its journal line via shared ref_LedgertransId.

IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
   AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'DW_EK_Finanskladdelinje')
BEGIN
    ALTER TABLE [fct].[Postering] ADD [DW_EK_Finanskladdelinje] INT NOT NULL DEFAULT -1;
    PRINT 'Added DW_EK_Finanskladdelinje column to fct.Postering';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'IX_Fct_Postering_Finanskladdelinje')
   AND EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND type = 'U')
BEGIN
    CREATE NONCLUSTERED INDEX [IX_Fct_Postering_Finanskladdelinje]
    ON [fct].[Postering] ([DW_EK_Finanskladdelinje] ASC) ON [PRIMARY];
    PRINT 'Created IX_Fct_Postering_Finanskladdelinje index on fct.Postering';
END
GO
