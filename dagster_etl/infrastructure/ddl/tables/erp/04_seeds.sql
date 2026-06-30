-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Seed Tables and Reference Data
--
-- EAN -> SOR mapping: ref.EAN_SOR_Mapping removed; use conceptmap.ean_to_sor
-- (built by cm_ean_to_sor, synced to DSO). Organisation on invoice facts is exposed
-- via vw_fct.Faktura / vw_fct.Fakturalinje as DW_EK_Organisation_Regningsmodtager.
-- ============================================================================

-- Seed: UNKNOWN sentinel for map.LagerbeholdningEgenskaber (warehouse stock)
-- The dim.LagerbeholdningEgenskaber sentinel is handled in 02_dims.sql.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[LagerbeholdningEgenskaber] WHERE [DW_EK_LagerbeholdningEgenskaber] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[LagerbeholdningEgenskaber] ON;
    INSERT INTO [map].[LagerbeholdningEgenskaber] (
        [DW_EK_LagerbeholdningEgenskaber],
        [CleanCode_LagerbeholdningEgenskaber],
        [DW_Timestamp_Insert],
        [DW_Timestamp_Update]
    ) VALUES (
        -1, N'UNKNOWN', GETDATE(), GETDATE()
    );
    SET IDENTITY_INSERT [map].[LagerbeholdningEgenskaber] OFF;

    PRINT 'Seeded UNKNOWN row for map.LagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- Seed: UNKNOWN sentinel for map.AfdelingLagerbeholdningEgenskaber (department stock)
-- The dim.AfdelingLagerbeholdningEgenskaber sentinel is handled in 02_dims.sql.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[AfdelingLagerbeholdningEgenskaber] WHERE [DW_EK_AfdelingLagerbeholdningEgenskaber] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[AfdelingLagerbeholdningEgenskaber] ON;
    INSERT INTO [map].[AfdelingLagerbeholdningEgenskaber] (
        [DW_EK_AfdelingLagerbeholdningEgenskaber],
        [CleanCode_AfdelingLagerbeholdningEgenskaber],
        [DW_Timestamp_Insert],
        [DW_Timestamp_Update]
    ) VALUES (
        -1, N'UNKNOWN', GETDATE(), GETDATE()
    );
    SET IDENTITY_INSERT [map].[AfdelingLagerbeholdningEgenskaber] OFF;

    PRINT 'Seeded UNKNOWN row for map.AfdelingLagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- map.IlsVare sentinel
IF NOT EXISTS (SELECT 1 FROM [map].[IlsVare] WHERE [DW_EK_IlsVare] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[IlsVare] ON;
    INSERT INTO [map].[IlsVare] (
        DW_EK_IlsVare, CleanCode_IlsVare, DW_Timestamp_Insert, DW_Timestamp_Update
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[IlsVare] OFF;
END
GO

-- dim.IlsVare sentinel
IF NOT EXISTS (SELECT 1 FROM [dim].[IlsVare] WHERE [DW_EK_IlsVare] = -1)
BEGIN
    INSERT INTO [dim].[IlsVare] (
        DW_EK_IlsVare, CleanCode_IlsVare,
        DW_Timestamp_Insert, DW_Timestamp_Update
    ) VALUES (
        -1, 'UNKNOWN',
        CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO
