-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Seed Tables and Reference Data
--
-- ean -> SOR mapping: ref.EAN_SOR_Mapping removed; use conceptmap.ean_to_sor
-- (built by cm_ean_to_sor, synced to DSO). Organisation on invoice facts is exposed
-- via vw_fct.Faktura / vw_fct.Fakturalinje as dw_ek_Organisation_regningsmodtager.
-- ============================================================================

-- Seed: UNKNOWN sentinel for map.LagerbeholdningEgenskaber (warehouse stock)
-- The dim.LagerbeholdningEgenskaber sentinel is handled in 02_dims.sql.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[LagerbeholdningEgenskaber] WHERE [dw_ek_LagerbeholdningEgenskaber] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[LagerbeholdningEgenskaber] ON;
    INSERT INTO [map].[LagerbeholdningEgenskaber] (
        [dw_ek_LagerbeholdningEgenskaber],
        [clean_code_LagerbeholdningEgenskaber],
        [dw_timestamp_insert],
        [dw_timestamp_update]
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
   AND NOT EXISTS (SELECT 1 FROM [map].[AfdelingLagerbeholdningEgenskaber] WHERE [dw_ek_AfdelingLagerbeholdningEgenskaber] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[AfdelingLagerbeholdningEgenskaber] ON;
    INSERT INTO [map].[AfdelingLagerbeholdningEgenskaber] (
        [dw_ek_AfdelingLagerbeholdningEgenskaber],
        [clean_code_AfdelingLagerbeholdningEgenskaber],
        [dw_timestamp_insert],
        [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN', GETDATE(), GETDATE()
    );
    SET IDENTITY_INSERT [map].[AfdelingLagerbeholdningEgenskaber] OFF;

    PRINT 'Seeded UNKNOWN row for map.AfdelingLagerbeholdningEgenskaber (DW_EK = -1)';
END
GO

-- map.ILSVare sentinel
IF NOT EXISTS (SELECT 1 FROM [map].[ILSVare] WHERE [dw_ek_ILSVare] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[ILSVare] ON;
    INSERT INTO [map].[ILSVare] (
        dw_ek_ILSVare, clean_code_ILSVare, dw_timestamp_insert, dw_timestamp_update
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[ILSVare] OFF;
END
GO

-- map.ILSBruger sentinel (the dim.ILSBruger sentinel is in 02_dims.sql)
-- Dynamic SQL avoids compile-time column resolution.
IF NOT EXISTS (SELECT 1 FROM [map].[ILSBruger] WHERE [dw_ek_ILSBruger] = -1)
BEGIN
    EXEC('
        SET IDENTITY_INSERT [map].[ILSBruger] ON;
        INSERT INTO [map].[ILSBruger] (
            [dw_ek_ILSBruger], [clean_code_ILSBruger], [dw_timestamp_insert], [dw_timestamp_update]
        ) VALUES (
            -1, N''UNKNOWN'', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
        );
        SET IDENTITY_INSERT [map].[ILSBruger] OFF;
    ');
END
GO

-- dim.ILSVare sentinel
IF NOT EXISTS (SELECT 1 FROM [dim].[ILSVare] WHERE [dw_ek_ILSVare] = -1)
BEGIN
    INSERT INTO [dim].[ILSVare] (
        dw_ek_ILSVare, clean_code_ILSVare,
        dw_timestamp_insert, dw_timestamp_update
    ) VALUES (
        -1, 'UNKNOWN',
        CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO
