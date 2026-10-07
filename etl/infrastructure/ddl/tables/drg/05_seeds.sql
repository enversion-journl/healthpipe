-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DRG: Seed rows
--
-- UNKNOWN sentinels (-1) for the map and dimension tables, so fact FK columns
-- can be NOT NULL DEFAULT -1 and referential-integrity checks can ignore the
-- sentinel member. Facts carry no sentinel row.
-- ============================================================================

-- map.DRGKontakt sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontakt]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGKontakt] WHERE [dw_ek_DRGKontakt] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGKontakt] ON;
    INSERT INTO [map].[DRGKontakt] (
        [dw_ek_DRGKontakt], [clean_code_DRGKontakt], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[DRGKontakt] OFF;
END
GO

-- dim.DRGKontaktEgenskaber sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGKontaktEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[DRGKontaktEgenskaber] WHERE [dw_ek_DRGKontakt] = -1)
BEGIN
    INSERT INTO [dim].[DRGKontaktEgenskaber] (
        [dw_ek_DRGKontakt], [clean_code_DRGKontakt], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- map.DRGKontaktKlassifikation sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGKontaktKlassifikation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGKontaktKlassifikation] WHERE [dw_ek_DRGKontaktKlassifikation] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGKontaktKlassifikation] ON;
    INSERT INTO [map].[DRGKontaktKlassifikation] (
        [dw_ek_DRGKontaktKlassifikation], [clean_code_DRGKontaktKlassifikation],
        [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[DRGKontaktKlassifikation] OFF;
END
GO

-- dim.DRGKontaktKlassifikation sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGKontaktKlassifikation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[DRGKontaktKlassifikation] WHERE [dw_ek_DRGKontaktKlassifikation] = -1)
BEGIN
    INSERT INTO [dim].[DRGKontaktKlassifikation] (
        [dw_ek_DRGKontaktKlassifikation], [clean_code_DRGKontaktKlassifikation],
        [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- map.DRGDiagnose sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnose]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGDiagnose] WHERE [dw_ek_DRGDiagnose] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGDiagnose] ON;
    INSERT INTO [map].[DRGDiagnose] ([dw_ek_DRGDiagnose], [clean_code_DRGDiagnose], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
    SET IDENTITY_INSERT [map].[DRGDiagnose] OFF;
END
GO

-- dim.DRGDiagnose sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGDiagnose]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[DRGDiagnose] WHERE [dw_ek_DRGDiagnose] = -1)
BEGIN
    INSERT INTO [dim].[DRGDiagnose] ([dw_ek_DRGDiagnose], [clean_code_DRGDiagnose], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
END
GO

-- map.DRGProcedure sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedure]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGProcedure] WHERE [dw_ek_DRGProcedure] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGProcedure] ON;
    INSERT INTO [map].[DRGProcedure] ([dw_ek_DRGProcedure], [clean_code_DRGProcedure], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
    SET IDENTITY_INSERT [map].[DRGProcedure] OFF;
END
GO

-- dim.DRGProcedure sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[DRGProcedure]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[DRGProcedure] WHERE [dw_ek_DRGProcedure] = -1)
BEGIN
    INSERT INTO [dim].[DRGProcedure] ([dw_ek_DRGProcedure], [clean_code_DRGProcedure], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
END
GO

-- map.DRGDiagnoseGruppe sentinel (-1 = the empty group: a contact with no
-- diagnoses; it has no rows in bridge.DRGDiagnoseGruppe)
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGDiagnoseGruppe]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGDiagnoseGruppe] WHERE [dw_ek_DRGDiagnoseGruppe] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGDiagnoseGruppe] ON;
    INSERT INTO [map].[DRGDiagnoseGruppe] ([dw_ek_DRGDiagnoseGruppe], [clean_code_DRGDiagnoseGruppe], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
    SET IDENTITY_INSERT [map].[DRGDiagnoseGruppe] OFF;
END
GO

-- map.DRGProcedureGruppe sentinel (-1 = the empty group)
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[DRGProcedureGruppe]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[DRGProcedureGruppe] WHERE [dw_ek_DRGProcedureGruppe] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[DRGProcedureGruppe] ON;
    INSERT INTO [map].[DRGProcedureGruppe] ([dw_ek_DRGProcedureGruppe], [clean_code_DRGProcedureGruppe], [dw_timestamp_insert], [dw_timestamp_update])
    VALUES (-1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2));
    SET IDENTITY_INSERT [map].[DRGProcedureGruppe] OFF;
END
GO
