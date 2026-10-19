-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Udredningsret: Seed rows
--
-- UNKNOWN sentinels (-1) for the map and dimension tables, so fact FK columns
-- can be NOT NULL DEFAULT -1 and referential-integrity checks can ignore the
-- sentinel member. Facts carry no sentinel row.
--
-- The classification sentinel also carries the 'Ukendt' texts for overholdt
-- and aarsag, and the marker sentinel the 'Ukendt' tekst, so a report that
-- reads the text column never shows an empty cell for an unresolved key.
-- ============================================================================

-- map.Helbredsforloeb sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Helbredsforloeb]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[Helbredsforloeb] WHERE [dw_ek_Helbredsforloeb] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[Helbredsforloeb] ON;
    INSERT INTO [map].[Helbredsforloeb] (
        [dw_ek_Helbredsforloeb], [clean_code_Helbredsforloeb], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[Helbredsforloeb] OFF;
END
GO

-- map.Udredningsforloeb sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Udredningsforloeb]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[Udredningsforloeb] WHERE [dw_ek_Udredningsforloeb] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[Udredningsforloeb] ON;
    INSERT INTO [map].[Udredningsforloeb] (
        [dw_ek_Udredningsforloeb], [clean_code_Udredningsforloeb], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[Udredningsforloeb] OFF;
END
GO

-- dim.UdredningsretEgenskaber sentinel (shares the key with map.Udredningsforloeb)
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UdredningsretEgenskaber]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[UdredningsretEgenskaber] WHERE [dw_ek_Udredningsforloeb] = -1)
BEGIN
    INSERT INTO [dim].[UdredningsretEgenskaber] (
        [dw_ek_Udredningsforloeb], [clean_code_Udredningsforloeb], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- map.UdredningsretKlassifikation sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretKlassifikation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[UdredningsretKlassifikation] WHERE [dw_ek_UdredningsretKlassifikation] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[UdredningsretKlassifikation] ON;
    INSERT INTO [map].[UdredningsretKlassifikation] (
        [dw_ek_UdredningsretKlassifikation], [clean_code_UdredningsretKlassifikation],
        [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[UdredningsretKlassifikation] OFF;
END
GO

-- dim.UdredningsretKlassifikation sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[UdredningsretKlassifikation]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[UdredningsretKlassifikation] WHERE [dw_ek_UdredningsretKlassifikation] = -1)
BEGIN
    INSERT INTO [dim].[UdredningsretKlassifikation] (
        [dw_ek_UdredningsretKlassifikation], [clean_code_UdredningsretKlassifikation],
        [overholdt_tekst], [aarsag_tekst], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, N'UNKNOWN', N'Ukendt', N'Ukendt', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- map.Forloebsmarkoer sentinel
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[Forloebsmarkoer]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[Forloebsmarkoer] WHERE [dw_ek_Forloebsmarkoer] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[Forloebsmarkoer] ON;
    INSERT INTO [map].[Forloebsmarkoer] (
        [dw_ek_Forloebsmarkoer], [clean_code_Forloebsmarkoer], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[Forloebsmarkoer] OFF;
END
GO

-- dim.Forloebsmarkoer sentinel (markers without a code resolve here)
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Forloebsmarkoer]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [dim].[Forloebsmarkoer] WHERE [dw_ek_Forloebsmarkoer] = -1)
BEGIN
    INSERT INTO [dim].[Forloebsmarkoer] (
        [dw_ek_Forloebsmarkoer], [clean_code_Forloebsmarkoer], [tekst],
        [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', N'Ukendt', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
END
GO

-- map.UdredningsretMarkoer sentinel
-- The marker has no dimension of its own, so no dim row follows this one. The
-- member exists so the domain's maps all reserve -1 the same way: the fact
-- resolves its own key through an INNER JOIN and therefore never writes the
-- sentinel, and IDENTITY starts at 1, so -1 can never be minted for a marker.
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[map].[UdredningsretMarkoer]') AND type = 'U')
   AND NOT EXISTS (SELECT 1 FROM [map].[UdredningsretMarkoer] WHERE [dw_ek_UdredningsretMarkoer] = -1)
BEGIN
    SET IDENTITY_INSERT [map].[UdredningsretMarkoer] ON;
    INSERT INTO [map].[UdredningsretMarkoer] (
        [dw_ek_UdredningsretMarkoer], [clean_code_UdredningsretMarkoer], [dw_timestamp_insert], [dw_timestamp_update]
    ) VALUES (
        -1, 'UNKNOWN', CAST(SYSDATETIME() AS DATETIME2), CAST(SYSDATETIME() AS DATETIME2)
    );
    SET IDENTITY_INSERT [map].[UdredningsretMarkoer] OFF;
END
GO
