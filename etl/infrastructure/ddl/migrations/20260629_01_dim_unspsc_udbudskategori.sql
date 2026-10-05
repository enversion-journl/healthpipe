-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration (#28710): add udbudskategori columns to dim.UNSPSC and a DW_Source_Udbud
-- flag to map.UNSPSC. Auto-applied to BOTH staging and DSO by the migration assets.
-- Each ADD is guarded so re-runs are no-ops. NOT NULL columns use a DEFAULT so the
-- ALTER backfills existing rows.

-- dim.UNSPSC: Source_Udbud
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Source_Udbud')
BEGIN
    ALTER TABLE [dim].[UNSPSC]
        ADD [Source_Udbud] BIT NOT NULL CONSTRAINT [DF_dim_UNSPSC_Source_Udbud] DEFAULT 0;
    PRINT 'Added dim.UNSPSC.Source_Udbud';
END
GO

-- dim.UNSPSC: Beskrivelse_Udbud
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Beskrivelse_Udbud')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Beskrivelse_Udbud] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Beskrivelse_Udbud';
END
GO

-- dim.UNSPSC: Udbudstype
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Udbudstype')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Udbudstype] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Udbudstype';
END
GO

-- dim.UNSPSC: Udbudskategori_niveau_1
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Udbudskategori_niveau_1')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Udbudskategori_niveau_1] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Udbudskategori_niveau_1';
END
GO

-- dim.UNSPSC: Udbudskategori_niveau_2
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Udbudskategori_niveau_2')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Udbudskategori_niveau_2] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Udbudskategori_niveau_2';
END
GO

-- dim.UNSPSC: Udbudskategori_niveau_3
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Udbudskategori_niveau_3')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Udbudskategori_niveau_3] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Udbudskategori_niveau_3';
END
GO

-- dim.UNSPSC: Udbudskategori_niveau_4
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Udbudskategori_niveau_4')
BEGIN
    ALTER TABLE [dim].[UNSPSC] ADD [Udbudskategori_niveau_4] NVARCHAR(255) NULL;
    PRINT 'Added dim.UNSPSC.Udbudskategori_niveau_4';
END
GO

-- map.UNSPSC: DW_Source_Udbud
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'map' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'DW_Source_Udbud')
BEGIN
    ALTER TABLE [map].[UNSPSC]
        ADD [DW_Source_Udbud] BIT NOT NULL CONSTRAINT [DF_map_UNSPSC_DW_Source_Udbud] DEFAULT 0;
    PRINT 'Added map.UNSPSC.DW_Source_Udbud';
END
GO
