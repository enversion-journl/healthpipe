-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration (#28710): rename dim.UNSPSC source-presence flags to the DW_Source_*
-- convention so the same concept has one name across stg.dim_unspsc, map.UNSPSC,
-- and dim.UNSPSC. Auto-applied to BOTH staging and DSO by the migration assets.
-- Sorts after 20260629_01_dim_unspsc_udbudskategori.sql, so Source_Udbud has
-- already been added before it is renamed here. Each rename is guarded (old column
-- present AND new column absent) so re-runs and fresh DBs (created from the updated
-- DDL with DW_Source_* already in place) are no-ops.

-- dim.UNSPSC.Source_ILS -> DW_Source_ILS
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Source_ILS')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'DW_Source_ILS')
BEGIN
    EXEC sp_rename 'dim.UNSPSC.Source_ILS', 'DW_Source_ILS', 'COLUMN';
    PRINT 'Renamed dim.UNSPSC.Source_ILS -> DW_Source_ILS';
END
GO

-- dim.UNSPSC.Source_KMD -> DW_Source_KMD
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Source_KMD')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'DW_Source_KMD')
BEGIN
    EXEC sp_rename 'dim.UNSPSC.Source_KMD', 'DW_Source_KMD', 'COLUMN';
    PRINT 'Renamed dim.UNSPSC.Source_KMD -> DW_Source_KMD';
END
GO

-- dim.UNSPSC.Source_Udbud -> DW_Source_Udbud
IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'Source_Udbud')
   AND NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'dim' AND TABLE_NAME = 'UNSPSC' AND COLUMN_NAME = 'DW_Source_Udbud')
BEGIN
    EXEC sp_rename 'dim.UNSPSC.Source_Udbud', 'DW_Source_Udbud', 'COLUMN';
    PRINT 'Renamed dim.UNSPSC.Source_Udbud -> DW_Source_Udbud';
END
GO
