-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: drop unused stg.mysupply_faktura_keys
--
-- Invoice-level MySupply key table had no downstream ETL consumers; UNSPSC apply-pass
-- resolves via stg.fct_fakturalinje_mysupply (CleanCode_Kreditor + CleanCode_Kreditor_Header).

IF OBJECT_ID(N'[stg].[mysupply_faktura_keys]', 'U') IS NOT NULL
BEGIN
    IF EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_stg_mysupply_faktura_keys_header'
          AND object_id = OBJECT_ID(N'[stg].[mysupply_faktura_keys]')
    )
        DROP INDEX [IX_stg_mysupply_faktura_keys_header]
            ON [stg].[mysupply_faktura_keys];

    DROP TABLE [stg].[mysupply_faktura_keys];
END
GO
