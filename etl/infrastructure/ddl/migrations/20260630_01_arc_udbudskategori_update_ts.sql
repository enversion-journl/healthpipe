-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- #28710: udbud deltager nu i det delte UNSPSC delta-filter (DW_Timestamp_Update).
-- Tilføj update-timestamp til arkivtabellen for eksisterende miljøer.
IF OBJECT_ID(N'[arc].[unspsc_udbudskategori]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'unspsc_udbudskategori'
         AND COLUMN_NAME = 'DW_Timestamp_Update'
   )
BEGIN
    ALTER TABLE [arc].[unspsc_udbudskategori]
        ADD [DW_Timestamp_Update] DATETIME2(0) NOT NULL
            CONSTRAINT [DF_arc_unspsc_udbudskategori_ts_upd] DEFAULT GETDATE();
END
GO
