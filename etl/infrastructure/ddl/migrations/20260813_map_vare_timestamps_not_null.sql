-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- OPS-28148: map.Vare — audit-kolonner NOT NULL.
-- dw_timestamp_update saettes nu ved INSERT (= dw_timestamp_insert, jf.
-- audit-konventionen), saa begge kolonner kan strammes til NOT NULL.
-- Backfill: update = insert for eksisterende raekker uden update-timestamp.
-- map.Vare findes kun i staging-laget; DSO-koerslen springes over af guarden.
-- Idempotent: safe to re-run.
-- ============================================================================

IF OBJECT_ID(N'[map].[Vare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'
        UPDATE [map].[Vare]
        SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
        WHERE dw_timestamp_insert IS NULL;
    ');
    EXEC(N'
        UPDATE [map].[Vare]
        SET dw_timestamp_update = dw_timestamp_insert
        WHERE dw_timestamp_update IS NULL;
    ');

    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[map].[Vare]')
          AND name = N'dw_timestamp_insert' AND is_nullable = 1
    )
        EXEC(N'ALTER TABLE [map].[Vare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');

    IF EXISTS (
        SELECT 1 FROM sys.columns
        WHERE object_id = OBJECT_ID(N'[map].[Vare]')
          AND name = N'dw_timestamp_update' AND is_nullable = 1
    )
        EXEC(N'ALTER TABLE [map].[Vare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO
