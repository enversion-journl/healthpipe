-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.ils_rekvundtagne  (1:1 from ils_pro2_log.dbo.rekvUndtagne)
-- Manual exclusion list for non-consumption rekvirents. Grain: (sgh, rnr).
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'ils_rekvundtagne'
)
BEGIN
    CREATE TABLE [arc].[ils_rekvundtagne] (
        [sgh]                   varchar(8)     NOT NULL,
        [rnr]                   varchar(16)    NOT NULL,
        [navn]                  varchar(80)    NULL,

        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL
            CONSTRAINT [DF_ils_rekvundtagne_dw_timestamp_insert] DEFAULT SYSUTCDATETIME(),
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL
            CONSTRAINT [DF_ils_rekvundtagne_dw_timestamp_update] DEFAULT SYSUTCDATETIME(),

        CONSTRAINT [PK_arc_ils_rekvundtagne] PRIMARY KEY CLUSTERED ([sgh], [rnr])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_ils_rekvundtagne_DW_Timestamp_Update'
      AND object_id = OBJECT_ID(N'[arc].[ils_rekvundtagne]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_ils_rekvundtagne_DW_Timestamp_Update]
        ON [arc].[ils_rekvundtagne] ([dw_timestamp_update] DESC);
END
GO
