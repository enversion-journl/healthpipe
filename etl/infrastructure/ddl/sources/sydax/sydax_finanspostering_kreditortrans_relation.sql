-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.finanspostering_kreditortrans_relation_sydax  (1:1 from SydAX [Finans].[FinansposteringKreditortransRelation])
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'sydax_finanspostering_kreditortrans_relation'
)
BEGIN
    CREATE TABLE [arc].[sydax_finanspostering_kreditortrans_relation] (
        -- Merge keys
        [Kildesystem]                              VARCHAR(10)    NOT NULL,
        [ref_LedgertransId]                        BIGINT         NOT NULL,
        [KreditortransaktionId]                    BIGINT         NOT NULL,

        -- Source columns (1:1 from [Finans].[FinansposteringKreditortransRelation], minus merge keys)
        -- Note: ref_ledgertrans_id above is a synthetic column not present in source DDL;
        --       populated via JOIN on Finans.Finanspostering in the Task 2 extract.
        [FinansposteringKreditortransRelationId]   BIGINT         NULL,
        [FinansposteringId]                        BIGINT         NULL,
        [RunId]                                    INT            NULL,
        [StartDateTime]                            DATETIME2(0)   NULL,
        [EndDateTime]                              DATETIME2(0)   NULL,
        [RdwTs]                                    DATETIME2(7)   NULL,
        [CreatedOn]                                DATETIME       NULL,
        [Modified]                                 DATETIME       NULL,

        -- DWH audit columns
        [dw_timestamp_insert]                      DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]                      DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_sydax_finanspostering_kreditortrans_relation] PRIMARY KEY CLUSTERED ([Kildesystem], [ref_LedgertransId], [KreditortransaktionId])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_kreditortrans_relation_Modified'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering_kreditortrans_relation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_kreditortrans_relation_Modified]
        ON [arc].[sydax_finanspostering_kreditortrans_relation] ([Modified] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_sydax_finanspostering_kreditortrans_relation_CreatedOn'
      AND object_id = OBJECT_ID(N'[arc].[sydax_finanspostering_kreditortrans_relation]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_sydax_finanspostering_kreditortrans_relation_CreatedOn]
        ON [arc].[sydax_finanspostering_kreditortrans_relation] ([CreatedOn] DESC);
END
GO
