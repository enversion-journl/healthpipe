-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ----------------------------------------------------------------------------
-- arc.statistic_invoice_line_allowance_charge_mysupply
-- ----------------------------------------------------------------------------

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'mysupply_statistic_invoice_line_allowance_charge'
)
BEGIN
    CREATE TABLE [arc].[mysupply_statistic_invoice_line_allowance_charge] (
        [id]                                          INT             NOT NULL,
        [statistic_invoice_line_id]                   INT             NOT NULL,
        [FakturaUdstederGebyrKategori]                NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriIndikering]      NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriÅrsag]           NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet]       NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet_Type]  NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriKvantitet_List]  NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriTotal]           NVARCHAR(MAX)   NULL,
        [FakturaUdstederGebyrKategoriFinanskonto]     NVARCHAR(MAX)   NULL,

        -- DWH audit columns
        [dw_timestamp_insert]   DATETIME2(0)   NOT NULL,
        [dw_timestamp_update]   DATETIME2(0)   NOT NULL,

        CONSTRAINT [PK_arc_mysupply_statistic_invoice_line_allowance_charge]
            PRIMARY KEY CLUSTERED ([id])
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_allowance_charge_dw_timestamp_update'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_allowance_charge_dw_timestamp_update]
        ON [arc].[mysupply_statistic_invoice_line_allowance_charge] ([dw_timestamp_update] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_arc_mysupply_statistic_invoice_line_allowance_charge_line_id'
      AND object_id = OBJECT_ID(N'[arc].[mysupply_statistic_invoice_line_allowance_charge]')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_mysupply_statistic_invoice_line_allowance_charge_line_id]
        ON [arc].[mysupply_statistic_invoice_line_allowance_charge] ([statistic_invoice_line_id]);
END
GO
