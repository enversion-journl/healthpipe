-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Archive: RH UNSPSC udbudskategori (manuelt vedligeholdt flad fil)
--
-- Landes fra DSI/csv_files/data/unspsc_udbudskategori.csv via
-- staging_infra_udbudskategori_seed (tom-tabel-tjek; reload kræver manuel DROP).
-- Kilden opdateres ~1 gang/år af Region Hovedstaden via PR.
-- ============================================================================

IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'arc' AND TABLE_NAME = 'unspsc_udbudskategori'
)
BEGIN
    CREATE TABLE [arc].[unspsc_udbudskategori] (
        [UNSPSC_Nr]             NVARCHAR(50)  NOT NULL,
        [UNSPSC_Tekst]          NVARCHAR(500) NULL,
        [Udbudstype]            NVARCHAR(500) NULL,
        [Udbudskategori_niv1]   NVARCHAR(500) NULL,
        [Udbudskategori_niv2]   NVARCHAR(500) NULL,
        [Udbudskategori_niv3]   NVARCHAR(500) NULL,
        [Udbudskategori_niv4]   NVARCHAR(500) NULL,
        [dw_timestamp_insert]   DATETIME2(0)  NOT NULL CONSTRAINT [DF_unspsc_udbudskategori_dw_timestamp_insert] DEFAULT GETDATE(),
        [dw_timestamp_update]   DATETIME2(0)  NOT NULL CONSTRAINT [DF_unspsc_udbudskategori_dw_timestamp_update] DEFAULT GETDATE()
    );
END
GO

IF OBJECT_ID(N'[arc].[unspsc_udbudskategori]', 'U') IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = 'IX_arc_unspsc_udbudskategori_nr'
          AND object_id = OBJECT_ID(N'[arc].[unspsc_udbudskategori]')
    )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_arc_unspsc_udbudskategori_nr]
        ON [arc].[unspsc_udbudskategori] ([UNSPSC_Nr]);
END
GO
