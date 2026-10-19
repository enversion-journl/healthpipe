-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- =============================================================================
-- dim.Borger: populationsansvar and the masking row -99
--
-- populationsansvar is the hospital responsible for the citizen's municipality of
-- residence. dim_Borger sets the column from ref.KommuneToPopulationsansvar; it is
-- NULL until the asset has run and for a citizen outside the table's municipalities.
--
-- Row -99 is the citizen that a masked fact row points to. -1 remains an
-- unknown citizen and -2 a row where citizen is not relevant.
--
-- In-place ALTER and INSERT; nothing is dropped, and no checkpoint is reset.
-- Runs on both databases and is a no-op where the column and the row exist.
-- The row is inserted with dynamic SQL because the column names exist only
-- after 20260924_dim_borger_fhir_patient_id.sql.
--
-- Base DDL in tables/shared_dimensions/02_dims.sql is the source for a new database.
-- =============================================================================

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = N'dim' AND TABLE_NAME = N'Borger'
         AND COLUMN_NAME = N'populationsansvar'
   )
    ALTER TABLE [dim].[Borger] ADD [populationsansvar] NVARCHAR(50) NULL;
GO

IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
   AND COL_LENGTH(N'[dim].[Borger]', N'cpr_nummer') IS NOT NULL
    EXEC(N'
        IF NOT EXISTS (SELECT 1 FROM [dim].[Borger] WHERE [dw_sk_Borger] = -99)
            INSERT INTO [dim].[Borger] (
                [dw_sk_Borger], [dw_ek_Borger], [cpr_nummer],
                [fornavne], [efternavn],
                [dw_valid_from], [dw_valid_to], [dw_is_current], [dw_is_deleted],
                [dw_timestamp_insert]
            ) VALUES (
                -99, -99, ''MASKERET'',
                N''Maskeret'', N''Borger'',
                CAST(''1900-01-01'' AS datetime2(0)), CAST(''9999-12-31 23:59:59'' AS datetime2(0)), 1, 0,
                CAST(SYSDATETIME() AS datetime2(0))
            );
    ');
GO
