-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- dim.Dato: tilføj BEFORE/AFTER sentinel-rækker (range-clamp).
--
-- Formål: dw_ek_Dato-nøgler clampes nu til tre sentinels i stedet for kun -1:
--   -1        UNKNOWN — kilde NULL/uparsebar (findes allerede)
--   -2        BEFORE  — gyldig dato før dim.Datos range (< 1970-01-01)
--   99999999  AFTER   — gyldig dato efter dim.Datos range (> 2050-12-31)
-- Rækkerne skal findes i dim.Dato, så out-of-range fact-nøgler resolver til en
-- rigtig række (ingen forældreløse keys). Fresh DB: base-DDL i
-- tables/shared_dimensions/02_dims.sql indsætter alle tre. Eksisterende DB:
-- denne migration tilføjer de manglende.
--
-- Ingen data-backfill af fact-nøgler — næste ERP full-load genberegner dem via
-- den opdaterede dato_key()-helper.
--
-- Idempotent + guarded: hver INSERT wrappes i IF OBJECT_ID(dim.Dato) + NOT
-- EXISTS på den konkrete dw_ek_Dato-værdi.
-- ============================================================================

IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [dim].[Dato] WHERE [dw_ek_Dato] = -2)
    INSERT INTO [dim].[Dato] (
        [dw_ek_Dato],
        [kvartal_aarnavn], [kvartal_navn],
        [maaned_aarnavn], [maaned_navn],
        [ugedag_navn], [uge_aarnavn], [uge_navn],
        [helligdag_dk_navn]
    ) VALUES (
        -2,
        N'Før range', N'Før range',
        N'Før range', N'Før range',
        N'Før range', N'Før range', N'Før range',
        N'Før range'
    );
GO

IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM [dim].[Dato] WHERE [dw_ek_Dato] = 99999999)
    INSERT INTO [dim].[Dato] (
        [dw_ek_Dato],
        [kvartal_aarnavn], [kvartal_navn],
        [maaned_aarnavn], [maaned_navn],
        [ugedag_navn], [uge_aarnavn], [uge_navn],
        [helligdag_dk_navn]
    ) VALUES (
        99999999,
        N'Efter range', N'Efter range',
        N'Efter range', N'Efter range',
        N'Efter range', N'Efter range', N'Efter range',
        N'Efter range'
    );
GO
