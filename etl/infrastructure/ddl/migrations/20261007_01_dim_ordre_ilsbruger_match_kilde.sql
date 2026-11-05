-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Idempotent: add ilsbruger_match_kilde to dim.Ordre.
-- Records whether dw_ek_ILSBruger_oprettet_af was matched on bruger_id or on the
-- initials. Runs on staging and DSO; the base DDL creates the column on new databases.

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[Ordre]') AND type = 'U')
   AND COL_LENGTH(N'dim.Ordre', N'ilsbruger_match_kilde') IS NULL
BEGIN
    ALTER TABLE [dim].[Ordre] ADD [ilsbruger_match_kilde] NVARCHAR(10) NULL;
END
GO
