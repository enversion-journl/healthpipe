-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Migration: add primaer_sygehus_navn to dim.ILSBruger
--
-- Runs on BOTH staging and DSO via the centralized migrations runner
-- (staging_migrations / dso_migrations), before the base DDL. Idempotent: a
-- no-op when the table does not exist yet (the base DDL then creates it with
-- the column) or already has the column.

IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[dim].[ILSBruger]') AND type = 'U')
   AND COL_LENGTH(N'dim.ILSBruger', N'primaer_sygehus_navn') IS NULL
BEGIN
    ALTER TABLE [dim].[ILSBruger] ADD [primaer_sygehus_navn] NVARCHAR(80) NULL;
    PRINT 'Added dim.ILSBruger.primaer_sygehus_navn';
END
GO
