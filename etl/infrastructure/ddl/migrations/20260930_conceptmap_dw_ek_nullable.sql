-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Make the DW_EK columns on four conceptmap.*ToSOR staging tables nullable.
--
-- update_conceptmap_dw_ek resolves them after the MERGE with a LEFT JOIN, so a
-- code without a map row is NULL. 20260701_shaksor_dw_ek_nullable.sql made the
-- same change for SHAKToSOR. STAGING only; ALTER to NULL is a no-op on re-run.
-- ============================================================================

IF DB_NAME() LIKE N'%staging%' AND OBJECT_ID(N'[conceptmap].[RekvirentToSOR]', 'U') IS NOT NULL
BEGIN
    ALTER TABLE [conceptmap].[RekvirentToSOR] ALTER COLUMN [dw_ek_Rekvirent] INT NULL;
    ALTER TABLE [conceptmap].[RekvirentToSOR] ALTER COLUMN [dw_ek_Organisation_target] INT NULL;
END
GO

IF DB_NAME() LIKE N'%staging%' AND OBJECT_ID(N'[conceptmap].[LagerToSOR]', 'U') IS NOT NULL
BEGIN
    ALTER TABLE [conceptmap].[LagerToSOR] ALTER COLUMN [dw_ek_Lager] INT NULL;
    ALTER TABLE [conceptmap].[LagerToSOR] ALTER COLUMN [dw_ek_Organisation_target] INT NULL;
END
GO

IF DB_NAME() LIKE N'%staging%' AND OBJECT_ID(N'[conceptmap].[EANToSOR]', 'U') IS NOT NULL
BEGIN
    ALTER TABLE [conceptmap].[EANToSOR] ALTER COLUMN [dw_ek_EAN] INT NULL;
    ALTER TABLE [conceptmap].[EANToSOR] ALTER COLUMN [dw_ek_Organisation_target] INT NULL;
END
GO

IF DB_NAME() LIKE N'%staging%' AND OBJECT_ID(N'[conceptmap].[OkostedToSOR]', 'U') IS NOT NULL
BEGIN
    ALTER TABLE [conceptmap].[OkostedToSOR] ALTER COLUMN [dw_ek_Okosted] INT NULL;
    ALTER TABLE [conceptmap].[OkostedToSOR] ALTER COLUMN [dw_ek_Organisation_target] INT NULL;
END
GO
