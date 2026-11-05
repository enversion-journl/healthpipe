-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- 20260703: Drop legacy security model.
-- Sikkerhed tildeles nu udelukkende via AD (sec.RolleDomaene / sec.RBOrganisation),
-- og Fritekst-maskering sker via sec.RolleDomaene (DPA_ERP).
-- Destruktiv: fjerner de gamle sec-objekter (og deres data) i eksisterende miljøer.
-- HentFritekstAdgang_v1 + kolonnen BrugerRoller.Fritekst droppes af
-- migration 20260703_erp_fritekst_rolle_domaene.sql; denne migration fjerner resten.
-- Base-DDL opretter dem ikke længere, så på friske installationer er dette et no-op.
-- ============================================================================

IF OBJECT_ID(N'[sec].[PopulateBrugerOrganisation]', 'P') IS NOT NULL
    DROP PROCEDURE [sec].[PopulateBrugerOrganisation];
GO

IF OBJECT_ID(N'[sec].[HentBrugerRolle_v2]') IS NOT NULL
    DROP FUNCTION [sec].[HentBrugerRolle_v2];
GO

IF OBJECT_ID(N'[sec].[BeskytDimBorger_v1]') IS NOT NULL
    DROP FUNCTION [sec].[BeskytDimBorger_v1];
GO

IF OBJECT_ID(N'[sec].[HentDPASikkerhed_v1]') IS NOT NULL
    DROP FUNCTION [sec].[HentDPASikkerhed_v1];
GO

IF OBJECT_ID(N'[sec].[HentRBSikkerhed_v2]') IS NOT NULL
    DROP FUNCTION [sec].[HentRBSikkerhed_v2];
GO

IF OBJECT_ID(N'[sec].[BrugerOrganisation]', 'U') IS NOT NULL
    DROP TABLE [sec].[BrugerOrganisation];
GO

IF OBJECT_ID(N'[sec].[PersonaleOrg]', 'U') IS NOT NULL
    DROP TABLE [sec].[PersonaleOrg];
GO

IF OBJECT_ID(N'[sec].[BrugerRoller]', 'U') IS NOT NULL
    DROP TABLE [sec].[BrugerRoller];
GO
