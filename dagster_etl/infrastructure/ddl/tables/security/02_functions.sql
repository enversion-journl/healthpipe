-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Security Functions
-- Role checking and PII masking
-- ============================================================================

-- sec.HentBrugerRolle_v2 - Get user role
CREATE OR ALTER FUNCTION [sec].[HentBrugerRolle_v2] (
    @Database NVARCHAR(128),
    @Bruger NVARCHAR(128)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [Database] = @Database,
        [Bruger] = @Bruger,
        [Ubegraenset] = ISNULL([br].[Ubegraenset], 0),
        [RaekkeBegraenset] = ISNULL([br].[RaekkeBegraenset], 0),
        [KolonneBegraenset] = CASE
            WHEN ISNULL([br].[Ubegraenset], 0) = 0
             AND ISNULL([br].[RaekkeBegraenset], 0) = 0
            THEN 1 ELSE 0 END
    FROM (SELECT [Database] = @Database, [Bruger] = @Bruger) AS [a]
    LEFT JOIN [sec].[BrugerRoller] AS [br]
        ON [br].[Database] = [a].[Database]
        AND [br].[Bruger] = [a].[Bruger]
);
GO

-- sec.HentDPASikkerhed_v1 - Get DPA security level
CREATE OR ALTER FUNCTION [sec].[HentDPASikkerhed_v1] (
    @UBRolle BIT,
    @RBRolle BIT,
    @RBMatch INT
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [DPASikkerhed] = CASE
            WHEN @UBRolle = 1 THEN 'UB'
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN 'RB'
            ELSE 'KB'
        END
);
GO

-- sec.BeskytDimBorger_v1 - Mask PII based on security role
CREATE OR ALTER FUNCTION [sec].[BeskytDimBorger_v1] (
    @UBRolle BIT,
    @RBRolle BIT,
    @RBMatch INT,
    @CPRNummer NVARCHAR(10),
    @CPRNummerKrypteret NVARCHAR(10),
    @Fornavne NVARCHAR(50),
    @Efternavn NVARCHAR(50),
    @Navn NVARCHAR(50),
    @Gade NVARCHAR(50),
    @Nuv_Gade NVARCHAR(50),
    @MorCPRNummer NVARCHAR(50),
    @MorCPRnummerKrypteret NVARCHAR(50),
    @Adressebeskyttelse NVARCHAR(50)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [CPRNummer] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @CPRNummer
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @CPRNummer
            ELSE @CPRNummerKrypteret
        END, N''),

        [Fornavne] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Fornavne
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Fornavne
            ELSE N'[MASKERET]'
        END, N''),

        [Efternavn] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Efternavn
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Efternavn
            ELSE N'[MASKERET]'
        END, N''),

        [Navn] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Navn
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Navn
            ELSE N'[MASKERET]'
        END, N''),

        [Gade] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Gade
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Gade
            ELSE N'[MASKERET]'
        END, N''),

        [Nuv_Gade] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Nuv_Gade
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Nuv_Gade
            ELSE N'[MASKERET]'
        END, N''),

        [MorCPRNummer] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @MorCPRNummer
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @MorCPRNummer
            ELSE @MorCPRnummerKrypteret
        END, N''),

        [Adressebeskyttelse] = ISNULL(CASE
            WHEN @UBRolle = 1 THEN @Adressebeskyttelse
            WHEN @RBRolle = 1 AND @RBMatch = 1 THEN @Adressebeskyttelse
            ELSE N'[MASKERET]'
        END, N''),

        [sec].[DPASikkerhed]
    FROM [sec].[HentDPASikkerhed_v1](@UBRolle, @RBRolle, @RBMatch) AS [sec]
);
GO

-- sec.HentFritekstAdgang_v1 - Does the user have free-text (PII-risk) column access?
-- Returns Fritekst = 1 when the user is granted the dedicated Fritekst flag OR is
-- Ubegraenset (unrestricted users see everything). Returns 0 when no row exists.
CREATE OR ALTER FUNCTION [sec].[HentFritekstAdgang_v1] (
    @Database NVARCHAR(128),
    @Bruger NVARCHAR(128)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [Fritekst] = ISNULL(MAX(CASE
            WHEN [br].[Fritekst] = 1 OR [br].[Ubegraenset] = 1 THEN 1
            ELSE 0
        END), 0)
    FROM [sec].[BrugerRoller] AS [br]
    WHERE [br].[Database] = @Database
      AND [br].[Bruger] = @Bruger
);
GO

-- sec.BeskytDimVare_v1 - Mask PII-risk free-text columns in dim.Vare based on access
-- Free-text columns (Varenavn/VarenavnKort) and the supplier item number
-- (VarenummerKreditor, which can be CPR-formatted) are masked unless the caller
-- has been granted Fritekst access. Downstream reporting joins use the surrogate
-- key DW_EK_Vare, so masking these columns does not break joins.
CREATE OR ALTER FUNCTION [sec].[BeskytDimVare_v1] (
    @Fritekst BIT,
    @VarenummerKreditor NVARCHAR(255),
    @Varenavn NVARCHAR(MAX),
    @VarenavnKort NVARCHAR(MAX)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [VarenummerKreditor] = CASE
            WHEN @Fritekst = 1 THEN @VarenummerKreditor
            ELSE N'[MASKERET]'
        END,
        [Varenavn] = CASE
            WHEN @Fritekst = 1 THEN @Varenavn
            ELSE N'[MASKERET]'
        END,
        [VarenavnKort] = CASE
            WHEN @Fritekst = 1 THEN @VarenavnKort
            ELSE N'[MASKERET]'
        END,
        [DPASikkerhed] = CASE WHEN @Fritekst = 1 THEN 'UB' ELSE 'KB' END
);
GO

-- sec.HentRBSikkerhed_v2 - For organisation access check in views
CREATE OR ALTER FUNCTION [sec].[HentRBSikkerhed_v2] (
    @Database NVARCHAR(128),
    @Bruger NVARCHAR(128),
    @DW_EK_Organisation INT
)
RETURNS TABLE
AS
RETURN (
    SELECT
        bo.[DW_EK_Organisation],
        bo.[IsDirectAffiliation],
        bo.[HierarchyLevel],
        org.[Code],
        org.[Name] AS OrganizationName
    FROM [sec].[BrugerOrganisation] bo
    LEFT JOIN [dim].[Organisation] org
        ON org.[DW_EK_Organisation] = bo.[DW_EK_Organisation]
    WHERE bo.[Bruger] = @Bruger
    AND bo.[DW_EK_Organisation] = @DW_EK_Organisation
);
GO
