-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*============================================================
  DPA Security Functions
  Purpose: Inline table-valued functions for DPA security
  Target:  sec schema
  Dependencies: 04_dpa_tables.sql (sec.RolleDomaene), 01_tables.sql (sec.RBOrganisation)
============================================================*/

/*------------------------------------------------------------
  sec.HentDPASikkerhed
  Constructs a pipe-delimited security context string used
  for auditing/debugging which security path was applied.
------------------------------------------------------------*/
CREATE OR ALTER FUNCTION [sec].[HentDPASikkerhed]
(
    @Rolle     nvarchar(100),
    @Sikkerhedsdomaene     nvarchar(100),
    @Populationsansvar     nvarchar(100),
    @Organisation nvarchar(200)
)
RETURNS TABLE
AS
RETURN
(
    SELECT DPASikkerhed = CONCAT(@Rolle, N'|', @Sikkerhedsdomaene, N'|', @Populationsansvar, N'|',@Organisation)
)
GO


/*------------------------------------------------------------
  sec.HentBrugerRolleDomaene
  Looks up a user's role in a security domain. Returns no rows
  when the user has no active assignment (views use CROSS JOIN,
  so callers without a role see no data). Lives here (05, after
  04_dpa_tables.sql creates sec.RolleDomaene) so a fresh-install
  DDL apply resolves the table reference.
  bruger er gemt som bart sAMAccountName; domænet strippes fra
  @bruger (typisk SUSER_SNAME() = DOMAIN\sam) én gang inden
  opslaget, så index-seek bevares.
------------------------------------------------------------*/
CREATE OR ALTER FUNCTION [sec].[HentBrugerRolleDomaene] (
    @bruger NVARCHAR(50),
    @Sikkerhedsdomaene NVARCHAR(100)
)
RETURNS TABLE
AS
RETURN (
    SELECT
        [a].[bruger],
        [Rolle] = [br].[Rolle],
        [Sikkerhedsdomaene] = [br].[Sikkerhedsdomaene],
        [Populationsansvar] = [br].[Populationsansvar]
    FROM (
        SELECT [bruger] = RIGHT(@bruger, CHARINDEX(N'\', REVERSE(@bruger) + N'\') - 1)
    ) AS [a]
    INNER JOIN [sec].[RolleDomaene] AS [br]
        ON [br].[bruger] = [a].[bruger]
       AND [br].[Sikkerhedsdomaene] = @Sikkerhedsdomaene
    WHERE [br].[dw_is_deleted] = 0
);
GO


/*------------------------------------------------------------
  sec.HentRBOrganisation
  Checks whether a user has row-based access to a specific
  organisation. Returns the organisation row if matched,
  empty result set otherwise.
  bruger er gemt som bart sAMAccountName; domænet strippes fra
  @bruger (typisk SUSER_SNAME() = DOMAIN\sam) én gang inden
  opslaget, så det clustrede index-seek bevares.
------------------------------------------------------------*/
CREATE OR ALTER FUNCTION [sec].[HentRBOrganisation] (
    @Sikkerhedsdomaene   NVARCHAR(50)
   ,@bruger             NVARCHAR(50)
   ,@dw_ek_Organisation BIGINT
)
RETURNS TABLE
AS
RETURN (
   SELECT   [sec].[DW_EK_RBOrganisation]
           ,[sec].[bruger]
           ,[sec].[SOR_kode]
   FROM (
       SELECT [bruger] = RIGHT(@bruger, CHARINDEX(N'\', REVERSE(@bruger) + N'\') - 1)
   ) AS [a]
   INNER JOIN [sec].[RBOrganisation] AS [sec]
       ON  [sec].[bruger] = [a].[bruger]
       AND [sec].[DW_EK_RBOrganisation] = @dw_ek_Organisation
);
GO
