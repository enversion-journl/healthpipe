-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

/*============================================================
  DPA Security Tables — RolleDomaene (ERP fritekst / DPA_ERP U-K-P roles)
  Target:  sec schema
  Dependencies: sec schema (created by common/00_schemas.sql)
============================================================*/

/*------------------------------------------------------------
  sec.RolleDomaene
  Maps a user to a security domain with a role (U/K/P); role P
  carries the population responsibility for the domain.
  PK = (bruger, Sikkerhedsdomaene)
------------------------------------------------------------*/
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'sec' AND TABLE_NAME = 'RolleDomaene'
)
BEGIN
    CREATE TABLE [sec].[RolleDomaene] (
        [bruger]              NVARCHAR(50)  NOT NULL,
        [Sikkerhedsdomaene]   NVARCHAR(50)  NOT NULL,
        [Rolle]               CHAR(1)       NOT NULL,
        [Populationsansvar]   NVARCHAR(50)  NOT NULL,
        [dw_timestamp_update] DATETIME2     NOT NULL CONSTRAINT [DF_RolleDomaene_dw_timestamp_update] DEFAULT SYSDATETIME(),
        [dw_is_deleted]        INT           NOT NULL,

        CONSTRAINT [PK_RolleDomaene]
            PRIMARY KEY CLUSTERED ([bruger], [Sikkerhedsdomaene]),

        CONSTRAINT [CK_Rolle]
            CHECK ([Rolle] IN ('U', 'K', 'P'))
    );
END;
GO
