-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- ERP: Dimension Views (vw_dim schema)
--
-- Simple pass-through views for dimension tables.
-- Used for security/access control layer on DSO.
-- ============================================================================

-- vw_dim.Kontoplan
CREATE OR ALTER VIEW [vw_dim].[Kontoplan]
AS
SELECT *
FROM [dim].[Kontoplan];
GO

-- vw_dim.Kreditor
CREATE OR ALTER VIEW [vw_dim].[Kreditor]
AS
SELECT *
FROM [dim].[Kreditor];
GO

-- vw_dim.Vare is defined in views/security/01_vw_dim_security.sql
-- (free-text/PII-risk columns are column-restricted, not a SELECT * pass-through).

-- vw_dim.Aftale (active rows only)
CREATE OR ALTER VIEW [vw_dim].[Aftale]
AS
SELECT *
FROM [dim].[Aftale]
WHERE DW_IsDeleted = 0;
GO

-- vw_dim.VareudleveringEgenskaber
CREATE OR ALTER VIEW [vw_dim].[VareudleveringEgenskaber]
AS
SELECT *
FROM [dim].[VareudleveringEgenskaber];
GO

-- vw_dim.Faktura (renamed from FakturaEgenskaber)
IF OBJECT_ID(N'[vw_dim].[FakturaEgenskaber]', 'V') IS NOT NULL
    DROP VIEW [vw_dim].[FakturaEgenskaber];
GO

CREATE OR ALTER VIEW [vw_dim].[Faktura]
AS
SELECT *
FROM [dim].[Faktura];
GO

-- vw_dim.Fakturalinje is defined in views/security/01_vw_dim_security.sql
-- (dim.Fakturalinje is entirely MySupply free-text and is column-restricted).

-- vw_dim.Lager
CREATE OR ALTER VIEW [vw_dim].[Lager]
AS
SELECT *
FROM [dim].[Lager];
GO

-- vw_dim.Postering
CREATE OR ALTER VIEW [vw_dim].[Postering]
AS
SELECT *
FROM [dim].[Postering];
GO

-- vw_dim.Rekvirent
CREATE OR ALTER VIEW [vw_dim].[Rekvirent]
AS
SELECT *
FROM [dim].[Rekvirent];
GO

-- vw_dim.EAN
CREATE OR ALTER VIEW [vw_dim].[EAN]
AS
SELECT *
FROM [dim].[EAN];
GO

CREATE OR ALTER VIEW [vw_dim].[Finanskladde] AS SELECT * FROM [dim].[Finanskladde];
GO
CREATE OR ALTER VIEW [vw_dim].[Finanskladdelinje] AS SELECT * FROM [dim].[Finanskladdelinje];
GO

-- vw_dim.BudgetEgenskaber
CREATE OR ALTER VIEW [vw_dim].[BudgetEgenskaber]
AS
SELECT *
FROM [dim].[BudgetEgenskaber];
GO

-- vw_dim.UNSPSC
CREATE OR ALTER VIEW [vw_dim].[UNSPSC]
AS
SELECT *
FROM [dim].[UNSPSC];
GO
