-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Aftale/Vare mange-mange refactor
-- 1) dim.Vare: drop aftale-scoped kolonner (flyttet til fct.AftaleVarePris)
-- 2) dim.Aftale: re-key til (lnr, aftale) — engangs-nulstilling af map + dim
-- Idempotent: alle steps guarded. Koerer paa baade staging og DSO.
-- ============================================================================
SET QUOTED_IDENTIFIER ON;
GO

-- 1) dim.Vare: drop default constraint paa dw_ek_Aftale foer column drop
IF OBJECT_ID(N'[dim].[Vare]', 'U') IS NOT NULL
BEGIN
    DECLARE @df sysname;
    SELECT @df = dc.name
    FROM sys.default_constraints dc
    INNER JOIN sys.columns c
        ON c.default_object_id = dc.object_id
    WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vare]')
      AND c.name = 'dw_ek_Aftale';
    IF @df IS NOT NULL
        EXEC('ALTER TABLE [dim].[Vare] DROP CONSTRAINT [' + @df + ']');

    IF COL_LENGTH('dim.Vare', 'dw_ek_Aftale') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [dw_ek_Aftale];
    IF COL_LENGTH('dim.Vare', 'Aftale') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [Aftale];
    IF COL_LENGTH('dim.Vare', 'aftaletype') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [aftaletype];
    IF COL_LENGTH('dim.Vare', 'aftaletekst') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [aftaletekst];
    IF COL_LENGTH('dim.Vare', 'pris1') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pris1];
    IF COL_LENGTH('dim.Vare', 'pris2') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pris2];
    IF COL_LENGTH('dim.Vare', 'pris3') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pris3];
    IF COL_LENGTH('dim.Vare', 'pakkestoerrelse1') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pakkestoerrelse1];
    IF COL_LENGTH('dim.Vare', 'pakkestoerrelse2') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pakkestoerrelse2];
    IF COL_LENGTH('dim.Vare', 'pakkestoerrelse3') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [pakkestoerrelse3];
    IF COL_LENGTH('dim.Vare', 'kreditorvarenummer1') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [kreditorvarenummer1];
    IF COL_LENGTH('dim.Vare', 'kreditorvarenummer2') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [kreditorvarenummer2];
    IF COL_LENGTH('dim.Vare', 'kreditorvarenummer3') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [kreditorvarenummer3];
    IF COL_LENGTH('dim.Vare', 'maerke') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [maerke];
    IF COL_LENGTH('dim.Vare', 'ils_dsa_aftale_varenummer') IS NOT NULL
        ALTER TABLE [dim].[Vare] DROP COLUMN [ils_dsa_aftale_varenummer];
END
GO

-- 2) dim.Aftale re-key: engangs (detekteres paa manglende leverandoernummer-kolonne).
--    CleanCode-formatet aendres til AFTALE_{lnr}_{aftalenr}, saa alle DW_EK'er
--    re-seedes. Historik genopbygges fra arc ved naeste fulde materialisering.
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
   AND COL_LENGTH('dim.Aftale', 'leverandoernummer') IS NULL
BEGIN
    DELETE FROM [dim].[Aftale];

    IF OBJECT_ID(N'[map].[Aftale]', 'U') IS NOT NULL
        TRUNCATE TABLE [map].[Aftale];

    ALTER TABLE [dim].[Aftale] ADD
        [leverandoernummer] NVARCHAR(100) NULL,
        [dw_ek_Kreditor] INT NOT NULL DEFAULT -1,
        [aftale_dato] DATETIME2 NULL,
        [aftale_ansvarlig] NVARCHAR(100) NULL;
END
GO
