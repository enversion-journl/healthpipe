-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- DW standard-kolonner NOT NULL — tvaergaaende sweep.
--
-- Formaal: alle dw_timestamp_insert / dw_timestamp_update audit-kolonner samt
-- udvalgte dw_ek_* entity-key-kolonner strammes til NOT NULL paa tvaers af
-- map.* / dim.* / bridge.* (og oevrige domaener i senere sektioner).
--
-- Fresh DB: base-DDL i tables/{domaene}/*.sql opretter kolonnerne NOT NULL.
-- Eksisterende DB: denne migration backfiller NULL-vaerdier, tilfoejer DEFAULT
--                  hvor det mangler, og ALTER COLUMN ... NOT NULL.
--
-- Idempotent + guarded (safe to re-run):
--   * Hver kolonne-operation er wrappet i IF OBJECT_ID(...) IS NOT NULL.
--   * Alle kolonne-navngivende statements koeres via EXEC(N'...') dynamisk SQL,
--     saa batchen kan kompileres selv hvis kolonnen endnu ikke findes.
--   * ALTER COLUMN ... NOT NULL guardes paa sys.columns.is_nullable = 1.
--   * ADD DEFAULT guardes paa NOT EXISTS mod sys.default_constraints.
--   * Ingen CURSOR / WHILE / procedural batches — kun flade guarded blokke.
--   * Blokke adskilles med GO.
--
-- Type + praecision bevares (ALTER COLUMN gentager kolonnens eksisterende
-- type-token: DATETIME2 for timestamps, INT for entity-keys).
--
-- Sektioneret pr. domaene med "-- === <domaene> ===" headers. Senere tasks
-- APPENDER deres domaene-sektioner til denne fil.
-- ============================================================================

-- === erp ===

-- map.Kontoplan
IF OBJECT_ID(N'[map].[Kontoplan]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Kontoplan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Kontoplan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kontoplan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kontoplan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kontoplan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kontoplan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Kreditor
IF OBJECT_ID(N'[map].[Kreditor]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Kreditor]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Kreditor]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Kreditor]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kreditor] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Kreditor]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Kreditor] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Faktura
IF OBJECT_ID(N'[map].[Faktura]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Aftale
IF OBJECT_ID(N'[map].[Aftale]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Aftale]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Aftale]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Aftale]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Aftale] ADD CONSTRAINT [DF_Aftale_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Aftale] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Aftale]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Aftale] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.AftaleVarePris
IF OBJECT_ID(N'[map].[AftaleVarePris]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[AftaleVarePris]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AftaleVarePris]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AftaleVarePris]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AftaleVarePris]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AftaleVarePris] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Fakturalinje
IF OBJECT_ID(N'[map].[Fakturalinje]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Postering
IF OBJECT_ID(N'[map].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Vareudlevering
IF OBJECT_ID(N'[map].[Vareudlevering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Lager
IF OBJECT_ID(N'[map].[Lager]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Lager]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Lager]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lager]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lager]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Lager] ADD CONSTRAINT [DF_Lager_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lager] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lager]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lager] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Rekvirent
IF OBJECT_ID(N'[map].[Rekvirent]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Rekvirent]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Rekvirent]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Rekvirent]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Rekvirent]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Rekvirent] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Rekvirent]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Rekvirent] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.LagerbeholdningEgenskaber
IF OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[LagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[LagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.AfdelingLagerbeholdningEgenskaber
IF OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Lagerbeholdning
IF OBJECT_ID(N'[map].[Lagerbeholdning]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Lagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Lagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Lagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Lagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.AfdelingLagerbeholdning
IF OBJECT_ID(N'[map].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[AfdelingLagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.ean
IF OBJECT_ID(N'[map].[ean]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[ean]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[ean]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ean]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ean]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[ean] ADD CONSTRAINT [DF_ean_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ean]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ean] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ean]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ean] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.BudgetEgenskaber
IF OBJECT_ID(N'[map].[BudgetEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[BudgetEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[BudgetEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Budget
IF OBJECT_ID(N'[map].[Budget]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Budget]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Budget]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Budget]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Budget]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Budget]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Budget] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Budget]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Budget] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.ILSVare
IF OBJECT_ID(N'[map].[ILSVare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[ILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[ILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[ILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[ILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[ILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Finanskladde
IF OBJECT_ID(N'[map].[Finanskladde]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Finanskladde]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Finanskladde]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladde]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladde]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladde] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladde]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladde] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Finanskladdelinje
IF OBJECT_ID(N'[map].[Finanskladdelinje]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Finanskladdelinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Finanskladdelinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Finanskladdelinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Finanskladdelinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.Okosted
IF OBJECT_ID(N'[map].[Okosted]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Okosted]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Okosted]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Okosted]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Okosted]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Okosted] ADD CONSTRAINT [DF_Okosted_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Okosted]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Okosted] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Okosted]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Okosted] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- map.SHAK
IF OBJECT_ID(N'[map].[SHAK]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[SHAK]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[SHAK]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[SHAK]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[SHAK]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[SHAK] ADD CONSTRAINT [DF_SHAK_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[SHAK]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[SHAK] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[SHAK]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[SHAK] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Kontoplan
IF OBJECT_ID(N'[dim].[Kontoplan]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Kontoplan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Kontoplan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ADD CONSTRAINT [DF_Kontoplan_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kontoplan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Kreditor
IF OBJECT_ID(N'[dim].[Kreditor]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Kreditor]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Kreditor]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kreditor]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Kreditor]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Kreditor] ADD CONSTRAINT [DF_Kreditor_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kreditor] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Kreditor]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Kreditor] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Leverandoervare (renamed from dim.Vare in 20260813_rename_vare_to_leverandoervare.sql).
-- Handled under BOTH names so this block is correct whether it runs before the
-- rename (combined deploy pass -> old name still present) or after it
-- (rename already applied in an earlier development-only deploy -> new name).
IF OBJECT_ID(N'[dim].[Leverandoervare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Leverandoervare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Leverandoervare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Leverandoervare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ADD CONSTRAINT [DF_Leverandoervare_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Leverandoervare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Leverandoervare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Vare (pre-rename name; fires only when this migration runs before the rename)
IF OBJECT_ID(N'[dim].[Vare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Vare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Vare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Vare] ADD CONSTRAINT [DF_Vare_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Vare] ADD CONSTRAINT [DF_Vare_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Aftale
IF OBJECT_ID(N'[dim].[Aftale]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Aftale]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Aftale]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Aftale] ADD CONSTRAINT [DF_Aftale_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Aftale]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Aftale] ADD CONSTRAINT [DF_Aftale_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Aftale] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Aftale]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Aftale] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Faktura
IF OBJECT_ID(N'[dim].[Faktura]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Faktura] ADD CONSTRAINT [DF_Faktura_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Faktura] ADD CONSTRAINT [DF_Faktura_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Fakturalinje
IF OBJECT_ID(N'[dim].[Fakturalinje]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Postering
IF OBJECT_ID(N'[dim].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Postering] ADD CONSTRAINT [DF_Postering_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Postering] ADD CONSTRAINT [DF_Postering_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Vareudlevering
IF OBJECT_ID(N'[dim].[Vareudlevering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Lager
IF OBJECT_ID(N'[dim].[Lager]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Lager]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Lager]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Lager]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Lager] ADD CONSTRAINT [DF_Lager_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Lager]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Lager] ADD CONSTRAINT [DF_Lager_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Lager] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Lager] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Lager.dw_ek_Organisation
IF OBJECT_ID(N'[dim].[Lager]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Lager] SET [dw_ek_Organisation] = -1 WHERE [dw_ek_Organisation] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Lager]') AND c.name = N'dw_ek_Organisation')
        EXEC(N'ALTER TABLE [dim].[Lager] ADD CONSTRAINT [DF_Lager_dw_ek_Organisation] DEFAULT -1 FOR [dw_ek_Organisation];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Lager]')
               AND name = N'dw_ek_Organisation' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Lager] ALTER COLUMN [dw_ek_Organisation] INT NOT NULL;');
END
GO

-- dim.Rekvirent
IF OBJECT_ID(N'[dim].[Rekvirent]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Rekvirent]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Rekvirent]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Rekvirent.dw_ek_Organisation
IF OBJECT_ID(N'[dim].[Rekvirent]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Rekvirent] SET [dw_ek_Organisation] = -1 WHERE [dw_ek_Organisation] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Rekvirent]') AND c.name = N'dw_ek_Organisation')
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ADD CONSTRAINT [DF_Rekvirent_dw_ek_Organisation] DEFAULT -1 FOR [dw_ek_Organisation];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Rekvirent]')
               AND name = N'dw_ek_Organisation' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Rekvirent] ALTER COLUMN [dw_ek_Organisation] INT NOT NULL;');
END
GO

-- dim.LagerbeholdningEgenskaber
IF OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[LagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[LagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ADD CONSTRAINT [DF_LagerbeholdningEgenskaber_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[LagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[LagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.AfdelingLagerbeholdningEgenskaber
IF OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[AfdelingLagerbeholdningEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ADD CONSTRAINT [DF_AfdelingLagerbeholdningEgenskaber_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[AfdelingLagerbeholdningEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[AfdelingLagerbeholdningEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.ean
IF OBJECT_ID(N'[dim].[ean]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[ean]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[ean]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ean]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[ean] ADD CONSTRAINT [DF_ean_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ean]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[ean] ADD CONSTRAINT [DF_ean_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ean]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ean] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ean]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ean] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.ILSVare
IF OBJECT_ID(N'[dim].[ILSVare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[ILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[ILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[ILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[ILSVare] ADD CONSTRAINT [DF_ILSVare_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[ILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[ILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Finanskladde
IF OBJECT_ID(N'[dim].[Finanskladde]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Finanskladde]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Finanskladde]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladde]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ADD CONSTRAINT [DF_Finanskladde_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladde]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladde] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Finanskladdelinje
IF OBJECT_ID(N'[dim].[Finanskladdelinje]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Finanskladdelinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Finanskladdelinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ADD CONSTRAINT [DF_Finanskladdelinje_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Finanskladdelinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Finanskladdelinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.BudgetEgenskaber
IF OBJECT_ID(N'[dim].[BudgetEgenskaber]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[BudgetEgenskaber]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[BudgetEgenskaber]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ADD CONSTRAINT [DF_BudgetEgenskaber_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[BudgetEgenskaber]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[BudgetEgenskaber] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.Okosted
IF OBJECT_ID(N'[dim].[Okosted]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Okosted]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Okosted]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Okosted]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Okosted] ADD CONSTRAINT [DF_Okosted_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Okosted]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Okosted] ADD CONSTRAINT [DF_Okosted_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Okosted]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Okosted] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Okosted]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Okosted] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- dim.SHAK
IF OBJECT_ID(N'[dim].[SHAK]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[SHAK]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[SHAK]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[SHAK]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[SHAK] ADD CONSTRAINT [DF_SHAK_dim_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[SHAK]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[SHAK] ADD CONSTRAINT [DF_SHAK_dim_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[SHAK]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[SHAK] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[SHAK]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[SHAK] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- bridge.LeverandoervareILSVare (renamed from bridge.VareILSVare in
-- 20260813_rename_vare_to_leverandoervare.sql). Handled under BOTH names so this
-- block is correct whether it runs before or after that rename.
IF OBJECT_ID(N'[bridge].[LeverandoervareILSVare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [bridge].[LeverandoervareILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [bridge].[LeverandoervareILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ADD CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ADD CONSTRAINT [DF_LeverandoervareILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[LeverandoervareILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[LeverandoervareILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- bridge.VareILSVare (pre-rename name; fires only when this migration runs before the rename)
IF OBJECT_ID(N'[bridge].[VareILSVare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [bridge].[VareILSVare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [bridge].[VareILSVare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[VareILSVare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [bridge].[VareILSVare] ADD CONSTRAINT [DF_VareILSVare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[bridge].[VareILSVare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [bridge].[VareILSVare] ADD CONSTRAINT [DF_VareILSVare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[VareILSVare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[VareILSVare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[bridge].[VareILSVare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [bridge].[VareILSVare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- === erp facts ===

-- fct.Fakturalinje
IF OBJECT_ID(N'[fct].[Fakturalinje]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Fakturalinje]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Fakturalinje]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Fakturalinje]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ADD CONSTRAINT [DF_Fakturalinje_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Fakturalinje]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Fakturalinje] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Postering
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Postering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Faktura
IF OBJECT_ID(N'[fct].[Faktura]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Faktura]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Faktura]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Faktura]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Faktura] ADD CONSTRAINT [DF_Faktura_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Faktura] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Faktura]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Faktura] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Vareudlevering
IF OBJECT_ID(N'[fct].[Vareudlevering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Vareudlevering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Vareudlevering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Vareudlevering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ADD CONSTRAINT [DF_Vareudlevering_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Vareudlevering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Vareudlevering] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Lagerbeholdning
IF OBJECT_ID(N'[fct].[Lagerbeholdning]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Lagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Lagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ADD CONSTRAINT [DF_Lagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Lagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Lagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.AfdelingLagerbeholdning
IF OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[AfdelingLagerbeholdning]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[AfdelingLagerbeholdning]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ADD CONSTRAINT [DF_AfdelingLagerbeholdning_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AfdelingLagerbeholdning]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AfdelingLagerbeholdning] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Budget
IF OBJECT_ID(N'[fct].[Budget]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Budget]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[Budget]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Budget]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Budget]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[Budget] ADD CONSTRAINT [DF_Budget_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Budget] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Budget]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Budget] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.AftaleVarePris
IF OBJECT_ID(N'[fct].[AftaleVarePris]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[AftaleVarePris]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fct].[AftaleVarePris]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[AftaleVarePris]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ADD CONSTRAINT [DF_AftaleVarePris_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[AftaleVarePris]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[AftaleVarePris] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_godkendelse
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_godkendelse] = -1 WHERE [dw_ek_Dato_godkendelse] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_godkendelse')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_godkendelse] DEFAULT -1 FOR [dw_ek_Dato_godkendelse];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_godkendelse' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_godkendelse] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_lukket
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_lukket] = -1 WHERE [dw_ek_Dato_lukket] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_lukket')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_lukket] DEFAULT -1 FOR [dw_ek_Dato_lukket];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_lukket' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_lukket] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_trans_oprettet
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_trans_oprettet] = -1 WHERE [dw_ek_Dato_trans_oprettet] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_trans_oprettet')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_trans_oprettet] DEFAULT -1 FOR [dw_ek_Dato_trans_oprettet];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_trans_oprettet' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_trans_oprettet] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_forfald
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_forfald] = -1 WHERE [dw_ek_Dato_forfald] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_forfald')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_forfald] DEFAULT -1 FOR [dw_ek_Dato_forfald];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_forfald' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_forfald] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_journal_faktura
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_journal_faktura] = -1 WHERE [dw_ek_Dato_journal_faktura] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_journal_faktura')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_journal_faktura] DEFAULT -1 FOR [dw_ek_Dato_journal_faktura];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_journal_faktura' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_journal_faktura] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_sidst_udlignet
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_sidst_udlignet] = -1 WHERE [dw_ek_Dato_sidst_udlignet] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_sidst_udlignet')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_sidst_udlignet] DEFAULT -1 FOR [dw_ek_Dato_sidst_udlignet];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_sidst_udlignet' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_sidst_udlignet] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_transaktion
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_transaktion] = -1 WHERE [dw_ek_Dato_transaktion] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_transaktion')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_transaktion] DEFAULT -1 FOR [dw_ek_Dato_transaktion];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_transaktion' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_transaktion] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_bogfoering
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_bogfoering] = -1 WHERE [dw_ek_Dato_bogfoering] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_bogfoering')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_bogfoering] DEFAULT -1 FOR [dw_ek_Dato_bogfoering];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_bogfoering' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_bogfoering] INT NOT NULL;');
END
GO

-- fct.Postering.dw_ek_Dato_post_oprettet
IF OBJECT_ID(N'[fct].[Postering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fct].[Postering] SET [dw_ek_Dato_post_oprettet] = -1 WHERE [dw_ek_Dato_post_oprettet] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fct].[Postering]') AND c.name = N'dw_ek_Dato_post_oprettet')
        EXEC(N'ALTER TABLE [fct].[Postering] ADD CONSTRAINT [DF_Postering_dw_ek_Dato_post_oprettet] DEFAULT -1 FOR [dw_ek_Dato_post_oprettet];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[Postering]')
               AND name = N'dw_ek_Dato_post_oprettet' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fct].[Postering] ALTER COLUMN [dw_ek_Dato_post_oprettet] INT NOT NULL;');
END
GO

-- === fhir ===

-- fhir.Patient
IF OBJECT_ID(N'[fhir].[Patient]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Patient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Patient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Patient] ADD CONSTRAINT [DF_Patient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Patient] ADD CONSTRAINT [DF_Patient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Patient_Address
IF OBJECT_ID(N'[fhir].[Patient_Address]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Patient_Address]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Patient_Address]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ADD CONSTRAINT [DF_Patient_Address_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Patient_Address]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ADD CONSTRAINT [DF_Patient_Address_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Patient_Address]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Patient_Address] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Encounter
IF OBJECT_ID(N'[fhir].[Encounter]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Encounter]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Encounter]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Encounter] ADD CONSTRAINT [DF_Encounter_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Encounter] ADD CONSTRAINT [DF_Encounter_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Encounter_Location
IF OBJECT_ID(N'[fhir].[Encounter_Location]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Encounter_Location]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Encounter_Location]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ADD CONSTRAINT [DF_Encounter_Location_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Encounter_Location]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ADD CONSTRAINT [DF_Encounter_Location_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Encounter_Location]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Encounter_Location] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.DocumentReference
IF OBJECT_ID(N'[fhir].[DocumentReference]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[DocumentReference]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[DocumentReference]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ADD CONSTRAINT [DF_DocumentReference_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ADD CONSTRAINT [DF_DocumentReference_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.DocumentReference_Recipient
IF OBJECT_ID(N'[fhir].[DocumentReference_Recipient]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[DocumentReference_Recipient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[DocumentReference_Recipient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ADD CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ADD CONSTRAINT [DF_DocumentReference_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[DocumentReference_Recipient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[DocumentReference_Recipient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Condition
IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Condition]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Condition]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Condition] ADD CONSTRAINT [DF_Condition_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Condition] ADD CONSTRAINT [DF_Condition_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Condition_tillaegskode
IF OBJECT_ID(N'[fhir].[Condition_tillaegskode]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Condition_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Condition_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ADD CONSTRAINT [DF_Condition_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ADD CONSTRAINT [DF_Condition_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Condition_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Condition_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Observation
IF OBJECT_ID(N'[fhir].[Observation]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Observation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Observation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Observation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Observation] ADD CONSTRAINT [DF_Observation_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Observation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Observation] ADD CONSTRAINT [DF_Observation_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[Observation]')
                 AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_SystemDWEffective')
            EXEC(N'DROP INDEX [IX_FHIR_Observation_SystemDWEffective] ON [fhir].[Observation];');
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[Observation] DROP COLUMN [dw_timestamp_effective];');
        EXEC(N'ALTER TABLE [fhir].[Observation] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
        EXEC(N'ALTER TABLE [fhir].[Observation] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[Observation] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[Observation]') AND name = N'IX_FHIR_Observation_SystemDWEffective')
            EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_Observation_SystemDWEffective] ON [fhir].[Observation] ([element_system] ASC, [dw_timestamp_effective] ASC) INCLUDE ([observation_id]) ON [PRIMARY];');
    END
END
GO

-- fhir.EpisodeOfCare
IF OBJECT_ID(N'[fhir].[EpisodeOfCare]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[EpisodeOfCare]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[EpisodeOfCare]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ADD CONSTRAINT [DF_EpisodeOfCare_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[EpisodeOfCare]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[EpisodeOfCare] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Location
IF OBJECT_ID(N'[fhir].[Location]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Location]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Location]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Location]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Location] ADD CONSTRAINT [DF_Location_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Location]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Location] ADD CONSTRAINT [DF_Location_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Location] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Location]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Location] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Organization
IF OBJECT_ID(N'[fhir].[Organization]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Organization]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Organization]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Organization] ADD CONSTRAINT [DF_Organization_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Organization]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Organization] ADD CONSTRAINT [DF_Organization_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Organization]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Organization] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.CarePlan
IF OBJECT_ID(N'[fhir].[CarePlan]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[CarePlan]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[CarePlan]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD CONSTRAINT [DF_CarePlan_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ADD CONSTRAINT [DF_CarePlan_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.CarePlan_Activity
IF OBJECT_ID(N'[fhir].[CarePlan_Activity]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[CarePlan_Activity]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[CarePlan_Activity]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ADD CONSTRAINT [DF_CarePlan_Activity_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[CarePlan_Activity]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[CarePlan_Activity] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.ServiceRequest
IF OBJECT_ID(N'[fhir].[ServiceRequest]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[ServiceRequest]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD CONSTRAINT [DF_ServiceRequest_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ADD CONSTRAINT [DF_ServiceRequest_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.ServiceRequest_tillaegskode
IF OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[ServiceRequest_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ADD CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ADD CONSTRAINT [DF_ServiceRequest_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.ServiceRequest_Recipient
IF OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[ServiceRequest_Recipient]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[ServiceRequest_Recipient]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ADD CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ADD CONSTRAINT [DF_ServiceRequest_Recipient_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[ServiceRequest_Recipient]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[ServiceRequest_Recipient] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Procedure
IF OBJECT_ID(N'[fhir].[Procedure]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Procedure]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Procedure]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Procedure] ADD CONSTRAINT [DF_Procedure_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Procedure] ADD CONSTRAINT [DF_Procedure_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Procedure_tillaegskode
IF OBJECT_ID(N'[fhir].[Procedure_tillaegskode]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Procedure_tillaegskode]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Procedure_tillaegskode]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ADD CONSTRAINT [DF_Procedure_tillaegskode_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ADD CONSTRAINT [DF_Procedure_tillaegskode_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Procedure_tillaegskode]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Procedure_tillaegskode] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Questionnaire
IF OBJECT_ID(N'[fhir].[Questionnaire]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Questionnaire]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Questionnaire]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ADD CONSTRAINT [DF_Questionnaire_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ADD CONSTRAINT [DF_Questionnaire_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.Questionnaire_item
IF OBJECT_ID(N'[fhir].[Questionnaire_item]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Questionnaire_item]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Questionnaire_item]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ADD CONSTRAINT [DF_Questionnaire_item_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ADD CONSTRAINT [DF_Questionnaire_item_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Questionnaire_item]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Questionnaire_item] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- fhir.QuestionnaireResponse
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD CONSTRAINT [DF_QuestionnaireResponse_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]')
                 AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_FHIR_QR_DWEffective')
            EXEC(N'DROP INDEX [IX_FHIR_QR_DWEffective] ON [fhir].[QuestionnaireResponse];');
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] DROP COLUMN [dw_timestamp_effective];');
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse]') AND name = N'IX_FHIR_QR_DWEffective')
            EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_QR_DWEffective] ON [fhir].[QuestionnaireResponse] ([dw_timestamp_effective] ASC) INCLUDE ([qr_id]) ON [PRIMARY];');
    END
END
GO

-- fhir.QuestionnaireResponse_item
IF OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse_item]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[QuestionnaireResponse_item]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD CONSTRAINT [DF_QuestionnaireResponse_item_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]')
                 AND name IN (N'dw_timestamp_insert', N'dw_timestamp_update') AND is_nullable = 1)
    BEGIN
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_FHIR_QRI_DWEffective')
            EXEC(N'DROP INDEX [IX_FHIR_QRI_DWEffective] ON [fhir].[QuestionnaireResponse_item];');
        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] DROP COLUMN [dw_timestamp_effective];');
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
        EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'dw_timestamp_effective')
            EXEC(N'ALTER TABLE [fhir].[QuestionnaireResponse_item] ADD [dw_timestamp_effective] AS ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED;');
        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[fhir].[QuestionnaireResponse_item]') AND name = N'IX_FHIR_QRI_DWEffective')
            EXEC(N'CREATE NONCLUSTERED INDEX [IX_FHIR_QRI_DWEffective] ON [fhir].[QuestionnaireResponse_item] ([dw_timestamp_effective] ASC) INCLUDE ([qr_id]) ON [PRIMARY];');
    END
END
GO

-- fhir.Appointment
IF OBJECT_ID(N'[fhir].[Appointment]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [fhir].[Appointment]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [fhir].[Appointment]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Appointment]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [fhir].[Appointment] ADD CONSTRAINT [DF_Appointment_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[fhir].[Appointment]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [fhir].[Appointment] ADD CONSTRAINT [DF_Appointment_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Appointment] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fhir].[Appointment]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [fhir].[Appointment] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- === reference/shared/security/organisation ===

-- codesystem.SFIAlias
IF OBJECT_ID(N'[codesystem].[SFIAlias]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [codesystem].[SFIAlias]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIAlias]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ADD CONSTRAINT [DF_SFIAlias_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAlias]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ADD CONSTRAINT [DF_SFIAlias_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAlias]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAlias] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- codesystem.SFIAliasMember
IF OBJECT_ID(N'[codesystem].[SFIAliasMember]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [codesystem].[SFIAliasMember]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFIAliasMember]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ADD CONSTRAINT [DF_SFIAliasMember_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ADD CONSTRAINT [DF_SFIAliasMember_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFIAliasMember] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- codesystem.SFISearchDocument
IF OBJECT_ID(N'[codesystem].[SFISearchDocument]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [codesystem].[SFISearchDocument]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFISearchDocument]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ADD CONSTRAINT [DF_SFISearchDocument_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ADD CONSTRAINT [DF_SFISearchDocument_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchDocument] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- codesystem.SFISearchOverride
IF OBJECT_ID(N'[codesystem].[SFISearchOverride]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [codesystem].[SFISearchOverride]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [codesystem].[SFISearchOverride]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ADD CONSTRAINT [DF_SFISearchOverride_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ADD CONSTRAINT [DF_SFISearchOverride_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [codesystem].[SFISearchOverride] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- meta.IFDBAktivitetstypeRouting
IF OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [meta].[IFDBAktivitetstypeRouting]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [meta].[IFDBAktivitetstypeRouting]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ADD CONSTRAINT [DF_IFDBAktivitetstypeRouting_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ADD CONSTRAINT [DF_IFDBAktivitetstypeRouting_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetstypeRouting]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetstypeRouting] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- meta.IFDBAktivitetsresultatRoutingDecision
IF OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [meta].[IFDBAktivitetsresultatRoutingDecision]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [meta].[IFDBAktivitetsresultatRoutingDecision]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ADD CONSTRAINT [DF_IFDBAktivitetsresultatRoutingDecision_dw_timestamp_insert] DEFAULT SYSDATETIME() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ADD CONSTRAINT [DF_IFDBAktivitetsresultatRoutingDecision_dw_timestamp_update] DEFAULT SYSDATETIME() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ALTER COLUMN [dw_timestamp_insert] DATETIME2 NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[meta].[IFDBAktivitetsresultatRoutingDecision]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [meta].[IFDBAktivitetsresultatRoutingDecision] ALTER COLUMN [dw_timestamp_update] DATETIME2 NOT NULL;');
END
GO

-- ref.PatientToBorger
IF OBJECT_ID(N'[ref].[PatientToBorger]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [ref].[PatientToBorger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[PatientToBorger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ADD CONSTRAINT [DF_PatientToBorger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[PatientToBorger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ADD CONSTRAINT [DF_PatientToBorger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[PatientToBorger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[PatientToBorger] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- ref.Patient_Address_To_Borger
IF OBJECT_ID(N'[ref].[Patient_Address_To_Borger]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [ref].[Patient_Address_To_Borger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[Patient_Address_To_Borger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ADD CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ADD CONSTRAINT [DF_Patient_Address_To_Borger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Patient_Address_To_Borger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Patient_Address_To_Borger] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- ref.FakturaToPostering
IF OBJECT_ID(N'[ref].[FakturaToPostering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [ref].[FakturaToPostering]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[FakturaToPostering]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ADD CONSTRAINT [DF_FakturaToPostering_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ADD CONSTRAINT [DF_FakturaToPostering_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- ref.FakturaToPostering.dw_ek_Kreditor
IF OBJECT_ID(N'[ref].[FakturaToPostering]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [ref].[FakturaToPostering] SET [dw_ek_Kreditor] = -1 WHERE [dw_ek_Kreditor] IS NULL;');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[FakturaToPostering]') AND c.name = N'dw_ek_Kreditor')
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ADD CONSTRAINT [DF_FakturaToPostering_dw_ek_Kreditor] DEFAULT -1 FOR [dw_ek_Kreditor];');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[FakturaToPostering]')
               AND name = N'dw_ek_Kreditor' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[FakturaToPostering] ALTER COLUMN [dw_ek_Kreditor] INT NOT NULL;');
END
GO

-- ref.Organisation
IF OBJECT_ID(N'[ref].[Organisation]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [ref].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [ref].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [ref].[Organisation] ADD CONSTRAINT [DF_Ref_Organisation_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[ref].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [ref].[Organisation] ADD CONSTRAINT [DF_Ref_Organisation_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[ref].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [ref].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- dim.Borger
IF OBJECT_ID(N'[dim].[Borger]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Borger]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Borger]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Borger]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Borger] ADD CONSTRAINT [DF_Dim_Borger_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Borger]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Borger] ADD CONSTRAINT [DF_Dim_Borger_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Borger] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Borger]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Borger] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- dim.Dato
IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Dato]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, CAST(SYSDATETIME() AS datetime2(0)))
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Dato]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Dato]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Dato] ADD CONSTRAINT [DF_Dim_Dato_dw_timestamp_insert] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Dato]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Dato] ADD CONSTRAINT [DF_Dim_Dato_dw_timestamp_update] DEFAULT CAST(SYSDATETIME() AS datetime2(0)) FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Dato] ALTER COLUMN [dw_timestamp_insert] DATETIME2(0) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Dato]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Dato] ALTER COLUMN [dw_timestamp_update] DATETIME2(0) NOT NULL;');
END
GO

-- sec.RBOrganisation
IF OBJECT_ID(N'[sec].[RBOrganisation]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [sec].[RBOrganisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, GETDATE())
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[sec].[RBOrganisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [sec].[RBOrganisation] ADD CONSTRAINT [DF_RBOrganisation_dw_timestamp_update] DEFAULT GETDATE() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[sec].[RBOrganisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [sec].[RBOrganisation] ALTER COLUMN [dw_timestamp_update] DATETIME2(7) NOT NULL;');
END
GO

-- map.Organisation
IF OBJECT_ID(N'[map].[Organisation]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [map].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, GETDATE())
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [map].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [map].[Organisation] ADD CONSTRAINT [DF_Map_Organisation_dw_timestamp_insert] DEFAULT GETDATE() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[map].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [map].[Organisation] ADD CONSTRAINT [DF_Map_Organisation_dw_timestamp_update] DEFAULT GETDATE() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2(7) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[map].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [map].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2(7) NOT NULL;');
END
GO

-- dim.Organisation
IF OBJECT_ID(N'[dim].[Organisation]', 'U') IS NOT NULL
BEGIN
    EXEC(N'UPDATE [dim].[Organisation]
           SET dw_timestamp_insert = COALESCE(dw_timestamp_insert, dw_timestamp_update, GETDATE())
           WHERE dw_timestamp_insert IS NULL;');
    EXEC(N'UPDATE [dim].[Organisation]
           SET dw_timestamp_update = COALESCE(dw_timestamp_update, dw_timestamp_insert)
           WHERE dw_timestamp_update IS NULL;');

    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Organisation]') AND c.name = N'dw_timestamp_insert')
        EXEC(N'ALTER TABLE [dim].[Organisation] ADD CONSTRAINT [DF_Dim_Organisation_dw_timestamp_insert] DEFAULT GETDATE() FOR [dw_timestamp_insert];');
    IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
                   JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                   WHERE dc.parent_object_id = OBJECT_ID(N'[dim].[Organisation]') AND c.name = N'dw_timestamp_update')
        EXEC(N'ALTER TABLE [dim].[Organisation] ADD CONSTRAINT [DF_Dim_Organisation_dw_timestamp_update] DEFAULT GETDATE() FOR [dw_timestamp_update];');

    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
               AND name = N'dw_timestamp_insert' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Organisation] ALTER COLUMN [dw_timestamp_insert] DATETIME2(7) NOT NULL;');
    IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dim].[Organisation]')
               AND name = N'dw_timestamp_update' AND is_nullable = 1)
        EXEC(N'ALTER TABLE [dim].[Organisation] ALTER COLUMN [dw_timestamp_update] DATETIME2(7) NOT NULL;');
END
GO
