-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Drop [ref].* CodeSystem tables on DSO.
--
-- CodeSystem data now lives in [codesystem] on both staging and DSO (synced by
-- dso_sync_codesystem_*). The [ref] schema is reserved for genuine cross-entity
-- bridge tables (Patient_Borger, Patient_Address_Borger, Faktura_To_Postering).
-- Companion to 20260612_drop_codesystem_ref_views.sql (which dropped type='V').
-- Data re-derives from [codesystem] on the next sync, so a plain DROP is safe.
-- DSO-only migration.
-- ============================================================================

DECLARE @tables TABLE (name SYSNAME);
INSERT INTO @tables (name) VALUES
    ('SKS_Concept'), ('SKS_Properties'), ('SKS_Hierarchy'),
    ('SOR_Concept'), ('SOR_Properties'), ('SOR_Hierarchy'),
    ('EAN_Concept'), ('EAN_Properties'), ('EAN_Hierarchy'),
    ('Lager_Concept'), ('Lager_Properties'), ('Lager_Hierarchy'),
    ('Okosted_Concept'), ('Okosted_Properties'), ('Okosted_Hierarchy'),
    ('Rekvirent_Concept'), ('Rekvirent_Properties'), ('Rekvirent_Hierarchy'),
    ('SFI_Concept'), ('SFI_Properties'), ('SFI_Hierarchy');

DECLARE @name SYSNAME, @sql NVARCHAR(MAX);
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM @tables;
OPEN c;
FETCH NEXT FROM c INTO @name;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(N'[ref].[' + @name + N']', 'U') IS NOT NULL
    BEGIN
        SET @sql = N'DROP TABLE [ref].[' + @name + N'];';
        EXEC sp_executesql @sql;
        PRINT 'Dropped ref.' + @name;
    END
    FETCH NEXT FROM c INTO @name;
END
CLOSE c;
DEALLOCATE c;
GO
