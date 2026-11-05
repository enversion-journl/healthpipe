-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Allow role 'P' (populationsansvar per security domain) in sec.RolleDomaene:
-- recreate CK_Rolle as IN ('U', 'K', 'P').
--
-- Idempotent: the drop runs only while CK_Rolle does not allow 'P', and the add
-- runs only while CK_Rolle is absent. Runs on both staging and DSO.
-- ============================================================================

IF OBJECT_ID(N'[sec].[RolleDomaene]', 'U') IS NOT NULL
   AND EXISTS (
       SELECT 1 FROM sys.check_constraints
       WHERE [name] = N'CK_Rolle'
         AND [parent_object_id] = OBJECT_ID(N'[sec].[RolleDomaene]')
         AND [definition] NOT LIKE N'%''P''%'
   )
    ALTER TABLE [sec].[RolleDomaene] DROP CONSTRAINT [CK_Rolle];
GO

IF OBJECT_ID(N'[sec].[RolleDomaene]', 'U') IS NOT NULL
   AND NOT EXISTS (
       SELECT 1 FROM sys.check_constraints
       WHERE [name] = N'CK_Rolle'
         AND [parent_object_id] = OBJECT_ID(N'[sec].[RolleDomaene]')
   )
    ALTER TABLE [sec].[RolleDomaene]
        ADD CONSTRAINT [CK_Rolle] CHECK ([Rolle] IN ('U', 'K', 'P'));
GO
