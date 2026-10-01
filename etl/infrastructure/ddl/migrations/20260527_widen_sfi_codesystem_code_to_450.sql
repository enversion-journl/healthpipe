-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- Widen SFI codesystem Code columns from NVARCHAR(255) to NVARCHAR(450).
-- SFI hierarchical paths from IFDB XML can exceed 255 chars due to nested
-- group/element names used as fallback identifiers in generate_sfi_code().
-- 450 is the max for a clustered PK on NVARCHAR (900-byte index key limit).
-- Truncation is not acceptable as it would break PK uniqueness guarantees.

-- sfi_concept: widen Code PK
IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'codesystem' AND t.name = 'sfi_concept'
      AND c.name = 'Code' AND c.max_length < 900
)
BEGIN
    -- Must drop and recreate PK to alter a PK column
    ALTER TABLE [codesystem].[sfi_concept] DROP CONSTRAINT [PK_codesystem_sfi_concept];
    ALTER TABLE [codesystem].[sfi_concept] ALTER COLUMN [Code] NVARCHAR(450) NOT NULL;
    ALTER TABLE [codesystem].[sfi_concept] ADD CONSTRAINT [PK_codesystem_sfi_concept] PRIMARY KEY CLUSTERED ([Code]);
END
GO

-- sfi_properties: widen Code (part of composite PK)
IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'codesystem' AND t.name = 'sfi_properties'
      AND c.name = 'Code' AND c.max_length < 900
)
BEGIN
    ALTER TABLE [codesystem].[sfi_properties] DROP CONSTRAINT [PK_codesystem_sfi_properties];
    -- Drop index that includes Code
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_codesystem_sfi_properties_PropertyCode' AND object_id = OBJECT_ID('[codesystem].[sfi_properties]'))
        DROP INDEX [IX_codesystem_sfi_properties_PropertyCode] ON [codesystem].[sfi_properties];
    ALTER TABLE [codesystem].[sfi_properties] ALTER COLUMN [Code] NVARCHAR(450) NOT NULL;
    ALTER TABLE [codesystem].[sfi_properties] ADD CONSTRAINT [PK_codesystem_sfi_properties] PRIMARY KEY CLUSTERED ([Code], [PropertyCode]);
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_properties_PropertyCode]
        ON [codesystem].[sfi_properties] ([PropertyCode]) INCLUDE ([Code], [Value]);
END
GO

-- sfi_hierarchy: widen ParentCode + ChildCode (composite PK)
IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'codesystem' AND t.name = 'sfi_hierarchy'
      AND c.name = 'ParentCode' AND c.max_length < 900
)
BEGIN
    ALTER TABLE [codesystem].[sfi_hierarchy] DROP CONSTRAINT [PK_codesystem_sfi_hierarchy];
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_codesystem_sfi_hierarchy_ChildCode' AND object_id = OBJECT_ID('[codesystem].[sfi_hierarchy]'))
        DROP INDEX [IX_codesystem_sfi_hierarchy_ChildCode] ON [codesystem].[sfi_hierarchy];
    ALTER TABLE [codesystem].[sfi_hierarchy] ALTER COLUMN [ParentCode] NVARCHAR(450) NOT NULL;
    ALTER TABLE [codesystem].[sfi_hierarchy] ALTER COLUMN [ChildCode] NVARCHAR(450) NOT NULL;
    ALTER TABLE [codesystem].[sfi_hierarchy] ADD CONSTRAINT [PK_codesystem_sfi_hierarchy] PRIMARY KEY CLUSTERED ([ParentCode], [ChildCode]);
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_hierarchy_ChildCode]
        ON [codesystem].[sfi_hierarchy] ([ChildCode]) INCLUDE ([ParentCode]);
END
GO

-- sfi_alias_member: widen SfiCode (FK-like reference to sfi_concept.Code)
IF EXISTS (
    SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'codesystem' AND t.name = 'sfi_alias_member'
      AND c.name = 'SfiCode' AND c.max_length < 900
)
BEGIN
    ALTER TABLE [codesystem].[sfi_alias_member] DROP CONSTRAINT [PK_codesystem_sfi_alias_member];
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_codesystem_sfi_alias_member_SfiCode' AND object_id = OBJECT_ID('[codesystem].[sfi_alias_member]'))
        DROP INDEX [IX_codesystem_sfi_alias_member_SfiCode] ON [codesystem].[sfi_alias_member];
    ALTER TABLE [codesystem].[sfi_alias_member] ALTER COLUMN [SfiCode] NVARCHAR(450) NOT NULL;
    ALTER TABLE [codesystem].[sfi_alias_member] ADD CONSTRAINT [PK_codesystem_sfi_alias_member] PRIMARY KEY CLUSTERED ([AliasCode], [SfiCode]);
    CREATE NONCLUSTERED INDEX [IX_codesystem_sfi_alias_member_SfiCode]
        ON [codesystem].[sfi_alias_member] ([SfiCode]) INCLUDE ([AliasCode], [MemberRole], [SortOrder]);
END
GO
