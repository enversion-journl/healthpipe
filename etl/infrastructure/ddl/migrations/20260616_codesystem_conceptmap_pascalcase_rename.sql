-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Rename codesystem.* and conceptmap.* tables from snake_case to PascalCase
-- (acronyms UPPER, no underscores), in place, preserving data. Also renames
-- their PRIMARY KEY constraints and named NONCLUSTERED indexes for catalog
-- parity with the (PascalCase) table DDL.
--
-- Runs on BOTH staging and dso. Flat, idempotent, order-independent guarded
-- sp_rename blocks (house style; T-SQL CURSOR batches do not persist through
-- ddl_loader.execute_sql). Every rename is a no-op when the source is absent
-- or the target already exists. Conceptmap secondary indexes exist on STAGING
-- only; their guards make them no-ops on DSO.
--
-- DEFAULT (DF_*) constraints and the ftc_codesystem_sfi_search fulltext catalog
-- are intentionally left snake_case.
-- ============================================================================

-- ---- 1) Table renames ----------------------------------------------------
IF OBJECT_ID(N'[codesystem].[rekvirent_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[RekvirentConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[rekvirent_concept]', N'RekvirentConcept';
    PRINT 'Renamed codesystem.rekvirent_concept -> RekvirentConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[rekvirent_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[RekvirentProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[rekvirent_properties]', N'RekvirentProperties';
    PRINT 'Renamed codesystem.rekvirent_properties -> RekvirentProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[rekvirent_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[RekvirentHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[rekvirent_hierarchy]', N'RekvirentHierarchy';
    PRINT 'Renamed codesystem.rekvirent_hierarchy -> RekvirentHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[lager_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[LagerConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[lager_concept]', N'LagerConcept';
    PRINT 'Renamed codesystem.lager_concept -> LagerConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[lager_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[LagerProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[lager_properties]', N'LagerProperties';
    PRINT 'Renamed codesystem.lager_properties -> LagerProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[lager_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[LagerHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[lager_hierarchy]', N'LagerHierarchy';
    PRINT 'Renamed codesystem.lager_hierarchy -> LagerHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[ean_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[EANConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[ean_concept]', N'EANConcept';
    PRINT 'Renamed codesystem.ean_concept -> EANConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[ean_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[EANProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[ean_properties]', N'EANProperties';
    PRINT 'Renamed codesystem.ean_properties -> EANProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[ean_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[EANHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[ean_hierarchy]', N'EANHierarchy';
    PRINT 'Renamed codesystem.ean_hierarchy -> EANHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[sor_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SORConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sor_concept]', N'SORConcept';
    PRINT 'Renamed codesystem.sor_concept -> SORConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[sor_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SORProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sor_properties]', N'SORProperties';
    PRINT 'Renamed codesystem.sor_properties -> SORProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[sor_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SORHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sor_hierarchy]', N'SORHierarchy';
    PRINT 'Renamed codesystem.sor_hierarchy -> SORHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[okosted_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[OkostedConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[okosted_concept]', N'OkostedConcept';
    PRINT 'Renamed codesystem.okosted_concept -> OkostedConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[okosted_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[OkostedProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[okosted_properties]', N'OkostedProperties';
    PRINT 'Renamed codesystem.okosted_properties -> OkostedProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[okosted_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[OkostedHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[okosted_hierarchy]', N'OkostedHierarchy';
    PRINT 'Renamed codesystem.okosted_hierarchy -> OkostedHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFIConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_concept]', N'SFIConcept';
    PRINT 'Renamed codesystem.sfi_concept -> SFIConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFIProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_properties]', N'SFIProperties';
    PRINT 'Renamed codesystem.sfi_properties -> SFIProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFIHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_hierarchy]', N'SFIHierarchy';
    PRINT 'Renamed codesystem.sfi_hierarchy -> SFIHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_alias]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFIAlias]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_alias]', N'SFIAlias';
    PRINT 'Renamed codesystem.sfi_alias -> SFIAlias';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_alias_member]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFIAliasMember]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_alias_member]', N'SFIAliasMember';
    PRINT 'Renamed codesystem.sfi_alias_member -> SFIAliasMember';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_search_document]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFISearchDocument]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_search_document]', N'SFISearchDocument';
    PRINT 'Renamed codesystem.sfi_search_document -> SFISearchDocument';
END
GO
IF OBJECT_ID(N'[codesystem].[sfi_search_override]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SFISearchOverride]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sfi_search_override]', N'SFISearchOverride';
    PRINT 'Renamed codesystem.sfi_search_override -> SFISearchOverride';
END
GO
IF OBJECT_ID(N'[codesystem].[sks_concept]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SKSConcept]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sks_concept]', N'SKSConcept';
    PRINT 'Renamed codesystem.sks_concept -> SKSConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[sks_properties]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SKSProperties]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sks_properties]', N'SKSProperties';
    PRINT 'Renamed codesystem.sks_properties -> SKSProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[sks_hierarchy]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[SKSHierarchy]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[sks_hierarchy]', N'SKSHierarchy';
    PRINT 'Renamed codesystem.sks_hierarchy -> SKSHierarchy';
END
GO
IF OBJECT_ID(N'[conceptmap].[rekvirent_to_sor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[RekvirentToSOR]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[rekvirent_to_sor]', N'RekvirentToSOR';
    PRINT 'Renamed conceptmap.rekvirent_to_sor -> RekvirentToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[lager_to_sor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[LagerToSOR]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[lager_to_sor]', N'LagerToSOR';
    PRINT 'Renamed conceptmap.lager_to_sor -> LagerToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[ean_to_sor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[EANToSOR]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[ean_to_sor]', N'EANToSOR';
    PRINT 'Renamed conceptmap.ean_to_sor -> EANToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[okosted_to_sor]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[OkostedToSOR]', 'U') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[okosted_to_sor]', N'OkostedToSOR';
    PRINT 'Renamed conceptmap.okosted_to_sor -> OkostedToSOR';
END
GO

-- ---- 2) Primary-key constraint renames -----------------------------------
IF OBJECT_ID(N'[codesystem].[PK_codesystem_rekvirent_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_RekvirentConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_rekvirent_concept]', N'PK_codesystem_RekvirentConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_rekvirent_concept -> PK_codesystem_RekvirentConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_rekvirent_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_RekvirentProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_rekvirent_properties]', N'PK_codesystem_RekvirentProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_rekvirent_properties -> PK_codesystem_RekvirentProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_rekvirent_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_RekvirentHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_rekvirent_hierarchy]', N'PK_codesystem_RekvirentHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_rekvirent_hierarchy -> PK_codesystem_RekvirentHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_lager_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_LagerConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_lager_concept]', N'PK_codesystem_LagerConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_lager_concept -> PK_codesystem_LagerConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_lager_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_LagerProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_lager_properties]', N'PK_codesystem_LagerProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_lager_properties -> PK_codesystem_LagerProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_lager_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_LagerHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_lager_hierarchy]', N'PK_codesystem_LagerHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_lager_hierarchy -> PK_codesystem_LagerHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_ean_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_EANConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_ean_concept]', N'PK_codesystem_EANConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_ean_concept -> PK_codesystem_EANConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_ean_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_EANProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_ean_properties]', N'PK_codesystem_EANProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_ean_properties -> PK_codesystem_EANProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_ean_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_EANHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_ean_hierarchy]', N'PK_codesystem_EANHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_ean_hierarchy -> PK_codesystem_EANHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sor_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SORConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sor_concept]', N'PK_codesystem_SORConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sor_concept -> PK_codesystem_SORConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sor_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SORProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sor_properties]', N'PK_codesystem_SORProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sor_properties -> PK_codesystem_SORProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sor_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SORHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sor_hierarchy]', N'PK_codesystem_SORHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sor_hierarchy -> PK_codesystem_SORHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_okosted_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_OkostedConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_okosted_concept]', N'PK_codesystem_OkostedConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_okosted_concept -> PK_codesystem_OkostedConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_okosted_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_OkostedProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_okosted_properties]', N'PK_codesystem_OkostedProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_okosted_properties -> PK_codesystem_OkostedProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_okosted_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_OkostedHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_okosted_hierarchy]', N'PK_codesystem_OkostedHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_okosted_hierarchy -> PK_codesystem_OkostedHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFIConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_concept]', N'PK_codesystem_SFIConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_concept -> PK_codesystem_SFIConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFIProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_properties]', N'PK_codesystem_SFIProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_properties -> PK_codesystem_SFIProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFIHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_hierarchy]', N'PK_codesystem_SFIHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_hierarchy -> PK_codesystem_SFIHierarchy';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_alias]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFIAlias]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_alias]', N'PK_codesystem_SFIAlias', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_alias -> PK_codesystem_SFIAlias';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_alias_member]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFIAliasMember]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_alias_member]', N'PK_codesystem_SFIAliasMember', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_alias_member -> PK_codesystem_SFIAliasMember';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_search_document]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFISearchDocument]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_search_document]', N'PK_codesystem_SFISearchDocument', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_search_document -> PK_codesystem_SFISearchDocument';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sfi_search_override]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SFISearchOverride]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sfi_search_override]', N'PK_codesystem_SFISearchOverride', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sfi_search_override -> PK_codesystem_SFISearchOverride';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sks_concept]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SKSConcept]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sks_concept]', N'PK_codesystem_SKSConcept', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sks_concept -> PK_codesystem_SKSConcept';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sks_properties]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SKSProperties]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sks_properties]', N'PK_codesystem_SKSProperties', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sks_properties -> PK_codesystem_SKSProperties';
END
GO
IF OBJECT_ID(N'[codesystem].[PK_codesystem_sks_hierarchy]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[codesystem].[PK_codesystem_SKSHierarchy]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[codesystem].[PK_codesystem_sks_hierarchy]', N'PK_codesystem_SKSHierarchy', N'OBJECT';
    PRINT 'Renamed PK PK_codesystem_sks_hierarchy -> PK_codesystem_SKSHierarchy';
END
GO
IF OBJECT_ID(N'[conceptmap].[PK_conceptmap_rekvirent_to_sor]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[PK_conceptmap_RekvirentToSOR]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[PK_conceptmap_rekvirent_to_sor]', N'PK_conceptmap_RekvirentToSOR', N'OBJECT';
    PRINT 'Renamed PK PK_conceptmap_rekvirent_to_sor -> PK_conceptmap_RekvirentToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[PK_conceptmap_lager_to_sor]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[PK_conceptmap_LagerToSOR]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[PK_conceptmap_lager_to_sor]', N'PK_conceptmap_LagerToSOR', N'OBJECT';
    PRINT 'Renamed PK PK_conceptmap_lager_to_sor -> PK_conceptmap_LagerToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[PK_conceptmap_ean_to_sor]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[PK_conceptmap_EANToSOR]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[PK_conceptmap_ean_to_sor]', N'PK_conceptmap_EANToSOR', N'OBJECT';
    PRINT 'Renamed PK PK_conceptmap_ean_to_sor -> PK_conceptmap_EANToSOR';
END
GO
IF OBJECT_ID(N'[conceptmap].[PK_conceptmap_okosted_to_sor]', 'PK') IS NOT NULL
   AND OBJECT_ID(N'[conceptmap].[PK_conceptmap_OkostedToSOR]', 'PK') IS NULL
BEGIN
    EXEC sp_rename N'[conceptmap].[PK_conceptmap_okosted_to_sor]', N'PK_conceptmap_OkostedToSOR', N'OBJECT';
    PRINT 'Renamed PK PK_conceptmap_okosted_to_sor -> PK_conceptmap_OkostedToSOR';
END
GO

-- ---- 3) Named nonclustered index renames (objname = schema.NEWtable.index) -
IF OBJECT_ID(N'[codesystem].[RekvirentProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND name = N'IX_codesystem_rekvirent_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentProperties]') AND name = N'IX_codesystem_RekvirentProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[RekvirentProperties].[IX_codesystem_rekvirent_properties_PropertyCode]', N'IX_codesystem_RekvirentProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_rekvirent_properties_PropertyCode -> IX_codesystem_RekvirentProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[RekvirentHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND name = N'IX_codesystem_rekvirent_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[RekvirentHierarchy]') AND name = N'IX_codesystem_RekvirentHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[RekvirentHierarchy].[IX_codesystem_rekvirent_hierarchy_ChildCode]', N'IX_codesystem_RekvirentHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_rekvirent_hierarchy_ChildCode -> IX_codesystem_RekvirentHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[LagerProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND name = N'IX_codesystem_lager_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerProperties]') AND name = N'IX_codesystem_LagerProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[LagerProperties].[IX_codesystem_lager_properties_PropertyCode]', N'IX_codesystem_LagerProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_lager_properties_PropertyCode -> IX_codesystem_LagerProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[LagerHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND name = N'IX_codesystem_lager_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[LagerHierarchy]') AND name = N'IX_codesystem_LagerHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[LagerHierarchy].[IX_codesystem_lager_hierarchy_ChildCode]', N'IX_codesystem_LagerHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_lager_hierarchy_ChildCode -> IX_codesystem_LagerHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[EANProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND name = N'IX_codesystem_ean_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANProperties]') AND name = N'IX_codesystem_EANProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[EANProperties].[IX_codesystem_ean_properties_PropertyCode]', N'IX_codesystem_EANProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_ean_properties_PropertyCode -> IX_codesystem_EANProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[EANHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND name = N'IX_codesystem_ean_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[EANHierarchy]') AND name = N'IX_codesystem_EANHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[EANHierarchy].[IX_codesystem_ean_hierarchy_ChildCode]', N'IX_codesystem_EANHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_ean_hierarchy_ChildCode -> IX_codesystem_EANHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SORProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND name = N'IX_codesystem_sor_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORProperties]') AND name = N'IX_codesystem_SORProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SORProperties].[IX_codesystem_sor_properties_PropertyCode]', N'IX_codesystem_SORProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sor_properties_PropertyCode -> IX_codesystem_SORProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SORHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND name = N'IX_codesystem_sor_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SORHierarchy]') AND name = N'IX_codesystem_SORHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SORHierarchy].[IX_codesystem_sor_hierarchy_ChildCode]', N'IX_codesystem_SORHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sor_hierarchy_ChildCode -> IX_codesystem_SORHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[OkostedProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND name = N'IX_codesystem_okosted_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedProperties]') AND name = N'IX_codesystem_OkostedProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[OkostedProperties].[IX_codesystem_okosted_properties_PropertyCode]', N'IX_codesystem_OkostedProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_okosted_properties_PropertyCode -> IX_codesystem_OkostedProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[OkostedHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND name = N'IX_codesystem_okosted_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[OkostedHierarchy]') AND name = N'IX_codesystem_OkostedHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[OkostedHierarchy].[IX_codesystem_okosted_hierarchy_ChildCode]', N'IX_codesystem_OkostedHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_okosted_hierarchy_ChildCode -> IX_codesystem_OkostedHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SFIConcept]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND name = N'IX_codesystem_sfi_concept_Display')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIConcept]') AND name = N'IX_codesystem_SFIConcept_Display')
BEGIN
    EXEC sp_rename N'[codesystem].[SFIConcept].[IX_codesystem_sfi_concept_Display]', N'IX_codesystem_SFIConcept_Display', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_concept_Display -> IX_codesystem_SFIConcept_Display';
END
GO
IF OBJECT_ID(N'[codesystem].[SFIProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND name = N'IX_codesystem_sfi_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIProperties]') AND name = N'IX_codesystem_SFIProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SFIProperties].[IX_codesystem_sfi_properties_PropertyCode]', N'IX_codesystem_SFIProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_properties_PropertyCode -> IX_codesystem_SFIProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SFIHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND name = N'IX_codesystem_sfi_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIHierarchy]') AND name = N'IX_codesystem_SFIHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SFIHierarchy].[IX_codesystem_sfi_hierarchy_ChildCode]', N'IX_codesystem_SFIHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_hierarchy_ChildCode -> IX_codesystem_SFIHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SFIAliasMember]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND name = N'IX_codesystem_sfi_alias_member_SfiCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFIAliasMember]') AND name = N'IX_codesystem_SFIAliasMember_SfiCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SFIAliasMember].[IX_codesystem_sfi_alias_member_SfiCode]', N'IX_codesystem_SFIAliasMember_SfiCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_alias_member_SfiCode -> IX_codesystem_SFIAliasMember_SfiCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SFISearchDocument]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND name = N'IX_codesystem_sfi_search_document_Model')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchDocument]') AND name = N'IX_codesystem_SFISearchDocument_Model')
BEGIN
    EXEC sp_rename N'[codesystem].[SFISearchDocument].[IX_codesystem_sfi_search_document_Model]', N'IX_codesystem_SFISearchDocument_Model', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_search_document_Model -> IX_codesystem_SFISearchDocument_Model';
END
GO
IF OBJECT_ID(N'[codesystem].[SFISearchOverride]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND name = N'IX_codesystem_sfi_search_override_Phrase')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SFISearchOverride]') AND name = N'IX_codesystem_SFISearchOverride_Phrase')
BEGIN
    EXEC sp_rename N'[codesystem].[SFISearchOverride].[IX_codesystem_sfi_search_override_Phrase]', N'IX_codesystem_SFISearchOverride_Phrase', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sfi_search_override_Phrase -> IX_codesystem_SFISearchOverride_Phrase';
END
GO
IF OBJECT_ID(N'[codesystem].[SKSProperties]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND name = N'IX_codesystem_sks_properties_PropertyCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSProperties]') AND name = N'IX_codesystem_SKSProperties_PropertyCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SKSProperties].[IX_codesystem_sks_properties_PropertyCode]', N'IX_codesystem_SKSProperties_PropertyCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sks_properties_PropertyCode -> IX_codesystem_SKSProperties_PropertyCode';
END
GO
IF OBJECT_ID(N'[codesystem].[SKSHierarchy]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND name = N'IX_codesystem_sks_hierarchy_ChildCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[codesystem].[SKSHierarchy]') AND name = N'IX_codesystem_SKSHierarchy_ChildCode')
BEGIN
    EXEC sp_rename N'[codesystem].[SKSHierarchy].[IX_codesystem_sks_hierarchy_ChildCode]', N'IX_codesystem_SKSHierarchy_ChildCode', N'INDEX';
    PRINT 'Renamed index IX_codesystem_sks_hierarchy_ChildCode -> IX_codesystem_SKSHierarchy_ChildCode';
END
GO
IF OBJECT_ID(N'[conceptmap].[RekvirentToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND name = N'IX_conceptmap_rekvirent_to_sor_TargetCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[RekvirentToSOR]') AND name = N'IX_conceptmap_RekvirentToSOR_TargetCode')
BEGIN
    EXEC sp_rename N'[conceptmap].[RekvirentToSOR].[IX_conceptmap_rekvirent_to_sor_TargetCode]', N'IX_conceptmap_RekvirentToSOR_TargetCode', N'INDEX';
    PRINT 'Renamed index IX_conceptmap_rekvirent_to_sor_TargetCode -> IX_conceptmap_RekvirentToSOR_TargetCode';
END
GO
IF OBJECT_ID(N'[conceptmap].[LagerToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND name = N'IX_conceptmap_lager_to_sor_TargetCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[LagerToSOR]') AND name = N'IX_conceptmap_LagerToSOR_TargetCode')
BEGIN
    EXEC sp_rename N'[conceptmap].[LagerToSOR].[IX_conceptmap_lager_to_sor_TargetCode]', N'IX_conceptmap_LagerToSOR_TargetCode', N'INDEX';
    PRINT 'Renamed index IX_conceptmap_lager_to_sor_TargetCode -> IX_conceptmap_LagerToSOR_TargetCode';
END
GO
IF OBJECT_ID(N'[conceptmap].[EANToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND name = N'IX_conceptmap_ean_to_sor_TargetCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[EANToSOR]') AND name = N'IX_conceptmap_EANToSOR_TargetCode')
BEGIN
    EXEC sp_rename N'[conceptmap].[EANToSOR].[IX_conceptmap_ean_to_sor_TargetCode]', N'IX_conceptmap_EANToSOR_TargetCode', N'INDEX';
    PRINT 'Renamed index IX_conceptmap_ean_to_sor_TargetCode -> IX_conceptmap_EANToSOR_TargetCode';
END
GO
IF OBJECT_ID(N'[conceptmap].[OkostedToSOR]', 'U') IS NOT NULL
   AND EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND name = N'IX_conceptmap_okosted_to_sor_TargetCode')
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[conceptmap].[OkostedToSOR]') AND name = N'IX_conceptmap_OkostedToSOR_TargetCode')
BEGIN
    EXEC sp_rename N'[conceptmap].[OkostedToSOR].[IX_conceptmap_okosted_to_sor_TargetCode]', N'IX_conceptmap_OkostedToSOR_TargetCode', N'INDEX';
    PRINT 'Renamed index IX_conceptmap_okosted_to_sor_TargetCode -> IX_conceptmap_OkostedToSOR_TargetCode';
END
GO
