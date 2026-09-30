-- SPDX-FileCopyrightText: 2026 Enversion Journl ApS
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Healthpipe. See the LICENSE file in the project
-- root for the full AGPL-3.0 license text.

-- ============================================================================
-- Re-apply the 2026-07-08 index / primary-key renames.
--
-- 20260708_rename_90_indexes.sql renamed these objects on databases that
-- already held them. It could not touch a table that did not exist yet at that
-- point, and the base DDL under ddl/tables/ went on creating those tables with
-- the PRE-rename names afterwards. Any table provisioned after the original
-- rename therefore ended up back on the old name, and any table that already
-- existed ended up with BOTH: the renamed original plus a duplicate that the
-- base DDL re-created under the old name, because its
-- "IF NOT EXISTS (... WHERE name = '<old>')" guard no longer matched.
--
-- The base DDL now emits the post-rename names, which fixes new databases.
-- This migration brings already-provisioned databases to the same shape:
--
--   * old name present, new name free      -> rename it
--   * both present and structurally equal  -> drop the redundant old duplicate
--   * both present but NOT equal           -> leave alone and report
--
-- Name matching is case-sensitive (Latin1_General_100_CS_AS): many of these
-- renames differ only in case (IX_..._DW_Timestamp_Update ->
-- IX_..._dw_timestamp_update), which the database's case-insensitive default
-- collation would treat as already-applied. Matching is by index name, unique
-- per database for every name below, so a rename follows its object even if
-- the table was renamed in between. Every branch is guarded, so re-running is
-- a no-op.
-- ============================================================================

DECLARE @renames TABLE (old_name SYSNAME NOT NULL PRIMARY KEY, new_name SYSNAME NOT NULL);

INSERT INTO @renames (old_name, new_name) VALUES
    (N'IX_Dim_AfdelingLagerbeholdningEgenskaber_CleanCode', N'IX_Dim_AfdelingLagerbeholdningEgenskaber_clean_code'),
    (N'IX_Dim_IlsVare_CleanCode', N'IX_Dim_ILSVare_clean_code'),
    (N'IX_Dim_IlsVare_UNSPSC', N'IX_Dim_ILSVare_UNSPSC'),
    (N'IX_Dim_IlsVare_UNSPSC_Intern', N'IX_Dim_ILSVare_UNSPSC_Intern'),
    (N'IX_Dim_LagerbeholdningEgenskaber_CleanCode', N'IX_Dim_LagerbeholdningEgenskaber_clean_code'),
    (N'IX_Dim_Vareudlevering_CleanCode', N'IX_Dim_Vareudlevering_clean_code'),
    (N'IX_FHIR_Procedure_Code', N'IX_FHIR_Procedure_code'),
    (N'IX_Fct_AfdelingLagerbeholdning_IlsVare', N'IX_Fct_AfdelingLagerbeholdning_ILSVare'),
    (N'IX_Fct_Faktura_EAN', N'IX_Fct_Faktura_ean'),
    (N'IX_Fct_Fakturalinje_CleanCode', N'IX_Fct_Fakturalinje_clean_code'),
    (N'IX_Fct_Fakturalinje_EAN', N'IX_Fct_Fakturalinje_ean'),
    (N'IX_Fct_Lagerbeholdning_IlsVare', N'IX_Fct_Lagerbeholdning_ILSVare'),
    (N'IX_Fct_Vareudlevering_CleanCode', N'IX_Fct_Vareudlevering_clean_code'),
    (N'IX_Fct_Vareudlevering_IlsVare', N'IX_Fct_Vareudlevering_ILSVare'),
    (N'IX_Map_AfdelingLagerbeholdningEgenskaber_CleanCode', N'IX_Map_AfdelingLagerbeholdningEgenskaber_clean_code'),
    (N'IX_Map_AfdelingLagerbeholdning_CleanCode', N'IX_Map_AfdelingLagerbeholdning_clean_code'),
    (N'IX_Map_Fakturalinje_CleanCode_FakturaLines', N'IX_Map_Fakturalinje_clean_code_FakturaLines'),
    (N'IX_Map_IlsVare_DW_EK', N'IX_Map_ILSVare_DW_EK'),
    (N'IX_Map_LagerbeholdningEgenskaber_CleanCode', N'IX_Map_LagerbeholdningEgenskaber_clean_code'),
    (N'IX_Map_Lagerbeholdning_CleanCode', N'IX_Map_Lagerbeholdning_clean_code'),
    (N'IX_Map_Postering_CleanCode', N'IX_Map_Postering_clean_code'),
    (N'IX_RBOrganisation_Bruger', N'IX_RBOrganisation_bruger'),
    (N'IX_Ref_Patient_Address_Borger_Temporal', N'IX_Ref_Patient_Address_To_Borger_Temporal'),
    (N'IX_arc_encounter_ifdb_DW_Timestamp_Update', N'IX_arc_ifdb_encounter_dw_timestamp_update'),
    (N'IX_arc_encounter_ifdb_IFDB_MODIFICATION_DATETIME', N'IX_arc_ifdb_encounter_IFDB_MODIFICATION_DATETIME'),
    (N'IX_arc_encounter_ifdb_PATIENT_ID', N'IX_arc_ifdb_encounter_patient_id_ifdb'),
    (N'IX_arc_episode_of_care_ifdb_DW_Timestamp_Update', N'IX_arc_ifdb_episode_of_care_dw_timestamp_update'),
    (N'IX_arc_episode_of_care_ifdb_IFDB_MODIFICATION_DATETIME', N'IX_arc_ifdb_episode_of_care_IFDB_MODIFICATION_DATETIME'),
    (N'IX_arc_episode_of_care_ifdb_PATIENT_ID', N'IX_arc_ifdb_episode_of_care_patient_id_ifdb'),
    (N'IX_arc_ils_afdlgrbeh_DW_Timestamp_Update', N'IX_arc_ils_afdlgrbeh_dw_timestamp_update'),
    (N'IX_arc_ils_indkfak_DW_Timestamp_Update', N'IX_arc_ils_indkfak_dw_timestamp_update'),
    (N'IX_arc_ils_laftd_DW_Timestamp_Update', N'IX_arc_ils_laftd_dw_timestamp_update'),
    (N'IX_arc_ils_laftdpris_DW_Timestamp_Update', N'IX_arc_ils_laftdpris_dw_timestamp_update'),
    (N'IX_arc_ils_laftdstam_DW_Timestamp_Update', N'IX_arc_ils_laftdstam_dw_timestamp_update'),
    (N'IX_arc_ils_lafth_DW_Timestamp_Update', N'IX_arc_ils_lafth_dw_timestamp_update'),
    (N'IX_arc_ils_lev_DW_Timestamp_Update', N'IX_arc_ils_lev_dw_timestamp_update'),
    (N'IX_arc_ils_lgr_DW_Timestamp_Update', N'IX_arc_ils_lgr_dw_timestamp_update'),
    (N'IX_arc_ils_lgrbeh_DW_Timestamp_Update', N'IX_arc_ils_lgrbeh_dw_timestamp_update'),
    (N'IX_arc_ils_lgrvare_DW_Timestamp_Update', N'IX_arc_ils_lgrvare_dw_timestamp_update'),
    (N'IX_arc_ils_moms_DW_Timestamp_Update', N'IX_arc_ils_moms_dw_timestamp_update'),
    (N'IX_arc_ils_pmdm_DW_Timestamp_Update', N'IX_arc_ils_pmdm_dw_timestamp_update'),
    (N'IX_arc_ils_rekv_DW_Timestamp_Update', N'IX_arc_ils_rekv_dw_timestamp_update'),
    (N'IX_arc_ils_rekvgrp_DW_Timestamp_Update', N'IX_arc_ils_rekvgrp_dw_timestamp_update'),
    (N'IX_arc_ils_sgh_DW_Timestamp_Update', N'IX_arc_ils_sgh_dw_timestamp_update'),
    (N'IX_arc_ils_udlev_DW_Timestamp_Update', N'IX_arc_ils_udlev_dw_timestamp_update'),
    (N'IX_arc_ils_vareart_DW_Timestamp_Update', N'IX_arc_ils_vareart_dw_timestamp_update'),
    (N'IX_arc_ils_varegrp_DW_Timestamp_Update', N'IX_arc_ils_varegrp_dw_timestamp_update'),
    (N'IX_arc_ils_varetype_DW_Timestamp_Update', N'IX_arc_ils_varetype_dw_timestamp_update'),
    (N'IX_arc_ils_zz_oper_DW_Timestamp_Update', N'IX_arc_ils_zz_oper_dw_timestamp_update'),
    (N'IX_arc_location_ifdb_DW_Timestamp_Update', N'IX_arc_ifdb_location_dw_timestamp_update'),
    (N'IX_arc_location_ifdb_LOCATION_CODE', N'IX_arc_ifdb_location_LOCATION_CODE'),
    (N'IX_arc_mysupply_metadata_DW_Timestamp_Update', N'IX_arc_mysupply_metadata_dw_timestamp_update'),
    (N'IX_arc_mysupply_statistic_invoice_header_DW_Timestamp_Update', N'IX_arc_mysupply_statistic_invoice_header_dw_timestamp_update'),
    (N'IX_arc_mysupply_statistic_invoice_header_allowance_charge_DW_Timestamp_Update', N'IX_arc_mysupply_statistic_invoice_header_allowance_charge_dw_timestamp_update'),
    (N'IX_arc_mysupply_statistic_invoice_line_DW_Timestamp_Update', N'IX_arc_mysupply_statistic_invoice_line_dw_timestamp_update'),
    (N'IX_arc_mysupply_statistic_invoice_line_allowance_charge_DW_Timestamp_Update', N'IX_arc_mysupply_statistic_invoice_line_allowance_charge_dw_timestamp_update'),
    (N'IX_arc_patient_ifdb_CPR', N'IX_arc_ifdb_patient_CPR'),
    (N'IX_arc_patient_ifdb_DW_Timestamp_Update', N'IX_arc_ifdb_patient_dw_timestamp_update'),
    (N'IX_arc_patient_ifdb_MODTIDSPUNKT', N'IX_arc_ifdb_patient_MODTIDSPUNKT'),
    (N'IX_codesystem_EANHierarchy_ChildCode', N'IX_codesystem_EANHierarchy_child_code'),
    (N'IX_codesystem_EANProperties_PropertyCode', N'IX_codesystem_EANProperties_property_code'),
    (N'IX_codesystem_LagerHierarchy_ChildCode', N'IX_codesystem_LagerHierarchy_child_code'),
    (N'IX_codesystem_LagerProperties_PropertyCode', N'IX_codesystem_LagerProperties_property_code'),
    (N'IX_codesystem_OkostedHierarchy_ChildCode', N'IX_codesystem_OkostedHierarchy_child_code'),
    (N'IX_codesystem_OkostedProperties_PropertyCode', N'IX_codesystem_OkostedProperties_property_code'),
    (N'IX_codesystem_RekvirentHierarchy_ChildCode', N'IX_codesystem_RekvirentHierarchy_child_code'),
    (N'IX_codesystem_RekvirentProperties_PropertyCode', N'IX_codesystem_RekvirentProperties_property_code'),
    (N'IX_codesystem_SFIAliasMember_SfiCode', N'IX_codesystem_SFIAliasMember_sfi_code'),
    (N'IX_codesystem_SFIConcept_Display', N'IX_codesystem_SFIConcept_display'),
    (N'IX_codesystem_SFIHierarchy_ChildCode', N'IX_codesystem_SFIHierarchy_child_code'),
    (N'IX_codesystem_SFIProperties_PropertyCode', N'IX_codesystem_SFIProperties_property_code'),
    (N'IX_codesystem_SHAKHierarchy_ChildCode', N'IX_codesystem_SHAKHierarchy_child_code'),
    (N'IX_codesystem_SHAKProperties_PropertyCode', N'IX_codesystem_SHAKProperties_property_code'),
    (N'IX_codesystem_SKSHierarchy_ChildCode', N'IX_codesystem_SKSHierarchy_child_code'),
    (N'IX_codesystem_SKSProperties_PropertyCode', N'IX_codesystem_SKSProperties_property_code'),
    (N'IX_codesystem_SORHierarchy_ChildCode', N'IX_codesystem_SORHierarchy_child_code'),
    (N'IX_codesystem_SORProperties_PropertyCode', N'IX_codesystem_SORProperties_property_code'),
    (N'IX_conceptmap_EANToSOR_TargetCode', N'IX_conceptmap_EANToSOR_target_code'),
    (N'IX_conceptmap_LagerToSOR_TargetCode', N'IX_conceptmap_LagerToSOR_target_code'),
    (N'IX_conceptmap_OkostedToSOR_TargetCode', N'IX_conceptmap_OkostedToSOR_target_code'),
    (N'IX_conceptmap_RekvirentToSOR_TargetCode', N'IX_conceptmap_RekvirentToSOR_target_code'),
    (N'IX_conceptmap_SHAKToSOR_TargetCode', N'IX_conceptmap_SHAKToSOR_target_code'),
    (N'IX_dfm_run_log_running', N'IX_DFMRunLog_running'),
    (N'IX_dim_EAN_EAN', N'IX_dim_ean_ean'),
    (N'IX_dim_Lager_Code', N'IX_dim_Lager_code'),
    (N'IX_dim_Okosted_CodeOkosted', N'IX_dim_Okosted_code_okosted'),
    (N'IX_dim_Rekvirent_Code', N'IX_dim_Rekvirent_code'),
    (N'IX_dim_SHAK_CodeSHAK', N'IX_dim_SHAK_code_shak'),
    (N'IX_dqa_results_latest', N'IX_dqa_Results_latest'),
    (N'IX_map_EAN_CleanCode', N'IX_map_ean_clean_code'),
    (N'IX_map_Lager_CleanCode', N'IX_map_Lager_clean_code'),
    (N'IX_map_Okosted_CleanCode_Okosted', N'IX_map_Okosted_clean_code_Okosted'),
    (N'IX_map_Organisation_CleanCode', N'IX_map_Organisation_clean_code'),
    (N'IX_map_Rekvirent_CleanCode', N'IX_map_Rekvirent_clean_code'),
    (N'IX_map_SHAK_CleanCode_SHAK', N'IX_map_SHAK_clean_code_SHAK'),
    (N'IX_ref_kmd_fakturalinje_unspsc_match_lookup', N'IX_ref_KMDFakturalinjeUNSPSCMatch_lookup'),
    (N'IX_ref_kmd_fakturalinje_unspsc_match_timestamp', N'IX_ref_KMDFakturalinjeUNSPSCMatch_timestamp'),
    (N'PK_Dim_IlsVare', N'PK_Dim_ILSVare'),
    (N'PK_Map_EAN', N'PK_Map_ean'),
    (N'PK_Map_IlsVare', N'PK_Map_ILSVare'),
    (N'PK_Ref_Faktura_To_Postering', N'PK_Ref_FakturaToPostering'),
    (N'PK_Ref_Patient_Address_Borger', N'PK_Ref_Patient_Address_To_Borger'),
    (N'PK_Ref_Patient_Borger', N'PK_Ref_PatientToBorger'),
    (N'PK_arc_encounter_ifdb', N'PK_arc_ifdb_encounter'),
    (N'PK_arc_episode_of_care_ifdb', N'PK_arc_ifdb_episode_of_care'),
    (N'PK_arc_location_ifdb', N'PK_arc_ifdb_location'),
    (N'PK_arc_patient_ifdb', N'PK_arc_ifdb_patient'),
    (N'PK_dfm_run_log', N'PK_DFMRunLog'),
    (N'PK_dfm_state', N'PK_DFMState'),
    (N'PK_ifdb_aktivitetstype_routing', N'PK_IFDBAktivitetstypeRouting'),
    (N'PK_sync_watermarks', N'PK_SyncWatermarks'),
    (N'UQ_RBOrganisation_Bruger_Org', N'UQ_RBOrganisation_bruger_Org'),
    (N'UQ_Ref_Patient_Borger_PatientId', N'UQ_Ref_PatientToBorger_PatientId'),
    (N'UX_Ref_Patient_Address_Borger_PatientPeriod', N'UX_Ref_Patient_Address_To_Borger_PatientPeriod'),
    (N'UX_ref_kmd_fakturalinje_unspsc_match_keys', N'UX_ref_KMDFakturalinjeUNSPSCMatch_keys');

DECLARE @old SYSNAME, @new SYSNAME, @table NVARCHAR(520), @qualified NVARCHAR(776);
DECLARE @object_id INT, @old_index_id INT, @new_index_id INT;
DECLARE @old_is_constraint BIT, @old_sig NVARCHAR(MAX), @new_sig NVARCHAR(MAX);
DECLARE @sql NVARCHAR(MAX);
DECLARE @renamed INT = 0, @dropped INT = 0, @conflicted INT = 0;

DECLARE rename_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT old_name, new_name FROM @renames;

OPEN rename_cursor;
FETCH NEXT FROM rename_cursor INTO @old, @new;

WHILE @@FETCH_STATUS = 0
BEGIN
    SELECT @object_id = NULL, @old_index_id = NULL, @new_index_id = NULL,
           @table = NULL, @old_is_constraint = NULL;

    SELECT @object_id        = i.object_id,
           @old_index_id     = i.index_id,
           @old_is_constraint = CASE WHEN i.is_primary_key = 1 OR i.is_unique_constraint = 1
                                     THEN 1 ELSE 0 END,
           @table            = QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
    FROM sys.indexes i
    JOIN sys.tables  t ON t.object_id = i.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE i.name COLLATE Latin1_General_100_CS_AS = @old COLLATE Latin1_General_100_CS_AS;

    IF @object_id IS NOT NULL
    BEGIN
        SELECT @new_index_id = i.index_id
        FROM sys.indexes i
        WHERE i.object_id = @object_id
          AND i.name COLLATE Latin1_General_100_CS_AS = @new COLLATE Latin1_General_100_CS_AS;

        IF @new_index_id IS NULL
        BEGIN
            SET @qualified = @table + N'.' + QUOTENAME(@old);
            EXEC sp_rename @qualified, @new, 'INDEX';
            SET @renamed = @renamed + 1;
        END
        ELSE
        BEGIN
            -- Both names exist: the base DDL re-created a duplicate under the old
            -- name. Drop it only when it is provably the same index.
            SET @old_sig = CONVERT(NVARCHAR(MAX), (
            SELECT i.type_desc, i.is_unique, i.is_primary_key, ISNULL(i.filter_definition, N''),
                   ISNULL(STUFF((SELECT N',' + c.name
                                        + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
                                 FROM sys.index_columns ic
                                 JOIN sys.columns c ON c.object_id = ic.object_id
                                                   AND c.column_id = ic.column_id
                                 WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                                   AND ic.is_included_column = 0
                                 ORDER BY ic.key_ordinal FOR XML PATH('')), 1, 1, N''), N''),
                   ISNULL(STUFF((SELECT N',' + c.name
                                 FROM sys.index_columns ic
                                 JOIN sys.columns c ON c.object_id = ic.object_id
                                                   AND c.column_id = ic.column_id
                                 WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                                   AND ic.is_included_column = 1
                                 ORDER BY c.name FOR XML PATH('')), 1, 1, N''), N'')
            FROM sys.indexes i
            WHERE i.object_id = @object_id AND i.index_id = @old_index_id
            FOR XML PATH('sig')
        ));
            SET @new_sig = CONVERT(NVARCHAR(MAX), (
            SELECT i.type_desc, i.is_unique, i.is_primary_key, ISNULL(i.filter_definition, N''),
                   ISNULL(STUFF((SELECT N',' + c.name
                                        + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
                                 FROM sys.index_columns ic
                                 JOIN sys.columns c ON c.object_id = ic.object_id
                                                   AND c.column_id = ic.column_id
                                 WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                                   AND ic.is_included_column = 0
                                 ORDER BY ic.key_ordinal FOR XML PATH('')), 1, 1, N''), N''),
                   ISNULL(STUFF((SELECT N',' + c.name
                                 FROM sys.index_columns ic
                                 JOIN sys.columns c ON c.object_id = ic.object_id
                                                   AND c.column_id = ic.column_id
                                 WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                                   AND ic.is_included_column = 1
                                 ORDER BY c.name FOR XML PATH('')), 1, 1, N''), N'')
            FROM sys.indexes i
            WHERE i.object_id = @object_id AND i.index_id = @new_index_id
            FOR XML PATH('sig')
        ));

            IF @old_is_constraint = 0 AND @old_sig = @new_sig
            BEGIN
                SET @sql = N'DROP INDEX ' + QUOTENAME(@old) + N' ON ' + @table;
                EXEC sp_executesql @sql;
                SET @dropped = @dropped + 1;
            END
            ELSE
            BEGIN
                SET @conflicted = @conflicted + 1;
                PRINT 'Left in place (not a plain duplicate): ' + @table + N'.' + @old;
            END
        END
    END

    FETCH NEXT FROM rename_cursor INTO @old, @new;
END

CLOSE rename_cursor;
DEALLOCATE rename_cursor;

PRINT 'Index rename re-apply: ' + CAST(@renamed AS NVARCHAR(10)) + ' renamed, '
    + CAST(@dropped AS NVARCHAR(10)) + ' redundant duplicate(s) dropped, '
    + CAST(@conflicted AS NVARCHAR(10)) + ' left for review (of 115 known renames).';
