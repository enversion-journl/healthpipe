# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Sync Configurations

Defines table configurations for syncing data from staging (DSI/DSA) to DSO database.
Each configuration specifies the table schema, name, primary key(s), and sync mode.

Primary Key Notes:
- Map tables: Usually CleanCode column(s) are the business key (IDENTITY generates DW_EK)
- Dim tables: Usually DW_EK_* is the primary key
- Fct tables: Various composite keys or DW_EK_* columns

Special Cases:
- dim.Borger: Type 2 SCD, DW_SK_Borger carried from ref.Patient_Address_Borger (merge mode)
- Tables with IDENTITY columns: Cannot MERGE on IDENTITY - use business key instead
"""

from DSO.load.sync_utils import SyncTableConfig

# =============================================================================
# ERP DOMAIN CONFIGURATIONS
# =============================================================================

# Dimension tables - DW_EK is PK (not IDENTITY in dim tables)
ERP_DIM_CONFIGS = {
    "dim_Kreditor": SyncTableConfig(
        schema="dim",
        table="Kreditor",
        primary_key="dw_ek_Kreditor",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Leverandoervare": SyncTableConfig(
        schema="dim",
        table="Leverandoervare",
        primary_key="dw_ek_Leverandoervare",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Aftale": SyncTableConfig(
        schema="dim",
        table="Aftale",
        primary_key=["dw_ek_Aftale", "dw_valid_from"],
        mode="truncate_insert",
    ),
    "dim_Kontoplan": SyncTableConfig(
        schema="dim",
        table="Kontoplan",
        primary_key="dw_ek_Kontoplan",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Faktura": SyncTableConfig(
        schema="dim",
        table="Faktura",
        primary_key="dw_ek_Faktura",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Fakturalinje": SyncTableConfig(
        schema="dim",
        table="Fakturalinje",
        primary_key="dw_ek_Fakturalinje",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Postering": SyncTableConfig(
        schema="dim",
        table="Postering",
        primary_key="dw_ek_Postering",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Finanskladde": SyncTableConfig(
        schema="dim",
        table="Finanskladde",
        primary_key="dw_ek_Finanskladde",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Finanskladdelinje": SyncTableConfig(
        schema="dim",
        table="Finanskladdelinje",
        primary_key="dw_ek_Finanskladdelinje",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Vareudlevering": SyncTableConfig(
        schema="dim",
        table="Vareudlevering",
        primary_key="dw_ek_Vareudlevering",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Lager": SyncTableConfig(
        schema="dim",
        table="Lager",
        primary_key="dw_ek_Lager",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_Rekvirent": SyncTableConfig(
        schema="dim",
        table="Rekvirent",
        primary_key="dw_ek_Rekvirent",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_EAN": SyncTableConfig(
        schema="dim",
        table="ean",
        primary_key="dw_ek_EAN",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_LagerbeholdningEgenskaber": SyncTableConfig(
        schema="dim",
        table="LagerbeholdningEgenskaber",
        primary_key="dw_ek_LagerbeholdningEgenskaber",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_AfdelingLagerbeholdningEgenskaber": SyncTableConfig(
        schema="dim",
        table="AfdelingLagerbeholdningEgenskaber",
        primary_key="dw_ek_AfdelingLagerbeholdningEgenskaber",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_ILSVare": SyncTableConfig(
        schema="dim",
        table="ILSVare",
        primary_key="dw_ek_ILSVare",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_BudgetEgenskaber": SyncTableConfig(
        schema="dim",
        table="BudgetEgenskaber",
        primary_key="dw_ek_BudgetEgenskaber",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_UNSPSC": SyncTableConfig(
        schema="dim",
        table="UNSPSC",
        primary_key="dw_ek_UNSPSC",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    # Bridge: soft-deletes er updates (dw_is_deleted + timestamp-bump), saa
    # watermark-delta fanger alt — ingen fysiske deletes at propagere.
    "bridge_LeverandoervareILSVare": SyncTableConfig(
        schema="bridge",
        table="LeverandoervareILSVare",
        primary_key=["dw_ek_Leverandoervare", "dw_ek_ILSVare"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
}

# Fact tables
ERP_FCT_CONFIGS = {
    "fct_Faktura": SyncTableConfig(
        schema="fct",
        table="Faktura",
        primary_key="dw_ek_Faktura",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
    "fct_Fakturalinje": SyncTableConfig(
        schema="fct",
        table="Fakturalinje",
        primary_key="dw_ek_Fakturalinje",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
    "fct_Postering": SyncTableConfig(
        schema="fct",
        table="Postering",
        primary_key="ref_ledgertrans_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
    "fct_Finanskladdelinje": SyncTableConfig(
        schema="fct",
        table="Finanskladdelinje",
        primary_key="dw_ek_Finanskladdelinje",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
    "fct_Vareudlevering": SyncTableConfig(
        schema="fct",
        table="Vareudlevering",
        primary_key="dw_ek_Vareudlevering",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fct_AftaleVarePris": SyncTableConfig(
        schema="fct",
        table="AftaleVarePris",
        primary_key="dw_sk_AftaleVarePris",
        mode="watermark",
        identity_insert=True,
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
    "fct_Lagerbeholdning": SyncTableConfig(
        schema="fct",
        table="Lagerbeholdning",
        primary_key="dw_ek_Lagerbeholdning",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fct_AfdelingLagerbeholdning": SyncTableConfig(
        schema="fct",
        table="AfdelingLagerbeholdning",
        primary_key="dw_ek_AfdelingLagerbeholdning",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fct_Budget": SyncTableConfig(
        schema="fct",
        table="Budget",
        primary_key="dw_ek_Budget",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
    ),
}

# Combined erp configs
ERP_SYNC_CONFIGS = {
    **ERP_DIM_CONFIGS,
    **ERP_FCT_CONFIGS,
}

# Backward-compatible alias used by existing integration tests and legacy call-sites.
INDKOEB_SYNC_CONFIGS = ERP_SYNC_CONFIGS


# =============================================================================
# SHARED DIMENSIONS CONFIGURATIONS
# =============================================================================

SHARED_DIMS_MAP_CONFIGS = {
    # map.Borger removed — dw_ek_Borger is assigned by ref.PatientToBorger
    # (INT IDENTITY) in the new FHIR + ref-table architecture.
    # map.Organisation removed — dim.Organisation is built from
    # ref.Organisation (fhir.Organization), and the SOR source now flows
    # through arc.sor_register -> codesystem.SOR*. The persisted
    # map.Organisation table remains as the one-time ref.Organisation seed
    # source (migration 20260618_ref_organisation.sql).
}

SHARED_DIMS_REF_CONFIGS = {
    # ref.PatientToBorger: merge on patient_id business key, preserve INT IDENTITY
    # on dw_ek_Borger so surrogates stay stable across syncs.
    "ref_patient_borger": SyncTableConfig(
        schema="ref",
        table="PatientToBorger",
        primary_key="patient_id",
        mode="merge",
        exclude_columns=["dw_ek_Borger"],
    ),
    # ref.Patient_Address_To_Borger: MERGE on (patient_id, address_period_start).
    # dw_sk_Borger is IDENTITY locally; DSO sync uses merge on business key.
    "ref_patient_address_borger": SyncTableConfig(
        schema="ref",
        table="Patient_Address_To_Borger",
        primary_key=["patient_id", "address_period_start"],
        mode="merge",
        exclude_columns=["dw_sk_Borger"],
        unique_current_key="patient_id",
    ),
    # ref.Organisation: merge on organization_id business key, preserve INT
    # IDENTITY on dw_ek_Organisation so surrogates stay stable across syncs.
    "ref_organisation": SyncTableConfig(
        schema="ref",
        table="Organisation",
        primary_key="organization_id",
        mode="merge",
        exclude_columns=["dw_ek_Organisation"],
    ),
    # Manual ILS exclusion list for reelt forbrug. Full snapshot; PK is the
    # pad-only CleanCode. Orphans keep dw_ek_Rekvirent NULL.
    "ref_RekvirentUndtagelse": SyncTableConfig(
        schema="ref",
        table="RekvirentUndtagelse",
        primary_key="clean_code_Rekvirent",
        mode="truncate_insert",
    ),
}

SHARED_DIMS_DIM_CONFIGS = {
    # dim.Borger: Type 2 SCD. dw_sk_Borger carried from ref.Patient_Address_To_Borger
    # (no local IDENTITY), safe for MERGE.
    "dim_Borger": SyncTableConfig(
        schema="dim",
        table="Borger",
        primary_key="dw_sk_Borger",
        mode="merge",
        unique_current_key="dw_ek_Borger",
    ),
    "dim_Organisation": SyncTableConfig(
        schema="dim",
        table="Organisation",
        primary_key="dw_ek_Organisation",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        insert_only_columns=["dw_timestamp_insert"],
    ),
    "dim_Okosted": SyncTableConfig(
        schema="dim",
        table="Okosted",
        primary_key="dw_ek_Okosted",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        insert_only_columns=["dw_timestamp_insert"],
    ),
    "dim_SHAK": SyncTableConfig(
        schema="dim",
        table="SHAK",
        primary_key="dw_ek_SHAK",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        insert_only_columns=["dw_timestamp_insert"],
    ),
}

# Combined shared dimensions configs
SHARED_DIMS_SYNC_CONFIGS = {
    **SHARED_DIMS_MAP_CONFIGS,
    **SHARED_DIMS_REF_CONFIGS,
    **SHARED_DIMS_DIM_CONFIGS,
}


# =============================================================================
# SECURITY CONFIGURATIONS
# =============================================================================

SECURITY_SYNC_CONFIGS = {
    # DPA security model — populated on staging by the AD load
    # script (infrastructure/security/load_healthpipe_ad_to_mssql.py), synced
    # staging -> DSO here. Both tables are composite-PK without IDENTITY
    # (sec.RolleDomaene in 04_dpa_tables.sql, sec.RBOrganisation in
    # 01_tables.sql), so no exclude_columns.
    "sec_rolledomaene": SyncTableConfig(
        schema="sec",
        table="RolleDomaene",
        primary_key=["bruger", "Sikkerhedsdomaene"],
        mode="truncate_insert",  # Full refresh for security data
    ),
    "sec_rborganisation": SyncTableConfig(
        schema="sec",
        table="RBOrganisation",
        primary_key=["bruger", "DW_EK_RBOrganisation"],
        mode="truncate_insert",  # Full refresh for security data
    ),
}


# =============================================================================
# CODESYSTEM CONFIGURATIONS (codesystem.* schema, truncate_insert)
# =============================================================================

CODESYSTEM_SYNC_CONFIGS = {
    # SFI (already synced to [codesystem]; kept as-is)
    "codesystem_sfi_concept": SyncTableConfig(
        schema="codesystem",
        table="SFIConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_sfi_properties": SyncTableConfig(
        schema="codesystem",
        table="SFIProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_sfi_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SFIHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # SKS — properties PK includes [Value] (a code can belong to several classifications)
    "codesystem_sks_concept": SyncTableConfig(
        schema="codesystem",
        table="SKSConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_sks_properties": SyncTableConfig(
        schema="codesystem",
        table="SKSProperties",
        primary_key=["code", "property_code", "value"],
        mode="truncate_insert",
    ),
    "codesystem_sks_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SKSHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # SOR
    "codesystem_sor_concept": SyncTableConfig(
        schema="codesystem",
        table="SORConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_sor_properties": SyncTableConfig(
        schema="codesystem",
        table="SORProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_sor_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SORHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # ean
    "codesystem_ean_concept": SyncTableConfig(
        schema="codesystem",
        table="EANConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_ean_properties": SyncTableConfig(
        schema="codesystem",
        table="EANProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_ean_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="EANHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # Lager
    "codesystem_lager_concept": SyncTableConfig(
        schema="codesystem",
        table="LagerConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_lager_properties": SyncTableConfig(
        schema="codesystem",
        table="LagerProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_lager_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="LagerHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # Okosted
    "codesystem_okosted_concept": SyncTableConfig(
        schema="codesystem",
        table="OkostedConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_okosted_properties": SyncTableConfig(
        schema="codesystem",
        table="OkostedProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_okosted_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="OkostedHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # SHAK
    "codesystem_shak_concept": SyncTableConfig(
        schema="codesystem",
        table="SHAKConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_shak_properties": SyncTableConfig(
        schema="codesystem",
        table="SHAKProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_shak_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SHAKHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
    # Rekvirent
    "codesystem_rekvirent_concept": SyncTableConfig(
        schema="codesystem",
        table="RekvirentConcept",
        primary_key="code",
        mode="truncate_insert",
    ),
    "codesystem_rekvirent_properties": SyncTableConfig(
        schema="codesystem",
        table="RekvirentProperties",
        primary_key=["code", "property_code"],
        mode="truncate_insert",
    ),
    "codesystem_rekvirent_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="RekvirentHierarchy",
        primary_key=["parent_code", "child_code"],
        mode="truncate_insert",
    ),
}


# =============================================================================
# FHIR (SQL on FHIR) CONFIGURATIONS
# =============================================================================

FHIR_SYNC_CONFIGS = {
    "fhir_patient": SyncTableConfig(
        schema="fhir",
        table="Patient",
        primary_key="patient_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_patient_address": SyncTableConfig(
        schema="fhir",
        table="Patient_Address",
        primary_key=["patient_id", "address_period_start"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_encounter": SyncTableConfig(
        schema="fhir",
        table="Encounter",
        primary_key="encounter_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_encounter_location": SyncTableConfig(
        schema="fhir",
        table="Encounter_Location",
        primary_key=["encounter_id", "location_id", "location_period_start"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_observation": SyncTableConfig(
        schema="fhir",
        table="Observation",
        primary_key="observation_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        exclude_columns=["dw_timestamp_effective"],
    ),
    "fhir_questionnaire": SyncTableConfig(
        schema="fhir",
        table="Questionnaire",
        primary_key="questionnaire_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_questionnaire_item": SyncTableConfig(
        schema="fhir",
        table="Questionnaire_item",
        primary_key=["questionnaire_id", "link_id", "item_key"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_questionnaire_response": SyncTableConfig(
        schema="fhir",
        table="QuestionnaireResponse",
        primary_key="qr_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        exclude_columns=["dw_timestamp_effective"],
    ),
    "fhir_questionnaire_response_item": SyncTableConfig(
        schema="fhir",
        table="QuestionnaireResponse_item",
        primary_key=["qr_id", "link_id", "item_key", "answer_index"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
        exclude_columns=["dw_timestamp_effective"],
    ),
    "fhir_procedure": SyncTableConfig(
        schema="fhir",
        table="Procedure",
        primary_key="procedure_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_service_request": SyncTableConfig(
        schema="fhir",
        table="ServiceRequest",
        primary_key="service_request_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    # Rehabilitation_Plan-receivers. Composite PK like Encounter_Location /
    # Patient_Address; only source_table='REHABILITATION_PLAN' ServiceRequests
    # contribute rows. Synced via the DFM spec list (unpartitioned).
    "fhir_service_request_recipient": SyncTableConfig(
        schema="fhir",
        table="ServiceRequest_Recipient",
        primary_key=["service_request_id", "recipient_key"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_condition": SyncTableConfig(
        schema="fhir",
        table="Condition",
        primary_key="condition_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_condition_tillaegskode": SyncTableConfig(
        schema="fhir",
        table="Condition_tillaegskode",
        primary_key=["condition_id", "tillaegskode_kode", "tillaegskode_system"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_procedure_tillaegskode": SyncTableConfig(
        schema="fhir",
        table="Procedure_tillaegskode",
        primary_key=["procedure_id", "tillaegskode_kode", "tillaegskode_system"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_service_request_tillaegskode": SyncTableConfig(
        schema="fhir",
        table="ServiceRequest_tillaegskode",
        primary_key=["service_request_id", "tillaegskode_kode", "tillaegskode_system"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_episode_of_care": SyncTableConfig(
        schema="fhir",
        table="EpisodeOfCare",
        primary_key="episode_of_care_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_location": SyncTableConfig(
        schema="fhir",
        table="Location",
        primary_key="location_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_appointment": SyncTableConfig(
        schema="fhir",
        table="Appointment",
        primary_key="appointment_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    # Epikrise + udskrivningsrapport document index; one row per resource,
    # sources distinguished by source_table.
    "fhir_documentreference": SyncTableConfig(
        schema="fhir",
        table="DocumentReference",
        primary_key="documentreference_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    # Composite PK like ServiceRequest_Recipient: the recipients extension is
    # flattened one row per recipient, keyed by recipient_key.
    "fhir_documentreference_recipient": SyncTableConfig(
        schema="fhir",
        table="DocumentReference_Recipient",
        primary_key=["documentreference_id", "recipient_key"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_organization": SyncTableConfig(
        schema="fhir",
        table="Organization",
        primary_key="organization_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_careplan": SyncTableConfig(
        schema="fhir",
        table="CarePlan",
        primary_key="careplan_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "fhir_careplan_activity": SyncTableConfig(
        schema="fhir",
        table="CarePlan_Activity",
        primary_key="ifdb_eventmarker_id",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
}


# =============================================================================
# CONCEPTMAP (staging vw_* views -> DSO conceptmap.* tables, watermark delta)
# =============================================================================

CONCEPTMAP_SYNC_CONFIGS = {
    "conceptmap_rekvirent_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="RekvirentToSOR",
        source_table="vw_rekvirent_to_sor",
        primary_key=["dw_ek_Rekvirent"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "conceptmap_lager_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="LagerToSOR",
        source_table="vw_lager_to_sor",
        primary_key=["dw_ek_Lager"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "conceptmap_ean_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="EANToSOR",
        source_table="vw_ean_to_sor",
        primary_key=["dw_ek_EAN"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "conceptmap_okosted_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="OkostedToSOR",
        source_table="vw_okosted_to_sor",
        primary_key=["dw_ek_Okosted"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "conceptmap_shak_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="SHAKToSOR",
        source_table="vw_shak_to_sor",
        primary_key=["dw_ek_SHAK"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
}


# =============================================================================
# HAIBA CONFIGURATIONS
# =============================================================================

# The dm rebuild is itself TRUNCATE + INSERT, so every dw_timestamp is renewed
# on each run — a watermark sync would move the full table anyway and only add
# machinery. truncate_insert states what actually happens. Fact and egenskaber
# share the dw_ek primary key (1:1). map.HAIBA_* is deliberately absent: it is
# staging-internal and its clean_code carries CPR on the case tables.
#
# Follow-up: the map now mints a dw_ek that is stable across
# runs and soft-deletes — exactly the key a MERGE needs. Rebuilding dm with a
# MERGE on dw_ek instead of TRUNCATE + INSERT would make dw_timestamp_update
# meaningful, and these configs then drop to mode="watermark": the weekly
# change volume is ~16,500 revised rows (see arc 00_archive.sql), not the
# ~6.7M that truncate_insert rewrites across the linked server today.


def _haiba_dm_config(table: str) -> SyncTableConfig:
    # The egenskaber table shares the fact's dw_ek (HAIBA, unlike the older
    # *Egenskaber dims that carry their own key), so strip the suffix to reach
    # it. PascalCase means no separating underscore before Egenskaber.
    base = table.removesuffix("Egenskaber")
    return SyncTableConfig(
        schema="dm",
        table=table,
        primary_key=f"dw_ek_{base}",
        mode="truncate_insert",
        chunk_size=20_000,
    )


# The DSO-sync Dagster asset id derives from the config KEY
# (dso_sync_{key}.lower()), so the key is kept in the pre-rename snake form to
# keep the asset graph and its materialisation history stable; the config's
# `table` is the renamed PascalCase dm table. Renaming the key instead would
# both orphan the asset history and mash the id (…casesinfektionegenskaber).
_HAIBA_DM_TABLE_BY_KEY = {
    "HAIBA_Agg_Infektion": "HAIBA_AggInfektion",
    "HAIBA_Agg_Infektion_Egenskaber": "HAIBA_AggInfektionEgenskaber",
    "HAIBA_Agg_Ortopaedi": "HAIBA_AggOrtopaedi",
    "HAIBA_Agg_Ortopaedi_Egenskaber": "HAIBA_AggOrtopaediEgenskaber",
    "HAIBA_Cases_Infektion": "HAIBA_CasesInfektion",
    "HAIBA_Cases_Infektion_Egenskaber": "HAIBA_CasesInfektionEgenskaber",
    "HAIBA_Cases_SSI": "HAIBA_CasesSSI",
    "HAIBA_Cases_SSI_Egenskaber": "HAIBA_CasesSSIEgenskaber",
}
HAIBA_DM_CONFIGS = {
    f"dm_{key}": _haiba_dm_config(table) for key, table in _HAIBA_DM_TABLE_BY_KEY.items()
}


# =============================================================================
# DRG CONFIGURATIONS
# =============================================================================

# Every DRG dim/fct/bridge carries dw_timestamp_update and is maintained
# incrementally in the DSA layer (fct MERGE + soft-delete; dims/group-bridges
# are append-only), so a watermark delta on dw_timestamp_update moves exactly
# the changed rows — no benefit to truncate_insert. map.DRG* is deliberately
# absent: it mints the IDENTITY keys and is staging-internal (and never carries
# CPR either). The group keys the fact references live in map.DRG*Gruppe; DSO
# exposes them through the group->code bridges, not a group dimension.
DRG_SYNC_CONFIGS = {
    "dim_DRGKontaktEgenskaber": SyncTableConfig(
        schema="dim",
        table="DRGKontaktEgenskaber",
        primary_key="dw_ek_DRGKontakt",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_DRGKontaktKlassifikation": SyncTableConfig(
        schema="dim",
        table="DRGKontaktKlassifikation",
        primary_key="dw_ek_DRGKontaktKlassifikation",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_DRGDiagnose": SyncTableConfig(
        schema="dim",
        table="DRGDiagnose",
        primary_key="dw_ek_DRGDiagnose",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "dim_DRGProcedure": SyncTableConfig(
        schema="dim",
        table="DRGProcedure",
        primary_key="dw_ek_DRGProcedure",
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "bridge_DRGDiagnoseGruppe": SyncTableConfig(
        schema="bridge",
        table="DRGDiagnoseGruppe",
        primary_key=["dw_ek_DRGDiagnoseGruppe", "dw_ek_DRGDiagnose"],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    "bridge_DRGProcedureGruppe": SyncTableConfig(
        schema="bridge",
        table="DRGProcedureGruppe",
        primary_key=[
            "dw_ek_DRGProcedureGruppe",
            "dw_ek_DRGProcedure",
            "dw_ek_Organisation_producerende",
        ],
        mode="watermark",
        watermark_column="dw_timestamp_update",
    ),
    # fct.DRGKontakt is wide (42 cols) and carries 11 nonclustered indexes on
    # top of the clustered PK. The first load moves all ~35.6M rows into an
    # empty DSO target (dso_infra_drg creates the table but never seeds it), and
    # maintaining 11 nonclustered B-trees per row over 35.6M rows is pure random
    # I/O — the sync hung on PAGEIOLATCH_EX for hours on dev. disable_target_
    # indexes_during_sync makes the sync drop the nonclustered indexes for the
    # bulk-load path only (empty target / truncate_insert), so the insert
    # maintains just the clustered PK and the 11 indexes are rebuilt once,
    # sorted, afterwards. The nightly watermark delta lands on a populated
    # target and takes the MERGE path, where the indexes are left intact (see
    # _sync_via_linked_server_batched): rebuilding every index over the whole
    # table to absorb a small delta would cost far more than per-row upkeep.
    "fct_DRGKontakt": SyncTableConfig(
        schema="fct",
        table="DRGKontakt",
        primary_key="dw_ek_DRGKontakt",
        mode="watermark",
        watermark_column="dw_timestamp_update",
        chunk_size=20_000,
        disable_target_indexes_during_sync=True,
    ),
}


# =============================================================================
# ALL CONFIGURATIONS
# =============================================================================

ALL_SYNC_CONFIGS = {
    **ERP_SYNC_CONFIGS,
    **SHARED_DIMS_SYNC_CONFIGS,
    **SECURITY_SYNC_CONFIGS,
    **CODESYSTEM_SYNC_CONFIGS,
    **FHIR_SYNC_CONFIGS,
    **CONCEPTMAP_SYNC_CONFIGS,
    **HAIBA_DM_CONFIGS,
    **DRG_SYNC_CONFIGS,
}


def get_config(config_name: str) -> SyncTableConfig:
    """Get a sync configuration by name."""
    if config_name not in ALL_SYNC_CONFIGS:
        raise ValueError(
            f"Unknown sync config: {config_name}. Available: {list(ALL_SYNC_CONFIGS.keys())}"
        )
    return ALL_SYNC_CONFIGS[config_name]


def get_watermark_sync_tables() -> list[tuple[str, str, str]]:
    """
    Return unique (schema, table, watermark_column) for all sync configs with mode='watermark'.

    Used to create watermark indexes programmatically on staging and DSO so delta MERGE
    (COALESCE(watermark_column, dw_timestamp_insert) >= @ts) can use an index on the source.
    """
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str, str]] = []
    for config in ALL_SYNC_CONFIGS.values():
        if config.mode != "watermark" or not config.watermark_column:
            continue
        key = (config.schema, config.table)
        if key in seen:
            continue
        seen.add(key)
        out.append((config.schema, config.table, config.watermark_column))
    return out
