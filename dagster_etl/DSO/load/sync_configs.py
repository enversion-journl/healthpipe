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
    "dim_kreditor": SyncTableConfig(
        schema="dim",
        table="Kreditor",
        primary_key="DW_EK_Kreditor",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_vare": SyncTableConfig(
        schema="dim",
        table="Vare",
        primary_key="DW_EK_Vare",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_aftale": SyncTableConfig(
        schema="dim",
        table="Aftale",
        primary_key=["DW_EK_Aftale", "DW_ValidFrom"],
        mode="truncate_insert",
    ),
    "dim_kontoplan": SyncTableConfig(
        schema="dim",
        table="Kontoplan",
        primary_key="DW_EK_Kontoplan",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_faktura": SyncTableConfig(
        schema="dim",
        table="Faktura",
        primary_key="DW_EK_Faktura",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_fakturalinje": SyncTableConfig(
        schema="dim",
        table="Fakturalinje",
        primary_key="DW_EK_Fakturalinje",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_postering": SyncTableConfig(
        schema="dim",
        table="Postering",
        primary_key="DW_EK_Postering",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_finanskladde": SyncTableConfig(
        schema="dim",
        table="Finanskladde",
        primary_key="DW_EK_Finanskladde",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_finanskladdelinje": SyncTableConfig(
        schema="dim",
        table="Finanskladdelinje",
        primary_key="DW_EK_Finanskladdelinje",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_vareudlevering_egenskaber": SyncTableConfig(
        schema="dim",
        table="VareudleveringEgenskaber",
        primary_key="DW_EK_VareudleveringEgenskaber",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_lager": SyncTableConfig(
        schema="dim",
        table="Lager",
        primary_key="DW_EK_Lager",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_rekvirent": SyncTableConfig(
        schema="dim",
        table="Rekvirent",
        primary_key="DW_EK_Rekvirent",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_ean": SyncTableConfig(
        schema="dim",
        table="EAN",
        primary_key="DW_EK_EAN",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_LagerbeholdningEgenskaber": SyncTableConfig(
        schema="dim",
        table="LagerbeholdningEgenskaber",
        primary_key="DW_EK_LagerbeholdningEgenskaber",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_AfdelingLagerbeholdningEgenskaber": SyncTableConfig(
        schema="dim",
        table="AfdelingLagerbeholdningEgenskaber",
        primary_key="DW_EK_AfdelingLagerbeholdningEgenskaber",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_ilsvare": SyncTableConfig(
        schema="dim",
        table="IlsVare",
        primary_key="DW_EK_IlsVare",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_budget_egenskaber": SyncTableConfig(
        schema="dim",
        table="BudgetEgenskaber",
        primary_key="DW_EK_BudgetEgenskaber",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "dim_UNSPSC": SyncTableConfig(
        schema="dim",
        table="UNSPSC",
        primary_key="DW_EK_UNSPSC",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
}

# Fact tables
ERP_FCT_CONFIGS = {
    "fct_faktura": SyncTableConfig(
        schema="fct",
        table="Faktura",
        primary_key="DW_EK_Faktura",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        chunk_size=20_000,
    ),
    "fct_fakturalinje": SyncTableConfig(
        schema="fct",
        table="Fakturalinje",
        primary_key="DW_EK_Fakturalinje",
        mode="watermark",
        identity_insert=True,
        watermark_column="DW_Timestamp_Update",
        chunk_size=20_000,
    ),
    "fct_postering": SyncTableConfig(
        schema="fct",
        table="Postering",
        primary_key="ref_LedgertransId",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        chunk_size=20_000,
    ),
    "fct_finanskladdelinje": SyncTableConfig(
        schema="fct",
        table="Finanskladdelinje",
        primary_key="DW_EK_Finanskladdelinje",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        chunk_size=20_000,
    ),
    "fct_vareudlevering": SyncTableConfig(
        schema="fct",
        table="Vareudlevering",
        primary_key="DW_EK_Vareudlevering",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fct_Lagerbeholdning": SyncTableConfig(
        schema="fct",
        table="Lagerbeholdning",
        primary_key="DW_EK_Lagerbeholdning",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fct_AfdelingLagerbeholdning": SyncTableConfig(
        schema="fct",
        table="AfdelingLagerbeholdning",
        primary_key="DW_EK_AfdelingLagerbeholdning",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fct_budget": SyncTableConfig(
        schema="fct",
        table="Budget",
        primary_key="DW_EK_Budget",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        chunk_size=20_000,
    ),
    "fct_indkoebsanalyse_linje": SyncTableConfig(
        schema="fct",
        table="IndkoebsanalyseLinje",
        primary_key="DW_EK_IndkoebsanalyseLinje",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
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
    # map.Borger removed — DW_EK_Borger is assigned by ref.Patient_Borger
    # (INT IDENTITY) in the new FHIR + ref-table architecture.
    "map_organisation": SyncTableConfig(
        schema="map",
        table="Organisation",
        primary_key="CleanCode",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        exclude_columns=["DW_EK_Organisation"],
        insert_only_columns=["DW_Timestamp_Insert"],
    ),
}

SHARED_DIMS_REF_CONFIGS = {
    # ref.Patient_Borger: merge on patient_id business key, preserve INT IDENTITY
    # on DW_EK_Borger so surrogates stay stable across syncs.
    "ref_patient_borger": SyncTableConfig(
        schema="ref",
        table="Patient_Borger",
        primary_key="patient_id",
        mode="merge",
        exclude_columns=["DW_EK_Borger"],
    ),
    # ref.Patient_Address_Borger: MERGE on (patient_id, address_period_start).
    # DW_SK_Borger is IDENTITY locally; DSO sync uses merge on business key.
    "ref_patient_address_borger": SyncTableConfig(
        schema="ref",
        table="Patient_Address_Borger",
        primary_key=["patient_id", "address_period_start"],
        mode="merge",
        exclude_columns=["DW_SK_Borger"],
    ),
    # ref.Organisation: merge on organization_id business key, preserve INT
    # IDENTITY on DW_EK_Organisation so surrogates stay stable across syncs.
    "ref_organisation": SyncTableConfig(
        schema="ref",
        table="Organisation",
        primary_key="organization_id",
        mode="merge",
        exclude_columns=["DW_EK_Organisation"],
    ),
}

SHARED_DIMS_DIM_CONFIGS = {
    # dim.Borger: Type 2 SCD. DW_SK_Borger carried from ref.Patient_Address_Borger
    # (no local IDENTITY), safe for MERGE.
    "dim_borger": SyncTableConfig(
        schema="dim",
        table="Borger",
        primary_key="DW_SK_Borger",
        mode="merge",
    ),
    "dim_organisation": SyncTableConfig(
        schema="dim",
        table="Organisation",
        primary_key="DW_EK_Organisation",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        insert_only_columns=["DW_Timestamp_Insert"],
    ),
    "dim_okosted": SyncTableConfig(
        schema="dim",
        table="Okosted",
        primary_key="DW_EK_Okosted",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        insert_only_columns=["DW_Timestamp_Insert"],
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
    "sec_brugerroller": SyncTableConfig(
        schema="sec",
        table="BrugerRoller",
        primary_key="BrugerAD",
        mode="merge",
    ),
    "sec_personaleorg": SyncTableConfig(
        schema="sec",
        table="PersonaleOrg",
        primary_key="SOR_Kode",
        mode="truncate_insert",  # Full refresh for security data
    ),
    "sec_brugerorganisation": SyncTableConfig(
        schema="sec",
        table="BrugerOrganisation",
        primary_key=["BrugerAD", "DW_EK_Organisation"],
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
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_sfi_properties": SyncTableConfig(
        schema="codesystem",
        table="SFIProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_sfi_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SFIHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # SKS — properties PK includes [Value] (a code can belong to several classifications)
    "codesystem_sks_concept": SyncTableConfig(
        schema="codesystem",
        table="SKSConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_sks_properties": SyncTableConfig(
        schema="codesystem",
        table="SKSProperties",
        primary_key=["Code", "PropertyCode", "Value"],
        mode="truncate_insert",
    ),
    "codesystem_sks_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SKSHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # SOR
    "codesystem_sor_concept": SyncTableConfig(
        schema="codesystem",
        table="SORConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_sor_properties": SyncTableConfig(
        schema="codesystem",
        table="SORProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_sor_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="SORHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # EAN
    "codesystem_ean_concept": SyncTableConfig(
        schema="codesystem",
        table="EANConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_ean_properties": SyncTableConfig(
        schema="codesystem",
        table="EANProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_ean_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="EANHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # Lager
    "codesystem_lager_concept": SyncTableConfig(
        schema="codesystem",
        table="LagerConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_lager_properties": SyncTableConfig(
        schema="codesystem",
        table="LagerProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_lager_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="LagerHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # Okosted
    "codesystem_okosted_concept": SyncTableConfig(
        schema="codesystem",
        table="OkostedConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_okosted_properties": SyncTableConfig(
        schema="codesystem",
        table="OkostedProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_okosted_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="OkostedHierarchy",
        primary_key=["ParentCode", "ChildCode"],
        mode="truncate_insert",
    ),
    # Rekvirent
    "codesystem_rekvirent_concept": SyncTableConfig(
        schema="codesystem",
        table="RekvirentConcept",
        primary_key="Code",
        mode="truncate_insert",
    ),
    "codesystem_rekvirent_properties": SyncTableConfig(
        schema="codesystem",
        table="RekvirentProperties",
        primary_key=["Code", "PropertyCode"],
        mode="truncate_insert",
    ),
    "codesystem_rekvirent_hierarchy": SyncTableConfig(
        schema="codesystem",
        table="RekvirentHierarchy",
        primary_key=["ParentCode", "ChildCode"],
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
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_patient_address": SyncTableConfig(
        schema="fhir",
        table="Patient_Address",
        primary_key=["patient_id", "address_period_start"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_encounter": SyncTableConfig(
        schema="fhir",
        table="Encounter",
        primary_key="encounter_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_encounter_location": SyncTableConfig(
        schema="fhir",
        table="Encounter_Location",
        primary_key=["encounter_id", "location_id", "location_period_start"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_observation": SyncTableConfig(
        schema="fhir",
        table="Observation",
        primary_key="observation_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        exclude_columns=["DW_Timestamp_Effective"],
    ),
    "fhir_questionnaire": SyncTableConfig(
        schema="fhir",
        table="Questionnaire",
        primary_key="questionnaire_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_questionnaire_item": SyncTableConfig(
        schema="fhir",
        table="Questionnaire_item",
        primary_key=["questionnaire_id", "link_id", "item_key"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_questionnaire_response": SyncTableConfig(
        schema="fhir",
        table="QuestionnaireResponse",
        primary_key="qr_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        exclude_columns=["DW_Timestamp_Effective"],
    ),
    "fhir_questionnaire_response_item": SyncTableConfig(
        schema="fhir",
        table="QuestionnaireResponse_item",
        primary_key=["qr_id", "link_id", "item_key", "answer_index"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
        exclude_columns=["DW_Timestamp_Effective"],
    ),
    "fhir_procedure": SyncTableConfig(
        schema="fhir",
        table="Procedure",
        primary_key="procedure_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_service_request": SyncTableConfig(
        schema="fhir",
        table="ServiceRequest",
        primary_key="service_request_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_condition": SyncTableConfig(
        schema="fhir",
        table="Condition",
        primary_key=["condition_id", "code", "code_system"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_episode_of_care": SyncTableConfig(
        schema="fhir",
        table="EpisodeOfCare",
        primary_key="episode_of_care_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_location": SyncTableConfig(
        schema="fhir",
        table="Location",
        primary_key="location_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_appointment": SyncTableConfig(
        schema="fhir",
        table="Appointment",
        primary_key="appointment_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_organization": SyncTableConfig(
        schema="fhir",
        table="Organization",
        primary_key="organization_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_careplan": SyncTableConfig(
        schema="fhir",
        table="CarePlan",
        primary_key="careplan_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "fhir_careplan_activity": SyncTableConfig(
        schema="fhir",
        table="CarePlan_Activity",
        primary_key="ifdb_eventmarker_id",
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
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
        primary_key=["DW_EK_Rekvirent"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "conceptmap_lager_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="LagerToSOR",
        source_table="vw_lager_to_sor",
        primary_key=["DW_EK_Lager"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "conceptmap_ean_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="EANToSOR",
        source_table="vw_ean_to_sor",
        primary_key=["DW_EK_EAN"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
    ),
    "conceptmap_okosted_to_sor": SyncTableConfig(
        schema="conceptmap",
        table="OkostedToSOR",
        source_table="vw_okosted_to_sor",
        primary_key=["DW_EK_Okosted"],
        mode="watermark",
        watermark_column="DW_Timestamp_Update",
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
    (COALESCE(watermark_column, DW_Timestamp_Insert) >= @ts) can use an index on the source.
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
