# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR DSO Sync Assets

Syncs FHIR flat tables from staging (DSA) database to DSO (reporting) database
using SQL Server Linked Server.

Each FHIR table has a daily and monthly variant:
- Daily variant depends on the corresponding sof_*_daily asset
- Monthly variant depends on the corresponding sof_*_monthly asset

Both variants perform the same full-table sync (MERGE); the difference is only
which upstream SOF asset triggers the sync in the pipeline.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.FHIR.SqlOnFhir.DWKeys_dfm import DW_KEYS_CONFIG
from DSO.load.sync_configs import FHIR_SYNC_CONFIGS
from DSO.load.sync_utils import (
    SyncTableConfig,
    compare_row_counts,
    sync_table_via_linked_server,
)


def _sync_fhir_table(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
    config: SyncTableConfig,
) -> dg.MaterializeResult:
    result = sync_table_via_linked_server(
        context, mssql_target_dso, config, mssql_source_staging=mssql_source_staging
    )
    counts = compare_row_counts(
        context,
        mssql_target_dso,
        config.schema,
        config.table,
        mssql_source_staging=mssql_source_staging,
    )
    return dg.MaterializeResult(
        metadata={
            "rows_affected": result["rows_affected"],
            "sync_mode": result["mode"],
            "staging_count": counts["staging_count"],
            "dso_count": counts["dso_count"],
            "counts_match": counts["match"],
        }
    )


# =============================================================================
# Factory functions
# =============================================================================


def _create_dso_sync_daily(
    config_key: str,
    sof_name: str,
    mssql_table: str,
) -> dg.AssetsDefinition:
    config = FHIR_SYNC_CONFIGS[config_key]

    @dg.asset(
        name=f"dso_sync_fhir_{sof_name}_daily",
        group_name="fhir_dso_sync_daily",
        compute_kind="mssql",
        # dso_infra_fhir applies the DSO fhir.* tables + their migrations (e.g.
        # the item_key occurrence-grain PK widening); depend on it so the MERGE
        # never runs against a stale target schema/PK.
        deps=[f"sof_{sof_name}_daily", "dso_infra_fhir"],
        tags={"domain": "fhir", "layer": "dso_sync"},
        description=f"Sync fhir.{mssql_table} from staging to DSO (daily upstream)",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _sync_fhir_table(context, mssql_target_dso, mssql_source_staging, config)

    return _asset


def _create_dso_sync_monthly(
    config_key: str, sof_name: str, mssql_table: str
) -> dg.AssetsDefinition:
    config = FHIR_SYNC_CONFIGS[config_key]

    @dg.asset(
        name=f"dso_sync_fhir_{sof_name}_monthly",
        group_name="fhir_dso_sync_monthly",
        compute_kind="mssql",
        # See _create_dso_sync_daily: gate on dso_infra_fhir so the MERGE never
        # runs against a stale DSO target schema/PK.
        deps=[f"sof_{sof_name}_monthly", "dso_infra_fhir"],
        tags={"domain": "fhir", "layer": "dso_sync"},
        description=f"Sync fhir.{mssql_table} from staging to DSO (monthly upstream)",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _sync_fhir_table(context, mssql_target_dso, mssql_source_staging, config)

    return _asset


# =============================================================================
# Spec table: (sync_config_key, sof_resource_name, mssql_table)
# =============================================================================

# Every FHIR resource is now produced on the DFM path, so no flat table has a
# partitioned upstream left to sync from. The daily/monthly factories below stay
# for the next source that arrives partitioned; today they generate nothing.
# Organization is handled separately as unpartitioned (see below).
_DSO_SYNC_SPECS: list[tuple[str, str, str]] = []

# =============================================================================
# Generated asset instances
# =============================================================================

all_fhir_dso_sync_daily_assets = [
    _create_dso_sync_daily(cfg_key, sof, tbl) for cfg_key, sof, tbl in _DSO_SYNC_SPECS
]

all_fhir_dso_sync_monthly_assets = [
    _create_dso_sync_monthly(cfg_key, sof, tbl) for cfg_key, sof, tbl in _DSO_SYNC_SPECS
]

# =============================================================================
# Unpartitioned Organization DSO sync (runs in organisation_cs_job)
# =============================================================================

_org_config = FHIR_SYNC_CONFIGS["fhir_organization"]


@dg.asset(
    name="dso_sync_fhir_organization",
    group_name="fhir_organization_sor",
    compute_kind="mssql",
    # Gate on dso_infra_fhir (DSO fhir.* tables + migrations) so the MERGE never
    # runs against a stale target schema/PK.
    deps=["sof_organization", "dso_infra_fhir"],
    tags={"domain": "fhir", "layer": "dso_sync"},
    description="Sync fhir.Organization from staging to DSO (unpartitioned, full refresh)",
)
def dso_sync_fhir_organization(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    return _sync_fhir_table(context, mssql_target_dso, mssql_source_staging, _org_config)


# =============================================================================
# DFM-triggered DSO sync (unpartitioned, depends on DFM SOF assets)
# =============================================================================

_DFM_DSO_SYNC_SPECS: list[tuple[str, str, str]] = [
    ("fhir_patient", "patient", "Patient"),
    ("fhir_patient_address", "patient_address", "Patient_Address"),
    ("fhir_episode_of_care", "episode_of_care", "EpisodeOfCare"),
    ("fhir_encounter", "encounter", "Encounter"),
    ("fhir_encounter_location", "encounter_location", "Encounter_Location"),
    ("fhir_location", "location", "Location"),
    ("fhir_service_request_recipient", "service_request_recipient", "ServiceRequest_Recipient"),
    # Every producer of these flat tables is on the DFM path.
    # Condition is the last one to qualify — it has four producers (Diagnose,
    # PAS-diagnose, ServiceRequest, Aktivitetsresultat/SFI) and the final two
    # are themselves DFM-based.
    ("fhir_condition", "condition", "Condition"),
    (
        "fhir_condition_supplementary_code",
        "condition_supplementary_code",
        "Condition_SupplementaryCode",
    ),
    ("fhir_condition_henvisning", "condition_henvisning", "Condition_henvisning"),
    ("fhir_observation_speciale", "observation_speciale", "Observation_Speciale"),
    ("fhir_observation", "observation", "Observation"),
    ("fhir_procedure", "procedure", "Procedure"),
    (
        "fhir_procedure_supplementary_code",
        "procedure_supplementary_code",
        "Procedure_SupplementaryCode",
    ),
    ("fhir_questionnaire", "questionnaire", "Questionnaire"),
    ("fhir_questionnaire_item", "questionnaire_item", "Questionnaire_item"),
    ("fhir_questionnaire_response", "questionnaire_response", "QuestionnaireResponse"),
    (
        "fhir_questionnaire_response_item",
        "questionnaire_response_item",
        "QuestionnaireResponse_item",
    ),
    ("fhir_service_request", "service_request", "ServiceRequest"),
    (
        "fhir_service_request_supplementary_code",
        "service_request_supplementary_code",
        "ServiceRequest_SupplementaryCode",
    ),
    ("fhir_appointment", "appointment", "Appointment"),
    ("fhir_ward_unit", "ward_unit", "WardUnit"),
    ("fhir_ward_unit_capacity", "ward_unit_capacity", "WardUnit_Capacity"),
    ("fhir_bed", "bed", "Bed"),
    ("fhir_encounter_bed_space", "encounter_bed_space", "Encounter_BedSpace"),
    ("fhir_encounter_absence", "encounter_absence", "Encounter_Absence"),
    # Both producers (epikrise, udskrivningsrapport) are on the DFM path, so the
    # SOF assets have no partitioned counterpart to race with.
    ("fhir_documentreference", "documentreference", "DocumentReference"),
    (
        "fhir_documentreference_recipient",
        "documentreference_recipient",
        "DocumentReference_Recipient",
    ),
    # CarePlan was the last partitioned producer; its move to the DFM path is
    # what emptied _DSO_SYNC_SPECS above.
    ("fhir_careplan", "careplan", "CarePlan"),
    ("fhir_careplan_activity", "careplan_activity", "CarePlan_Activity"),
    # Practitioner is deliberately NOT here — see the list below.
]

# Practitioner owns its chain end to end in practitioner_daily_job, so its two
# syncs get their own group rather than the shared one the IFDB jobs select. In
# the shared group they would run on IFDB's schedule, where nothing guarantees
# that the archives, resources and SOF extraction had all succeeded first —
# and a sync is the last link, so it is exactly where that matters.
_DFM_DSO_SYNC_SPECS_PRACTITIONER: list[tuple[str, str, str]] = [
    ("fhir_practitioner", "practitioner", "Practitioner"),
    ("fhir_practitioner_role", "practitioner_role", "PractitionerRole"),
]
_PRACTITIONER_DSO_GROUP = "fhir_practitioner_dso"


def _create_dso_sync_dfm(
    config_key: str,
    sof_name: str,
    mssql_table: str,
    group_name: str = "fhir_dso_sync_dfm",
) -> dg.AssetsDefinition:
    config = FHIR_SYNC_CONFIGS[config_key]
    deps = [f"sof_{sof_name}_dfm", "dso_infra_fhir"]
    if sof_name in DW_KEYS_CONFIG:
        # The dw-keys must be on the rows before they are synced.
        deps.append(DW_KEYS_CONFIG[sof_name].asset_name)

    @dg.asset(
        name=f"dso_sync_fhir_{sof_name}_dfm",
        group_name=group_name,
        compute_kind="mssql",
        # dso_infra_fhir applies the DSO fhir.* tables + their migrations (e.g.
        # the item_key occurrence-grain PK widening); depend on it so the MERGE
        # never runs against a stale target schema/PK.
        deps=deps,
        tags={"domain": "fhir", "layer": "dso_sync"},
        description=f"Sync fhir.{mssql_table} from staging to DSO (DFM upstream)",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_dso: MSSQLResource,
        mssql_source_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        return _sync_fhir_table(context, mssql_target_dso, mssql_source_staging, config)

    return _asset


all_fhir_dso_sync_dfm_assets = [
    _create_dso_sync_dfm(cfg_key, sof, tbl) for cfg_key, sof, tbl in _DFM_DSO_SYNC_SPECS
] + [
    _create_dso_sync_dfm(cfg_key, sof, tbl, group_name=_PRACTITIONER_DSO_GROUP)
    for cfg_key, sof, tbl in _DFM_DSO_SYNC_SPECS_PRACTITIONER
]

all_fhir_dso_sync_assets = (
    all_fhir_dso_sync_daily_assets + all_fhir_dso_sync_monthly_assets + all_fhir_dso_sync_dfm_assets
)
