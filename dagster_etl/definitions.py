# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Main Dagster Definitions file - central entry point
for all assets, jobs, resources, and schedules.

Structure:
- DSI (Data Staging In): extract/, pre_stg/, stg/
- DSA (Data Staging Area): FHIR/, SQL/
- DSO (Data Staging Out): views/, load/

Multi-deployment support:
- Set DAGSTER_BRANCH_POSTFIX env var to deploy feature branches
- Only applies when value starts with "feature" (e.g., feature-patient-validation)
- Each feature branch gets unique asset/job names with postfix to avoid collisions
- Example: DAGSTER_BRANCH_POSTFIX=feature-123 -> assets get "_feature_123" suffix
"""

# Apply warning filters BEFORE any dagster imports to suppress BetaWarning for
# observable_source_asset. This must be done before importing dagster.
from core.warning_filters import apply_observable_source_asset_filter

apply_observable_source_asset_filter()

import os

# Load environment variables with override=True to ensure latest values are used
# This is important when running dagster dev as environment may be cached
from pathlib import Path

import dagster as dg
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
env_files = [
    project_root / ".env",
    project_root / "tests" / "config" / "test.env",
]

for env_file in env_files:
    if env_file.exists():
        load_dotenv(env_file, override=True)
        break
else:
    # Fallback to default behavior if no .env file found
    load_dotenv(override=True)

# Branch postfixing for multi-deployment support (feature branches only)
from core.branch_postfix import (
    build_key_replacement_map,
    get_postfixed_name,
    log_branch_info,
    postfix_asset_checks,
    postfix_assets,
    postfix_jobs,
    postfix_schedules,
)

# Log branch deployment info at module load
log_branch_info()

# Core resources and utilities
from core.io_managers import BatchAwareFilesystemIOManager
from core.resources import (
    IFDBOracleResource,
    ILSMSSQLResource,
    KmdUddataMSSQLResource,
    MySupplyMSSQLResource,
    SydaxMSSQLResource,
    fhir_defs,
    mssql_defs,
)

# Data Quality checks (grain consistency, deleted-resource reconciliation)
from data_quality import all_assets as data_quality_assets

# DSA CodeSystem and ConceptMap assets (persistent FHIR tables)
from DSA.CodeSystem import all_assets as codesystem_dsa_assets
from DSA.ConceptMap import all_assets as conceptmap_dsa_assets

# =============================================================================
# ASSETS
# =============================================================================
# DSA FHIR domain assets
from DSA.FHIR.assets import all_assets as fhir_assets

# DW Data Quality Checks
from DSA.SQL.checks import (
    dq_dim_map_referential_check,
    dq_fact_dim_referential_check,
    dq_faktura_postering_match_check,
    dq_fakturalinje_postering_match_check,
    dq_null_check,
    dq_partition_completeness_budget,
    dq_partition_completeness_faktura,
    dq_partition_completeness_fakturalinje,
    dq_partition_completeness_finanskladdelinje,
    dq_partition_completeness_postering,
    dq_archive_completeness_vareudlevering,
    dw_quality,
)
from DSA.SQL.dev import all_assets as dev_assets

# DSA SQL — DFM operational assets
from DSA.SQL.dfm import dfm_assets
from DSA.SQL.dim import all_assets as dim_assets
from DSA.SQL.fct import all_assets as fct_assets
from DSA.SQL.map import all_assets as map_assets
from DSA.SQL.ref import all_assets as ref_assets

# DSA SQL — artifact-type directories
from DSA.SQL.staging import all_assets as staging_assets

# DSI CSV file extraction assets (manual ConceptMap mappings)
from DSI.csv_files import all_extract_assets as csv_extract_assets

# DSI Extract - ERP (staging assets from source systems)
from DSI.mssql import (
    all_archive_assets,
)
from DSI.mssql import all_extract_assets as erp_extract_assets
from DSI.mssql import (
    all_factory_extract_assets,
)
from DSI.mssql.extract.sydax_extract_configs import (
    sydax_archive_extract_assets,
    sydax_flow_start_assets,
)

# DSI Extract - IFDB (extraction from Oracle for FHIR ETL)
from DSI.oracle import all_extraction_assets as ifdb_extract_assets
from DSI.oracle import all_ifdb_archive_assets

# DSI Organisation extraction assets (API-based organisation inputs)
from DSI.organisation import all_extract_assets as organisation_extract_assets

# DSO Load - Sync assets (sync staging data to DSO database via linked server)
from DSO.load import (
    all_dso_sync_assets,
    all_fhir_dso_sync_assets,
    all_sfi_datapoint_refresh_assets,
    dso_sync_fhir_organization,
)

# =============================================================================
# CENTRALIZED JOBS AND SCHEDULES
# =============================================================================
from jobs import all_jobs

# Migrations (one-shot assets for schema changes)
from migrations.run_migration_asset import run_migration_dso, run_migration_staging
from schedules import all_schedules
from sensors import (
    all_sensors,
    build_dfm_base_job_failure_sensor,
    build_dfm_base_job_success_sensor,
)

# Source assets (unchanged - still in sources/)
from sources.mssql.epj_assets import (
    mssql_encounter_source,
    mssql_episode_of_care_source,
    mssql_eventmarker_source,
    mssql_healthissue_source,
    mssql_location_source,
    mssql_patient_columna_source,
    mssql_patient_officielle_source,
)
from sources.mssql.ils_assets import (
    ils_laftd_source,
    ils_laftdpris_source,
    ils_laftdstam_source,
    ils_lafth_source,
    ils_lev_source,
    ils_udlev_source,
    ils_vare_source,
    ils_vareart_source,
    ils_varegrp_source,
    ils_varetype_source,
)
from sources.mssql.mysupply_assets import (
    mysupply_metadata_source,
    mysupply_statistic_invoice_header_source,
    mysupply_statistic_invoice_line_source,
)
from sources.mssql.sydax_assets import (
    sydax_afdeling_source,
    sydax_finanskonto_source,
    sydax_finanspostering_kreditor_trans_relation_source,
    sydax_finanspostering_source,
    sydax_kreditor_source,
    sydax_kreditortransaktion_source,
)
from sources.oracle.ifdb_assets import (
    ifdb_observable_assets,
)

# Infrastructure Module (centralized DDL for staging and DSO databases)
from infrastructure import (
    all_infrastructure_assets,
)

# Database availability checks removed for production performance
# Connection errors will surface naturally when assets attempt to execute
# This prevents blocking webserver/daemon startup and improves initialization time


# Define a list of all assets
all_assets = [
    # Infrastructure (centralized DDL for staging and DSO)
    *all_infrastructure_assets,
    # Migrations (one-shot schema changes — staging + DSO)
    run_migration_staging,
    run_migration_dso,
    # DSA SQL — artifact-type assets
    *staging_assets,
    *dim_assets,
    *map_assets,
    *ref_assets,
    *fct_assets,
    # DSA SQL — dev-only assets (e.g. faktura table reset)
    *dev_assets,
    # DSA CodeSystem and ConceptMap (persistent FHIR tables)
    *codesystem_dsa_assets,
    *conceptmap_dsa_assets,
    # DSI Extract assets (staging from source systems)
    *erp_extract_assets,
    *all_factory_extract_assets,
    *all_archive_assets,
    *sydax_archive_extract_assets,
    *sydax_flow_start_assets,
    *ifdb_extract_assets,
    # DSI IFDB archive extract + merge assets (DFM)
    *all_ifdb_archive_assets,
    # DSA SQL — DFM operational assets
    *dfm_assets,
    # DSI CSV extraction assets (manual ConceptMap mappings)
    *csv_extract_assets,
    # DSI Organisation extraction assets (e.g. NemHandel API)
    *organisation_extract_assets,
    # DSO Sync Assets (sync staging to DSO via linked server)
    *all_dso_sync_assets,
    # DSO FHIR Sync Assets (sync fhir.* tables from staging to DSO)
    *all_fhir_dso_sync_assets,
    dso_sync_fhir_organization,
    *all_sfi_datapoint_refresh_assets,
    # DWH Organisation (now included in dim_assets and map_assets above)
    # Undervisning (training) assets (module removed on this branch)
    # load_training_users_to_brugerroller,
    # create_training_database,
    # grant_training_users_database_access,
    # MSSQL Source Assets
    mssql_patient_columna_source,
    mssql_patient_officielle_source,
    mssql_episode_of_care_source,
    mssql_encounter_source,
    mssql_eventmarker_source,
    mssql_location_source,
    mssql_healthissue_source,
    # MySupply MSSQL Source Assets
    mysupply_statistic_invoice_header_source,
    mysupply_statistic_invoice_line_source,
    mysupply_metadata_source,
    # SYDAX MSSQL Source Assets
    sydax_afdeling_source,
    sydax_finanspostering_kreditor_trans_relation_source,
    sydax_finanskonto_source,
    sydax_finanspostering_source,
    sydax_kreditortransaktion_source,
    sydax_kreditor_source,
    # ILS MSSQL Source Assets
    ils_vare_source,
    ils_vareart_source,
    ils_varegrp_source,
    ils_varetype_source,
    ils_lev_source,
    ils_lafth_source,
    ils_laftd_source,
    ils_laftdpris_source,
    ils_laftdstam_source,
    ils_udlev_source,
    # IFDB Oracle Source Assets
    *ifdb_observable_assets,
    # FHIR Assets
    *fhir_assets,
    # Data Quality checks (grain consistency, deleted-resource reconciliation)
    *data_quality_assets,
    # DW Data Quality (anchor asset for cross-domain checks)
    dw_quality,
]

# Define resources
# Get MSSQL resources from mssql_defs (staging and DSO resources)
# - mssql_source_staging, mssql_target_staging: DSI/DSA layer (stg, map, dim, fct)
# - mssql_source_dso, mssql_target_dso: DSO layer (views, reporting)
mssql_resources = mssql_defs().resources
# Get FHIR resources from fhir_defs (fhir_server)
fhir_resources = fhir_defs().resources


resources = {
    **mssql_resources,
    "mysupply_mssql": MySupplyMSSQLResource(),
    "sydax_mssql": SydaxMSSQLResource(),
    "ils_mssql": ILSMSSQLResource(),
    "kmd_uddata_mssql": KmdUddataMSSQLResource(),
    "ifdb_oracle": IFDBOracleResource(),
    **fhir_resources,
    "io_manager": dg.FilesystemIOManager(
        base_dir=os.path.join(os.environ.get("DAGSTER_HOME", "."), "storage")
    ),
    "batched_extract_io_manager": BatchAwareFilesystemIOManager(
        base_dir=os.path.join(os.environ.get("DAGSTER_HOME", "."), "storage")
    ),
    "streaming_batched_extract_io_manager": BatchAwareFilesystemIOManager(
        base_dir=os.path.join(os.environ.get("DAGSTER_HOME", "."), "storage"),
        stream_batched_inputs=True,
    ),
}

# Apply branch postfixes for multi-deployment support
# When DAGSTER_BRANCH_POSTFIX starts with "feature", all assets/jobs/schedules get postfixed
# to avoid name collisions between feature branch deployments
#
# Step 1: Build key replacement map from all assets (original key -> postfixed key)
# This map is used by both asset and job postfixing for consistency
key_replacements = build_key_replacement_map(all_assets)

if key_replacements:
    import logging as _logging

    _logging.getLogger(__name__).info(
        "Built key replacement map with %d entries", len(key_replacements)
    )

# Step 2: Apply postfixes to assets using the key replacement map
postfixed_assets = postfix_assets(all_assets, key_replacements)

# Step 3: Apply postfixes to jobs, passing the key replacement map
# This ensures job selections (AssetSelection.assets()) reference postfixed keys
postfixed_jobs = postfix_jobs(all_jobs, key_replacements)

# Step 4: Apply postfixes to schedules, passing postfixed jobs
# This ensures schedules reference the postfixed job versions
postfixed_schedules = postfix_schedules(all_schedules, postfixed_jobs)

# Step 5: Apply postfixes to asset checks so they reference the postfixed anchor asset
postfixed_checks = postfix_asset_checks(
    [
        dq_null_check,
        dq_fact_dim_referential_check,
        dq_faktura_postering_match_check,
        dq_fakturalinje_postering_match_check,
        dq_dim_map_referential_check,
        dq_partition_completeness_budget,
        dq_partition_completeness_faktura,
        dq_partition_completeness_fakturalinje,
        dq_partition_completeness_finanskladdelinje,
        dq_partition_completeness_postering,
        dq_archive_completeness_vareudlevering,
    ],
    key_replacements,
)

# Step 6: Build the DFM base-job sensor with (postfixed) job references.
# This must happen after job postfixing so the sensor receives correctly-named
# jobs, and targets display correctly in the Dagster UI.
_postfixed_job_by_name = {j.name: j for j in postfixed_jobs}
_pfx = get_postfixed_name
_dfm_base_job = _postfixed_job_by_name[_pfx("fhir_ifdb_dfm_base_job")]
_dfm_daily_job = _postfixed_job_by_name[_pfx("fhir_etl_ifdb_daily_job")]
_dfm_sof_dso_job = _postfixed_job_by_name[_pfx("fhir_ifdb_dfm_sof_dso_job")]
dfm_sensor = build_dfm_base_job_success_sensor(
    base_job=_dfm_base_job,
    daily_job=_dfm_daily_job,
    sof_dso_job=_dfm_sof_dso_job,
)
dfm_failure_sensor = build_dfm_base_job_failure_sensor(
    base_job=_dfm_base_job,
    daily_job=_dfm_daily_job,
)
postfixed_sensors = [*all_sensors, dfm_sensor, dfm_failure_sensor]

defs = dg.Definitions(
    assets=postfixed_assets,
    resources=resources,
    jobs=postfixed_jobs,
    schedules=postfixed_schedules,
    asset_checks=postfixed_checks,
    sensors=postfixed_sensors,
    # Enable multiprocess execution for parallel asset processing (production performance)
    executor=dg.multiprocess_executor.configured({"max_concurrent": 10}),
)
