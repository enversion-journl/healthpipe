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
from typing import Any, cast

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

# Install the process-wide PII redaction net (masks CPR in log records) as early
# as possible so it covers logging from all assets/resources loaded below.
from core.logging_redaction import install_pii_redaction

install_pii_redaction()

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
    EPJIOracleResource,
    IFDBOracleResource,
    ILSMSSQLResource,
    KmdUddataMSSQLResource,
    MaxManusMSSQLResource,
    MySupplyMSSQLResource,
    PatologiMSSQLResource,
    SimaArkivMSSQLResource,
    SimaMSSQLResource,
    SydaxMSSQLResource,
    fhir_defs,
    missing_resource_env_vars,
    mssql_defs,
    os2mo_defs,
)

# Data Quality checks (grain consistency, deleted-resource reconciliation)
from data_quality import all_assets as data_quality_assets

# DQA framework (config-driven anchor assets + asset_checks per subject)
from dqa import (
    DQA_AFTER_ETL_SENSOR_SPECS,
    build_dqa_after_etl_sensor,
    dqa_arc_ifdb_replay,
    dqa_arc_ifdb_replay_job,
    dqa_arc_ifdb_vanished_cleanup,
    dqa_arc_ifdb_vanished_cleanup_job,
    dqa_asset_checks,
    dqa_assets,
    dqa_fhir_server_cleanup,
    dqa_fhir_server_cleanup_job,
    dqa_jobs,
    dqa_patient_delete_propagation,
    dqa_patient_delete_propagation_job,
    dqa_remediation_replay,
    dqa_remediation_replay_job,
    dqa_schedules,
    dqa_summary,
    dso_asset_checks,
    dso_assets,
    dso_jobs,
)

# DSA CodeSystem and ConceptMap assets (persistent FHIR tables)
from DSA.CodeSystem import all_assets as codesystem_dsa_assets
from DSA.ConceptMap import all_assets as conceptmap_dsa_assets

# =============================================================================
# ASSETS
# =============================================================================
# DSA FHIR domain assets
from DSA.FHIR.assets import all_assets as fhir_assets

# DSA SQL — DFM operational assets
from DSA.SQL.bridge import all_assets as bridge_assets

# DW Data Quality Checks
from DSA.SQL.checks import (
    dq_archive_completeness_vareudlevering,
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
    dw_quality,
    silkeborgdata_e26_pers_checkpoint_alignment,
    silkeborgdata_e26_pers_checkpoint_not_stalled,
    silkeborgdata_e26_pers_period_continuity,
    silkeborgdata_e26_pers_period_volume,
    silkeborgdata_practitionerrole_no_orphans,
)
from DSA.SQL.cpr import all_assets as cpr_dsa_assets
from DSA.SQL.dev import all_assets as dev_assets
from DSA.SQL.dfm import dfm_assets
from DSA.SQL.dim import all_assets as dim_assets
from DSA.SQL.dm import all_assets as dm_assets
from DSA.SQL.fct import all_assets as fct_assets
from DSA.SQL.map import all_assets as map_assets
from DSA.SQL.ref import all_assets as ref_assets

# DSA SQL — artifact-type directories
from DSA.SQL.staging import all_assets as staging_assets

# DSI CSV file extraction assets (manual ConceptMap mappings)
from DSI.csv_files import all_extract_assets as csv_extract_assets
from DSI.csv_files.cpr import cpr_extract_checks

# DSI Extract - ERP (staging assets from source systems)
from DSI.mssql import (
    all_archive_assets,
    all_factory_extract_assets,
)
from DSI.mssql.extract.sydax_extract_configs import (
    sydax_archive_extract_assets,
    sydax_flow_start_assets,
)
from DSI.oracle import all_epji_archive_assets, all_ifdb_archive_assets

# DSI Organisation extraction assets (API-based organisation inputs)
from DSI.organisation import all_extract_assets as organisation_extract_assets
from DSI.os2mo import os2mo_extract_assets
from DSO.export import os2mo_export_assets

# DSO Load - Sync assets (sync staging data to DSO database via linked server)
from DSO.load import (
    all_dso_sync_assets,
    all_fhir_dso_sync_assets,
    all_sfi_datapoint_refresh_assets,
    dso_sync_fhir_organization,
)
from infrastructure.assets.db_space import all_db_space_checks
from infrastructure.assets.ifdb_backfill_window_reconciliation import (
    ifdb_backfill_window_reconciliation,
)

# Meta-asset: fuldindlæsning af IFDB -> FHIR-kæden, ét vindue ad gangen
from infrastructure.assets.ifdb_fhir_fuldload import (
    ifdb_fhir_fuldload,
    ifdb_fhir_fuldload_job,
)

# DPA security AD pipeline (extract from AD -> populate staging; feeds dso_sync_security)
from infrastructure.assets.security_ad_assets import all_security_ad_assets

# Operatørudløst nulstilling af SFI/procedure/diagnose-sporet før genkørsel
from infrastructure.assets.sfi_genkoersel_nulstilling import (
    sfi_genkoersel_nulstilling,
    sfi_genkoersel_nulstilling_job,
)

# MIDLERTIDIG: verifikation af genkørslens indlæste data mod grenens kontrakter
from infrastructure.assets.sfi_genkoersel_verifikation import (
    sfi_genkoersel_verifikation,
    sfi_genkoersel_verifikation_job,
)

# Målebænk og status for storens fysiske layout (partitionering, columnstore)
from infrastructure.assets.sfi_store_layout_benchmark import (
    dso_sfi_store_layout_benchmark,
    dso_sfi_store_partition_status,
    sfi_store_layout_benchmark_job,
)

# Målebænk for SOF-MERGE'n mod en kopi af måltabellen
from infrastructure.assets.sof_merge_benchmark import (
    sof_merge_benchmark,
    sof_merge_benchmark_job,
)

# =============================================================================
# CENTRALIZED JOBS AND SCHEDULES
# =============================================================================
from jobs import all_jobs
from schedules import all_schedules
from sensors import (
    all_sensors,
    build_cpr_backlog_sensor,
    build_drg_publish_sensor,
    build_drg_queue_failure_sensor,
    build_drg_queue_sensor,
    build_haiba_publish_sensor,
    build_os2mo_delivery_sensor,
    build_udredningsret_publish_sensor,
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
    ils_projekt_source,
    ils_udlev_source,
    ils_vare_source,
    ils_vareart_source,
    ils_varegrp_source,
    ils_varetype2_source,
    ils_varetype_source,
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
    # DSA SQL — artifact-type assets
    *staging_assets,
    *dim_assets,
    *dm_assets,
    *bridge_assets,
    *map_assets,
    *ref_assets,
    *cpr_dsa_assets,
    *fct_assets,
    # DSA SQL — dev-only assets (e.g. faktura table reset)
    *dev_assets,
    # DSA CodeSystem and ConceptMap (persistent FHIR tables)
    *codesystem_dsa_assets,
    *conceptmap_dsa_assets,
    # DSI Extract assets (staging from source systems)
    *all_factory_extract_assets,
    *all_archive_assets,
    *sydax_archive_extract_assets,
    *sydax_flow_start_assets,
    # DSI IFDB archive extract + merge assets (DFM)
    *all_ifdb_archive_assets,
    # DSI EPJI SHAK-a-like extract + archive merge assets (DFM)
    *all_epji_archive_assets,
    # DSA SQL — DFM operational assets
    *dfm_assets,
    # DSI CSV extraction assets (manual ConceptMap mappings)
    *csv_extract_assets,
    # OS2mo (Magenta) integration — codesystems out, organisation back in
    *os2mo_export_assets,
    *os2mo_extract_assets,
    # DSI Organisation extraction assets (e.g. NemHandel API)
    *organisation_extract_assets,
    # DPA security AD pipeline (extract from AD -> populate staging)
    *all_security_ad_assets,
    # DSO Sync Assets (sync staging to DSO via linked server)
    *all_dso_sync_assets,
    # DSO FHIR Sync Assets (sync fhir.* tables from staging to DSO)
    *all_fhir_dso_sync_assets,
    dso_sync_fhir_organization,
    *all_sfi_datapoint_refresh_assets,
    # MSSQL Source Assets
    mssql_patient_columna_source,
    mssql_patient_officielle_source,
    mssql_episode_of_care_source,
    mssql_encounter_source,
    mssql_eventmarker_source,
    mssql_location_source,
    mssql_healthissue_source,
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
    ils_varetype2_source,
    ils_projekt_source,
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
    # DQA framework anchor assets (one per subject per cadence)
    *dqa_assets,
    # DQA DSO-vs-DSA reconciliation anchors (one per domain)
    *dso_assets,
    # DQA overview rollup (reads dqa.results)
    dqa_summary,
    # DQA remediation: operator-triggered replay of queued dangling references
    dqa_remediation_replay,
    # DQA remediation: operator-triggered ids-mode re-extract of missing arc keys (IFDB)
    dqa_arc_ifdb_replay,
    # DQA remediation: operator-triggered soft-delete of arc rows vanished from IFDB
    dqa_arc_ifdb_vanished_cleanup,
    # DQA remediation: operator-triggered soft-delete of flat rows deleted on the FHIR server
    dqa_fhir_server_cleanup,
    # DQA remediation: operator-triggered soft-delete of fully-deleted patients (arc -> fhir)
    dqa_patient_delete_propagation,
    # Operatørudløst nulstilling af SFI/procedure/diagnose-sporet før genkørsel
    sfi_genkoersel_nulstilling,
    # MIDLERTIDIG: verifikation af genkørslens indlæste data
    sfi_genkoersel_verifikation,
    # Fuldindlæsning af IFDB -> FHIR-kæden, ét vindue ad gangen
    ifdb_backfill_window_reconciliation,
    ifdb_fhir_fuldload,
    # Målebænk og status for storens fysiske layout
    dso_sfi_store_layout_benchmark,
    dso_sfi_store_partition_status,
    # Målebænk for SOF-MERGE'n
    sof_merge_benchmark,
]

# Define resources
# Get MSSQL resources from mssql_defs (staging and DSO resources)
# - mssql_source_staging, mssql_target_staging: DSI/DSA layer (stg, map, dim, fct)
# - mssql_source_dso, mssql_target_dso: DSO layer (views, reporting)
mssql_resources = cast(dict[str, Any], mssql_defs().resources)
# Get FHIR resources from fhir_defs (fhir_server)
fhir_resources = cast(dict[str, Any], fhir_defs().resources)
# Get the OS2mo (Magenta) GraphQL resource from os2mo_defs (os2mo)
os2mo_resources = cast(dict[str, Any], os2mo_defs().resources)


resources = {
    **mssql_resources,
    "mysupply_mssql": MySupplyMSSQLResource(),
    "sydax_mssql": SydaxMSSQLResource(),
    "ils_mssql": ILSMSSQLResource(),
    "kmd_uddata_mssql": KmdUddataMSSQLResource(),
    "maxmanus_mssql": MaxManusMSSQLResource(),
    "sima_mssql": SimaMSSQLResource(),
    "sima_arkiv_mssql": SimaArkivMSSQLResource(),
    "patologi_mssql": PatologiMSSQLResource(),
    "ifdb_oracle": IFDBOracleResource(),
    "epji_oracle": EPJIOracleResource(),
    **fhir_resources,
    **os2mo_resources,
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

if missing_env_vars := missing_resource_env_vars(resources):
    dg.get_dagster_logger().warning(
        f"Environment variable(s) not set: {', '.join(missing_env_vars)}"
    )

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
postfixed_jobs = postfix_jobs(
    [
        *all_jobs,
        *dqa_jobs,
        *dso_jobs,
        dqa_remediation_replay_job,
        dqa_arc_ifdb_replay_job,
        dqa_arc_ifdb_vanished_cleanup_job,
        dqa_fhir_server_cleanup_job,
        dqa_patient_delete_propagation_job,
        sfi_genkoersel_nulstilling_job,
        sfi_genkoersel_verifikation_job,
        ifdb_fhir_fuldload_job,
        sfi_store_layout_benchmark_job,
        sof_merge_benchmark_job,
    ],
    key_replacements,
)

# Step 4: Apply postfixes to schedules, passing postfixed jobs
# This ensures schedules reference the postfixed job versions
postfixed_schedules = postfix_schedules([*all_schedules, *dqa_schedules], postfixed_jobs)

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
        # SilkeborgData E26PERS archive — guards the DFM contract
        silkeborgdata_e26_pers_period_continuity,
        silkeborgdata_e26_pers_period_volume,
        silkeborgdata_e26_pers_checkpoint_alignment,
        silkeborgdata_e26_pers_checkpoint_not_stalled,
        silkeborgdata_practitionerrole_no_orphans,
        *dqa_asset_checks,
        *dso_asset_checks,
        *all_db_space_checks,
        *cpr_extract_checks,
    ],
    key_replacements,
)

# Step 6: Resolve (postfixed) job references for the sensors built below.
# This must happen after job postfixing so the sensors receive correctly-named
# jobs, and targets display correctly in the Dagster UI.
_postfixed_job_by_name = {j.name: j for j in postfixed_jobs}
_pfx = get_postfixed_name
_ifdb_dfm_daily_job = _postfixed_job_by_name[_pfx("ifdb_dfm_daily_job")]

# HAIBA publish sensor: the dm rebuild must run AFTER the SOF+Borger chain, so
# stub patients created by haiba_ingest_job at 05:00 have a dw_ek_Borger by the
# time the cases are published. Gated on the archive's delivery watermark, so a
# night without a new SDS delivery does not republish 6.7M rows to DSO.
_haiba_publish_job = _postfixed_job_by_name[_pfx("haiba_publish_job")]
haiba_publish_sensor = build_haiba_publish_sensor(
    borger_chain_job=_ifdb_dfm_daily_job,
    publish_job=_haiba_publish_job,
)

# OS2mo delivery sensor: deliver the codesystems once organisation_cs_job has
# rebuilt them, rather than at a clock time that only usually comes after.
os2mo_delivery_sensor = build_os2mo_delivery_sensor(
    codesystem_job=_postfixed_job_by_name[_pfx("organisation_cs_job")],
    delivery_job=_postfixed_job_by_name[_pfx("os2mo_codesystem_export_job")],
)

# DRG publish sensor: same reason as HAIBA — the unknown-CPR stubs the DRG
# ingest queue posts to Aidbox only have a dw_ek_Borger once the SOF+Borger
# chain has minted them. Ungated: the publish is delta, so a night without a
# delivery is a no-op, and running nightly re-resolves dw_ek_Borger = -1.
_drg_publish_job = _postfixed_job_by_name[_pfx("drg_publish_job")]
drg_publish_sensor = build_drg_publish_sensor(
    borger_chain_job=_ifdb_dfm_daily_job,
    publish_job=_drg_publish_job,
)

# Udredningsret publish sensor: same shape and same reason — stub patients
# created by udredningsret_ingest_job only have a dw_ek_Borger once the SOF+Borger
# chain has minted them, so the DSA rebuild and the DSO sync wait for it. Gated on
# meta.UdredningsretLeverance's delivery watermark, so a night without a new SDS
# delivery does not re-stage 8.2M archive rows.
_udredningsret_publish_job = _postfixed_job_by_name[_pfx("udredningsret_publish_job")]
udredningsret_publish_sensor = build_udredningsret_publish_sensor(
    borger_chain_job=_ifdb_dfm_daily_job,
    publish_job=_udredningsret_publish_job,
)

# DQA post-ETL sensors: run a track's DQA job after the ETL that feeds it
# succeeds. Built here so they reference the (postfixed) job objects. Skip a
# spec whose request/monitored jobs aren't present in this deployment.
dqa_after_etl_sensors = []
for _spec in DQA_AFTER_ETL_SENSOR_SPECS:
    _monitored = [
        _postfixed_job_by_name[_pfx(m)]
        for m in _spec["monitored"]
        if _pfx(m) in _postfixed_job_by_name
    ]
    _daily = (
        _postfixed_job_by_name.get(_pfx(_spec["daily_job"])) if _spec.get("daily_job") else None
    )
    _weekly = [
        _postfixed_job_by_name[_pfx(w)]
        for w in _spec.get("weekly_jobs", [])
        if _pfx(w) in _postfixed_job_by_name
    ]
    # The AND-join needs ALL monitored jobs present, and there must be something to
    # request (a daily job and/or Saturday weekly jobs). Otherwise skip the spec.
    if len(_monitored) == len(_spec["monitored"]) and (_daily is not None or _weekly):
        _weekly_names = _spec.get("weekly_jobs", [])
        dqa_after_etl_sensors.append(
            build_dqa_after_etl_sensor(
                name=_spec["name"],
                monitored_jobs=_monitored,
                daily_job=_daily,
                weekly_jobs=_weekly,
                default_status=_spec.get("default_status"),
                description=(
                    f"After {', '.join(_spec['monitored'])} succeed: run "
                    f"{_spec.get('daily_job') or '(no daily job)'} daily"
                    + (f" + {', '.join(_weekly_names)} on Saturday" if _weekly_names else "")
                    + "."
                ),
            )
        )

# CPR backlog sensor: one more ingest run per successful ingest while deliveries
# are still waiting on the share, so a backlog is caught up in order.
cpr_backlog_sensor = build_cpr_backlog_sensor(_postfixed_job_by_name[_pfx("cpr_ingest_job")])
# DRG ingest queue: the schedule requests the queue's head, this sensor walks
# the rest one run at a time. Serialising by chaining rather than by a
# concurrency tag keeps the ext tables safe without instance configuration —
# and gives "U only after every M succeeded" as a property of the queue order.
# Indexed directly (no presence guard): all three jobs always live in
# all_jobs, and a missing one must fail at load — a silently shrunk
# monitored_jobs would just stop the queue with nothing to see.
_drg_ingest_jobs = [
    _postfixed_job_by_name[_pfx(name)]
    for name in (
        "drg_ingest_somatik_maaned_job",
        "drg_ingest_somatik_uge_job",
        "drg_ingest_psyk_maaned_job",
    )
]
# The chain advances on SUCCESS only, so a failed run stops the cycle and the
# units behind it are never requested. The failed run is red; the runs that
# never happened are an absence — this names them.
drg_queue_failure_sensor = build_drg_queue_failure_sensor(ingest_jobs=_drg_ingest_jobs)
drg_queue_sensor = build_drg_queue_sensor(
    ingest_jobs=_drg_ingest_jobs,
    job_for_unit={
        ("DRG", "M"): _postfixed_job_by_name[_pfx("drg_ingest_somatik_maaned_job")],
        ("DRG", "U"): _postfixed_job_by_name[_pfx("drg_ingest_somatik_uge_job")],
        ("DRGPSYK", "M"): _postfixed_job_by_name[_pfx("drg_ingest_psyk_maaned_job")],
    },
)

postfixed_sensors = [
    *all_sensors,
    haiba_publish_sensor,
    os2mo_delivery_sensor,
    cpr_backlog_sensor,
    drg_publish_sensor,
    drg_queue_sensor,
    drg_queue_failure_sensor,
    udredningsret_publish_sensor,
    *dqa_after_etl_sensors,
]

defs = dg.Definitions(
    assets=postfixed_assets,
    resources=resources,
    jobs=postfixed_jobs,
    schedules=postfixed_schedules,
    asset_checks=postfixed_checks,
    sensors=postfixed_sensors,
    # Enable multiprocess execution for parallel asset processing (production performance)
    executor=dg.multiprocess_executor.configured({"max_concurrent": 8}),
)
