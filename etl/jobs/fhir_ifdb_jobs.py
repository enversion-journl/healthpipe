# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR ETL job definitions for IFDB data.

This module defines asset jobs for running FHIR ETL pipelines from IFDB Oracle,
including both DSI extraction and DSA transformation/posting.
"""

import os

import dagster as dg

# Cap per-run asset concurrency. Default multiprocess_executor uses
# multiprocessing.cpu_count(), which on 16-core pods would run 16 assets
# in parallel — and each heavy asset (e.g. AKTIVITETSRESULTAT) spawns its
# own ProcessPool for XML parsing. Combined, that oversubscribed the pods'
# memory budget and triggered SIGKILL/OOM on XML workers. Daily keeps a cap of
# 4 to preserve throughput on smaller partitions. Monthly/full-load partitions
# are substantially heavier, so they default lower while still allowing one
# global override or cadence-specific overrides per environment.


def _max_concurrent(env_name: str, default: int) -> int:
    return int(
        os.environ.get(
            env_name,
            os.environ.get("FHIR_IFDB_MAX_CONCURRENT", str(default)),
        )
    )


_DAILY_MAX_CONCURRENT = _max_concurrent("FHIR_IFDB_DAILY_MAX_CONCURRENT", 4)
_MONTHLY_MAX_CONCURRENT = _max_concurrent("FHIR_IFDB_MONTHLY_MAX_CONCURRENT", 2)
_FULL_LOAD_MAX_CONCURRENT = _max_concurrent("FHIR_IFDB_FULL_LOAD_MAX_CONCURRENT", 2)

_ifdb_daily_multiprocess_executor = dg.multiprocess_executor.configured(
    {"max_concurrent": _DAILY_MAX_CONCURRENT}
)
_ifdb_monthly_multiprocess_executor = dg.multiprocess_executor.configured(
    {"max_concurrent": _MONTHLY_MAX_CONCURRENT}
)
_ifdb_full_load_multiprocess_executor = dg.multiprocess_executor.configured(
    {"max_concurrent": _FULL_LOAD_MAX_CONCURRENT}
)
_sfi_reconciliation_executor = dg.multiprocess_executor.configured({"max_concurrent": 1})

# ============================================================================
# Daily Partition Jobs
# ============================================================================

# DSI extraction layer - daily
ifdb_extraction_daily_assets = dg.AssetSelection.groups("ifdb_extraction_daily")

# DSA FHIR layer - daily
fhir_ifdb_daily_assets = dg.AssetSelection.groups("fhir_ifdb_daily")

# SOF extraction needs the latest Aidbox ViewDefinitions materialized before the
# views are read. Include the infra assets in the operational jobs so a deploy
# with changed ViewDefinitions is picked up on the next run.
fhir_sof_infrastructure_assets = dg.AssetSelection.assets(
    "staging_infra_fhir",
    "fhir_sof_infrastructure",
)
fhir_sof_daily_assets = fhir_sof_infrastructure_assets | dg.AssetSelection.groups("fhir_sof_daily")

# DSO sync (staging -> reporting), split by partition cadence
fhir_dso_sync_daily = dg.AssetSelection.groups("fhir_dso_sync_daily")
fhir_dso_sync_monthly = dg.AssetSelection.groups("fhir_dso_sync_monthly")
sfi_datapoint_reconciliation_assets = dg.AssetSelection.assets("dso_reconcile_sfi_datapoint_store")

# Assets tagged "dfm_migrated" are excluded — they run via fhir_ifdb_dfm_base_job instead.
_dfm_migrated = dg.AssetSelection.tag("dfm_migrated", "true")

# Combined daily job: remaining partitioned resources (CarePlan). SFI_datapoint
# refresh and codesystem.sfi_* sync moved to fhir_ifdb_dfm_sof_dso_job so they
# wait for the DFM Observation/QR DSO syncs (sensor fires this job in parallel
# with SOF+DSO — a dep on an asset outside the job is treated as external).
fhir_etl_ifdb_daily_job = dg.define_asset_job(
    name="fhir_etl_ifdb_daily_job",
    selection=(
        (ifdb_extraction_daily_assets - _dfm_migrated)
        | (fhir_ifdb_daily_assets - _dfm_migrated)
        | (fhir_sof_daily_assets - _dfm_migrated)
        | (fhir_dso_sync_daily - _dfm_migrated)
    ),
    executor_def=_ifdb_daily_multiprocess_executor,
    description=(
        "Daily IFDB to FHIR ETL for remaining partitioned resources (CarePlan). "
        "All other IFDB FHIR producers run in fhir_ifdb_dfm_base_job; "
        "SOF, DSO and SFI_datapoint refresh run in fhir_ifdb_dfm_sof_dso_job."
    ),
)

# ============================================================================
# Monthly Partition Jobs
# ============================================================================

# DSI extraction layer - monthly
ifdb_extraction_monthly_assets = dg.AssetSelection.groups("ifdb_extraction_monthly")

# DSA FHIR layer - monthly
fhir_ifdb_monthly_assets = dg.AssetSelection.groups("fhir_ifdb_monthly")

# SOF monthly extraction
fhir_sof_monthly_assets = fhir_sof_infrastructure_assets | dg.AssetSelection.groups(
    "fhir_sof_monthly"
)

# Combined monthly job: remaining partitioned resources (CarePlan).
# Infrastructure (indexes etc.) is provisioned separately via full_infrastructure_setup.
fhir_etl_ifdb_monthly_job = dg.define_asset_job(
    name="fhir_etl_ifdb_monthly_job",
    selection=(
        ifdb_extraction_monthly_assets
        | fhir_ifdb_monthly_assets
        | fhir_sof_monthly_assets
        | fhir_dso_sync_monthly
    ),
    executor_def=_ifdb_monthly_multiprocess_executor,
    description=(
        "Monthly IFDB to FHIR ETL for remaining partitioned resources (CarePlan). "
        "SFI has no monthly DFM cadence — nightly delta covers it."
    ),
)

# ============================================================================
# Maintenance / Reconciliation Jobs
# ============================================================================

sfi_datapoint_reconciliation_job = dg.define_asset_job(
    name="sfi_datapoint_reconciliation_job",
    selection=sfi_datapoint_reconciliation_assets,
    executor_def=_sfi_reconciliation_executor,
    description=(
        "Manual/periodic batched reconciliation of fhir.SFI_datapoint_store. Use this "
        "after full-load/backfill upstream runs to rebuild historical SFI datapoints."
    ),
)

# ============================================================================
# Full Load Partition Jobs
# ============================================================================

# DSI extraction layer - full load
ifdb_extraction_full_load_assets = dg.AssetSelection.groups("ifdb_extraction_full_load")

# DSA FHIR layer - full load
fhir_ifdb_full_load_assets = dg.AssetSelection.groups("fhir_ifdb_full_load")

# Combined full load job: extraction + FHIR transformation/posting.
# Infrastructure (indexes etc.) is provisioned separately via full_infrastructure_setup.
fhir_etl_ifdb_full_load_job = dg.define_asset_job(
    name="fhir_etl_ifdb_full_load_job",
    selection=ifdb_extraction_full_load_assets | fhir_ifdb_full_load_assets,
    executor_def=_ifdb_full_load_multiprocess_executor,
    description="Full load IFDB to FHIR ETL: extracts all data and posts to FHIR server",
)

__all__ = [
    "fhir_etl_ifdb_daily_job",
    "fhir_etl_ifdb_monthly_job",
    "fhir_etl_ifdb_full_load_job",
    "sfi_datapoint_reconciliation_job",
]
