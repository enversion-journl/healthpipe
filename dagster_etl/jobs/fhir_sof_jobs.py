# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR (SOF) job definitions.

Jobs for extracting FHIR data from Aidbox sof views into MSSQL DW fhir.* tables
and syncing to the DSO reporting database.

NOTE: Infrastructure is now centralized. Use `full_infrastructure_job` from
the `infrastructure` module for all DDL setup (staging + DSO + FHIR SOF ViewDefinitions).
"""

import dagster as dg
from core.partitions import daily_partitions, monthly_partitions

# ============================================================================
# Asset Selections
# ============================================================================

fhir_sof_infrastructure_assets = dg.AssetSelection.assets(
    "staging_infra_fhir",
    "fhir_sof_infrastructure",
)
fhir_sof_daily_assets = fhir_sof_infrastructure_assets | dg.AssetSelection.groups("fhir_sof_daily")
fhir_sof_monthly_assets = fhir_sof_infrastructure_assets | dg.AssetSelection.groups(
    "fhir_sof_monthly"
)
fhir_dso_sync_daily_assets = dg.AssetSelection.groups("fhir_dso_sync_daily")
codesystem_sfi_sync_assets = dg.AssetSelection.assets("cs_sfi") | dg.AssetSelection.groups(
    "codesystem_dso_sync"
)

# ============================================================================
# Daily Job: Daily Extraction (infrastructure via full_infrastructure_job)
# ============================================================================

fhir_sof_daily_job = dg.define_asset_job(
    name="fhir_sof_daily_job",
    selection=fhir_sof_daily_assets,
    partitions_def=daily_partitions,
    tags={"concurrency_key": "one"},
    description=(
        "Daily SQL on FHIR extraction: extract from sof views, "
        "MERGE into fhir.* tables in staging DW. "
        "Ensures FHIR tables and Aidbox SOF ViewDefinitions before extraction."
    ),
)

# ============================================================================
# Monthly Job: Monthly Extraction (infrastructure via full_infrastructure_job)
# ============================================================================

fhir_sof_monthly_job = dg.define_asset_job(
    name="fhir_sof_monthly_job",
    selection=fhir_sof_monthly_assets,
    partitions_def=monthly_partitions,
    tags={"concurrency_key": "one"},
    description=(
        "Monthly SQL on FHIR extraction: extract from sof views, "
        "MERGE into fhir.* tables in staging DW. "
        "Ensures FHIR tables and Aidbox SOF ViewDefinitions before extraction."
    ),
)

# ============================================================================
# DSO Sync Job (unpartitioned)
# ============================================================================

fhir_dso_sync_job = dg.define_asset_job(
    name="fhir_dso_sync_job",
    selection=fhir_dso_sync_daily_assets,
    description="Sync FHIR flat tables (fhir.*) from staging to DSO reporting database",
)


codesystem_sfi_sync_job = dg.define_asset_job(
    name="codesystem_sfi_sync_job",
    selection=codesystem_sfi_sync_assets,
    description=(
        "Build cs_sfi in staging and sync codesystem.sfi_* to DSO. "
        "Run this when source_model/version metadata in SFI_datapoint is unexpectedly null."
    ),
)

__all__ = [
    "fhir_sof_daily_job",
    "fhir_sof_monthly_job",
    "fhir_dso_sync_job",
    "codesystem_sfi_sync_job",
]
