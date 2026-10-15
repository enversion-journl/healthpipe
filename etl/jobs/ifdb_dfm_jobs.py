# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DFM-based job definitions for IFDB archive and FHIR pipelines.

These jobs use DFM checkpoint tracking instead of Dagster partitions.

``ifdb_dfm_daily_job`` is the whole nightly pipeline in one job. It replaced the
three-job sensor chain (base -> partitioned daily + SOF/DSO) once CarePlan — the
last partitioned producer — moved to the DFM path: with nothing partitioned left,
there is no partition key to hand a downstream run and therefore no reason to
split the pipeline across jobs and sensors.

The rest are stage-scoped operator jobs for re-running one layer on its own
(archive, FHIR transform, SOF), plus the DFM reset and period orchestrator.
"""

import dagster as dg
from jobs.fhir_ifdb_jobs import _max_concurrent
from jobs.fhir_sof_jobs import codesystem_sfi_sync_assets, fhir_sof_infrastructure_assets

# Cap per-run asset concurrency. SFI's XML ProcessPool multiplies each asset's
# footprint, so the Definitions default of 8 exceeds the pod's memory; 4 fits.
_dfm_multiprocess_executor = dg.multiprocess_executor.configured(
    {"max_concurrent": _max_concurrent("FHIR_IFDB_DFM_MAX_CONCURRENT", 4)}
)

# ---------------------------------------------------------------------------
# Asset selections
# ---------------------------------------------------------------------------

ifdb_archive_assets = dg.AssetSelection.groups("ifdb_archive")
fhir_ifdb_dfm_assets = dg.AssetSelection.groups("fhir_ifdb_dfm")

# SOF extraction reads Aidbox views that Aidbox materializes from the deployed
# ViewDefinitions, so every job that reads them must first apply the current
# ones (fhir_sof_infrastructure) and the staging fhir.* DDL they MERGE into
# (staging_infra_fhir) — otherwise a deploy with a changed ViewDefinition is
# never picked up, and the MERGE runs against a stale target schema.
fhir_sof_dfm_assets = fhir_sof_infrastructure_assets | dg.AssetSelection.groups("fhir_sof_dfm")
fhir_dso_sync_dfm_assets = dg.AssetSelection.groups("fhir_dso_sync_dfm")
fhir_quality_dfm_assets = dg.AssetSelection.groups("fhir_quality_dfm")
dfm_operations_assets = dg.AssetSelection.groups("dfm_operations")

_IFDB_RESOURCES = (
    "Patient, EpisodeOfCare, Encounter, Location, Location_SOR, DocumentReference, "
    "Condition, Procedure, ServiceRequest, Aktivitetsresultat/SFI, Appointment, CarePlan"
)

# Borger chain after Patient SOF — explicit keys so `ref` / shared_dimensions_dso_sync
# do not pull Organisation (or other shared dims) into the FHIR DFM jobs.
borger_dim_assets = dg.AssetSelection.assets(
    "ref_patient_borger",
    "ref_patient_address_borger",
    "dim_Borger",
    "dso_sync_dim_borger",
)

# ---------------------------------------------------------------------------
# Archive job — extract from Oracle + MERGE into arc.*
# ---------------------------------------------------------------------------

ifdb_archive_job = dg.define_asset_job(
    name="ifdb_archive_job",
    selection=ifdb_archive_assets,
    description=(
        f"Extract IFDB {_IFDB_RESOURCES} from Oracle to ext.*, "
        "then MERGE into arc.*. Uses DFM checkpoint tracking."
    ),
)

# ---------------------------------------------------------------------------
# FHIR DFM job — transform from arc.* and post to FHIR server
# ---------------------------------------------------------------------------

fhir_ifdb_dfm_job = dg.define_asset_job(
    name="fhir_ifdb_dfm_job",
    selection=fhir_ifdb_dfm_assets,
    executor_def=_dfm_multiprocess_executor,
    description=(
        f"Transform {_IFDB_RESOURCES} from arc.* archive tables "
        "to FHIR resources and post to FHIR server. DFM checkpoint-based."
    ),
)

# ---------------------------------------------------------------------------
# Full pipeline: archive + FHIR + SOF
# ---------------------------------------------------------------------------

ifdb_dfm_full_job = dg.define_asset_job(
    name="ifdb_dfm_full_job",
    selection=ifdb_archive_assets | fhir_ifdb_dfm_assets | fhir_sof_dfm_assets,
    executor_def=_dfm_multiprocess_executor,
    description=("Full DFM pipeline: extract from Oracle → archive → FHIR server → SOF."),
)

# ---------------------------------------------------------------------------
# The nightly IFDB → FHIR pipeline, end to end, in one job
# ---------------------------------------------------------------------------

ifdb_dfm_daily_job = dg.define_asset_job(
    name="ifdb_dfm_daily_job",
    selection=(
        ifdb_archive_assets
        | fhir_ifdb_dfm_assets
        | fhir_sof_dfm_assets
        | fhir_dso_sync_dfm_assets
        | fhir_quality_dfm_assets
        | codesystem_sfi_sync_assets
        | borger_dim_assets
    ),
    executor_def=_dfm_multiprocess_executor,
    description=(
        f"The nightly IFDB → FHIR pipeline for {_IFDB_RESOURCES}: "
        "Oracle → ext → arc → FHIR server → SOF fhir.* → DSO sync, then the "
        "Borger chain (ref_patient_borger → ref_patient_address_borger → "
        "dim_Borger → dso_sync_dim_borger) and the SFI_datapoint refresh. "
        "Triggered by ifdb_dfm_daily_schedule at 06:00 CET."
    ),
)

# ---------------------------------------------------------------------------
# SOF DFM job — extract from FHIR server views and MERGE into fhir.* tables
# ---------------------------------------------------------------------------

fhir_sof_dfm_job = dg.define_asset_job(
    name="fhir_sof_dfm_job",
    selection=fhir_sof_dfm_assets,
    description=(
        "Extract DFM FHIR resources from Aidbox sof views "
        "and MERGE into fhir.* tables. DFM checkpoint-based."
    ),
)

# ---------------------------------------------------------------------------
# DFM Reset job
# ---------------------------------------------------------------------------

dfm_reset_job = dg.define_asset_job(
    name="dfm_reset_job",
    selection=dg.AssetSelection.assets("dfm_reset"),
    description="Reset a DFM checkpoint to a specific value. Set config in Launchpad.",
)

# ---------------------------------------------------------------------------
# Period Orchestrator job
# ---------------------------------------------------------------------------

period_orchestrator_job = dg.define_asset_job(
    name="period_orchestrator_job",
    selection=dg.AssetSelection.assets("period_orchestrator"),
    description=(
        "Run a target job for each date partition in a range. "
        "Use for historical reloads. Set config in Launchpad."
    ),
)
