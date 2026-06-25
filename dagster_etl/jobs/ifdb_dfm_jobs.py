# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DFM-based job definitions for IFDB archive and FHIR pipelines.

These jobs use DFM checkpoint tracking instead of Dagster partitions.

Job hierarchy:
  - fhir_ifdb_dfm_base_job: Oracle → ext → arc → FHIR server (Patient, EoC, Encounter, Location)
  - fhir_ifdb_dfm_sof_dso_job: SOF + DSO sync for DFM resources (triggered by sensor)
  - ifdb_dfm_daily_job: Full end-to-end DFM pipeline (base + SOF + DSO sync)
"""

import dagster as dg

# ---------------------------------------------------------------------------
# Asset selections
# ---------------------------------------------------------------------------

ifdb_archive_assets = dg.AssetSelection.groups("ifdb_archive")
fhir_ifdb_dfm_assets = dg.AssetSelection.groups("fhir_ifdb_dfm")
fhir_sof_dfm_assets = dg.AssetSelection.groups("fhir_sof_dfm")
fhir_dso_sync_dfm_assets = dg.AssetSelection.groups("fhir_dso_sync_dfm")
fhir_quality_dfm_assets = dg.AssetSelection.groups("fhir_quality_dfm")
dfm_operations_assets = dg.AssetSelection.groups("dfm_operations")

# ---------------------------------------------------------------------------
# Archive job — extract from Oracle + MERGE into arc.*
# ---------------------------------------------------------------------------

ifdb_archive_job = dg.define_asset_job(
    name="ifdb_archive_job",
    selection=ifdb_archive_assets,
    description=(
        "Extract IFDB Patient, EpisodeOfCare, Encounter, and Location from Oracle to ext.*, "
        "then MERGE into arc.*. Uses DFM checkpoint tracking."
    ),
)

# ---------------------------------------------------------------------------
# FHIR DFM job — transform from arc.* and post to FHIR server
# ---------------------------------------------------------------------------

fhir_ifdb_dfm_job = dg.define_asset_job(
    name="fhir_ifdb_dfm_job",
    selection=fhir_ifdb_dfm_assets,
    description=(
        "Transform Patient, EpisodeOfCare, Encounter, and Location from arc.* archive tables "
        "to FHIR resources and post to FHIR server. DFM checkpoint-based."
    ),
)

# ---------------------------------------------------------------------------
# Full pipeline: archive + FHIR + SOF
# ---------------------------------------------------------------------------

ifdb_dfm_full_job = dg.define_asset_job(
    name="ifdb_dfm_full_job",
    selection=ifdb_archive_assets | fhir_ifdb_dfm_assets | fhir_sof_dfm_assets,
    description=("Full DFM pipeline: extract from Oracle → archive → FHIR server → SOF."),
)

# ---------------------------------------------------------------------------
# Full pipeline + DSO sync (daily-job equivalent for DFM core IFDB resources)
# ---------------------------------------------------------------------------

ifdb_dfm_daily_job = dg.define_asset_job(
    name="ifdb_dfm_daily_job",
    selection=(
        ifdb_archive_assets
        | fhir_ifdb_dfm_assets
        | fhir_sof_dfm_assets
        | fhir_dso_sync_dfm_assets
        | fhir_quality_dfm_assets
    ),
    description=(
        "DFM daily equivalent for Patient + EpisodeOfCare + Encounter + Location: "
        "Oracle → ext → arc → FHIR server → SOF fhir.* → DSO sync. "
        "Replaces the partition-based fhir_etl_ifdb_daily_job for these resources."
    ),
)

# ---------------------------------------------------------------------------
# DFM FHIR base job — Oracle → ext → arc → FHIR server for all 4 core resources
# (Patient, EoC, Encounter, Location). Sensor triggers downstream on success.
# ---------------------------------------------------------------------------

fhir_ifdb_dfm_base_job = dg.define_asset_job(
    name="fhir_ifdb_dfm_base_job",
    selection=ifdb_archive_assets | fhir_ifdb_dfm_assets,
    description=(
        "DFM FHIR base: Oracle → ext → arc → FHIR server for Patient, "
        "EpisodeOfCare, Encounter, Location. On success, a sensor triggers "
        "the remaining daily partitioned job and the DFM SOF+DSO job."
    ),
)

# ---------------------------------------------------------------------------
# DFM SOF+DSO job — SOF extraction + DSO sync for DFM resources
# (triggered by sensor after base job completes)
# ---------------------------------------------------------------------------

fhir_ifdb_dfm_sof_dso_job = dg.define_asset_job(
    name="fhir_ifdb_dfm_sof_dso_job",
    selection=fhir_sof_dfm_assets | fhir_dso_sync_dfm_assets | fhir_quality_dfm_assets,
    description=(
        "SOF extraction + DSO sync for DFM FHIR resources (Patient, PatientAddress, "
        "EpisodeOfCare, Encounter, Encounter_Location, Location). "
        "Triggered by sensor on fhir_ifdb_dfm_base_job success."
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
