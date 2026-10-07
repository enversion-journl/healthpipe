# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Infrastructure Jobs

Defines jobs for setting up database infrastructure:
- staging_infrastructure_job: Creates all objects on staging (DSI/DSA) database
- dso_infrastructure_job: Creates all objects on DSO (reporting) database
- full_infrastructure_job: Creates all objects on both databases
"""

import dagster as dg

# ============================================================================
# STAGING INFRASTRUCTURE JOB (DSI/DSA Layer)
# ============================================================================

staging_infrastructure_job = dg.define_asset_job(
    name="staging_infrastructure_setup",
    selection=dg.AssetSelection.groups("staging_infrastructure"),
    tags={
        "layer": "staging",
        "type": "infrastructure",
        "run_once": "true",
    },
    description="Setup infrastructure on staging (DSI/DSA) database.",
)


# ============================================================================
# DSO INFRASTRUCTURE JOB (Reporting Layer)
# ============================================================================

dso_infrastructure_job = dg.define_asset_job(
    name="dso_infrastructure_setup",
    selection=dg.AssetSelection.groups("dso_infrastructure"),
    tags={
        "layer": "dso",
        "type": "infrastructure",
        "run_once": "true",
    },
    description="Setup infrastructure on DSO (reporting) database.",
)


# ============================================================================
# FULL INFRASTRUCTURE JOB (Both Databases)
# ============================================================================

full_infrastructure_job = dg.define_asset_job(
    name="full_infrastructure_setup",
    selection=dg.AssetSelection.groups(
        "staging_infrastructure",
        "dso_infrastructure",
        "fhir_infrastructure",
    ),
    tags={
        "type": "infrastructure",
        "run_once": "true",
    },
    description=(
        "Setup infrastructure on staging, DSO, and FHIR (Aidbox PostgreSQL) "
        "databases — schemas, functions, views, and all indexes."
    ),
)

# ============================================================================
# DFM INFRASTRUCTURE JOB (DFM tables + IFDB archive tables)
# ============================================================================

dfm_infrastructure_job = dg.define_asset_job(
    name="dfm_infrastructure_setup",
    selection=dg.AssetSelection.assets(
        "staging_infra_schemas",
        "staging_infra_dfm",
        "staging_infra_ifdb_archive",
    ),
    tags={
        "layer": "staging",
        "type": "infrastructure",
    },
    description=(
        "Setup DFM infrastructure: meta.DFMState, meta.DFMRunLog, "
        "meta.usp_dfm_reset, arc.ifdb_encounter, arc.ifdb_location."
    ),
)


# ============================================================================
# DSO SyncWatermarks → DFMState cutover (one-shot per env)
# ============================================================================

migrate_dso_sync_watermarks_to_dfm_job = dg.define_asset_job(
    name="migrate_dso_sync_watermarks_to_dfm_job",
    selection=dg.AssetSelection.assets("migrate_dso_sync_watermarks_to_dfm"),
    tags={
        "layer": "cutover",
        "type": "migration",
        "run_once": "true",
    },
    description=(
        "One-shot cutover: copy DSO meta.SyncWatermarks into staging "
        "meta.DFMState (scope='dso'). Skips sfi_datapoint. Idempotent. "
        "Run once per environment before first watermark DSO sync on DFM code."
    ),
)


# ============================================================================
# LINKED SERVER JOB (Staging user + DSO linked server + verification)
# ============================================================================

linked_server_job = dg.define_asset_job(
    name="linked_server_setup",
    selection=dg.AssetSelection.assets(
        "staging_infra_linked_server_user",
        "dso_infra_linked_server",
        "dso_verify_linked_server",
    ),
    tags={
        "type": "infrastructure",
        "object": "linked_server",
    },
    description=(
        "Create and verify linked server from DSO to Staging: "
        "staging login/user, linked server config, connectivity verification."
    ),
)


__all__ = [
    "staging_infrastructure_job",
    "dso_infrastructure_job",
    "full_infrastructure_job",
    "dfm_infrastructure_job",
    "migrate_dso_sync_watermarks_to_dfm_job",
    "linked_server_job",
]
