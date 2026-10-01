# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Job definitions for FHIR CarePlan ETL.

This module defines jobs for full load, daily, and monthly partition strategies:
- Full load: Load of all markers CarePlans
- Daily: Delta update for patients with marker changes
- Monthly: Delta update for a full month (useful for backfilling)

Architecture:
- Extract asset: ["ifdb_extract", "eventmarker_alle_markoerer_{partition}"]
- Transform + Post asset: fhir_careplan_{partition}

Key characteristics:
- One CarePlan per PATIENT_ID containing ALL their markers
- No filtering or grouping by marker type
- Preserves original marker codes and timestamps
- Useful for comprehensive patient timeline analysis
"""

import dagster as dg

# =============================================================================
# Full Load Job
# =============================================================================

fhir_careplan_full_load_job = dg.define_asset_job(
    name="fhir_careplan_full_load_etl",
    description="""
    FHIR CarePlan Bulk ETL Job.

    Full load of all CarePlans containing all EVENTMARKER data per patient.

    Pipeline (2-asset pattern):
    1. Extract: ALL EVENTMARKER rows
    2. Transform + Post: Group by patient -> FHIR -> post

    Use for:
    - Initial load
    - Full refresh
    - Data reconciliation

    Prerequisites:
    - Patient resources must be loaded to FHIR server
    - FHIR server must be available
    """,
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_full_load"]),
    )
    | dg.AssetSelection.assets("fhir_careplan_full_load_ifdb"),
    tags={
        "domain": "careplan",
        "target": "fhir",
        "type": "etl",
        "partition": "full_load",
    },
)


# =============================================================================
# Daily Delta Job
# =============================================================================

fhir_careplan_daily_job = dg.define_asset_job(
    name="fhir_careplan_daily_etl",
    description="""
    FHIR CarePlan Daily ETL Job.

    Delta update for patients with marker changes in the partition window.

    Pipeline (2-asset pattern):
    1. Extract: Find patients with IFDB_MODIFICATION_DATETIME in partition window
               -> Extract ALL markers for those patients
    2. Transform + Post: Group by patient -> FHIR -> post

    Delta strategy:
    - All CarePlans for affected patients are fully recalculated
    - Uses upsert to create or update CarePlans

    Prerequisites:
    - Patient resources must be loaded to FHIR server (full load first)
    - FHIR server must be available
    """,
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_daily"]),
    )
    | dg.AssetSelection.assets("fhir_careplan_daily_ifdb"),
    tags={
        "domain": "careplan",
        "target": "fhir",
        "type": "etl",
        "partition": "daily",
    },
)


# =============================================================================
# Monthly Delta Job
# =============================================================================

fhir_careplan_monthly_job = dg.define_asset_job(
    name="fhir_careplan_monthly_etl",
    description="""
    FHIR CarePlan Monthly ETL Job.

    Delta update for patients with marker changes in the monthly partition window.

    Pipeline (2-asset pattern):
    1. Extract: Find patients with IFDB_MODIFICATION_DATETIME in partition window
               -> Extract ALL markers for those patients
    2. Transform + Post: Group by patient -> FHIR -> post

    Delta strategy:
    - All CarePlans for affected patients are fully recalculated
    - Uses upsert to create or update CarePlans

    Use for:
    - Monthly batch processing
    - Backfilling historical data
    - Catchup after outages

    Prerequisites:
    - Patient resources must be loaded to FHIR server (full load first)
    - FHIR server must be available
    """,
    selection=dg.AssetSelection.assets(
        dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_monthly"]),
    )
    | dg.AssetSelection.assets("fhir_careplan_monthly_ifdb"),
    tags={
        "domain": "careplan",
        "target": "fhir",
        "type": "etl",
        "partition": "monthly",
    },
)


__all__ = [
    "fhir_careplan_full_load_job",
    "fhir_careplan_daily_job",
    "fhir_careplan_monthly_job",
]
