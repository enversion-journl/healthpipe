# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP Jobs

Separates infrastructure setup (DDL) from daily data processing (DML).
Includes DSO sync as final step to push data to reporting database.

NOTE: Infrastructure is now centralized. Use `staging_infrastructure_job` from
the `infrastructure` module for staging DDL, and `dso_infrastructure_job` for DSO DDL.
The `erp_infrastructure_job` here provides a convenient way to run both
staging and DSO infrastructure together for the ERP domain.

Jobs:
- erp_daily_etl - Daily partitioned ETL for transactional data
- erp_monthly_etl - Monthly partitioned ETL for aggregated data
- erp_infrastructure_setup - DDL setup job (centralized)
"""

from core.partitions import daily_partitions_erp, monthly_partitions_erp
from dagster import AssetSelection, define_asset_job

# Daily ETL Job - Data assets + DSO sync
# Pipeline order: Staging → Maps → Dimensions → Facts → DSO Sync
erp_daily_job = define_asset_job(
    name="erp_daily_etl",
    description="Daily ETL: Staging → Maps → Dimensions → Facts → DSO Sync",
    selection=AssetSelection.groups(
        "organisation_codesystem_extraction",  # EAN codesystem (concepts.csv) etc.
        "erp_extract",
        "erp_extract_daily",  # Daily partitioned extract assets
        "erp_staging",
        "erp_maps",
        "erp_dimensions",
        "erp_facts",
        "erp_dso_sync",  # Sync to DSO database as final step
    )
    | AssetSelection.assets("ref_faktura_to_postering"),
    partitions_def=daily_partitions_erp,
    tags={
        "dagster/priority": "high",
        "domain": "datawarehouse",
        "type": "etl",
        "frequency": "daily",
    },
)

# NOTE: Lagerbeholdning assets (fct.Lagerbeholdning, fct.AfdelingLagerbeholdning)
# are NOT included in monthly runs. Source tables dbo.lgrbeh and dbo.afdlgrbeh
# contain current state only (no history). Rows are updated in-place and may be
# deleted when consumed. Monthly backfill cannot reconstruct historical stock —
# only daily snapshots of current stock are meaningful.

# Monthly ETL Job - Monthly partitioned staging assets + downstream processing
# Same pipeline structure as daily but with monthly aggregation
erp_monthly_job = define_asset_job(
    name="erp_monthly_etl",
    description="Monthly ETL: Staging → Maps → Dimensions → Facts → DSO Sync",
    selection=AssetSelection.groups(
        "organisation_codesystem_extraction",  # EAN codesystem (concepts.csv) etc.
        "erp_extract",  # ILS full-load extract assets (small reference tables)
        "erp_extract_monthly",  # Monthly partitioned extract assets
        "erp_staging",
        "erp_maps",
        "erp_dimensions",
        "erp_facts",
        "erp_dso_sync",  # Sync to DSO database as final step
    )
    | AssetSelection.assets("ref_faktura_to_postering"),
    partitions_def=monthly_partitions_erp,
    tags={
        "dagster/priority": "high",
        "domain": "datawarehouse",
        "type": "etl",
        "frequency": "monthly",
    },
)

# Full-Load ETL Job - On-demand full extraction without date partition filters
# Extracts all data from source tables (no WHERE clause on partition columns).
# Non-partitioned — trigger manually for initial loads or re-extractions.
erp_full_load_job = define_asset_job(
    name="erp_full_load_etl",
    description=(
        "Full-load ETL: Extracts all source data without partition date filters. "
        "Use for initial loads or full re-extractions."
    ),
    selection=AssetSelection.groups(
        "erp_extract",
        "erp_extract_full_load",
        "erp_staging",
        "erp_maps",
        "erp_dimensions",
        "erp_facts",
        "erp_dso_sync",
    )
    | AssetSelection.assets("ref_faktura_to_postering"),
    tags={
        "dagster/priority": "low",
        "domain": "datawarehouse",
        "type": "etl",
        "frequency": "on_demand",
    },
)

# Infrastructure Setup Job - Uses centralized infrastructure for both staging and DSO
# This creates all schemas, functions, tables, indexes on both databases
erp_infrastructure_job = define_asset_job(
    name="erp_infrastructure_setup",
    description="Create/update all infrastructure on staging and DSO: schemas, functions, tables, indexes, SQL-managed views",
    selection=AssetSelection.groups(
        "staging_infrastructure",  # Centralized staging DDL (creates tables on staging DB)
        "dso_infrastructure",  # Centralized DSO DDL (creates tables on DSO DB)
    ),
    tags={
        "domain": "datawarehouse",
        "type": "ddl",
        "frequency": "on_demand",
    },
)

# ============================================================================
# DEV-ONLY: Isolated job for fact_vareudlevering development
# ============================================================================
# NOTE: This job is purely for development/debugging of the vareudlevering pipeline.
# It should NOT be used in production — use erp_daily_etl / erp_monthly_etl instead.
# Remove this job once the vareudlevering feature is validated and merged.
erp_dev_vareudlevering_job = define_asset_job(
    name="erp_dev_vareudlevering",
    description=(
        "DEV ONLY — Isolated pipeline for fact_vareudlevering: "
        "Extract (ext_udlev_ils_daily) → Pre-staging → Maps → Dim Lager → Fact staging → Fact load"
    ),
    selection=(
        # Extract: ext_udlev_ils_daily (individual asset, no multi_asset neighbors needed)
        AssetSelection.assets("ext_udlev_ils_daily")
        # Pre-staging
        | AssetSelection.assets(["erp", "pre_stg_vareudlevering_ils"])
        # Dimension staging (Layer 1 — feeds MapUpd + DimUpd)
        | AssetSelection.assets(["erp", "stg_dim_vareudlevering_ils"])
        # Map: assign DW_EK_Vareudlevering (the fact's own surrogate key)
        | AssetSelection.assets("map_vareudlevering")
        # Lager keys used by Organisation lookup path in fct_vareudlevering
        | AssetSelection.assets("map_lager")
        | AssetSelection.assets("dim_lager")
        # Dimension
        | AssetSelection.assets("dim_vareudlevering_egenskaber")
        # Fact staging
        | AssetSelection.assets(["erp", "stg_fct_vareudlevering_ils"])
        # Fact load (looks up DW_EK_Lager, DW_EK_Vare, DW_EK_Dato_Vareudlevering via LEFT JOIN)
        | AssetSelection.assets("fct_vareudlevering")
    ),
    tags={
        "domain": "datawarehouse",
        "type": "etl",
        "frequency": "dev_only",
    },
)

# ============================================================================
# DEV-ONLY: Faktura pipeline reset
# ============================================================================
erp_faktura_reset_job = define_asset_job(
    name="erp_faktura_reset",
    description=(
        "DEV ONLY — Clear faktura-related tables (ref, fct, dim, map, staging) "
        "for a fresh reload cycle."
    ),
    selection=AssetSelection.assets("erp_faktura_reset_tables"),
    tags={
        "domain": "datawarehouse",
        "type": "dev_only",
        "frequency": "on_demand",
    },
)


__all__ = [
    "erp_daily_job",
    "erp_monthly_job",
    "erp_full_load_job",
    "erp_infrastructure_job",
    "erp_dev_vareudlevering_job",
    "erp_faktura_reset_job",
]
