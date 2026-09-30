# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ERP Jobs

Infrastructure setup (DDL) and a dev-only faktura reset helper for the ERP
domain. Daily data processing now runs through ``erp_daily_dfm_job`` (see
``jobs/erp_daily_dfm_jobs.py``); the legacy partitioned ETL jobs and their
source-extract assets were retired.

NOTE: Infrastructure is now centralized. Use `staging_infrastructure_job` from
the `infrastructure` module for staging DDL, and `dso_infrastructure_job` for DSO DDL.
The `erp_infrastructure_job` here provides a convenient way to run both
staging and DSO infrastructure together for the ERP domain.

Jobs:
- erp_infrastructure_setup - DDL setup job (centralized)
- erp_faktura_reset - dev-only faktura pipeline reset
"""

from dagster import AssetSelection, define_asset_job

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
    "erp_infrastructure_job",
    "erp_faktura_reset_job",
]
