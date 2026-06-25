# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized Jobs Module

All Dagster job definitions are consolidated here for discoverability and maintainability.
Jobs orchestrate asset execution but do not contain business logic.

Structure:
- fhir_ifdb_jobs.py: FHIR ETL jobs for IFDB Oracle source
- erp_jobs.py: Procurement ETL jobs
- organization_jobs.py: Organization dimension job
- shared_dimensions_jobs.py: Shared dimensions jobs
- infrastructure_jobs.py: Database infrastructure setup jobs
"""

# FHIR IFDB jobs
# Data quality jobs
from jobs.data_quality_jobs import data_quality_job

# SQL ERP jobs
from jobs.erp_jobs import (
    erp_daily_job,
    erp_dev_vareudlevering_job,
    erp_faktura_reset_job,
    erp_full_load_job,
    erp_infrastructure_job,
    erp_monthly_job,
)

# FHIR CarePlan jobs
from jobs.fhir_careplan_jobs import (
    fhir_careplan_daily_job,
    fhir_careplan_full_load_job,
    fhir_careplan_monthly_job,
)
from jobs.fhir_ifdb_jobs import (
    fhir_etl_ifdb_daily_job,
    fhir_etl_ifdb_full_load_job,
    fhir_etl_ifdb_monthly_job,
    sfi_datapoint_reconciliation_job,
)

# FHIR SOF jobs (SQL on FHIR extraction + DSO sync)
from jobs.fhir_sof_jobs import (
    codesystem_sfi_sync_job,
    fhir_dso_sync_job,
    fhir_sof_daily_job,
    fhir_sof_monthly_job,
)

# Infrastructure jobs
from jobs.infrastructure_jobs import (
    dfm_infrastructure_job,
    dso_infrastructure_job,
    full_infrastructure_job,
    staging_infrastructure_job,
)

# SQL Lager jobs
from jobs.lager_jobs import lager_dim_job

# ERP daily DFM job (lagerbeholdning + KMD archive + dim.UNSPSC)
from jobs.erp_daily_dfm_jobs import erp_daily_dfm_job

# SQL KMD UdData jobs
from jobs.kmd_uddata_jobs import kmd_uddata_archive_job

# MySupply DFM archive job
from jobs.mysupply_jobs import mysupply_archive_job

# ILS DFM archive job
from jobs.ils_jobs import ils_archive_job

# SQL Organization jobs
from jobs.organization_jobs import (
    organisation_cs_job,
)

# FHIR CodeSystem job (clinical SKS/SOR -> DSO + FHIR terminology server)
from jobs.codesystem_jobs import (
    fhir_codesystem_job,
)

# SQL Shared Dimensions jobs
from jobs.shared_dimensions_jobs import (
    shared_dimensions_data_job,
)

# SydAX archive job (ext.*_sydax_dfm → arc.*_sydax)
from jobs.sydax_archive_job import sydax_archive_job

# IFDB DFM jobs (archive + FHIR with DFM checkpoint tracking)
from jobs.ifdb_dfm_jobs import (
    dfm_reset_job,
    fhir_ifdb_dfm_base_job,
    fhir_ifdb_dfm_job,
    fhir_ifdb_dfm_sof_dso_job,
    fhir_sof_dfm_job,
    ifdb_archive_job,
    ifdb_dfm_daily_job,
    ifdb_dfm_full_job,
    period_orchestrator_job,
)

# Migration jobs (staging + DSO)
from migrations.run_migration_asset import (
    run_migration_dso_job,
    run_migration_staging_job,
)

# Aggregate all jobs for easy import in definitions.py
all_jobs = [
    # FHIR Jobs - IFDB
    fhir_etl_ifdb_daily_job,
    fhir_etl_ifdb_monthly_job,
    fhir_etl_ifdb_full_load_job,
    sfi_datapoint_reconciliation_job,
    # DWH Organisation Jobs
    organisation_cs_job,
    # FHIR CodeSystem Job (clinical SKS/SOR terminology -> DSO + FHIR)
    fhir_codesystem_job,
    # DWH Lager Jobs
    lager_dim_job,
    # ERP Daily DFM Job (lagerbeholdning + KMD archive + dim.UNSPSC)
    erp_daily_dfm_job,
    # KMD UdData Jobs
    kmd_uddata_archive_job,
    # MySupply DFM Archive Job
    mysupply_archive_job,
    # ILS DFM Archive Job
    ils_archive_job,
    # Shared Dimensions Jobs
    shared_dimensions_data_job,
    # FHIR CarePlan Jobs
    fhir_careplan_full_load_job,
    fhir_careplan_daily_job,
    fhir_careplan_monthly_job,
    # ERP Jobs
    erp_daily_job,
    erp_dev_vareudlevering_job,
    erp_faktura_reset_job,
    erp_full_load_job,
    erp_monthly_job,
    erp_infrastructure_job,
    # FHIR SOF Jobs (SQL on FHIR extraction + DSO sync)
    fhir_sof_daily_job,
    fhir_sof_monthly_job,
    fhir_dso_sync_job,
    codesystem_sfi_sync_job,
    # Centralized Infrastructure Jobs
    staging_infrastructure_job,
    dso_infrastructure_job,
    full_infrastructure_job,
    dfm_infrastructure_job,
    # Migrations (staging + DSO)
    run_migration_staging_job,
    run_migration_dso_job,
    # Data Quality
    data_quality_job,
    # SydAX Archive Job
    sydax_archive_job,
    # IFDB DFM Jobs (archive + FHIR with checkpoint tracking)
    ifdb_archive_job,
    fhir_ifdb_dfm_job,
    fhir_ifdb_dfm_base_job,
    fhir_ifdb_dfm_sof_dso_job,
    ifdb_dfm_full_job,
    ifdb_dfm_daily_job,
    fhir_sof_dfm_job,
    dfm_reset_job,
    period_orchestrator_job,
]

__all__ = [
    # FHIR IFDB jobs
    "fhir_etl_ifdb_daily_job",
    "fhir_etl_ifdb_monthly_job",
    "fhir_etl_ifdb_full_load_job",
    "sfi_datapoint_reconciliation_job",
    # ERP jobs
    "erp_daily_job",
    "erp_dev_vareudlevering_job",
    "erp_faktura_reset_job",
    "erp_full_load_job",
    "erp_monthly_job",
    "erp_infrastructure_job",
    # FHIR CarePlan jobs
    "fhir_careplan_daily_job",
    "fhir_careplan_full_load_job",
    "fhir_careplan_monthly_job",
    # Organisation jobs
    "organisation_cs_job",
    # FHIR CodeSystem job
    "fhir_codesystem_job",
    # Lager jobs
    "lager_dim_job",
    # ERP Daily DFM job
    "erp_daily_dfm_job",
    # KMD UdData jobs
    "kmd_uddata_archive_job",
    # MySupply DFM Archive job
    "mysupply_archive_job",
    # ILS DFM Archive job
    "ils_archive_job",
    # Shared Dimensions jobs
    "shared_dimensions_data_job",
    # FHIR SOF jobs
    "fhir_sof_daily_job",
    "fhir_sof_monthly_job",
    "fhir_dso_sync_job",
    "codesystem_sfi_sync_job",
    # Infrastructure jobs
    "staging_infrastructure_job",
    "dso_infrastructure_job",
    "full_infrastructure_job",
    "dfm_infrastructure_job",
    # Data Quality jobs
    "data_quality_job",
    # IFDB DFM jobs
    "ifdb_archive_job",
    "fhir_ifdb_dfm_job",
    "fhir_ifdb_dfm_base_job",
    "fhir_ifdb_dfm_sof_dso_job",
    "ifdb_dfm_full_job",
    "ifdb_dfm_daily_job",
    "fhir_sof_dfm_job",
    "dfm_reset_job",
    "period_orchestrator_job",
    # SydAX archive job
    "sydax_archive_job",
    # All jobs collection
    "all_jobs",
]
