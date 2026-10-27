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
- ifdb_dfm_jobs.py: the nightly IFDB -> FHIR pipeline + stage-scoped operator jobs
- fhir_ifdb_jobs.py: FHIR maintenance jobs for the IFDB Oracle source
- erp_jobs.py: Procurement ETL jobs
- organization_jobs.py: Organization dimension job
- infrastructure_jobs.py: Database infrastructure setup jobs
"""

# FHIR CodeSystem job (clinical SKS/SOR -> DSO + FHIR terminology server)
from jobs.codesystem_jobs import (
    fhir_codesystem_job,
)

# DRG job (SDS DRG-grouped LPR -> ext -> arc)
from jobs.drg_jobs import (
    DRG_INGEST_JOBS,
    drg_freshness_job,
    drg_publish_job,
)

# HAIBA job (weekly SDS deliveries -> ext -> arc)
from jobs.cpr_jobs import cpr_ingest_job
from jobs.haiba_jobs import haiba_ingest_job, haiba_publish_job

# LPR3 Grunddata jobs (weekly/monthly SDS snapshot -> ext -> arc; share freshness)
from jobs.lpr3_jobs import lpr3_freshness_job, lpr3_ingest_job

# Udredningsret job (SDS LPR_Udredningsret weekly snapshot -> ext -> arc)
from jobs.udredningsret_jobs import udredningsret_ingest_job, udredningsret_publish_job

# FHIR IFDB jobs
# Data quality jobs
from jobs.data_quality_jobs import data_quality_job

# ERP daily DFM job (lagerbeholdning + KMD archive + dim.UNSPSC)
from jobs.erp_daily_dfm_jobs import erp_daily_dfm_job
from jobs.dim_dato_jobs import dim_dato_daily_job
from jobs.populationsansvar_jobs import populationsansvar_ref_job

# SQL ERP jobs
from jobs.erp_jobs import (
    erp_faktura_reset_job,
    erp_infrastructure_job,
)

from jobs.fhir_ifdb_jobs import (
    sfi_datapoint_reconciliation_job,
)

# FHIR SOF jobs (SQL on FHIR extraction)
from jobs.fhir_sof_jobs import (
    codesystem_sfi_sync_job,
)

# IFDB DFM jobs (archive + FHIR with DFM checkpoint tracking)
from jobs.ifdb_dfm_jobs import (
    dfm_reset_job,
    fhir_ifdb_dfm_job,
    fhir_sof_dfm_job,
    ifdb_archive_job,
    ifdb_dfm_daily_job,
    ifdb_dfm_full_job,
    period_orchestrator_job,
)

# ILS DFM archive job
from jobs.ils_jobs import ils_archive_job

# Infrastructure jobs
from jobs.db_space_jobs import db_space_daily_job
from jobs.infrastructure_jobs import (
    dfm_infrastructure_job,
    dso_infrastructure_job,
    full_infrastructure_job,
    linked_server_job,
    migrate_dso_sync_watermarks_to_dfm_job,
    staging_infrastructure_job,
)

# SQL KMD UdData jobs
from jobs.kmd_uddata_jobs import kmd_uddata_archive_job

# SilkeborgData archive job (delivery share -> ext -> arc)
from jobs.silkeborgdata_jobs import silkeborgdata_daily_job

# SQL Lager jobs
from jobs.lager_jobs import lager_dim_job

# MySupply DFM archive job
from jobs.mysupply_jobs import mysupply_archive_job

# SQL Organization jobs
from jobs.organization_jobs import (
    organisation_cs_job,
)

# OS2mo (Magenta) integration — codesystems out, enriched units back in
from jobs.os2mo_jobs import (
    os2mo_codesystem_export_job,
)

# Security (DPA / RBAC) daily job
from jobs.security_jobs import security_daily_job

# SydAX archive job (ext.*_sydax_dfm → arc.*_sydax)
from jobs.sydax_archive_job import sydax_archive_job

# Env-gated dev-only assets (empty in prod deployments)
from DSA.SQL.dev import all_assets as erp_dev_assets

# Aggregate all jobs for easy import in definitions.py
all_jobs = [
    # FHIR Jobs - IFDB
    sfi_datapoint_reconciliation_job,
    # DWH Organisation Jobs
    organisation_cs_job,
    # OS2mo (Magenta) organisation integration
    os2mo_codesystem_export_job,
    # FHIR CodeSystem Job (clinical SKS/SOR terminology -> DSO + FHIR)
    fhir_codesystem_job,
    *DRG_INGEST_JOBS,
    drg_freshness_job,
    drg_publish_job,
    cpr_ingest_job,
    haiba_ingest_job,
    haiba_publish_job,
    lpr3_ingest_job,
    lpr3_freshness_job,
    udredningsret_ingest_job,
    udredningsret_publish_job,
    # DWH Lager Jobs
    lager_dim_job,
    # ERP Daily DFM Job (lagerbeholdning + KMD archive + dim.UNSPSC)
    erp_daily_dfm_job,
    # Shared date dimension daily refresh
    dim_dato_daily_job,
    # Municipality to population responsibility reference table
    populationsansvar_ref_job,
    # KMD UdData Jobs
    kmd_uddata_archive_job,
    # SilkeborgData Archive Job
    silkeborgdata_daily_job,
    # MySupply DFM Archive Job
    mysupply_archive_job,
    # ILS DFM Archive Job
    ils_archive_job,
    # ERP Jobs
    erp_infrastructure_job,
    # FHIR SOF Jobs (SQL on FHIR extraction)
    codesystem_sfi_sync_job,
    # Centralized Infrastructure Jobs
    staging_infrastructure_job,
    dso_infrastructure_job,
    full_infrastructure_job,
    dfm_infrastructure_job,
    migrate_dso_sync_watermarks_to_dfm_job,
    linked_server_job,
    db_space_daily_job,
    # Data Quality
    data_quality_job,
    # Security Daily Job
    security_daily_job,
    # SydAX Archive Job
    sydax_archive_job,
    # IFDB DFM Jobs (archive + FHIR with checkpoint tracking)
    ifdb_archive_job,
    fhir_ifdb_dfm_job,
    ifdb_dfm_full_job,
    ifdb_dfm_daily_job,
    fhir_sof_dfm_job,
    dfm_reset_job,
    period_orchestrator_job,
]

# The dev-only reset job selects erp_faktura_reset_tables, whose registration is
# env-gated; the job must only exist in deployments where the asset does,
# otherwise Definitions fails to resolve the selection.
if erp_dev_assets:
    all_jobs.append(erp_faktura_reset_job)

__all__ = [
    # FHIR IFDB jobs
    "sfi_datapoint_reconciliation_job",
    # ERP jobs
    "erp_faktura_reset_job",
    "erp_infrastructure_job",
    # Organisation jobs
    "organisation_cs_job",
    "os2mo_codesystem_export_job",
    # FHIR CodeSystem job
    "fhir_codesystem_job",
    "DRG_INGEST_JOBS",
    "drg_freshness_job",
    "drg_publish_job",
    "cpr_ingest_job",
    "haiba_ingest_job",
    "haiba_publish_job",
    # LPR3 jobs
    "lpr3_ingest_job",
    "lpr3_freshness_job",
    "udredningsret_ingest_job",
    "udredningsret_publish_job",
    # Lager jobs
    "lager_dim_job",
    # ERP Daily DFM job
    "erp_daily_dfm_job",
    # Shared date dimension daily job
    "dim_dato_daily_job",
    # Municipality to population responsibility reference job
    "populationsansvar_ref_job",
    # KMD UdData jobs
    "kmd_uddata_archive_job",
    # SilkeborgData archive job
    "silkeborgdata_daily_job",
    # MySupply DFM Archive job
    "mysupply_archive_job",
    # ILS DFM Archive job
    "ils_archive_job",
    # FHIR SOF jobs
    "codesystem_sfi_sync_job",
    # Infrastructure jobs
    "staging_infrastructure_job",
    "dso_infrastructure_job",
    "full_infrastructure_job",
    "dfm_infrastructure_job",
    "migrate_dso_sync_watermarks_to_dfm_job",
    "linked_server_job",
    "db_space_daily_job",
    # Data Quality jobs
    "data_quality_job",
    # IFDB DFM jobs
    "ifdb_archive_job",
    "fhir_ifdb_dfm_job",
    "ifdb_dfm_full_job",
    "ifdb_dfm_daily_job",
    "fhir_sof_dfm_job",
    "dfm_reset_job",
    "period_orchestrator_job",
    # SydAX archive job
    "sydax_archive_job",
    # Security daily job
    "security_daily_job",
    # All jobs collection
    "all_jobs",
]
