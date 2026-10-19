# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized Schedules Module

All Dagster schedule definitions are consolidated here for discoverability and maintainability.

Structure:
- fhir_schedules.py: FHIR CodeSystem refresh schedule
- sql_schedules.py: SQL ETL schedules (erp, organization)
- ifdb_dfm_schedules.py: the nightly IFDB -> FHIR pipeline schedule
- ifdb_fhir_fuldload_schedules.py: aftenskemaet for historikkens fuldindlæsning
"""

from schedules.erp_daily_dfm_schedules import erp_daily_dfm_schedule
from schedules.db_space_schedules import db_space_daily_schedule
from schedules.dim_dato_schedules import dim_dato_daily_schedule
from schedules.drg_schedules import (
    drg_freshness_daily_schedule,
    drg_psyk_ingest_schedule,
    drg_somatik_ingest_schedule,
)
from schedules.fhir_schedules import fhir_codesystem_weekly_schedule
from schedules.cpr_schedules import cpr_ingest_daily_schedule
from schedules.haiba_schedules import haiba_ingest_daily_schedule
from schedules.lpr3_schedules import lpr3_freshness_daily_schedule, lpr3_ingest_daily_schedule
from schedules.ifdb_dfm_schedules import ifdb_dfm_daily_schedule
from schedules.ifdb_fhir_fuldload_schedules import ifdb_fhir_fuldload_nightly_schedule
from schedules.linked_server_schedules import linked_server_daily_schedule
from schedules.security_schedules import security_daily_schedule
from schedules.silkeborgdata_schedules import silkeborgdata_daily_schedule
from schedules.sql_schedules import (
    organization_daily_schedule,
)
from schedules.udredningsret_schedules import udredningsret_ingest_daily_schedule

# Aggregate all schedules for easy import in definitions.py
all_schedules = [
    fhir_codesystem_weekly_schedule,
    organization_daily_schedule,
    erp_daily_dfm_schedule,
    dim_dato_daily_schedule,
    ifdb_dfm_daily_schedule,
    cpr_ingest_daily_schedule,
    ifdb_fhir_fuldload_nightly_schedule,
    haiba_ingest_daily_schedule,
    lpr3_ingest_daily_schedule,
    lpr3_freshness_daily_schedule,
    drg_somatik_ingest_schedule,
    drg_psyk_ingest_schedule,
    drg_freshness_daily_schedule,
    udredningsret_ingest_daily_schedule,
    linked_server_daily_schedule,
    security_daily_schedule,
    silkeborgdata_daily_schedule,
    db_space_daily_schedule,
]

__all__ = [
    # FHIR schedules
    "fhir_codesystem_weekly_schedule",
    # SQL schedules
    "organization_daily_schedule",
    # ERP Daily DFM schedule
    "erp_daily_dfm_schedule",
    # Shared date dimension daily schedule
    "dim_dato_daily_schedule",
    # HAIBA ingest schedule
    "cpr_ingest_daily_schedule",
    "haiba_ingest_daily_schedule",
    # LPR3 ingest and share-freshness schedules
    "lpr3_ingest_daily_schedule",
    "lpr3_freshness_daily_schedule",
    # DRG ingest queue schedules + the standalone freshness check
    "drg_somatik_ingest_schedule",
    "drg_psyk_ingest_schedule",
    "drg_freshness_daily_schedule",
    # Udredningsret ingest schedule
    "udredningsret_ingest_daily_schedule",
    # IFDB DFM schedule
    "ifdb_dfm_daily_schedule",
    # Linked Server schedule
    "linked_server_daily_schedule",
    # Security Daily schedule
    "security_daily_schedule",
    # SilkeborgData daily schedule
    "silkeborgdata_daily_schedule",
    "db_space_daily_schedule",
    # All schedules collection
    "all_schedules",
]
