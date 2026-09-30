# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized Schedules Module

All Dagster schedule definitions are consolidated here for discoverability and maintainability.

Structure:
- fhir_schedules.py: helper for legacy IFDB partition payloads (not registered)
- sql_schedules.py: SQL ETL schedules (erp, organization)
- ifdb_dfm_schedules.py: IFDB DFM base job schedule
"""

from schedules.erp_daily_dfm_schedules import erp_daily_dfm_schedule
from schedules.dim_dato_schedules import dim_dato_daily_schedule
from schedules.fhir_schedules import (
    fhir_codesystem_weekly_schedule,
    fhir_etl_schedule,
)
from schedules.ifdb_dfm_schedules import ifdb_dfm_base_schedule
from schedules.linked_server_schedules import linked_server_daily_schedule
from schedules.security_schedules import security_daily_schedule
from schedules.sql_schedules import (
    organization_daily_schedule,
)

# Aggregate all schedules for easy import in definitions.py
# NOTE: fhir_etl_schedule is NOT registered here — the legacy daily job is now
# triggered by the dfm_base_job_success_sensor after fhir_ifdb_dfm_base_job
# succeeds. It stays importable only for the schedule contract tests; it is a
# plain helper function, not a ScheduleDefinition.
all_schedules = [
    fhir_codesystem_weekly_schedule,
    organization_daily_schedule,
    erp_daily_dfm_schedule,
    dim_dato_daily_schedule,
    ifdb_dfm_base_schedule,
    linked_server_daily_schedule,
    security_daily_schedule,
]

__all__ = [
    # FHIR schedules
    "fhir_etl_schedule",
    "fhir_codesystem_weekly_schedule",
    # SQL schedules
    "organization_daily_schedule",
    # ERP Daily DFM schedule
    "erp_daily_dfm_schedule",
    # Shared date dimension daily schedule
    "dim_dato_daily_schedule",
    # IFDB DFM schedule
    "ifdb_dfm_base_schedule",
    # Linked Server schedule
    "linked_server_daily_schedule",
    # Security Daily schedule
    "security_daily_schedule",
    # All schedules collection
    "all_schedules",
]
