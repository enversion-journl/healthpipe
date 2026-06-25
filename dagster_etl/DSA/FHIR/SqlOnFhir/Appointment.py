# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Appointment extraction assets

Extracts Appointment flat view from Aidbox sof schema and MERGEs into fhir.Appointment.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
from core.partitions import daily_partitions, monthly_partitions
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    extract_and_merge_sof_view,
)

_CFG = SOF_RESOURCE_CONFIGS["appointment_view"]


@dg.asset(
    name="sof_appointment_daily",
    group_name="fhir_sof_daily",
    compute_kind="postgres",
    partitions_def=daily_partitions,
    deps=["fhir_sof_infrastructure", "fhir_appointment_daily_ifdb"],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    description="Extract Appointment flat view from Aidbox sof schema (daily) and MERGE into fhir.Appointment",
)
def sof_appointment_daily(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    start_dt = datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=UTC)
    end_dt = start_dt + timedelta(days=1)

    row_count, rows_affected = extract_and_merge_sof_view(
        fhir_database,
        mssql_target_staging,
        _CFG["view_name"],
        _CFG["resource_table"],
        _CFG["id_column"],
        _CFG["mssql_table"],
        _CFG["primary_keys"],
        partition_start=start_dt.isoformat(),
        partition_end=end_dt.isoformat(),
        context=context,
        null_sentinel_columns=_CFG.get("null_sentinel_columns"),
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "rows_affected": rows_affected,
            "partition": partition_key,
            "source_view": f"sof.{_CFG['view_name']}",
            "target_table": f"fhir.{_CFG['mssql_table']}",
        }
    )


@dg.asset(
    name="sof_appointment_monthly",
    group_name="fhir_sof_monthly",
    compute_kind="postgres",
    partitions_def=monthly_partitions,
    deps=["fhir_sof_infrastructure", "fhir_appointment_monthly_ifdb"],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    description="Extract Appointment flat view from Aidbox sof schema (monthly) and MERGE into fhir.Appointment",
)
def sof_appointment_monthly(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    start_dt = datetime.strptime(partition_key, "%Y-%m").replace(day=1, tzinfo=UTC)
    if start_dt.month == 12:
        end_dt = start_dt.replace(year=start_dt.year + 1, month=1)
    else:
        end_dt = start_dt.replace(month=start_dt.month + 1)

    row_count, rows_affected = extract_and_merge_sof_view(
        fhir_database,
        mssql_target_staging,
        _CFG["view_name"],
        _CFG["resource_table"],
        _CFG["id_column"],
        _CFG["mssql_table"],
        _CFG["primary_keys"],
        partition_start=start_dt.isoformat(),
        partition_end=end_dt.isoformat(),
        context=context,
        null_sentinel_columns=_CFG.get("null_sentinel_columns"),
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "rows_affected": rows_affected,
            "partition": partition_key,
            "source_view": f"sof.{_CFG['view_name']}",
            "target_table": f"fhir.{_CFG['mssql_table']}",
        }
    )
