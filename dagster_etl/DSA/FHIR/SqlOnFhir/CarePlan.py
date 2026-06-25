# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: CarePlan extraction assets.

Creates daily + monthly assets for the CarePlan SoF view.
"""

from datetime import UTC, datetime, timedelta

import dagster as dg
from core.partitions import daily_partitions, monthly_partitions
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_ASSET_RETRY_POLICY,
    SOF_RESOURCE_CONFIGS,
    extract_and_merge_sof_view,
)


def _create_sof_careplan_daily(view_key: str, ifdb_dep: str) -> dg.AssetsDefinition:
    cfg = SOF_RESOURCE_CONFIGS[view_key]
    short_name = view_key.removesuffix("_view")

    @dg.asset(
        name=f"sof_{short_name}_daily",
        group_name="fhir_sof_daily",
        compute_kind="postgres",
        partitions_def=daily_partitions,
        deps=["fhir_sof_infrastructure", ifdb_dep],
        tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
        retry_policy=SOF_ASSET_RETRY_POLICY,
        description=(
            f"Extract {short_name} flat view from Aidbox sof schema (daily) "
            f"and MERGE into fhir.{cfg['mssql_table']}"
        ),
    )
    def _asset(
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
            cfg["view_name"],
            cfg["resource_table"],
            cfg["id_column"],
            cfg["mssql_table"],
            cfg["primary_keys"],
            partition_start=start_dt.isoformat(),
            partition_end=end_dt.isoformat(),
            context=context,
            null_sentinel_columns=cfg.get("null_sentinel_columns"),
        )
        return dg.MaterializeResult(
            metadata={
                "row_count": row_count,
                "rows_affected": rows_affected,
                "partition": partition_key,
                "source_view": f"sof.{cfg['view_name']}",
                "target_table": f"fhir.{cfg['mssql_table']}",
            }
        )

    return _asset


def _create_sof_careplan_monthly(view_key: str, ifdb_dep: str) -> dg.AssetsDefinition:
    cfg = SOF_RESOURCE_CONFIGS[view_key]
    short_name = view_key.removesuffix("_view")

    @dg.asset(
        name=f"sof_{short_name}_monthly",
        group_name="fhir_sof_monthly",
        compute_kind="postgres",
        partitions_def=monthly_partitions,
        deps=["fhir_sof_infrastructure", ifdb_dep],
        tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
        retry_policy=SOF_ASSET_RETRY_POLICY,
        description=(
            f"Extract {short_name} flat view from Aidbox sof schema (monthly) "
            f"and MERGE into fhir.{cfg['mssql_table']}"
        ),
    )
    def _asset(
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
            cfg["view_name"],
            cfg["resource_table"],
            cfg["id_column"],
            cfg["mssql_table"],
            cfg["primary_keys"],
            partition_start=start_dt.isoformat(),
            partition_end=end_dt.isoformat(),
            context=context,
            null_sentinel_columns=cfg.get("null_sentinel_columns"),
        )
        return dg.MaterializeResult(
            metadata={
                "row_count": row_count,
                "rows_affected": rows_affected,
                "partition": partition_key,
                "source_view": f"sof.{cfg['view_name']}",
                "target_table": f"fhir.{cfg['mssql_table']}",
            }
        )

    return _asset


# --- Generated asset instances ------------------------------------------------

sof_careplan_daily = _create_sof_careplan_daily("careplan_view", "fhir_careplan_daily_ifdb")
sof_careplan_monthly = _create_sof_careplan_monthly("careplan_view", "fhir_careplan_monthly_ifdb")
