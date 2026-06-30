# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Questionnaire and QuestionnaireResponse extraction assets.

Covers four ViewDefinitions so the SFI datapoint union view has both sides:
- questionnaire_view              -> fhir.Questionnaire
- questionnaire_item_view         -> fhir.Questionnaire_item
- questionnaire_response_view     -> fhir.QuestionnaireResponse
- questionnaire_response_item_view -> fhir.QuestionnaireResponse_item

SFI lives on Questionnaire.item.code (system='urn:dk:ifdb:sfi'); the
QR-item projection deliberately omits element_system/element_code because
QuestionnaireResponse.item has no native code field. fhir.SFI_datapoint
joins QR-item to Questionnaire_item via
(questionnaire_canonical, link_id, item_key) to enrich with
element_system/element_code without collapsing repeated SFI codes.

Uses the streaming ``extract_and_merge_sof_view`` path consistent with the
other SOF assets in this directory.
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


def _daily_window(partition_key: str) -> tuple[datetime, datetime]:
    start_dt = datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=UTC)
    return start_dt, start_dt + timedelta(days=1)


def _monthly_window(partition_key: str) -> tuple[datetime, datetime]:
    start_dt = datetime.strptime(partition_key, "%Y-%m").replace(day=1, tzinfo=UTC)
    end_dt = (
        start_dt.replace(year=start_dt.year + 1, month=1)
        if start_dt.month == 12
        else start_dt.replace(month=start_dt.month + 1)
    )
    return start_dt, end_dt


def _run_sof_extract(
    cfg: dict,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    context: dg.AssetExecutionContext,
    start_dt: datetime,
    end_dt: datetime,
) -> tuple[int, int]:
    return extract_and_merge_sof_view(
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


def _materialize_result(
    cfg: dict,
    row_count: int,
    rows_affected: int,
    partition_key: str,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "rows_affected": rows_affected,
            "partition": partition_key,
            "source_view": f"sof.{cfg['view_name']}",
            "target_table": f"fhir.{cfg['mssql_table']}",
        }
    )


# NOTE: The SOF_RESOURCE_CONFIGS entries for questionnaire_view,
# questionnaire_item_view, questionnaire_response_view and
# questionnaire_response_item_view live in DSA/FHIR/utils/sql_on_fhir/sof_utils.py.
# The asset functions below look them up lazily so import errors point to the
# specific missing view configuration.


def _cfg(view_name: str) -> dict:
    try:
        return SOF_RESOURCE_CONFIGS[view_name]
    except KeyError as exc:
        raise RuntimeError(
            f"SOF_RESOURCE_CONFIGS is missing '{view_name}'. Add it "
            f"to DSA/FHIR/utils/sql_on_fhir/sof_utils.py before this asset can "
            f"materialise."
        ) from exc


# =============================================================================
# Questionnaire (header)
# =============================================================================


@dg.asset(
    name="sof_questionnaire_daily",
    group_name="fhir_sof_daily",
    compute_kind="postgres",
    partitions_def=daily_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_questionnaire_daily_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract Questionnaire flat view from Aidbox sof schema (daily) and MERGE into fhir.Questionnaire",
)
def sof_questionnaire_daily(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_view")
    start_dt, end_dt = _daily_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


@dg.asset(
    name="sof_questionnaire_monthly",
    group_name="fhir_sof_monthly",
    compute_kind="postgres",
    partitions_def=monthly_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_questionnaire_monthly_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract Questionnaire flat view from Aidbox sof schema (monthly) and MERGE into fhir.Questionnaire",
)
def sof_questionnaire_monthly(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_view")
    start_dt, end_dt = _monthly_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


# =============================================================================
# Questionnaire_item (flat Questionnaire.item projection)
# =============================================================================


@dg.asset(
    name="sof_questionnaire_item_daily",
    group_name="fhir_sof_daily",
    compute_kind="postgres",
    partitions_def=daily_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_questionnaire_daily_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract Questionnaire_item flat view from Aidbox sof schema (daily) and MERGE into fhir.Questionnaire_item",
)
def sof_questionnaire_item_daily(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_item_view")
    start_dt, end_dt = _daily_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


@dg.asset(
    name="sof_questionnaire_item_monthly",
    group_name="fhir_sof_monthly",
    compute_kind="postgres",
    partitions_def=monthly_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_questionnaire_monthly_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract Questionnaire_item flat view from Aidbox sof schema (monthly) and MERGE into fhir.Questionnaire_item",
)
def sof_questionnaire_item_monthly(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_item_view")
    start_dt, end_dt = _monthly_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


# =============================================================================
# QuestionnaireResponse (header)
# =============================================================================


@dg.asset(
    name="sof_questionnaire_response_daily",
    group_name="fhir_sof_daily",
    compute_kind="postgres",
    partitions_def=daily_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_aktivitetsresultat_daily_ifdb",
        "fhir_questionnaire_response_daily_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract QuestionnaireResponse flat view from Aidbox sof schema (daily) and MERGE into fhir.QuestionnaireResponse",
)
def sof_questionnaire_response_daily(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_response_view")
    start_dt, end_dt = _daily_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


@dg.asset(
    name="sof_questionnaire_response_monthly",
    group_name="fhir_sof_monthly",
    compute_kind="postgres",
    partitions_def=monthly_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_aktivitetsresultat_monthly_ifdb",
        "fhir_questionnaire_response_monthly_ifdb",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract QuestionnaireResponse flat view from Aidbox sof schema (monthly) and MERGE into fhir.QuestionnaireResponse",
)
def sof_questionnaire_response_monthly(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_response_view")
    start_dt, end_dt = _monthly_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


# =============================================================================
# QuestionnaireResponse_item (forEach over item + forEachOrNull answer)
# =============================================================================


@dg.asset(
    name="sof_questionnaire_response_item_daily",
    group_name="fhir_sof_daily",
    compute_kind="postgres",
    partitions_def=daily_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_aktivitetsresultat_daily_ifdb",
        "fhir_questionnaire_response_daily_ifdb",
        "sof_questionnaire_item_daily",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract QuestionnaireResponse_item flat view from Aidbox sof schema (daily) and MERGE into fhir.QuestionnaireResponse_item",
)
def sof_questionnaire_response_item_daily(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_response_item_view")
    start_dt, end_dt = _daily_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)


@dg.asset(
    name="sof_questionnaire_response_item_monthly",
    group_name="fhir_sof_monthly",
    compute_kind="postgres",
    partitions_def=monthly_partitions,
    deps=[
        "fhir_sof_infrastructure",
        "fhir_aktivitetsresultat_monthly_ifdb",
        "fhir_questionnaire_response_monthly_ifdb",
        "sof_questionnaire_item_monthly",
    ],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    retry_policy=SOF_ASSET_RETRY_POLICY,
    description="Extract QuestionnaireResponse_item flat view from Aidbox sof schema (monthly) and MERGE into fhir.QuestionnaireResponse_item",
)
def sof_questionnaire_response_item_monthly(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    cfg = _cfg("questionnaire_response_item_view")
    start_dt, end_dt = _monthly_window(context.partition_key)
    row_count, rows_affected = _run_sof_extract(
        cfg, fhir_database, mssql_target_staging, context, start_dt, end_dt
    )
    return _materialize_result(cfg, row_count, rows_affected, context.partition_key)
