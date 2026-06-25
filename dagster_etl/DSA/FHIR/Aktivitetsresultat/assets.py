# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unified Dagster multi_assets for IFDB AKTIVITETSRESULTAT.

One partition pass produces Observation/Procedure, Questionnaire, and
QuestionnaireResponse while parsing XML and resolving routing once per row.

Asset keys match the former separate assets so SOF downstream deps are unchanged.
"""

from __future__ import annotations

from typing import Any

import dagster as dg
from core.partitions import daily_partitions, full_load_partitions, monthly_partitions
from core.resources import (
    FHIRDatabaseResource,
    IFDBOracleResource,
    MSSQLResource,
)
from dagster import AssetExecutionContext
from DSA.FHIR.Aktivitetsresultat.orchestrator import _run_aktivitetsresultat_pipeline_chunked
from DSA.FHIR.Aktivitetsresultat.routing_glue import _sfi_mode_enabled
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS

_AKTIVITETSRESULTAT_OUT = "aktivitetsresultat"
_QUESTIONNAIRE_OUT = "questionnaire"
_QUESTIONNAIRE_RESPONSE_OUT = "questionnaire_response"


_BULK_OUTS = {
    _AKTIVITETSRESULTAT_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_aktivitetsresultat_bulk_ifdb"),
        group_name="fhir_ifdb_full_load",
    ),
    _QUESTIONNAIRE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_bulk_ifdb"),
        group_name="fhir_ifdb_full_load",
    ),
    _QUESTIONNAIRE_RESPONSE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_response_bulk_ifdb"),
        group_name="fhir_ifdb_full_load",
    ),
}


_DAILY_OUTS = {
    _AKTIVITETSRESULTAT_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_aktivitetsresultat_daily_ifdb"),
        group_name="fhir_ifdb_daily",
    ),
    _QUESTIONNAIRE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_daily_ifdb"),
        group_name="fhir_ifdb_daily",
    ),
    _QUESTIONNAIRE_RESPONSE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_response_daily_ifdb"),
        group_name="fhir_ifdb_daily",
    ),
}

_MONTHLY_OUTS = {
    _AKTIVITETSRESULTAT_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_aktivitetsresultat_monthly_ifdb"),
        group_name="fhir_ifdb_monthly",
    ),
    _QUESTIONNAIRE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_monthly_ifdb"),
        group_name="fhir_ifdb_monthly",
    ),
    _QUESTIONNAIRE_RESPONSE_OUT: dg.AssetOut(
        key=dg.AssetKey("fhir_questionnaire_response_monthly_ifdb"),
        group_name="fhir_ifdb_monthly",
    ),
}


def _zero_sfi_metadata(context: AssetExecutionContext, method: str) -> dict[str, Any]:
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    return {
        "posted_count": dg.MetadataValue.int(0),
        "failed_count": dg.MetadataValue.int(0),
        "partition_key": dg.MetadataValue.text(partition_key),
        "method": dg.MetadataValue.text(method),
    }


def _run_unified_multi(
    *,
    context: AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> tuple[dg.MaterializeResult, dg.MaterializeResult, dg.MaterializeResult]:
    if _sfi_mode_enabled():
        result = _run_aktivitetsresultat_pipeline_chunked(
            extracted_data=extracted_data,
            context=context,
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            mssql_target_staging=mssql_target_staging,
            emit_questionnaire_outputs=True,
        )
        if not isinstance(result, tuple):
            raise TypeError("Expected unified SFI pipeline to return three materialization results")
        return result

    obs_result = _run_aktivitetsresultat_pipeline_chunked(
        extracted_data=extracted_data,
        context=context,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        mssql_target_staging=mssql_target_staging,
    )
    noop = _zero_sfi_metadata(context, "sfi_legacy_noop")
    return (
        obs_result,
        dg.MaterializeResult(metadata=noop),
        dg.MaterializeResult(metadata=noop),
    )


def _yield_multi_asset_output(
    *,
    output_name: str,
    result: dg.MaterializeResult,
):
    yield dg.Output(
        value=None,
        output_name=output_name,
        metadata=result.metadata,
    )


@dg.multi_asset(
    name="fhir_aktivitetsresultat_sfi_daily_ifdb",
    outs=_DAILY_OUTS,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "aktivitetsresultat_daily"]),
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    partitions_def=daily_partitions,
    op_tags={TAG_CHUNK_SIZE: "500", TAG_MAX_WORKERS: "4"},
)
def fhir_aktivitetsresultat_sfi_daily_ifdb(
    context,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
):
    obs_result, q_result, qr_result = _run_unified_multi(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        mssql_target_staging=mssql_target_staging,
    )
    yield from _yield_multi_asset_output(
        output_name=_AKTIVITETSRESULTAT_OUT,
        result=obs_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_OUT,
        result=q_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_RESPONSE_OUT,
        result=qr_result,
    )


@dg.multi_asset(
    name="fhir_aktivitetsresultat_sfi_monthly_ifdb",
    outs=_MONTHLY_OUTS,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "aktivitetsresultat_monthly"]),
            input_manager_key="streaming_batched_extract_io_manager",
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    partitions_def=monthly_partitions,
    op_tags={TAG_CHUNK_SIZE: "500", TAG_MAX_WORKERS: "8"},
)
def fhir_aktivitetsresultat_sfi_monthly_ifdb(
    context,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
):
    obs_result, q_result, qr_result = _run_unified_multi(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        mssql_target_staging=mssql_target_staging,
    )
    yield from _yield_multi_asset_output(
        output_name=_AKTIVITETSRESULTAT_OUT,
        result=obs_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_OUT,
        result=q_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_RESPONSE_OUT,
        result=qr_result,
    )


@dg.multi_asset(
    name="fhir_aktivitetsresultat_sfi_bulk_ifdb",
    outs=_BULK_OUTS,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "aktivitetsresultat_full_load"]),
            input_manager_key="streaming_batched_extract_io_manager",
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    partitions_def=full_load_partitions,
    op_tags={TAG_CHUNK_SIZE: "500", TAG_MAX_WORKERS: "16"},
)
def fhir_aktivitetsresultat_sfi_bulk_ifdb(
    context,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
):
    """Transform and post AKTIVITETSRESULTAT resources for bulk load.

    Mirrors the daily/monthly multi_asset contract so bulk Questionnaire and
    QuestionnaireResponse outputs are first-class asset keys. Preserves the
    historic ``fhir_aktivitetsresultat_bulk_ifdb`` key for downstream deps.
    """
    obs_result, q_result, qr_result = _run_unified_multi(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        mssql_target_staging=mssql_target_staging,
    )
    yield from _yield_multi_asset_output(
        output_name=_AKTIVITETSRESULTAT_OUT,
        result=obs_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_OUT,
        result=q_result,
    )
    yield from _yield_multi_asset_output(
        output_name=_QUESTIONNAIRE_RESPONSE_OUT,
        result=qr_result,
    )


all_assets = [
    fhir_aktivitetsresultat_sfi_bulk_ifdb,
    fhir_aktivitetsresultat_sfi_daily_ifdb,
    fhir_aktivitetsresultat_sfi_monthly_ifdb,
]
