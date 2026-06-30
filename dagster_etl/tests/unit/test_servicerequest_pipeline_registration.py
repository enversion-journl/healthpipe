# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Registration tests for ServiceRequest SOF and DSO pipeline."""

import json

from dagster import AssetKey
from DSA.FHIR.ServiceRequest import all_assets as servicerequest_assets
from DSA.FHIR.SqlOnFhir.ServiceRequest import (
    sof_service_request_daily,
    sof_service_request_monthly,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR
from DSI.extract.IFDB.referralinformation import REFERRALINFORMATION_CONFIG
from DSO.load.sync_fhir import all_fhir_dso_sync_daily_assets, all_fhir_dso_sync_monthly_assets


def test_service_request_fhir_assets_registered():
    names = {next(iter(asset.keys)).to_user_string() for asset in servicerequest_assets}
    assert names == {
        "fhir_service_request_daily_ifdb",
        "fhir_service_request_monthly_ifdb",
        "fhir_service_request_bulk_ifdb",
    }


def test_referralinformation_extract_uses_existing_ifdb_columns():
    assert "REFERRAL_ORG_SORCODE" in REFERRALINFORMATION_CONFIG.columns
    assert not any(
        column.startswith("INSTITUTION_") for column in REFERRALINFORMATION_CONFIG.columns
    )


def test_sof_service_request_config_matches_contract():
    cfg = SOF_RESOURCE_CONFIGS["servicerequest_view"]
    assert cfg["view_name"] == "servicerequest_view"
    assert cfg["resource_table"] == "servicerequest"
    assert cfg["id_column"] == "service_request_id"
    assert cfg["mssql_table"] == "ServiceRequest"
    assert cfg["primary_keys"] == ["service_request_id"]


def test_service_request_view_includes_required_columns():
    with open(VIEW_DEFINITIONS_DIR / "servicerequest_view.json") as f:
        view_def = json.load(f)

    columns = [c["name"] for block in view_def["select"] for c in block.get("column", [])]
    expected = {
        "service_request_id",
        "identifier_system",
        "identifier_value",
        "patient_id",
        "episode_of_care_id",
        "encounter_id",
        "status",
        "intent",
        "priority",
        "category_code",
        "code",
        "authored_on",
        "source_table",
        "is_deleted",
    }
    assert not (expected - set(columns))


def test_sof_service_request_assets_depend_on_fhir_assets():
    daily_key = AssetKey.from_user_string("sof_service_request_daily")
    monthly_key = AssetKey.from_user_string("sof_service_request_monthly")

    assert sof_service_request_daily.asset_deps[daily_key] == {
        AssetKey.from_user_string("fhir_sof_infrastructure"),
        AssetKey.from_user_string("fhir_service_request_daily_ifdb"),
    }
    assert sof_service_request_monthly.asset_deps[monthly_key] == {
        AssetKey.from_user_string("fhir_sof_infrastructure"),
        AssetKey.from_user_string("fhir_service_request_monthly_ifdb"),
    }


def test_dso_sync_service_request_assets_depend_on_sof_assets():
    daily = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_daily_assets}[
        "dso_sync_fhir_service_request_daily"
    ]
    monthly = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_monthly_assets}[
        "dso_sync_fhir_service_request_monthly"
    ]

    daily_key = AssetKey.from_user_string("dso_sync_fhir_service_request_daily")
    monthly_key = AssetKey.from_user_string("dso_sync_fhir_service_request_monthly")
    assert daily.asset_deps[daily_key] == {
        AssetKey.from_user_string("sof_service_request_daily"),
        AssetKey.from_user_string("dso_infra_fhir"),
    }
    assert monthly.asset_deps[monthly_key] == {
        AssetKey.from_user_string("sof_service_request_monthly"),
        AssetKey.from_user_string("dso_infra_fhir"),
    }
