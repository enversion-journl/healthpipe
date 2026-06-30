# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pin SOF ordering for Questionnaire item enrichment."""

import pytest
from dagster import AssetKey
from DSA.FHIR.SqlOnFhir.QuestionnaireResponse import (
    sof_questionnaire_item_daily,
    sof_questionnaire_item_monthly,
    sof_questionnaire_response_item_daily,
    sof_questionnaire_response_item_monthly,
)
from DSO.load.sync_configs import FHIR_SYNC_CONFIGS
from DSO.load.sync_fhir import all_fhir_dso_sync_daily_assets, all_fhir_dso_sync_monthly_assets
from infrastructure.assets.fhir_sof_infrastructure import fhir_sof_infrastructure


def _selected_asset_keys(job_name: str) -> set[str]:
    from definitions import defs

    job = defs.get_job_def(job_name)
    return {key.to_user_string() for key in job.asset_layer.selected_asset_keys}


@pytest.mark.unit
def test_sof_infrastructure_waits_for_staging_fhir_tables() -> None:
    """SOF syncs MERGE into staging fhir.* tables; the whole SOF chain depends on
    fhir_sof_infrastructure, so it must wait for staging_infra_fhir (tables +
    migrations) — otherwise a stale target schema/PK fails the MERGE at runtime."""
    key = AssetKey.from_user_string("fhir_sof_infrastructure")
    assert AssetKey.from_user_string("staging_infra_fhir") in (
        fhir_sof_infrastructure.asset_deps[key]
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "job_name",
    [
        "fhir_sof_daily_job",
        "fhir_sof_monthly_job",
        "fhir_etl_ifdb_daily_job",
        "fhir_etl_ifdb_monthly_job",
    ],
)
def test_operational_sof_jobs_select_view_definition_infrastructure(job_name: str) -> None:
    keys = _selected_asset_keys(job_name)

    assert "staging_infra_fhir" in keys
    assert "fhir_sof_infrastructure" in keys


@pytest.mark.unit
def test_questionnaire_response_item_waits_for_questionnaire_item_daily() -> None:
    qr_item_key = AssetKey.from_user_string("sof_questionnaire_response_item_daily")
    q_item_key = AssetKey.from_user_string("sof_questionnaire_item_daily")

    assert q_item_key in sof_questionnaire_response_item_daily.asset_deps[qr_item_key]
    assert qr_item_key not in sof_questionnaire_item_daily.asset_deps[q_item_key]


@pytest.mark.unit
def test_questionnaire_response_item_waits_for_questionnaire_item_monthly() -> None:
    qr_item_key = AssetKey.from_user_string("sof_questionnaire_response_item_monthly")
    q_item_key = AssetKey.from_user_string("sof_questionnaire_item_monthly")

    assert q_item_key in sof_questionnaire_response_item_monthly.asset_deps[qr_item_key]
    assert qr_item_key not in sof_questionnaire_item_monthly.asset_deps[q_item_key]


@pytest.mark.unit
def test_questionnaire_tables_have_dso_sync_configs() -> None:
    expected = {
        "fhir_questionnaire": ("Questionnaire", "questionnaire_id"),
        "fhir_questionnaire_item": (
            "Questionnaire_item",
            ["questionnaire_id", "link_id", "item_key"],
        ),
        "fhir_questionnaire_response": ("QuestionnaireResponse", "qr_id"),
        "fhir_questionnaire_response_item": (
            "QuestionnaireResponse_item",
            ["qr_id", "link_id", "item_key", "answer_index"],
        ),
    }

    for config_key, (table, primary_key) in expected.items():
        cfg = FHIR_SYNC_CONFIGS[config_key]
        assert cfg.schema == "fhir"
        assert cfg.table == table
        assert cfg.primary_key == primary_key
        assert cfg.mode == "watermark"
        assert cfg.watermark_column == "DW_Timestamp_Update"


@pytest.mark.unit
def test_questionnaire_dso_sync_assets_depend_on_matching_sof_assets() -> None:
    daily_assets = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_daily_assets}
    monthly_assets = {
        next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_monthly_assets
    }

    expected = {
        "questionnaire",
        "questionnaire_item",
        "questionnaire_response",
        "questionnaire_response_item",
    }

    for sof_name in expected:
        daily_asset_name = f"dso_sync_fhir_{sof_name}_daily"
        monthly_asset_name = f"dso_sync_fhir_{sof_name}_monthly"
        assert daily_asset_name in daily_assets
        assert monthly_asset_name in monthly_assets

        daily_key = AssetKey.from_user_string(daily_asset_name)
        monthly_key = AssetKey.from_user_string(monthly_asset_name)
        assert daily_assets[daily_asset_name].asset_deps[daily_key] == {
            AssetKey.from_user_string(f"sof_{sof_name}_daily"),
            AssetKey.from_user_string("dso_infra_fhir"),
        }
        assert monthly_assets[monthly_asset_name].asset_deps[monthly_key] == {
            AssetKey.from_user_string(f"sof_{sof_name}_monthly"),
            AssetKey.from_user_string("dso_infra_fhir"),
        }
