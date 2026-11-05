# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pin SOF ordering for Questionnaire item enrichment."""

import pytest
from dagster import AssetKey
from DSA.FHIR.SqlOnFhir.QuestionnaireResponse_dfm import (
    sof_questionnaire_item_dfm,
    sof_questionnaire_response_dfm,
    sof_questionnaire_response_item_dfm,
)
from DSO.load.sync_configs import FHIR_SYNC_CONFIGS
from DSO.load.sync_fhir import all_fhir_dso_sync_dfm_assets
from infrastructure.assets.fhir_sof_infrastructure import fhir_sof_infrastructure
from tests.utils.asset_graph_helpers import DSO_SYNC_PREFIXES, assert_deps


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
        "ifdb_dfm_daily_job",
        "fhir_sof_dfm_job",
    ],
)
def test_operational_sof_jobs_select_view_definition_infrastructure(job_name: str) -> None:
    keys = _selected_asset_keys(job_name)

    assert "staging_infra_fhir" in keys
    assert "fhir_sof_infrastructure" in keys


@pytest.mark.unit
def test_questionnaire_response_item_waits_for_questionnaire_item() -> None:
    """One DFM asset pair replaced the daily/monthly variants."""
    qr_item_key = AssetKey.from_user_string("sof_questionnaire_response_item_dfm")
    q_item_key = AssetKey.from_user_string("sof_questionnaire_item_dfm")

    assert q_item_key in sof_questionnaire_response_item_dfm.asset_deps[qr_item_key]
    assert qr_item_key not in sof_questionnaire_item_dfm.asset_deps[q_item_key]


@pytest.mark.unit
def test_questionnaire_response_waits_for_model_name_join_target() -> None:
    key = AssetKey("sof_questionnaire_response_dfm")
    assert AssetKey("sof_questionnaire_dfm") in sof_questionnaire_response_dfm.asset_deps[key]


@pytest.mark.unit
def test_condition_referrals_reach_dso_in_daily_job() -> None:
    key = AssetKey("dso_sync_fhir_condition_henvisning_dfm")
    (asset,) = [a for a in all_fhir_dso_sync_dfm_assets if key in a.keys]
    assert_deps(
        asset,
        "dso_sync_fhir_condition_henvisning_dfm",
        allowed_prefixes=DSO_SYNC_PREFIXES,
        required={"sof_condition_henvisning_dfm", "dso_infra_fhir"},
    )
    keys = _selected_asset_keys("ifdb_dfm_daily_job")
    assert {key.to_user_string(), "sof_condition_henvisning_dfm"} <= keys


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
        assert cfg.watermark_column == "dw_timestamp_update"


@pytest.mark.unit
def test_questionnaire_dso_sync_assets_depend_on_matching_sof_assets() -> None:
    """Each DFM DSO sync follows exactly its own DFM SOF asset."""
    dfm_assets = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_dfm_assets}

    for sof_name in (
        "questionnaire",
        "questionnaire_item",
        "questionnaire_response",
        "questionnaire_response_item",
    ):
        dso_name = f"dso_sync_fhir_{sof_name}_dfm"
        assert dso_name in dfm_assets, f"Missing DFM DSO sync asset: {dso_name}"
        assert_deps(
            dfm_assets[dso_name],
            dso_name,
            allowed_prefixes=DSO_SYNC_PREFIXES,
            required={
                f"sof_{sof_name}_dfm",
                # Gates the MERGE on the DSO fhir.* tables + their migrations, so
                # it never runs against a stale target schema/PK.
                "dso_infra_fhir",
            },
        )
