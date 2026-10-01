# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for CarePlan asset wiring in the combined IFDB -> FHIR jobs."""

import pytest
from dagster import AssetKey
from DSA.FHIR.CarePlan.CarePlan_IFDB import (
    fhir_careplan_daily_ifdb,
    fhir_careplan_full_load_ifdb,
    fhir_careplan_monthly_ifdb,
)
from DSA.FHIR.SqlOnFhir.CarePlan_Activity import (
    sof_careplan_activity_daily,
    sof_careplan_activity_monthly,
)


def _single_asset_group(asset_def, asset_name: str) -> str:
    return asset_def.group_names_by_key[AssetKey.from_user_string(asset_name)]


@pytest.mark.unit
def test_careplan_transform_assets_are_selected_by_partition_jobs() -> None:
    assert (
        _single_asset_group(fhir_careplan_daily_ifdb, "fhir_careplan_daily_ifdb")
        == "fhir_ifdb_daily"
    )
    assert (
        _single_asset_group(fhir_careplan_monthly_ifdb, "fhir_careplan_monthly_ifdb")
        == "fhir_ifdb_monthly"
    )


@pytest.mark.unit
def test_careplan_waits_for_patient_and_episode_of_care_transforms() -> None:
    full_load_key = AssetKey.from_user_string("fhir_careplan_full_load_ifdb")
    daily_key = AssetKey.from_user_string("fhir_careplan_daily_ifdb")
    monthly_key = AssetKey.from_user_string("fhir_careplan_monthly_ifdb")

    assert fhir_careplan_full_load_ifdb.asset_deps[full_load_key] == {
        AssetKey.from_user_string("ifdb_extract/eventmarker_alle_markoerer_full_load"),
        AssetKey.from_user_string("fhir_server_patient_dfm"),
        AssetKey.from_user_string("fhir_server_eoc_dfm"),
    }
    assert fhir_careplan_daily_ifdb.asset_deps[daily_key] == {
        AssetKey.from_user_string("ifdb_extract/eventmarker_alle_markoerer_daily"),
        AssetKey.from_user_string("fhir_server_patient_dfm"),
        AssetKey.from_user_string("fhir_server_eoc_dfm"),
    }
    assert fhir_careplan_monthly_ifdb.asset_deps[monthly_key] == {
        AssetKey.from_user_string("ifdb_extract/eventmarker_alle_markoerer_monthly"),
        AssetKey.from_user_string("fhir_server_patient_dfm"),
        AssetKey.from_user_string("fhir_server_eoc_dfm"),
    }


@pytest.mark.unit
def test_sof_careplan_activity_waits_for_careplan_transform() -> None:
    daily_key = AssetKey.from_user_string("sof_careplan_activity_daily")
    monthly_key = AssetKey.from_user_string("sof_careplan_activity_monthly")

    assert sof_careplan_activity_daily.asset_deps[daily_key] == {
        AssetKey.from_user_string("fhir_sof_infrastructure"),
        AssetKey.from_user_string("fhir_careplan_daily_ifdb"),
    }
    assert sof_careplan_activity_monthly.asset_deps[monthly_key] == {
        AssetKey.from_user_string("fhir_sof_infrastructure"),
        AssetKey.from_user_string("fhir_careplan_monthly_ifdb"),
    }
