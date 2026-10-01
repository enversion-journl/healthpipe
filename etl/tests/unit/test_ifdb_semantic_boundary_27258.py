# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Contract tests for semantic boundary decisions in work item 27258.

Review context:
- 52404/52412: HEALTHISSUE should not drive FHIR Condition.
- 52405: EVENTMARKER should not drive IFDB Observation in this track.

Implemented: legacy condition (HEALTHISSUE) and observation (EVENTMARKER)
assets are deactivated. DIAGNOSE, PAS_DIAGNOSE and AKTIVITETSRESULTAT are
the active sources.
"""

import pytest
from DSA.FHIR.Aktivitetsresultat.assets import all_assets as aktivitetsresultat_assets
from DSA.FHIR.Condition import all_assets as condition_assets
from DSA.FHIR.Observation import all_assets as observation_assets
from DSA.FHIR.Procedure import all_assets as procedure_assets

pytestmark = pytest.mark.unit


def _collect_asset_names(assets) -> set[str]:
    names: set[str] = set()
    for asset in assets:
        keys = getattr(asset, "keys", None)
        if keys:
            for multi_key in keys:
                if hasattr(multi_key, "path") and multi_key.path:
                    names.add(multi_key.path[-1])
            continue

        key = getattr(asset, "key", None)
        if key is not None and hasattr(key, "path") and key.path:
            names.add(key.path[-1])
            continue

        name = getattr(asset, "name", None)
        if isinstance(name, str):
            names.add(name)

    return names


def _all_ifdb_names() -> set[str]:
    return (
        _collect_asset_names(condition_assets)
        | _collect_asset_names(observation_assets)
        | _collect_asset_names(aktivitetsresultat_assets)
        | _collect_asset_names(procedure_assets)
    )


def test_legacy_healthissue_condition_assets_are_not_registered() -> None:
    """HEALTHISSUE->Condition is deactivated per WI 27258 / B1."""
    names = _all_ifdb_names()
    legacy_condition_assets = {
        "fhir_condition_daily_ifdb",
        "fhir_condition_monthly_ifdb",
        "fhir_condition_bulk_ifdb",
    }
    assert names.isdisjoint(legacy_condition_assets)


def test_legacy_eventmarker_observation_assets_are_not_registered() -> None:
    """EVENTMARKER->Observation is deactivated per WI 27258 / B2."""
    names = _all_ifdb_names()
    legacy_observation_assets = {
        "fhir_observation_daily_ifdb",
        "fhir_observation_monthly_ifdb",
        "fhir_observation_full_load_ifdb",
    }
    assert names.isdisjoint(legacy_observation_assets)


def test_new_condition_and_observation_sources_are_registered() -> None:
    names = _all_ifdb_names()
    expected_assets = {
        "fhir_server_condition_diagnose_dfm",
        "fhir_server_aktivitetsresultat_dfm",
        "fhir_server_questionnaire_dfm",
        "fhir_server_questionnaire_response_dfm",
    }
    assert expected_assets.issubset(names)


def test_pas_diagnose_condition_assets_are_registered() -> None:
    """PAS_DIAGNOSE->Condition asset must be present."""
    names = _collect_asset_names(condition_assets)
    expected = {
        "fhir_server_condition_pas_diagnose_dfm",
    }
    assert expected.issubset(names)


def test_procedure_assets_are_registered() -> None:
    """Administrative procedure and RIS ydelse assets must be present."""
    names = _collect_asset_names(procedure_assets)
    expected = {
        "fhir_server_procedure_admin_proc_dfm",
        "fhir_server_procedure_ris_ydelse_dfm",
    }
    assert expected.issubset(names)
