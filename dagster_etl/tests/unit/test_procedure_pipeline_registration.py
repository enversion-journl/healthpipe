# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the IFDB Procedure pipeline wiring (SOF + DSO sync).

These tests guard the end-to-end registration contract for Procedure data from
ADMINISTRATIVE_PROCEDURE and RIS_YDELSE:
  IFDB assets -> SOF procedure view/materialization -> DSO sync assets.
"""

import json

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.SqlOnFhir.procedure import sof_procedure_daily, sof_procedure_monthly
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR
from DSO.load.sync_fhir import all_fhir_dso_sync_daily_assets, all_fhir_dso_sync_monthly_assets


@pytest.mark.unit
class TestProcedurePipelineRegistration:
    """Validate Procedure wiring from SOF to DSO sync."""

    def test_sof_procedure_daily_depends_on_all_ifdb_procedure_sources(self):
        key = AssetKey.from_user_string("sof_procedure_daily")
        deps = sof_procedure_daily.asset_deps[key]
        # Three IFDB sources contribute to fhir.Procedure: administrative
        # procedures, RIS ydelser, and aktivitetsresultat. The SoF view must
        # see all of them materialised before extracting.
        assert deps == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_administrative_procedure_daily_ifdb"),
            AssetKey.from_user_string("fhir_ris_ydelse_daily_ifdb"),
            AssetKey.from_user_string("fhir_aktivitetsresultat_daily_ifdb"),
        }

    def test_sof_procedure_monthly_depends_on_all_ifdb_procedure_sources(self):
        key = AssetKey.from_user_string("sof_procedure_monthly")
        deps = sof_procedure_monthly.asset_deps[key]
        assert deps == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_administrative_procedure_monthly_ifdb"),
            AssetKey.from_user_string("fhir_ris_ydelse_monthly_ifdb"),
            AssetKey.from_user_string("fhir_aktivitetsresultat_monthly_ifdb"),
        }

    def test_procedure_view_config_matches_expected_sof_contract(self):
        cfg = SOF_RESOURCE_CONFIGS["procedure_view"]
        assert cfg["view_name"] == "procedure_view"
        assert cfg["resource_table"] == "procedure"
        assert cfg["id_column"] == "procedure_id"
        assert cfg["mssql_table"] == "Procedure"
        assert cfg["primary_keys"] == ["procedure_id"]

    def test_procedure_view_includes_required_columns_for_fhir_procedure_table(self):
        path = VIEW_DEFINITIONS_DIR / "procedure_view.json"
        with open(path) as f:
            view_def = json.load(f)

        columns = [c["name"] for block in view_def["select"] for c in block.get("column", [])]
        expected = {
            "procedure_id",
            "patient_id",
            "encounter_id",
            "episode_of_care_id",
            "status",
            "category_code",
            "category_display",
            "code",
            "code_system",
            "code_display",
            "performed_datetime",
            "source_system",
        }
        missing = expected - set(columns)
        assert not missing, f"procedure_view is missing required columns: {missing}"

    def test_procedure_view_uses_standard_encounter_and_extension_episode_paths(self):
        path = VIEW_DEFINITIONS_DIR / "procedure_view.json"
        with open(path) as f:
            view_def = json.load(f)

        path_by_col = {
            c["name"]: c["path"] for block in view_def["select"] for c in block.get("column", [])
        }
        assert path_by_col["encounter_id"] == "encounter.id.first()"
        assert "StructureDefinition/episodeOfCare" in path_by_col["episode_of_care_id"]
        assert ".valueReference.id" in path_by_col["episode_of_care_id"]

    def test_dso_sync_procedure_assets_depend_on_sof_procedure_assets(self):
        daily = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_daily_assets}[
            "dso_sync_fhir_procedure_daily"
        ]
        monthly = {
            next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_monthly_assets
        }["dso_sync_fhir_procedure_monthly"]

        daily_key = AssetKey.from_user_string("dso_sync_fhir_procedure_daily")
        monthly_key = AssetKey.from_user_string("dso_sync_fhir_procedure_monthly")
        assert daily.asset_deps[daily_key] == {
            AssetKey.from_user_string("sof_procedure_daily"),
            AssetKey.from_user_string("dso_infra_fhir"),
        }
        assert monthly.asset_deps[monthly_key] == {
            AssetKey.from_user_string("sof_procedure_monthly"),
            AssetKey.from_user_string("dso_infra_fhir"),
        }

    def test_sof_procedure_upstreams_are_materializable_assets(self):
        graph = defs.resolve_asset_graph()
        required = [
            "sof_procedure_daily",
            "sof_procedure_monthly",
            "fhir_administrative_procedure_daily_ifdb",
            "fhir_administrative_procedure_monthly_ifdb",
            "fhir_ris_ydelse_daily_ifdb",
            "fhir_ris_ydelse_monthly_ifdb",
            "fhir_service_request_daily_ifdb",
            "fhir_service_request_monthly_ifdb",
        ]
        for key_str in required:
            key = AssetKey.from_user_string(key_str)
            assert graph.has(key), f"Missing asset key: {key_str}"
            assert graph.get(key).is_materializable, f"Asset is not materializable: {key_str}"
