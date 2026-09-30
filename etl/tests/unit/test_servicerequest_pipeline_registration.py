# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Registration/contract tests for the ServiceRequest DFM pipeline.

Guards the wiring from the archive tables through the ServiceRequest DFM
transform into the SOF flat tables:
  arc.ifdb_henvisning (+historik, +referralinformation) / arc.ifdb_ris_ydelse
    -> fhir_server_service_request_dfm -> sof_service_request[_tillaegskode]_dfm
    -> fhir.* tables.

Unlike Condition and Procedure, the SOF/DSO layer moves to DFM in this batch:
both producers of fhir.ServiceRequest (this asset and the rehabilitation-plan
slice) are on the DFM path, so nothing is left in the daily job
that could post ServiceRequests after the SOF read.
"""

import json

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.ServiceRequest import all_assets as servicerequest_assets
from DSA.FHIR.SqlOnFhir.ServiceRequest_dfm import (
    sof_service_request_dfm,
    sof_service_request_tillaegskode_dfm,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import VIEW_DEFINITIONS_DIR
from DSO.load.sync_fhir import all_fhir_dso_sync_dfm_assets

_SOF_PRODUCER_KEYS = {
    AssetKey.from_user_string("fhir_sof_infrastructure"),
    AssetKey.from_user_string("fhir_server_service_request_dfm"),
    AssetKey.from_user_string("fhir_server_rehabilitation_plan_dfm"),
}


@pytest.mark.unit
class TestServiceRequestPipelineRegistration:
    """Validate ServiceRequest DFM wiring + SOF config/view contracts."""

    def test_service_request_dfm_depends_on_archive_and_upstreams(self):
        key = AssetKey.from_user_string("fhir_server_service_request_dfm")
        asset = next(
            a
            for a in servicerequest_assets
            if next(iter(a.keys)).to_user_string() == key.to_user_string()
        )
        assert asset.asset_deps[key] == {
            AssetKey.from_user_string("arc_henvisning_ifdb"),
            AssetKey.from_user_string("arc_henvisning_historik_ifdb"),
            AssetKey.from_user_string("arc_referralinformation_ifdb"),
            AssetKey.from_user_string("arc_ris_ydelse_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
            AssetKey.from_user_string("fhir_server_eoc_dfm"),
        }

    def test_sof_service_request_depends_on_both_dfm_producers(self):
        key = AssetKey.from_user_string("sof_service_request_dfm")
        assert sof_service_request_dfm.asset_deps[key] == _SOF_PRODUCER_KEYS

    def test_sof_service_request_tillaegskode_follows_its_parent(self):
        # The child view is derived from the parent's resources, so it follows
        # the parent SOF asset (Condition/Procedure child pattern) — the
        # producer edges are transitive through the parent.
        key = AssetKey.from_user_string("sof_service_request_tillaegskode_dfm")
        assert sof_service_request_tillaegskode_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("sof_service_request_dfm"),
        }

    def test_service_request_view_includes_required_columns(self):
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
            "dw_is_deleted",
        }
        assert not (expected - set(columns))

    def test_dso_sync_service_request_follows_the_dfm_sof_assets(self):
        by_name = {next(iter(a.keys)).to_user_string(): a for a in all_fhir_dso_sync_dfm_assets}
        for sof_name in ("service_request", "service_request_tillaegskode"):
            dso_name = f"dso_sync_fhir_{sof_name}_dfm"
            assert dso_name in by_name, f"Missing DFM DSO sync asset: {dso_name}"
            key = AssetKey.from_user_string(dso_name)
            assert by_name[dso_name].asset_deps[key] == {
                AssetKey.from_user_string(f"sof_{sof_name}_dfm"),
                # Gates the MERGE on the DSO fhir.* tables + their migrations, so
                # it never runs against a stale target schema/PK.
                AssetKey.from_user_string("dso_infra_fhir"),
            }

    def test_service_request_assets_are_materializable(self):
        graph = defs.resolve_asset_graph()
        required = [
            "ext_ifdb_henvisning",
            "ext_ifdb_henvisning_historik",
            "ext_ifdb_referralinformation",
            "arc_henvisning_ifdb",
            "arc_henvisning_historik_ifdb",
            "arc_referralinformation_ifdb",
            "fhir_server_service_request_dfm",
            "sof_service_request_dfm",
            "sof_service_request_tillaegskode_dfm",
        ]
        for key_str in required:
            key = AssetKey.from_user_string(key_str)
            assert graph.has(key), f"Missing asset key: {key_str}"
            assert graph.get(key).is_materializable, f"Asset is not materializable: {key_str}"

    def test_legacy_service_request_assets_are_gone(self):
        graph = defs.resolve_asset_graph()
        removed = [
            "fhir_service_request_daily_ifdb",
            "fhir_service_request_monthly_ifdb",
            "fhir_service_request_bulk_ifdb",
            "sof_service_request_daily",
            "sof_service_request_monthly",
            "dso_sync_fhir_service_request_daily",
        ]
        for key_str in removed:
            assert not graph.has(
                AssetKey.from_user_string(key_str)
            ), f"Legacy asset still registered: {key_str}"

    def test_fetch_columns_are_subset_of_archive_columns(self):
        """The local lookup column lists must stay a subset of the arc-table
        columns — the drift guard the deleted DSI extract test used to be."""
        from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_dfm import (
            _HISTORY_FETCH_COLUMNS,
            _REFINFO_FETCH_COLUMNS,
        )
        from DSI.oracle.ifdb_extract import (
            HENVISNING_HISTORIK_COLUMNS,
            REFERRALINFORMATION_COLUMNS,
        )

        assert set(_HISTORY_FETCH_COLUMNS) <= set(HENVISNING_HISTORIK_COLUMNS)
        assert set(_REFINFO_FETCH_COLUMNS) <= set(REFERRALINFORMATION_COLUMNS)
