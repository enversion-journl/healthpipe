# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the Procedure DFM pipeline.

Guards the wiring from the archive tables through the two Procedure DFM
transforms:
  arc.ifdb_administrative_procedure / arc.ifdb_ris_ydelse (+suppl codes)
    -> fhir_server_procedure_*_dfm -> sof_procedure*_dfm -> fhir.* tables.

SOF/DSO moved to the DFM path : the last Procedure producer
(``fhir_server_aktivitetsresultat_dfm``) is DFM, and ``sof_procedure_dfm``
names all three producers as deps.

Pure asset-graph + config/view-JSON validation (no live Aidbox/MSSQL). Mirrors
``test_condition_pipeline_registration.py``.
"""

import json

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm import fhir_server_procedure_admin_proc_dfm
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm import fhir_server_procedure_ris_ydelse_dfm
from DSA.FHIR.SqlOnFhir.Procedure_dfm import sof_procedure_dfm
from DSA.FHIR.utils.sql_on_fhir.sof_utils import VIEW_DEFINITIONS_DIR


@pytest.mark.unit
class TestProcedurePipelineRegistration:
    """Validate Procedure DFM wiring + SOF config/view contracts."""

    def test_admin_proc_dfm_depends_on_archive_and_upstreams(self):
        key = AssetKey.from_user_string("fhir_server_procedure_admin_proc_dfm")
        assert fhir_server_procedure_admin_proc_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_administrative_procedure_ifdb"),
            AssetKey.from_user_string("arc_adm_procedure_suppl_codes_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
            AssetKey.from_user_string("fhir_server_eoc_dfm"),
        }

    def test_ris_ydelse_dfm_depends_on_archive_and_upstreams(self):
        key = AssetKey.from_user_string("fhir_server_procedure_ris_ydelse_dfm")
        assert fhir_server_procedure_ris_ydelse_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_ris_ydelse_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
            AssetKey.from_user_string("fhir_server_eoc_dfm"),
            # Ordering dep: basedOn → ServiceRequest must post before the
            # RIS Procedure in the same base-job run (a dangling reference
            # only heals if the RIS row is touched again — maybe never).
            AssetKey.from_user_string("fhir_server_service_request_dfm"),
        }

    def test_sof_procedure_names_every_producer(self):
        """SOF moved to the DFM group; every producer is an explicit dep."""
        key = AssetKey.from_user_string("sof_procedure_dfm")
        assert sof_procedure_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_procedure_admin_proc_dfm"),
            AssetKey.from_user_string("fhir_server_procedure_ris_ydelse_dfm"),
            AssetKey.from_user_string("fhir_server_aktivitetsresultat_dfm"),
        }

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

    def test_procedure_assets_are_materializable(self):
        graph = defs.resolve_asset_graph()
        required = [
            "ext_ifdb_administrative_procedure",
            "ext_ifdb_adm_procedure_suppl_codes",
            "ext_ifdb_ris_ydelse",
            "arc_administrative_procedure_ifdb",
            "arc_adm_procedure_suppl_codes_ifdb",
            "arc_ris_ydelse_ifdb",
            "fhir_server_procedure_admin_proc_dfm",
            "fhir_server_procedure_ris_ydelse_dfm",
        ]
        for key_str in required:
            key = AssetKey.from_user_string(key_str)
            assert graph.has(key), f"Missing asset key: {key_str}"
            assert graph.get(key).is_materializable, f"Asset is not materializable: {key_str}"

    def test_legacy_procedure_assets_are_gone(self):
        # Old + new asset registered at the same time would double-post to the
        # FHIR server; the partitioned ris_ydelse extracts went with their last
        # consumer (ServiceRequest).
        graph = defs.resolve_asset_graph()
        removed = [
            "fhir_administrative_procedure_daily_ifdb",
            "fhir_ris_ydelse_daily_ifdb",
            "ifdb_extract/administrative_procedure_daily",
            "ifdb_extract/ris_ydelse_daily",
            "fhir_service_request_daily_ifdb",
        ]
        for key_str in removed:
            assert not graph.has(
                AssetKey.from_user_string(key_str)
            ), f"Legacy asset still registered: {key_str}"
