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
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)


@pytest.mark.unit
class TestProcedurePipelineRegistration:
    """Validate Procedure DFM wiring + SOF config/view contracts."""

    def test_admin_proc_dfm_depends_on_archive_and_upstreams(self):
        assert_deps(
            fhir_server_procedure_admin_proc_dfm,
            "fhir_server_procedure_admin_proc_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_administrative_procedure_ifdb",
                "arc_adm_procedure_suppl_codes_ifdb",
                "fhir_server_patient_dfm",
                "fhir_server_encounter_dfm",
                "fhir_server_eoc_dfm",
            },
        )

    def test_ris_ydelse_dfm_depends_on_archive_and_upstreams(self):
        assert_deps(
            fhir_server_procedure_ris_ydelse_dfm,
            "fhir_server_procedure_ris_ydelse_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_ris_ydelse_ifdb",
                "fhir_server_patient_dfm",
                "fhir_server_encounter_dfm",
                "fhir_server_eoc_dfm",
                # Ordering dep: basedOn → ServiceRequest must post before the
                # RIS Procedure in the same base-job run (a dangling reference
                # only heals if the RIS row is touched again — maybe never).
                "fhir_server_service_request_dfm",
            },
        )

    def test_sof_procedure_names_every_producer(self):
        """SOF moved to the DFM group; every producer is an explicit dep."""
        assert_deps(
            sof_procedure_dfm,
            "sof_procedure_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={
                "fhir_sof_infrastructure",
                "fhir_server_procedure_admin_proc_dfm",
                "fhir_server_procedure_ris_ydelse_dfm",
                "fhir_server_aktivitetsresultat_dfm",
            },
        )

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
