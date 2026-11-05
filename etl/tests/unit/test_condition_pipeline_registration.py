# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the Condition DFM pipeline.

Guards the wiring from the archive tables through the two Condition DFM
transforms:
  arc.ifdb_diagnose / arc.ifdb_pas_diagnose (+ref tables)
    -> fhir_server_condition_*_dfm -> sof_condition*_dfm -> fhir.* tables.

SOF/DSO moved to the DFM path : all four Condition producers
(Diagnose, PAS-diagnose, ServiceRequest, Aktivitetsresultat/SFI) are DFM, and
``sof_condition_dfm`` names them all as deps.

Pure asset-graph + config validation (no live Aidbox/MSSQL). Mirrors
``test_documentreference_pipeline_registration.py``.
"""

import json

import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose_dfm import fhir_server_condition_diagnose_dfm
from DSA.FHIR.Condition.Condition_IFDB_PasDiagnose_dfm import (
    fhir_server_condition_pas_diagnose_dfm,
)
from DSA.FHIR.SqlOnFhir.Condition_dfm import sof_condition_dfm
from DSA.FHIR.utils.sql_on_fhir.sof_utils import VIEW_DEFINITIONS_DIR
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)


@pytest.mark.unit
class TestConditionPipelineRegistration:
    """Validate Condition DFM wiring + SOF config contracts."""

    def test_diagnose_dfm_depends_on_archive_and_patient(self):
        assert_deps(
            fhir_server_condition_diagnose_dfm,
            "fhir_server_condition_diagnose_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_diagnose_ifdb",
                "arc_diagnose_tillaegskode_ifdb",
                "fhir_server_patient_dfm",
            },
        )

    def test_pas_diagnose_dfm_depends_on_archive_patient_and_encounter(self):
        assert_deps(
            fhir_server_condition_pas_diagnose_dfm,
            "fhir_server_condition_pas_diagnose_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_pas_diagnose_ifdb",
                "arc_pas_diagnose_encounter_ref_ifdb",
                "arc_pas_diagnose_tillaegskode_ifdb",
                "arc_pas_diagnose_henvisning_ref_ifdb",
                "fhir_server_patient_dfm",
                "fhir_server_encounter_dfm",
            },
        )

    def test_sof_condition_moved_to_dfm_with_all_producers(self):
        """Condition SOF moved to DFM: all four producers are on the base job.

        Diagnose + PAS-diagnose, ServiceRequest and
        Aktivitetsresultat/SFI. Nothing in the partitioned daily job
        can post a Condition after the SOF read any more.
        """
        assert_deps(
            sof_condition_dfm,
            "sof_condition_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={
                "fhir_sof_infrastructure",
                # All four Condition producers — a missing one lets SOF read
                # before that producer has posted.
                "fhir_server_condition_diagnose_dfm",
                "fhir_server_condition_pas_diagnose_dfm",
                "fhir_server_service_request_dfm",
                "fhir_server_aktivitetsresultat_dfm",
            },
        )

    def test_condition_view_includes_required_columns_for_fhir_condition_table(self):
        path = VIEW_DEFINITIONS_DIR / "condition_view.json"
        with open(path) as f:
            view_def = json.load(f)

        columns = [c["name"] for block in view_def["select"] for c in block.get("column", [])]
        expected = {
            "condition_id",
            "patient_id",
            "encounter_id",
            "episode_of_care_id",
            "source_identifier_value",
            "lpr3_ignored",
            "dw_is_deleted",
        }
        missing = expected - set(columns)
        assert not missing, f"condition_view is missing required columns: {missing}"
