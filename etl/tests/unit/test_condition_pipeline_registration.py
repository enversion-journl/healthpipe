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

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.Condition.Condition_IFDB_Diagnose_dfm import fhir_server_condition_diagnose_dfm
from DSA.FHIR.Condition.Condition_IFDB_PasDiagnose_dfm import (
    fhir_server_condition_pas_diagnose_dfm,
)
from DSA.FHIR.SqlOnFhir.Condition_dfm import sof_condition_dfm


@pytest.mark.unit
class TestConditionPipelineRegistration:
    """Validate Condition DFM wiring + SOF config contracts."""

    def test_diagnose_dfm_depends_on_archive_and_patient(self):
        key = AssetKey.from_user_string("fhir_server_condition_diagnose_dfm")
        assert fhir_server_condition_diagnose_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_diagnose_ifdb"),
            AssetKey.from_user_string("arc_diagnose_tillaegskode_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
        }

    def test_pas_diagnose_dfm_depends_on_archive_patient_and_encounter(self):
        key = AssetKey.from_user_string("fhir_server_condition_pas_diagnose_dfm")
        assert fhir_server_condition_pas_diagnose_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_pas_diagnose_ifdb"),
            AssetKey.from_user_string("arc_pas_diagnose_encounter_ref_ifdb"),
            AssetKey.from_user_string("arc_pas_diagnose_tillaegskode_ifdb"),
            AssetKey.from_user_string("arc_pas_diagnose_henvisning_ref_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
        }

    def test_sof_condition_moved_to_dfm_with_all_producers(self):
        """Condition SOF moved to DFM: all four producers are on the base job.

        Diagnose + PAS-diagnose, ServiceRequest and
        Aktivitetsresultat/SFI. Nothing in the partitioned daily job
        can post a Condition after the SOF read any more.
        """
        key = AssetKey.from_user_string("sof_condition_dfm")
        assert sof_condition_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_condition_diagnose_dfm"),
            AssetKey.from_user_string("fhir_server_condition_pas_diagnose_dfm"),
            AssetKey.from_user_string("fhir_server_service_request_dfm"),
            AssetKey.from_user_string("fhir_server_aktivitetsresultat_dfm"),
        }

    def test_legacy_condition_assets_are_gone(self):
        graph = defs.resolve_asset_graph()
        removed = [
            "fhir_diagnose_daily_ifdb",
            "fhir_pas_diagnose_daily_ifdb",
            "ifdb_extract/diagnose_daily",
            "ifdb_extract/pas_diagnose_daily",
        ]
        for key_str in removed:
            assert not graph.has(
                AssetKey.from_user_string(key_str)
            ), f"Legacy asset still registered: {key_str}"
