# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the Appointment + Location_SOR DFM pipeline.

Guards the wiring from the four BOM_IFDB archive tables through the DFM
transforms and SOF:
  arc.bom_ifdb_aftale (+deltager, patientinfo) -> fhir_server_appointment_dfm
  arc.bom_ifdb_orgenhed_sor                    -> fhir_server_location_sor_dfm
  sof_location_dfm waits for BOTH Location producers (multi-producer rule).

Pure asset-graph validation (no live Aidbox/MSSQL). Mirrors
``test_servicerequest_pipeline_registration.py``.
"""

import pytest
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.Appointment.Appointment_IFDB_dfm import fhir_server_appointment_dfm
from DSA.FHIR.Location.Location_SOR_IFDB_dfm import fhir_server_location_sor_dfm
from DSA.FHIR.SqlOnFhir.Appointment_dfm import sof_appointment_dfm
from DSA.FHIR.SqlOnFhir.Location_dfm import sof_location_dfm


@pytest.mark.unit
class TestAppointmentLocationPipelineRegistration:
    """Validate Appointment/Location_SOR DFM wiring."""

    def test_appointment_dfm_depends_on_archives_and_upstreams(self):
        key = AssetKey.from_user_string("fhir_server_appointment_dfm")
        assert fhir_server_appointment_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_bom_ifdb_aftale"),
            AssetKey.from_user_string("arc_bom_ifdb_aftaleressourcedeltager"),
            AssetKey.from_user_string("arc_bom_ifdb_patientinfo"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_location_sor_dfm"),
        }

    def test_location_sor_dfm_depends_on_archive_and_ifdb_location(self):
        key = AssetKey.from_user_string("fhir_server_location_sor_dfm")
        assert fhir_server_location_sor_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_bom_ifdb_orgenhed_sor"),
            AssetKey.from_user_string("fhir_server_location_dfm"),
        }

    def test_sof_appointment_waits_for_its_producer(self):
        key = AssetKey.from_user_string("sof_appointment_dfm")
        assert sof_appointment_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_appointment_dfm"),
        }

    def test_sof_location_waits_for_both_location_producers(self):
        """fhir.Location has two producers  — omitting the SOR
        producer lets the combined DFM jobs run SOF before SOR-Locations are
        posted (rows then slip a cycle; the checkpoint stamps wall-clock)."""
        key = AssetKey.from_user_string("sof_location_dfm")
        assert sof_location_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_location_dfm"),
            AssetKey.from_user_string("fhir_server_location_sor_dfm"),
        }

    def test_legacy_assets_are_gone(self):
        graph = defs.resolve_asset_graph()
        for key_str in [
            "fhir_appointment_daily_ifdb",
            "fhir_appointment_monthly_ifdb",
            "fhir_appointment_full_load_ifdb",
            "fhir_location_sor_daily_ifdb",
            "fhir_location_sor_monthly_ifdb",
            "fhir_location_sor_full_load_ifdb",
            "sof_appointment_daily",
            "sof_appointment_monthly",
        ]:
            assert not graph.has(
                AssetKey.from_user_string(key_str)
            ), f"Legacy asset still registered: {key_str}"
