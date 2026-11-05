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
  arc.bom_ifdb_orgenhed_sor                    -> fhir_server_location_bom_ifdb_dfm
  sof_location_dfm waits for BOTH Location producers (multi-producer rule).

Pure asset-graph validation (no live Aidbox/MSSQL). Mirrors
``test_servicerequest_pipeline_registration.py``.
"""

import pytest
from DSA.FHIR.Appointment.Appointment_IFDB_dfm import fhir_server_appointment_dfm
from DSA.FHIR.Location.Location_BOM_IFDB_dfm import fhir_server_location_bom_ifdb_dfm
from DSA.FHIR.SqlOnFhir.Appointment_dfm import sof_appointment_dfm
from DSA.FHIR.SqlOnFhir.Location_dfm import sof_location_dfm
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)


@pytest.mark.unit
class TestAppointmentLocationPipelineRegistration:
    """Validate Appointment/Location_SOR DFM wiring."""

    def test_appointment_dfm_depends_on_archives_and_upstreams(self):
        assert_deps(
            fhir_server_appointment_dfm,
            "fhir_server_appointment_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_bom_ifdb_aftale",
                "arc_bom_ifdb_aftaleressourcedeltager",
                "arc_bom_ifdb_patientinfo",
                "fhir_server_patient_dfm",
                "fhir_server_location_bom_ifdb_dfm",
            },
        )

    def test_location_sor_dfm_depends_on_archive_and_ifdb_location(self):
        assert_deps(
            fhir_server_location_bom_ifdb_dfm,
            "fhir_server_location_bom_ifdb_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_bom_ifdb_orgenhed_sor",
                "arc_location_ifdb",
                "fhir_server_location_dfm",
            },
        )

    def test_sof_appointment_waits_for_its_producer(self):
        assert_deps(
            sof_appointment_dfm,
            "sof_appointment_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={"fhir_sof_infrastructure", "fhir_server_appointment_dfm"},
        )

    def test_sof_location_waits_for_both_location_producers(self):
        """fhir.Location has two producers  — omitting the SOR
        producer lets the combined DFM jobs run SOF before SOR-Locations are
        posted (rows then slip a cycle; the checkpoint stamps wall-clock)."""
        assert_deps(
            sof_location_dfm,
            "sof_location_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={
                "fhir_sof_infrastructure",
                # BOTH Location producers — omitting the SOR one lets SOF run
                # before SOR-Locations are posted.
                "fhir_server_location_dfm",
                "fhir_server_location_bom_ifdb_dfm",
            },
        )
