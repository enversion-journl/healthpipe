# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Registration/contract tests for the Aktivitetsresultat/SFI DFM pipeline.

Guards the wiring from the three archive tables through the multi-asset DFM
transform. SOF for Observation moved to DFM : the EVENTMARKER
Observation assets are unregistered stubs (empty __init__) and post nothing,
so SFI is the only live producer — the DFM migration MUST land as DFM. Questionnaire/QR
SOF deps point at the DFM producer keys.
"""

import pytest
from DSA.FHIR.Aktivitetsresultat.Aktivitetsresultat_IFDB_dfm import (
    fhir_server_aktivitetsresultat_sfi_dfm,
)
from DSA.FHIR.SqlOnFhir.Observation_dfm import sof_observation_dfm
from tests.utils.asset_graph_helpers import (
    DFM_PRODUCER_PREFIXES,
    SOF_PREFIXES,
    assert_deps,
)


@pytest.mark.unit
class TestAktivitetsresultatPipelineRegistration:
    """Validate Aktivitetsresultat DFM wiring contracts."""

    def test_dfm_multi_asset_depends_on_archive_and_upstreams(self):
        assert_deps(
            fhir_server_aktivitetsresultat_sfi_dfm,
            "fhir_server_aktivitetsresultat_dfm",
            allowed_prefixes=DFM_PRODUCER_PREFIXES,
            required={
                "arc_aktivitetsresultat_ifdb",
                "arc_aktivitetsudfoerelse_ifdb",
                "arc_aktivitetsudfoerelse_speciale_ifdb",
                # Reference producers must post before this one.
                "fhir_server_patient_dfm",
                "fhir_server_encounter_dfm",
                "fhir_server_eoc_dfm",
            },
        )

    def test_sof_observation_depends_on_dfm_producer(self):
        """SFI is the only registered producer of fhir.Observation.

        The EVENTMARKER-based fhir_observation_*_ifdb assets are not registered
        (DSA/FHIR/Observation/__init__.py exports an empty list), so naming them
        would only pin a stub.
        """
        assert_deps(
            sof_observation_dfm,
            "sof_observation_dfm",
            allowed_prefixes=SOF_PREFIXES,
            required={"fhir_sof_infrastructure", "fhir_server_aktivitetsresultat_dfm"},
        )
