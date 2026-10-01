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
from dagster import AssetKey
from definitions import defs
from DSA.FHIR.Aktivitetsresultat.Aktivitetsresultat_IFDB_dfm import (
    fhir_server_aktivitetsresultat_sfi_dfm,
)
from DSA.FHIR.SqlOnFhir.Observation_dfm import sof_observation_dfm


@pytest.mark.unit
class TestAktivitetsresultatPipelineRegistration:
    """Validate Aktivitetsresultat DFM wiring contracts."""

    def test_dfm_multi_asset_depends_on_archive_and_upstreams(self):
        key = AssetKey.from_user_string("fhir_server_aktivitetsresultat_dfm")
        assert fhir_server_aktivitetsresultat_sfi_dfm.asset_deps[key] == {
            AssetKey.from_user_string("arc_aktivitetsresultat_ifdb"),
            AssetKey.from_user_string("arc_aktivitetsudfoerelse_ifdb"),
            AssetKey.from_user_string("arc_aktivitetsudfoerelse_speciale_ifdb"),
            AssetKey.from_user_string("fhir_server_patient_dfm"),
            AssetKey.from_user_string("fhir_server_encounter_dfm"),
            AssetKey.from_user_string("fhir_server_eoc_dfm"),
        }

    def test_sof_observation_depends_on_dfm_producer(self):
        """SFI is the only registered producer of fhir.Observation.

        The EVENTMARKER-based fhir_observation_*_ifdb assets are not registered
        (DSA/FHIR/Observation/__init__.py exports an empty list), so naming them
        would only pin a stub.
        """
        key = AssetKey.from_user_string("sof_observation_dfm")
        assert sof_observation_dfm.asset_deps[key] == {
            AssetKey.from_user_string("fhir_sof_infrastructure"),
            AssetKey.from_user_string("fhir_server_aktivitetsresultat_dfm"),
        }

    def test_legacy_aktivitetsresultat_assets_are_gone(self):
        graph = defs.resolve_asset_graph()
        removed = [
            "fhir_aktivitetsresultat_daily_ifdb",
            "fhir_questionnaire_daily_ifdb",
            "fhir_questionnaire_response_daily_ifdb",
            "ifdb_extract/aktivitetsresultat_daily",
            "ifdb_extract/aktivitetsudfoerelse_daily",
        ]
        for key_str in removed:
            assert not graph.has(
                AssetKey.from_user_string(key_str)
            ), f"Legacy asset still registered: {key_str}"
