# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: Observation extraction using DFM checkpoint.

Every producer of fhir.Observation is on the DFM path: Aktivitetsresultat/SFI is the only registered one — the EVENTMARKER-based
``fhir_observation_*_ifdb`` assets are not registered in the asset graph (see
DSA/FHIR/Observation/__init__.py) and post nothing.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_PRODUCERS = ["fhir_sof_infrastructure", "fhir_server_aktivitetsresultat_dfm"]

sof_observation_dfm = create_sof_dfm_asset(
    asset_name="sof_observation_dfm",
    config_key="observation_view",
    checkpoint_resource="Observation",
    deps=_PRODUCERS,
)

__all__ = ["sof_observation_dfm"]
