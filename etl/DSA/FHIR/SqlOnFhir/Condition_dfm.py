# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: Condition extraction using DFM checkpoint.

fhir.Condition has four producers and was therefore the last flat table to
qualify for the DFM path:

  - Diagnose and PAS-diagnose
  - ServiceRequest (RIS-diagnoser)
  - Aktivitetsresultat/SFI

With all four on the DFM base job there is nothing left in the partitioned daily
job that can post a Condition after this asset has read the view, so SOF and DSO
move here with them.

Grain: one row per Condition resource (PK condition_id). code/code_system/
code_display carry the PRIMARY diagnosis coding, selected in condition_view via
``code.coding.where(userSelected = true)``. SKS supplementary codes
(tillaegskoder — many are ATC medication codes) are projected to
fhir.Condition_SupplementaryCode.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_condition_diagnose_dfm",
    "fhir_server_condition_pas_diagnose_dfm",
    "fhir_server_service_request_dfm",
    "fhir_server_aktivitetsresultat_dfm",
]

sof_condition_dfm = create_sof_dfm_asset(
    asset_name="sof_condition_dfm",
    config_key="condition_view",
    checkpoint_resource="Condition",
    deps=_PRODUCERS,
)

# The child view is derived from the parent's resources, so it follows the
# parent rather than re-reading the producers.
sof_condition_supplementary_code_dfm = create_sof_dfm_asset(
    asset_name="sof_condition_supplementary_code_dfm",
    config_key="condition_supplementary_code_view",
    checkpoint_resource="Condition_SupplementaryCode",
    deps=["fhir_sof_infrastructure", "sof_condition_dfm"],
)

# Kobling diagnose -> henvisning (PAS_DIAGNOSE_HENVISNING_REF) i eget grain; samme
# afledning fra forældre-ressourcerne som tillægskoderne.
sof_condition_henvisning_dfm = create_sof_dfm_asset(
    asset_name="sof_condition_henvisning_dfm",
    config_key="condition_henvisning_view",
    checkpoint_resource="Condition_henvisning",
    deps=["fhir_sof_infrastructure", "sof_condition_dfm"],
)

__all__ = [
    "sof_condition_dfm",
    "sof_condition_supplementary_code_dfm",
    "sof_condition_henvisning_dfm",
]
