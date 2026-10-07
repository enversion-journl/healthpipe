# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Encounter extraction using DFM checkpoint.

Reads Encounter flat view from Aidbox sof schema filtered by DFM checkpoint
(instead of Dagster partitions) and MERGEs into fhir.Encounter.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_encounter_dfm = create_sof_dfm_asset(
    asset_name="sof_encounter_dfm",
    config_key="encounter_view",
    checkpoint_resource="Encounter",
    deps=["fhir_sof_infrastructure", "fhir_server_encounter_dfm"],
)
