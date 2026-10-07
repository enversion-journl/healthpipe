# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Location extraction using DFM checkpoint.

Reads Location flat view from Aidbox sof schema filtered by DFM checkpoint
(instead of Dagster partitions) and MERGEs into fhir.Location.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

# fhir.Location has TWO producers — the IFDB Location and
# the SOR-based Location. The SOF must wait for both (same multi-producer
# rule that gave sof_condition_dfm all four producers).
_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_location_dfm",
    "fhir_server_location_sor_dfm",
]

sof_location_dfm = create_sof_dfm_asset(
    asset_name="sof_location_dfm",
    config_key="location_view",
    checkpoint_resource="Location",
    deps=_PRODUCERS,
)
