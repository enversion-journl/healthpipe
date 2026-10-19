# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR assets for senge/kapacitet flat tables.

Each view has its own producer and its own fhir.* table. They must not
wait on (or feed) sof_location_dfm / sof_observation_dfm — those grains
are SOR Locations and SFI Observations.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_ward_unit_dfm = create_sof_dfm_asset(
    asset_name="sof_ward_unit_dfm",
    config_key="ward_unit_view",
    checkpoint_resource="Sengeenhed",
    deps=["fhir_sof_infrastructure", "fhir_server_location_ward_unit_dfm"],
)

sof_ward_unit_capacity_dfm = create_sof_dfm_asset(
    asset_name="sof_ward_unit_capacity_dfm",
    config_key="ward_unit_capacity_view",
    checkpoint_resource="Sengeenhed_Kapacitet",
    deps=[
        "fhir_sof_infrastructure",
        "fhir_server_observation_ward_unit_capacity_dfm",
    ],
)

sof_bed_dfm = create_sof_dfm_asset(
    asset_name="sof_bed_dfm",
    config_key="bed_view",
    checkpoint_resource="Bed",
    deps=["fhir_sof_infrastructure", "fhir_server_location_bed_dfm"],
)

sof_encounter_bed_space_dfm = create_sof_dfm_asset(
    asset_name="sof_encounter_bed_space_dfm",
    config_key="encounter_bed_space_view",
    checkpoint_resource="Encounter_BedSpace",
    deps=["fhir_sof_infrastructure", "fhir_server_encounter_dfm"],
)

sof_encounter_absence_dfm = create_sof_dfm_asset(
    asset_name="sof_encounter_absence_dfm",
    config_key="encounter_absence_view",
    checkpoint_resource="Encounter_Absence",
    deps=["fhir_sof_infrastructure", "fhir_server_encounter_dfm"],
)

__all__ = [
    "sof_ward_unit_dfm",
    "sof_ward_unit_capacity_dfm",
    "sof_bed_dfm",
    "sof_encounter_bed_space_dfm",
    "sof_encounter_absence_dfm",
]
