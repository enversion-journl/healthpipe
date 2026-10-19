# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Observation_IFDB_WardUnitCapacity_dfm import (
    fhir_server_observation_ward_unit_capacity_dfm,
)

all_assets = [
    fhir_server_observation_ward_unit_capacity_dfm,
]
