# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Location_IFDB_Bed_dfm import fhir_server_location_bed_dfm
from .Location_IFDB_WardUnit_dfm import fhir_server_location_ward_unit_dfm
from .Location_IFDB_dfm import fhir_server_location_dfm
from .Location_SOR_IFDB_dfm import fhir_server_location_sor_dfm

all_assets = [
    fhir_server_location_dfm,
    fhir_server_location_sor_dfm,
    fhir_server_location_ward_unit_dfm,
    fhir_server_location_bed_dfm,
]
