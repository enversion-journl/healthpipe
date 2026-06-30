# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Location_IFDB_dfm import fhir_server_location_dfm
from .Location_SOR_IFDB import (
    fhir_location_sor_daily_ifdb,
    fhir_location_sor_full_load_ifdb,
    fhir_location_sor_monthly_ifdb,
)

all_assets = [
    fhir_server_location_dfm,
    fhir_location_sor_daily_ifdb,
    fhir_location_sor_monthly_ifdb,
    fhir_location_sor_full_load_ifdb,
]
