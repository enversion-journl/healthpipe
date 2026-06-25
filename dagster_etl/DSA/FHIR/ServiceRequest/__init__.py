# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .ServiceRequest_IFDB import (
    fhir_service_request_bulk_ifdb,
    fhir_service_request_daily_ifdb,
    fhir_service_request_monthly_ifdb,
)

all_assets = [
    fhir_service_request_daily_ifdb,
    fhir_service_request_monthly_ifdb,
    fhir_service_request_bulk_ifdb,
]
