# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Appointment_IFDB import (
    fhir_appointment_daily_ifdb,
    fhir_appointment_full_load_ifdb,
    fhir_appointment_monthly_ifdb,
)

all_assets = [
    fhir_appointment_daily_ifdb,
    fhir_appointment_full_load_ifdb,
    fhir_appointment_monthly_ifdb,
]
