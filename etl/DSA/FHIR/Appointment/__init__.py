# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Appointment_IFDB_dfm import fhir_server_appointment_dfm

all_assets = [
    fhir_server_appointment_dfm,
]
