# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from DSA.FHIR.Practitioner.Practitioner_EPJI_dfm import fhir_server_practitioner_epji_dfm
from DSA.FHIR.Practitioner.Practitioner_SilkeborgData_dfm import fhir_server_practitioner_dfm
from DSA.FHIR.Practitioner.PractitionerRole_SilkeborgData_dfm import (
    fhir_server_practitioner_role_dfm,
)

all_assets = [
    fhir_server_practitioner_dfm,
    fhir_server_practitioner_role_dfm,
    fhir_server_practitioner_epji_dfm,
]
