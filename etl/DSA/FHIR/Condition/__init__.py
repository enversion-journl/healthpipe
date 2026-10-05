# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Condition_IFDB_Diagnose_dfm import fhir_server_condition_diagnose_dfm
from .Condition_IFDB_PasDiagnose_dfm import fhir_server_condition_pas_diagnose_dfm

all_assets = [
    fhir_server_condition_diagnose_dfm,
    fhir_server_condition_pas_diagnose_dfm,
]
