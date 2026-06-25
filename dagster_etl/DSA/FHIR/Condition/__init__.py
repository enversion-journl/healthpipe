# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Condition_IFDB_Diagnose import (
    fhir_diagnose_bulk_ifdb,
    fhir_diagnose_daily_ifdb,
    fhir_diagnose_monthly_ifdb,
)
from .Condition_IFDB_PasDiagnose import (
    fhir_pas_diagnose_bulk_ifdb,
    fhir_pas_diagnose_daily_ifdb,
    fhir_pas_diagnose_monthly_ifdb,
)

all_assets = [
    fhir_diagnose_daily_ifdb,
    fhir_diagnose_monthly_ifdb,
    fhir_diagnose_bulk_ifdb,
    fhir_pas_diagnose_daily_ifdb,
    fhir_pas_diagnose_monthly_ifdb,
    fhir_pas_diagnose_bulk_ifdb,
]
