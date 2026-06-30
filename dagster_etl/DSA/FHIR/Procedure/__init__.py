# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .Procedure_IFDB_AdminProc import (
    fhir_administrative_procedure_bulk_ifdb,
    fhir_administrative_procedure_daily_ifdb,
    fhir_administrative_procedure_monthly_ifdb,
)
from .Procedure_IFDB_RisYdelse import (
    fhir_ris_ydelse_bulk_ifdb,
    fhir_ris_ydelse_daily_ifdb,
    fhir_ris_ydelse_monthly_ifdb,
)

all_assets = [
    fhir_administrative_procedure_daily_ifdb,
    fhir_administrative_procedure_monthly_ifdb,
    fhir_administrative_procedure_bulk_ifdb,
    fhir_ris_ydelse_daily_ifdb,
    fhir_ris_ydelse_monthly_ifdb,
    fhir_ris_ydelse_bulk_ifdb,
]
