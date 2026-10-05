# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR DocumentReference assets (IFDB epikrise + udskrivningsrapport, US-FHIR-07)."""

from .DocumentReference_IFDB_CareDischargeReport_dfm import (
    fhir_server_documentreference_care_discharge_report_dfm,
)
from .DocumentReference_IFDB_Epikrise_dfm import (
    fhir_server_documentreference_epikrise_dfm,
)

all_assets = [
    fhir_server_documentreference_epikrise_dfm,
    fhir_server_documentreference_care_discharge_report_dfm,
]

__all__ = ["all_assets"]
