# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from .CarePlan_IFDB import (
    fhir_careplan_daily_ifdb,
    fhir_careplan_full_load_ifdb,
    fhir_careplan_monthly_ifdb,
)

all_assets = [
    fhir_careplan_daily_ifdb,
    fhir_careplan_full_load_ifdb,
    fhir_careplan_monthly_ifdb,
]
