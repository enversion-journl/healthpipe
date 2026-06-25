# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Data Quality checks.

Standardised, config-driven checks that live in one package so other
developers know exactly where to add new ones.

Current check families:
- **Grain checks** -- cross-tier distinct-count comparison (IFDB -> FHIR -> DSA -> DSO)
- **Deleted resource checks** -- IFDB-vs-FHIR orphan detection + soft-delete marking
"""

from data_quality.fhir_deleted_resource_checks import fhir_quality_check_deleted_resources
from data_quality.fhir_grain_checks import grain_check_assets

all_assets = [
    *grain_check_assets,
    fhir_quality_check_deleted_resources,
]

__all__ = ["all_assets"]
