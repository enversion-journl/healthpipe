# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSA FHIR utilities.

Sub-modules:
- code_normalization  -- SHAK/SOR/SKS code helpers
- fhir_utils          -- FHIR date/time formatting
- partition_utils     -- partition date-range helpers
- reference_resolver  -- batch reference resolution and application
- status_mapping      -- FHIR status mapping
- ifdb/               -- IFDB/Oracle-specific utilities
- sql_on_fhir/        -- SQL-on-FHIR utilities

Identifier systems and FHIR extensions live in ``utils.fhir.*``:
- ``from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_SOR, ...``
- ``from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE, ...``
"""
