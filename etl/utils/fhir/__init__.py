# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR utility modules.

This package consolidates FHIR-related utilities:
- identifier_systems: Centralized identifier system URIs
- fhir_extensions: Extension URLs for custom FHIR extensions
- is_deleted_extension: urn:fhir:isDeleted constant and builder
- fhir_helper: FHIR development helper for profiles and specifications
- fhir_validator: FHIR profile and validation tool
"""

from .fhir_extensions import (
    # CPR status codes
    CPR_STATUS_DECEASED,
    CPR_STATUS_INACTIVE,
    # Extension URLs
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DISTRICT,
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_PREVIOUS_EPISODE_OF_CARE,
    EXTENSION_PREVIOUS_EPISODE_TYPE,
    EXTENSION_SOCIAL_DISTRICT,
    EXTENSION_SOURCE_CREATED_AT,
    EXTENSION_SOURCE_MODIFIED_AT,
)
from .is_deleted_extension import EXTENSION_FHIR_IS_DELETED, build_is_deleted_extension
from .fhir_helper import FHIRHelper
from .fhir_validator import FHIRValidator
from .identifier_systems import (
    # Person identifiers
    SYSTEM_CPR,
    # Danish healthcare organization identifiers
    SYSTEM_SHAK,
    SYSTEM_SOR,
    # IFDB-derived identifiers
    SYSTEM_IFDB_ENCOUNTER_ID,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_ORGANIZATION_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

__all__ = [
    # Classes
    "FHIRHelper",
    "FHIRValidator",
    # Person identifiers
    "SYSTEM_CPR",
    # Danish healthcare organization identifiers
    "SYSTEM_SOR",
    "SYSTEM_SHAK",
    # IFDB-derived identifiers
    "SYSTEM_IFDB_OBJECT_ID",
    "SYSTEM_IFDB_PATIENT_ID",
    "SYSTEM_IFDB_EPISODE_OF_CARE_ID",
    "SYSTEM_IFDB_ENCOUNTER_ID",
    "SYSTEM_IFDB_ORGANIZATION_ID",
    # Extension URLs
    "EXTENSION_PREVIOUS_EPISODE_OF_CARE",
    "EXTENSION_PREVIOUS_EPISODE_TYPE",
    "EXTENSION_EPISODE_OF_CARE",
    "EXTENSION_SOURCE_CREATED_AT",
    "EXTENSION_SOURCE_MODIFIED_AT",
    "EXTENSION_MODTIDSPUNKT",
    "EXTENSION_DISTRICT",
    "EXTENSION_SOCIAL_DISTRICT",
    "EXTENSION_ADDRESS_PROTECTION",
    "EXTENSION_FHIR_IS_DELETED",
    "build_is_deleted_extension",
    # CPR status codes
    "CPR_STATUS_DECEASED",
    "CPR_STATUS_INACTIVE",
]
