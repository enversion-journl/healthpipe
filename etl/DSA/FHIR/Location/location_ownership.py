# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Ownership per field on the SOR-keyed FHIR Location; the key format is in fhir_etl_guide.md."""

from DSA.FHIR.utils.field_ownership import SOURCE_IFDB, OwnershipTable
from fhir.resources.R4B.location import Location
from utils.fhir.fhir_extensions import EXTENSION_LOCATION_VALIDITY_PERIOD
from utils.fhir.identifier_systems import SOURCE_LOCATION_BUILDER, SYSTEM_SOR

SOURCE_BOM_IFDB = "BOM_IFDB"

LOCATION_OWNERSHIP = OwnershipTable(
    resource_class=Location,
    sources=(SOURCE_BOM_IFDB, SOURCE_IFDB),
    builder_uri=SOURCE_LOCATION_BUILDER,
    fields={
        f"identifier:{SYSTEM_SOR}": (SOURCE_BOM_IFDB, SOURCE_IFDB),
        "managingOrganization": (SOURCE_BOM_IFDB, SOURCE_IFDB),
        "status": (SOURCE_BOM_IFDB, SOURCE_IFDB),
        "type": (SOURCE_BOM_IFDB,),
        "partOf": (SOURCE_BOM_IFDB,),
        f"extension:{EXTENSION_LOCATION_VALIDITY_PERIOD}": (SOURCE_BOM_IFDB,),
        # The name comes via managingOrganization, because the sources name one code differently.
        "name": (),
        "description": (),
    },
)

__all__ = ["LOCATION_OWNERSHIP", "SOURCE_BOM_IFDB", "SOURCE_IFDB"]
