# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Ownership per field on FHIR Patient: which source has the say, field by field.

Key format
----------
    "<field>"               top-level element on Patient, e.g. "name"
    "identifier:<system>"   the identifier elements with that system, as a group
    "extension:<url>"       the extension elements with that url, as a group
    "meta.tag:<code>"       only for coverage of the projection; the builder owns meta

The value is the order of the sources: the first one that has the field wins it. An empty tuple
is a field the projection reads but no source supplies yet. A contribution that supplies a field
the source does not own is an error.
"""

from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_CPR_UPDATED,
    EXTENSION_DISTRICT,
    EXTENSION_FATHER_IDENTIFIER,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_MOTHER_IDENTIFIER,
    EXTENSION_PATIENT_CITIZENSHIP,
    EXTENSION_RESIDENCE_MUNICIPALITY,
    EXTENSION_SOCIAL_DISTRICT,
)
from utils.fhir.fhir_extensions_planned import EXTENSION_DISCRETION_FLAG
from utils.fhir.identifier_systems import (
    SYSTEM_BOM_IFDB_PATIENTINFO_OID,
    SYSTEM_CPR,
    SYSTEM_CPR_PERSON_ID,
    SYSTEM_D_ECPR,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_X_ECPR,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

# The codes in meta.tag; HAIBA and SDSDRG match the meta.source of the existing stub resources.
SOURCE_CPR = "CPR"
SOURCE_IFDB = "IFDB"
SOURCE_HAIBA = "HAIBA"
SOURCE_DRG = "SDSDRG"
SOURCE_UDRED = "SDSUDRED"

# Canonical order for provenance tags and for the builder's list output.
ALL_SOURCES: tuple[str, ...] = (SOURCE_CPR, SOURCE_IFDB, SOURCE_HAIBA, SOURCE_DRG, SOURCE_UDRED)

# The name's slice: patient_view reads only name.where(use='official').
USE_OFFICIAL = "official"

# Choice-type elements: one logical field, several possible attributes on the model.
CHOICE_TYPES: dict[str, tuple[str, ...]] = {
    "deceased": ("deceasedBoolean", "deceasedDateTime"),
}

FIELD_OWNERSHIP: dict[str, tuple[str, ...]] = {
    # --- Identity -------------------------------------------------------------
    # The register owns the personal number first because it also carries the previous numbers.
    f"identifier:{SYSTEM_CPR}": (SOURCE_CPR, SOURCE_IFDB, SOURCE_HAIBA, SOURCE_DRG, SOURCE_UDRED),
    # The EPJ source id; the other EPJ producers and the dependency resolver find the patient by it.
    f"identifier:{SYSTEM_IFDB_PATIENT_ID}": (SOURCE_IFDB,),
    # Booking system patientinfo OID that BOM_IFDB.AFTALE points to; from the BOM contribution.
    f"identifier:{SYSTEM_BOM_IFDB_PATIENTINFO_OID}": (SOURCE_IFDB,),
    f"identifier:{SYSTEM_CPR_PERSON_ID}": (SOURCE_CPR,),
    # No source supplies replacement CPR yet; the register only knows people with a personal number.
    f"identifier:{SYSTEM_X_ECPR}": (),
    f"identifier:{SYSTEM_D_ECPR}": (),
    # --- Status and demographics -----------------------------------------------
    "active": (SOURCE_CPR, SOURCE_IFDB),
    "deceased": (SOURCE_CPR, SOURCE_IFDB),
    "name": (SOURCE_CPR, SOURCE_IFDB),
    "gender": (SOURCE_CPR, SOURCE_IFDB),
    "birthDate": (SOURCE_CPR, SOURCE_IFDB),
    "address": (SOURCE_CPR, SOURCE_IFDB),
    "maritalStatus": (SOURCE_CPR,),
    # The CPR extract has no general practitioner details.
    "generalPractitioner": (SOURCE_IFDB,),
    # --- Extensions ------------------------------------------------------------
    f"extension:{EXTENSION_PATIENT_CITIZENSHIP}": (SOURCE_CPR,),
    f"extension:{EXTENSION_MOTHER_IDENTIFIER}": (SOURCE_CPR,),
    f"extension:{EXTENSION_FATHER_IDENTIFIER}": (SOURCE_CPR,),
    f"extension:{EXTENSION_CPR_UPDATED}": (SOURCE_CPR,),
    f"extension:{EXTENSION_ADDRESS_PROTECTION}": (SOURCE_CPR, SOURCE_IFDB),
    f"extension:{EXTENSION_DISCRETION_FLAG}": (SOURCE_IFDB,),
    f"extension:{EXTENSION_DISTRICT}": (SOURCE_IFDB,),
    f"extension:{EXTENSION_SOCIAL_DISTRICT}": (SOURCE_IFDB,),
    f"extension:{EXTENSION_MODTIDSPUNKT}": (SOURCE_IFDB,),
    f"extension:{EXTENSION_FHIR_IS_DELETED}": (SOURCE_IFDB,),
    # --- Meta ------------------------------------------------------------------
    # MedCom test person: the projection reads the tag, but no source sets it.
    "meta.tag:test": (),
}

# Elements the builder itself owns; they must not appear in a contribution.
BUILDER_OWNED_FIELDS: frozenset[str] = frozenset({"id", "meta"})

# Projected fields the builder sets itself (contributors, municipality of residence); unowned.
BUILDER_PROJECTED_FIELDS: frozenset[str] = frozenset(
    {"meta.source", f"extension:{EXTENSION_RESIDENCE_MUNICIPALITY}"}
    | {f"meta.tag:{kilde}" for kilde in ALL_SOURCES}
)


def key_for_fhirpath(sti: str) -> str | None:
    """Translate a projection FHIRPath to the table's key; None for elements the server owns."""
    sti = sti.strip()
    if sti in ("id", "meta.lastUpdated", "r.cts"):
        return None
    if sti == "meta.source":
        return "meta.source"
    if sti.startswith("identifier.where(system='"):
        system = sti[len("identifier.where(system='") :].split("'", 1)[0]
        return f"identifier:{system}"
    if sti.startswith("extension('"):
        url = sti[len("extension('") :].split("'", 1)[0]
        return f"extension:{url}"
    if sti.startswith("meta.tag.where("):
        # The code tells tags apart, both in where(code='x') and where(system='...' and code='x').
        _, _, efter_kode = sti.partition("code='")
        if efter_kode:
            return "meta.tag:" + efter_kode.split("'", 1)[0]
        return "meta.tag"
    top = sti.split(".", 1)[0].split("(", 1)[0]
    for logisk, attributter in CHOICE_TYPES.items():
        if top in attributter:
            return logisk
    return top


def source_for_lineage_system(kildesystem: str) -> str:
    """The lineage file's source system names translated to the table's codes."""
    return {
        "datafordeler": SOURCE_CPR,
        "ifdb": SOURCE_IFDB,
        "haiba": SOURCE_HAIBA,
        "sds_drg": SOURCE_DRG,
        "sds_udredningsret": SOURCE_UDRED,
    }[kildesystem]


__all__ = [
    "ALL_SOURCES",
    "BUILDER_OWNED_FIELDS",
    "BUILDER_PROJECTED_FIELDS",
    "FIELD_OWNERSHIP",
    "SOURCE_CPR",
    "SOURCE_DRG",
    "SOURCE_HAIBA",
    "SOURCE_IFDB",
    "SOURCE_UDRED",
    "USE_OFFICIAL",
    "CHOICE_TYPES",
    "source_for_lineage_system",
    "key_for_fhirpath",
]
