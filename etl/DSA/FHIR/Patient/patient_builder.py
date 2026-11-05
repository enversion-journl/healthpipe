# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The builder: one Patient from several sources' contributions, merged by ``field_ownership``.

Pure transformation without database lookups or writes. For each field the first owner that has
the field wins; for identifier and extension this applies per system and per url respectively,
and the winner supplies all its elements in the group. Lists follow the canonical order of the
sources. A field without an owner, a field the source does not own, and two contributions from
the same source are hard failures.

After the merge the builder lifts the municipality of residence onto Patient, and for a person
with name and address protection it replaces name and addresses with the standard texts.
"""

from collections.abc import Iterable, Sequence

from DSA.FHIR.Patient.patient_ownership import PATIENT_OWNERSHIP, SOURCE_IFDB, USE_OFFICIAL
from DSA.FHIR.utils.field_ownership import (
    Contribution,
    apply_provenance,
    by_source,
    merged_fields,
    winner,
)
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.patient import Patient
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DK_MUNICIPALITY_CODES,
    EXTENSION_RESIDENCE_MUNICIPALITY,
)
from utils.fhir.identifier_systems import SYSTEM_DK_MUNICIPALITY_CODES
from utils.fhir.protection import ADDRESS_PROTECTED_TEXT, NAME_PROTECTED_TEXT


def _merged_fields(bidrag: Sequence[Contribution]) -> dict:
    """The patient's fields according to the ownership table, before they become a resource."""
    pr_kilde = by_source(PATIENT_OWNERSHIP, bidrag)
    felter = merged_fields(PATIENT_OWNERSHIP, pr_kilde)
    # Protection: a yes from one owner wins over another owner's no.
    beskyttet = winner(
        PATIENT_OWNERSHIP,
        f"extension:{EXTENSION_ADDRESS_PROTECTION}",
        pr_kilde,
        lambda p: _is_protected(p.extension or []),
    )
    if beskyttet is not None and felter.get("extension"):
        ja = next(e for e in pr_kilde[beskyttet].extension if e.url == EXTENSION_ADDRESS_PROTECTION)
        felter["extension"] = [
            ja if e.url == EXTENSION_ADDRESS_PROTECTION else e for e in felter["extension"]
        ]
    return felter


def assemble(bidrag: Sequence[Contribution]) -> Patient:
    """Merge the contributions into one Patient by the ownership table. Without id and meta."""
    return Patient(**_merged_fields(bidrag))


def _municipality_code(address: Address) -> str | None:
    """The address's municipality code: the register's extension, otherwise the EPJ district."""
    for extension in address.extension or []:
        if extension.url != EXTENSION_DK_MUNICIPALITY_CODES:
            continue
        concept = extension.valueCodeableConcept
        for coding in (concept.coding if concept is not None else None) or []:
            if coding.code:
                return str(coding.code)
    return address.district


def _residence_municipality(addresses: Sequence[Address]) -> str | None:
    """The municipality code of the current address, four digits as in the code system."""
    current = [a for a in addresses if a.use != "old"]
    if not current:
        return None
    code = (_municipality_code(current[-1]) or "").strip()
    if not code:
        return None
    return code.zfill(4) if code.isdigit() else code


def _is_protected(extensions: Sequence[Extension]) -> bool:
    return any(e.url == EXTENSION_ADDRESS_PROTECTION and e.valueBoolean is True for e in extensions)


def shield(felter: dict) -> dict:
    """Municipality of residence on Patient; name and addresses as standard text if protected."""
    felter = dict(felter)
    extensions = list(felter.get("extension") or [])
    code = _residence_municipality(felter.get("address") or [])
    if code:
        extensions.append(
            Extension(
                url=EXTENSION_RESIDENCE_MUNICIPALITY,
                valueCodeableConcept=CodeableConcept(
                    coding=[Coding(system=SYSTEM_DK_MUNICIPALITY_CODES, code=code)]
                ),
            )
        )
        felter["extension"] = extensions
    if _is_protected(extensions):
        felter["name"] = [
            HumanName(use=USE_OFFICIAL, family=NAME_PROTECTED_TEXT, text=NAME_PROTECTED_TEXT)
        ]
        felter["address"] = [Address(use="home", line=[ADDRESS_PROTECTED_TEXT])]
    return felter


def build(bidrag: Sequence[Contribution], bevarede_kilder: Iterable[str] = ()) -> Patient:
    """Assemble, protect and apply provenance. What is to be written — still without id.

    ``bevarede_kilder`` are sources the resource is already tagged with that do not contribute now.
    """
    patient = Patient(**shield(_merged_fields(bidrag)))
    return apply_provenance(
        PATIENT_OWNERSHIP, patient, [*(b.source for b in bidrag), *bevarede_kilder]
    )


# ---------------------------------------------------------------------------
# The EPJ contribution
# ---------------------------------------------------------------------------


def contributions_from_ifdb(patient_rows: list[dict], context) -> list[Contribution]:
    """The EPJ contribution per person, produced by the existing EPJ transform."""
    # Late import: the ownership table must be importable without the Dagster asset.
    from DSA.FHIR.Patient.patient_ifdb_contribution import _transform_patient_rows_to_fhir

    return [
        Contribution(source=SOURCE_IFDB, resource=p)
        for p in _transform_patient_rows_to_fhir(patient_rows, context)
    ]


__all__ = [
    "contributions_from_ifdb",
    "build",
    "assemble",
    "shield",
]
