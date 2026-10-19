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
from dataclasses import dataclass

from DSA.FHIR.Patient.field_ownership import (
    ALL_SOURCES,
    BUILDER_OWNED_FIELDS,
    CHOICE_TYPES,
    FIELD_OWNERSHIP,
    SOURCE_IFDB,
    USE_OFFICIAL,
)
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.meta import Meta
from fhir.resources.R4B.patient import Patient
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DK_MUNICIPALITY_CODES,
    EXTENSION_RESIDENCE_MUNICIPALITY,
)
from utils.fhir.identifier_systems import (
    SOURCE_PATIENT_BUILDER,
    SYSTEM_DK_MUNICIPALITY_CODES,
    SYSTEM_SOURCE_TAG,
)
from utils.fhir.protection import ADDRESS_PROTECTED_TEXT, NAME_PROTECTED_TEXT


class UnknownField(ValueError):
    """A contribution contains a field the table does not know."""


class NotOwner(ValueError):
    """A contribution supplies a field the source is not listed as owner of."""


class DuplicateContribution(ValueError):
    """Two contributions from the same source for the same person."""


@dataclass(frozen=True)
class Contribution:
    """One source's proposal for a person: the source and a partial Patient without id and meta."""

    kilde: str
    patient: Patient


# Attribute → logical key for the choice-type fields, so "deceasedBoolean" looks up "deceased".
_LOGICAL_FOR_ATTRIBUTE: dict[str, str] = {
    attribut: logisk for logisk, attributter in CHOICE_TYPES.items() for attribut in attributter
}

# Top-level fields the table knows; grouped list entries and meta fields are handled apart.
_TOP_LEVEL_FIELDS: tuple[str, ...] = tuple(
    n for n in FIELD_OWNERSHIP if ":" not in n and not n.startswith("meta.")
)


def _attributes_for(noegle: str) -> tuple[str, ...]:
    return CHOICE_TYPES.get(noegle, (noegle,))


def field_keys(patient: Patient) -> set[str]:
    """The table keys for everything set on a Patient. Used for the contract."""
    noegler: set[str] = set()
    for navn in patient.__fields__:
        if navn in BUILDER_OWNED_FIELDS or navn in ("resource_type",):
            continue
        vaerdi = getattr(patient, navn, None)
        if vaerdi is None or vaerdi == []:
            continue
        if navn == "identifier":
            noegler.update(f"identifier:{i.system}" for i in vaerdi)
        elif navn == "extension":
            noegler.update(f"extension:{e.url}" for e in vaerdi)
        else:
            noegler.add(_LOGICAL_FOR_ATTRIBUTE.get(navn, navn))
    return noegler


def _validate(bidrag: Sequence[Contribution]) -> dict[str, Patient]:
    pr_kilde: dict[str, Patient] = {}
    for b in bidrag:
        if b.kilde not in ALL_SOURCES:
            raise UnknownField(f"unknown source {b.kilde!r}; known sources are {ALL_SOURCES}")
        if b.kilde in pr_kilde:
            raise DuplicateContribution(f"two contributions from {b.kilde}")
        if b.patient.id is not None or b.patient.meta is not None:
            raise NotOwner(f"{b.kilde} sets id or meta; the builder owns those")
        for noegle in field_keys(b.patient):
            if noegle not in FIELD_OWNERSHIP:
                raise UnknownField(f"{b.kilde} supplies {noegle}, which no source owns")
            if b.kilde not in FIELD_OWNERSHIP[noegle]:
                raise NotOwner(f"{b.kilde} supplies {noegle} but does not own it")
        pr_kilde[b.kilde] = b.patient
    return pr_kilde


def _winner(noegle: str, pr_kilde: dict[str, Patient], har) -> str | None:
    """The first owner in the order that actually has the field."""
    for kilde in FIELD_OWNERSHIP[noegle]:
        patient = pr_kilde.get(kilde)
        if patient is not None and har(patient):
            return kilde
    return None


def _first_yes(noegle: str, pr_kilde: dict[str, Patient], siger_ja) -> Patient | None:
    """The first owner in the order whose contribution says yes."""
    for kilde in FIELD_OWNERSHIP[noegle]:
        patient = pr_kilde.get(kilde)
        if patient is not None and siger_ja(patient):
            return patient
    return None


def _assemble_list(navn: str, gruppe, pr_kilde: dict[str, Patient]) -> list | None:
    """Merge a list (identifier/extension): winner per group, source order in the output."""
    praefiks = f"{navn}:"
    vindere: dict[str, str] = {}
    for noegle in FIELD_OWNERSHIP:
        if not noegle.startswith(praefiks):
            continue
        gruppenoegle = noegle[len(praefiks) :]
        kilde = _winner(
            noegle,
            pr_kilde,
            lambda p, g=gruppenoegle: any(gruppe(e) == g for e in (getattr(p, navn) or [])),
        )
        if kilde is not None:
            vindere[gruppenoegle] = kilde

    resultat = []
    for kilde in ALL_SOURCES:
        patient = pr_kilde.get(kilde)
        if patient is None:
            continue
        for element in getattr(patient, navn) or []:
            if vindere.get(gruppe(element)) == kilde:
                resultat.append(element)
    return resultat or None


def _merged_fields(bidrag: Sequence[Contribution]) -> dict:
    """The patient's fields according to the ownership table, before they become a resource."""
    if not bidrag:
        raise ValueError("no contributions")
    pr_kilde = _validate(bidrag)

    felter: dict = {}
    for noegle in _TOP_LEVEL_FIELDS:
        attributter = _attributes_for(noegle)
        kilde = _winner(
            noegle,
            pr_kilde,
            lambda p, a=attributter: any(getattr(p, x, None) is not None for x in a),
        )
        if kilde is None:
            continue
        for attribut in attributter:
            vaerdi = getattr(pr_kilde[kilde], attribut, None)
            if vaerdi is not None:
                felter[attribut] = vaerdi

    identifiers = _assemble_list("identifier", lambda i: i.system, pr_kilde)
    extensions = _assemble_list("extension", lambda e: e.url, pr_kilde)
    # Protection: a yes from one owner wins over another owner's no.
    beskyttet = _first_yes(
        f"extension:{EXTENSION_ADDRESS_PROTECTION}",
        pr_kilde,
        lambda p: _is_protected(p.extension or []),
    )
    if beskyttet is not None and extensions:
        ja = next(e for e in beskyttet.extension if e.url == EXTENSION_ADDRESS_PROTECTION)
        extensions = [ja if e.url == EXTENSION_ADDRESS_PROTECTION else e for e in extensions]
    if identifiers:
        felter["identifier"] = identifiers
    if extensions:
        felter["extension"] = extensions
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


def provenance(kilder: Iterable[str]) -> Meta:
    """The builder as source, one tag per contributing source in canonical order."""
    bidragende = set(kilder)
    return Meta(
        source=SOURCE_PATIENT_BUILDER,
        tag=[Coding(system=SYSTEM_SOURCE_TAG, code=k) for k in ALL_SOURCES if k in bidragende],
    )


def apply_provenance(patient: Patient, kilder: Iterable[str]) -> Patient:
    """Set meta by assignment; rebuild only if the model does not allow assignment."""
    meta = provenance(kilder)
    try:
        patient.meta = meta
    except Exception:
        return Patient.parse_obj(
            {**patient.dict(exclude_none=True), "meta": meta.dict(exclude_none=True)}
        )
    return patient


def build(bidrag: Sequence[Contribution], bevarede_kilder: Iterable[str] = ()) -> Patient:
    """Assemble, protect and apply provenance. What is to be written — still without id.

    ``bevarede_kilder`` are sources the resource is already tagged with that do not contribute now.
    """
    patient = Patient(**shield(_merged_fields(bidrag)))
    return apply_provenance(patient, [*(b.kilde for b in bidrag), *bevarede_kilder])


# ---------------------------------------------------------------------------
# The EPJ contribution
# ---------------------------------------------------------------------------


def contributions_from_ifdb(patient_rows: list[dict], context) -> list[Contribution]:
    """The EPJ contribution per person, produced by the existing EPJ transform."""
    # Late import: the ownership table must be importable without the Dagster asset.
    from DSA.FHIR.Patient.patient_ifdb_contribution import _transform_patient_rows_to_fhir

    return [
        Contribution(kilde=SOURCE_IFDB, patient=p)
        for p in _transform_patient_rows_to_fhir(patient_rows, context)
    ]


__all__ = [
    "Contribution",
    "DuplicateContribution",
    "NotOwner",
    "UnknownField",
    "contributions_from_ifdb",
    "build",
    "field_keys",
    "apply_provenance",
    "provenance",
    "assemble",
    "shield",
]
