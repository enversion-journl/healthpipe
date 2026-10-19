# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contributions to the builder that do not come from a full source transform.

None of them set ``id`` or ``meta``; the builder owns those.
"""

from __future__ import annotations

from collections.abc import Iterable

from DSA.FHIR.Patient.field_ownership import (
    ALL_SOURCES,
    FIELD_OWNERSHIP,
    SOURCE_CPR,
    SOURCE_IFDB,
    USE_OFFICIAL,
)
from DSA.FHIR.Patient.patient_builder import Contribution, NotOwner
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import (
    SYSTEM_BOM_IFDB_PATIENTINFO_OID,
    SYSTEM_CPR,
    SYSTEM_IFDB_PATIENT_ID,
)

# Other EPJ ids are «secondary», or «old» when EPJ has deleted the row; the primary id has no use.
USE_SECONDARY = "secondary"
USE_OLD = "old"


def stub_contribution(cpr: str, source: str) -> Contribution:
    """Only the personal number is known, so only that is claimed. The source must own it."""
    if source not in ALL_SOURCES:
        raise NotOwner(f"unknown source {source!r}; known sources are {ALL_SOURCES}")
    if source not in FIELD_OWNERSHIP[f"identifier:{SYSTEM_CPR}"]:
        raise NotOwner(f"{source} does not own the personal number and cannot supply a stub")
    return Contribution(
        kilde=source,
        patient=Patient(identifier=[Identifier(system=SYSTEM_CPR, value=str(cpr).strip())]),
    )


def contribution_from_bom_patientinfo(rows: dict | Iterable[dict]) -> Contribution | None:
    """BOM_IFDB.PATIENTINFO rows as an EPJ contribution: personal number, all OIDs, name and gender.

    All OIDs are added because the appointments point to the row's OID. Name and gender are taken
    from the first row (lowest OID) that has them.
    """
    rows = [rows] if isinstance(rows, dict) else [r for r in rows if r]
    rows = [r for r in rows if r.get("CPRNR")]
    if not rows:
        return None
    from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id

    rows.sort(key=lambda r: (r.get("PATIENTINFOOID") is None, str(r.get("PATIENTINFOOID") or "")))
    cpr = str(rows[0]["CPRNR"]).strip()
    identifiers = [Identifier(system=SYSTEM_CPR, value=cpr)]
    seen: set[str] = set()
    for row in rows:
        oid = row.get("PATIENTINFOOID")
        if oid is None:
            continue
        value = normalize_oracle_id(oid)
        if value and value not in seen:
            seen.add(value)
            identifiers.append(Identifier(system=SYSTEM_BOM_IFDB_PATIENTINFO_OID, value=value))

    name = None
    for row in rows:
        given, family = row.get("FORNAVN"), row.get("EFTERNAVN")
        if given or family:
            name = [
                HumanName(use=USE_OFFICIAL, given=[given] if given else None, family=family or None)
            ]
            break

    gender = None
    for row in rows:
        koen = (row.get("KOEN") or "").strip().upper()
        if koen in {"M", "MALE"}:
            gender = "male"
        elif koen in {"K", "F", "FEMALE"}:
            gender = "female"
        elif koen:
            gender = "unknown"
        if gender:
            break

    return Contribution(
        kilde=SOURCE_IFDB, patient=Patient(identifier=identifiers, name=name, gender=gender)
    )


def with_additional_epj_ids(
    contribution: Contribution,
    extra_ids: Iterable[str],
    deleted_ids: Iterable[str] = (),
) -> Contribution:
    """Add the person's other EPJ ids to the EPJ contribution after the primary; skip duplicates."""
    if contribution.kilde != SOURCE_IFDB:
        raise NotOwner(f"{contribution.kilde} does not own EPJ patient ids")
    deleted = {str(d).strip() for d in deleted_ids}
    present = {
        i.value
        for i in (contribution.patient.identifier or [])
        if i.system == SYSTEM_IFDB_PATIENT_ID
    }
    extras = [
        Identifier(
            system=SYSTEM_IFDB_PATIENT_ID,
            value=e,
            use=USE_OLD if e in deleted else USE_SECONDARY,
        )
        for e in (str(x).strip() for x in extra_ids)
        if e and e not in present
    ]
    if not extras:
        return contribution
    patient = contribution.patient
    identifiers = list(patient.identifier or []) + extras
    try:
        patient.identifier = identifiers
    except Exception:
        patient = Patient(**{**patient.dict(exclude_none=True), "identifier": identifiers})
    return Contribution(kilde=contribution.kilde, patient=patient)


def register_identity_contribution(
    current_cpr: str, historical_cprs: Iterable[str]
) -> Contribution | None:
    """Register identity contribution: current number, then previous as ``use='old'``; None if no previous."""
    old = [
        str(c).strip() for c in historical_cprs if str(c).strip() and str(c).strip() != current_cpr
    ]
    if not old:
        return None
    identifiers = [Identifier(system=SYSTEM_CPR, value=str(current_cpr).strip())] + [
        Identifier(system=SYSTEM_CPR, value=c, use=USE_OLD) for c in old
    ]
    return Contribution(kilde=SOURCE_CPR, patient=Patient(identifier=identifiers))


__all__ = [
    "USE_OFFICIAL",
    "USE_OLD",
    "USE_SECONDARY",
    "contribution_from_bom_patientinfo",
    "register_identity_contribution",
    "stub_contribution",
    "with_additional_epj_ids",
]
