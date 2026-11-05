# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The single write point for FHIR Patient.

Callers hand over who (EPJ patient ids, personal numbers or archive rows). The write point
resolves the identity (``patient_identity``), gathers the contributions (EPJ, the CPR
register, BOM or stub) in ``patient_builder`` and writes once through ``write_patients``. The
register is read under the publication lock and is part of every write of whole resources.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import cast

import dagster as dg
from core.fhir.deletion import mark_resources_deleted
from core.resources import FHIRDatabaseResource, MSSQLResource
from core.safe_asset import aggregate_errors
from DSA.FHIR.Patient.patient_builder import build, contributions_from_ifdb
from DSA.FHIR.Patient.patient_contributions import (
    contribution_from_bom_patientinfo,
    register_identity_contribution,
    stub_contribution,
    with_additional_epj_ids,
)
from DSA.FHIR.Patient.patient_cpr_contribution import (
    RegisterPerson,
    contribution_from_register,
    open_register,
    ufortolkede_vaerdier,
)
from DSA.FHIR.Patient.patient_identity import (
    LOOKUP_CHUNK,
    IdentityRequest,
    Person,
    fetch_fhir_ids,
    resolve,
    row_is_deleted,
    row_patient_id,
)
from DSA.FHIR.Patient.patient_ownership import ALL_SOURCES, SOURCE_CPR, SOURCE_IFDB
from DSA.FHIR.utils.field_ownership import Contribution
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from fhir.resources.R4B.patient import Patient
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID, SYSTEM_SOURCE_TAG

# Person scope for the register's write path: known persons are the default.
SCOPE_KNOWN = "known"
SCOPE_REGISTER = "register"
SCOPES = (SCOPE_KNOWN, SCOPE_REGISTER)

# Sources that deliver only a personal number: their tag stays when others rebuild the resource.
STUB_SOURCES: tuple[str, ...] = tuple(k for k in ALL_SOURCES if k not in (SOURCE_CPR, SOURCE_IFDB))


@dataclass
class EnsureResult:
    """What the callers need: id per identity, and what happened."""

    by_ifdb_id: dict[str, str] = field(default_factory=dict)
    by_cpr: dict[str, str] = field(default_factory=dict)
    conflicts: int = 0  # identity groups with several existing resources
    written: int = 0  # resources written in this call (new and rebuilt)
    rebuilt: int = 0  # of these, existing ones that lacked an EPJ id and were rebuilt
    existing: int = 0  # persons that already had a complete resource — untouched
    from_register: int = 0  # persons whose resource got the register's contribution
    unresolved_ifdb_ids: tuple[str, ...] = ()  # nothing in the archive, nothing in FHIR
    unresolved_cprs: tuple[str, ...] = ()  # neither row, BOM row, register nor stub source

    def record(self, person: Person, fhir_id: str) -> None:
        for epj_id in person.epj_ids:
            self.by_ifdb_id[epj_id] = fhir_id
        for cpr in person.cprs:
            self.by_cpr[cpr] = fhir_id


@dataclass
class RegisterWriteResult:
    """The register's write path: what was rebuilt, created and skipped."""

    written: int = 0  # actually written
    rebuilt: int = 0  # existing resources rebuilt with the register's contribution
    created: int = 0  # persons without a resource, built from the register (and EPJ, if any)
    existing: int = 0  # new ones that turned out to exist in the write's transaction
    skipped_unknown: int = 0  # scope=known: in the register, but neither EPJ row nor resource
    untouched: int = 0  # known resource with no basis to rebuild from
    missing_in_register: int = 0  # requested, but in neither the register, EPJ nor FHIR
    conflicts: int = 0
    withheld: int = 0  # written without a new identifier value that is on another resource
    uninterpreted: int = 0  # values from the register that could not be interpreted


def _set_id(patient: Patient, fhir_id: str) -> Patient:
    try:
        patient.id = fhir_id
    except Exception:
        patient = Patient(**{**patient.dict(exclude_none=True), "id": fhir_id})
    return patient


def _log_superseded(context: dg.AssetExecutionContext, superseded: int) -> None:
    if superseded:
        context.log.warning(
            "%d number(s) unknown to the register gave way to the person's registered number "
            "and are kept as previous numbers.",
            superseded,
        )


def _log_conflicts(context: dg.AssetExecutionContext, conflicts: int) -> None:
    if conflicts:
        context.log.warning(
            "Patient: %d person(s) have several resources (old and new number on one each); "
            "written per resource, not merged",
            conflicts,
        )


def _bom_rows(bom_rows_by_cpr: dict | None) -> dict[str, list[dict]]:
    """The caller's BOM rows per number — one or more per number, normalised to lists."""
    out: dict[str, list[dict]] = {}
    for cpr, rows in (bom_rows_by_cpr or {}).items():
        out[str(cpr).strip()] = [rows] if isinstance(rows, dict) else list(rows)
    return out


def epj_contributions(
    persons: Iterable[Person], context: dg.AssetExecutionContext
) -> dict[str, Contribution]:
    """EPJ's contributions for all persons in one transform call, keyed on the primary EPJ id."""
    rows = [p.primary_row for p in persons if p.primary_row is not None]
    if not rows:
        return {}
    by_id: dict[str, Contribution] = {}
    for c in contributions_from_ifdb(rows, context):
        for i in c.resource.identifier or []:
            if i.system == SYSTEM_IFDB_PATIENT_ID and i.value:
                by_id[str(i.value).strip()] = c
                break
    return by_id


def stub_sources_on_resources(
    fhir_database: FHIRDatabaseResource, persons: Iterable[Person]
) -> dict[str, tuple[str, ...]]:
    """The stub sources' tags on the persons' existing resources, per resource id.

    Reads the builder's ``meta.tag`` and a stub resource's ``meta.source`` from before the builder.
    """
    ids = sorted({p.fhir_id for p in persons if p.fhir_id})
    found: dict[str, tuple[str, ...]] = {}
    if not ids:
        return found
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cursor:
            for start in range(0, len(ids), LOOKUP_CHUNK):
                cursor.execute(
                    "SELECT id, resource->'meta'->'tag', resource->'meta'->>'source' "
                    "FROM patient WHERE id = ANY(%s)",
                    (ids[start : start + LOOKUP_CHUNK],),
                )
                for fid, tags, source in cursor.fetchall():
                    codes = {
                        t.get("code") for t in tags or [] if t.get("system") == SYSTEM_SOURCE_TAG
                    }
                    kept = tuple(k for k in STUB_SOURCES if k in codes or k == source)
                    if kept:
                        found[str(fid)] = kept
    return found


def build_person(
    person: Person,
    context: dg.AssetExecutionContext,
    *,
    epj: Contribution | None = None,
    stub_source: str | None = None,
    bom_rows: list[dict] | None = None,
    register: RegisterPerson | None = None,
    kept_sources: Iterable[str] = (),
) -> Patient | None:
    """Build one person's resource from its contributions; None when none can be formed."""
    contributions: list[Contribution] = []

    if person.rows:
        if epj is None:
            built = contributions_from_ifdb([person.rows[0]], context)
            epj = built[0] if built else None
        if epj is not None:
            extra = person.epj_ids[1:]
            deleted = [row_patient_id(r) for r in person.rows[1:] if row_is_deleted(r)]
            contributions.append(with_additional_epj_ids(epj, extra, deleted))

    if not contributions and bom_rows:
        bom = contribution_from_bom_patientinfo(bom_rows)
        if bom is not None:
            contributions.append(bom)

    # The source's tag is included also when the register builds the person.
    if not contributions and stub_source and person.current_cpr:
        contributions.append(stub_contribution(person.current_cpr, stub_source))

    if register is not None and person.current_cpr:
        contributions.append(
            contribution_from_register(
                register, current_cpr=person.current_cpr, historical_cprs=person.cprs[1:]
            )
        )
    elif contributions and person.current_cpr and len(person.cprs) > 1:
        # Previous numbers must be on the resource so references to them can be resolved.
        reg = register_identity_contribution(person.current_cpr, person.cprs[1:])
        if reg is not None:
            contributions.append(reg)

    if not contributions:
        return None

    patient = build(contributions, kept_sources)
    return _set_id(patient, person.fhir_id) if person.fhir_id else patient


class PatientWriteResult(list[Patient]):
    """Resolved resources, including untouched matches, plus actual write counts."""

    def __init__(
        self,
        patients: Iterable[Patient],
        *,
        written: int,
        withheld: int = 0,
        uninterpreted: int = 0,
    ):
        super().__init__(patients)
        self.written = written
        self.withheld = withheld
        self.uninterpreted = uninterpreted
        self.existing = len(self) - written


def write_patients(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    patients: list[Patient],
    *,
    source_name: str,
    create_only: Iterable[bool] | None = None,
) -> PatientWriteResult:
    """The single write function: atomic match and write in PostgreSQL; fails on failed writes."""
    if not patients:
        return PatientWriteResult([], written=0)
    flags = list(create_only) if create_only is not None else [False] * len(patients)
    if len(flags) != len(patients):
        raise ValueError("create_only must have one flag per resource")
    had_id = [p.id is not None for p in patients]
    patients = prepare_resources_for_upsert(
        resources=patients,
        fhir_database=fhir_database,
        resource_type="Patient",
        preferred_id_systems=[SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR],
        logger=context.log,
    )
    pending = [
        (p, only_new)
        for p, only_new, known in zip(patients, flags, had_id, strict=True)
        if not (only_new and not known and p.id is not None)
    ]
    to_write = [p for p, _ in pending]
    skipped = len(patients) - len(to_write)
    if skipped:
        context.log.info("Patient: %d stub(s) skipped — the person already exists", skipped)
    if not to_write:
        _invalidate_patient_caches(fhir_database)
        return PatientWriteResult(patients, written=0)
    upserted = fhir_database.batch_upsert_resources(
        resources=to_write,
        resource_type="Patient",
        identity_safe=True,
        create_only=[only_new for _, only_new in pending],
    )
    successes, failures, failed_ids = upserted
    withheld = int(getattr(upserted, "withheld", 0) or 0)
    if failures:
        # Count only: failed_ids may carry the personal number.
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(to_write)} Patient resources "
                f"({source_name}) — checkpoint NOT advanced."
            )
        )
    _invalidate_patient_caches(fhir_database)
    context.log.info("Patient: %d written (%s)", successes, source_name)
    ufortolkede = ufortolkede_vaerdier()
    if ufortolkede:
        context.log.warning(
            "Patient: the register sent values that could not be interpreted (count per field): %s",
            ufortolkede,
        )
    if withheld:
        context.log.warning(
            "Patient: %d written without a new identifier value (%s) — the value is on another "
            "resource; candidate for merging",
            withheld,
            source_name,
        )
    return PatientWriteResult(
        patients,
        written=successes,
        withheld=withheld,
        uninterpreted=sum(ufortolkede.values()),
    )


def _invalidate_patient_caches(database):
    database.mark_stale("Patient")
    database.safe_clear_identifier_caches_for_resources(
        {"Patient"}, reason="Patient identity observed by central writer"
    )


# What the caller needs. The two guarantees do not cost the same.
NEED_REFERENCE = "reference"
NEED_PERSON = "person"


def _already_known(
    fhir_database: FHIRDatabaseResource,
    ifdb_patient_ids: Iterable[str],
    cprs: Iterable[str],
) -> tuple[dict[str, str], dict[str, str]]:
    """The identities already on a resource, per EPJ id and per personal number."""
    epj = [str(i).strip() for i in ifdb_patient_ids if str(i).strip()]
    numre = [str(c).strip() for c in cprs if str(c).strip()]
    return (
        fetch_fhir_ids(fhir_database, SYSTEM_IFDB_PATIENT_ID, epj) if epj else {},
        fetch_fhir_ids(fhir_database, SYSTEM_CPR, numre) if numre else {},
    )


def ensure_patients(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    *,
    ifdb_patient_ids: Iterable[str] = (),
    cprs: Iterable[str] = (),
    stub_source: str | None = None,
    bom_rows_by_cpr: dict | None = None,
    source_name: str = "ensure_patients",
    need: str = NEED_PERSON,
) -> EnsureResult:
    """Make sure the persons exist as Patient; return FHIR id per identity.

    Existing resources with all the person's EPJ ids are not written. Otherwise the person is
    built from the EPJ row, the register, the BOM rows or as a stub with ``stub_source``.
    ``need`` sets the price: ``NEED_PERSON`` runs the whole identity resolution and rebuilds an
    incomplete resource; ``NEED_REFERENCE`` stops when the identity is already on a resource.
    """
    if need not in (NEED_PERSON, NEED_REFERENCE):
        raise ValueError("need must be NEED_PERSON or NEED_REFERENCE")
    kendt_epj: dict[str, str] = {}
    kendt_cpr: dict[str, str] = {}
    if need == NEED_REFERENCE:
        kendt_epj, kendt_cpr = _already_known(fhir_database, ifdb_patient_ids, cprs)
        ifdb_patient_ids = [i for i in ifdb_patient_ids if str(i).strip() not in kendt_epj]
        cprs = [c for c in cprs if str(c).strip() not in kendt_cpr]
        if not ifdb_patient_ids and not cprs:
            faerdig = EnsureResult(by_ifdb_id=dict(kendt_epj), by_cpr=dict(kendt_cpr))
            faerdig.existing = len(set(kendt_epj.values()) | set(kendt_cpr.values()))
            return faerdig

    request = IdentityRequest.of(ifdb_patient_ids=ifdb_patient_ids, cprs=cprs)
    bom_rows = _bom_rows(bom_rows_by_cpr)
    to_write: list[tuple[Person, Patient]] = []
    create_only: list[bool] = []
    unresolved_cprs: list[str] = []

    with open_register(mssql) as register:
        resolution = resolve(
            request,
            mssql,
            fhir_database,
            members=register.members if register.available else None,
        )
        result = EnsureResult(
            by_ifdb_id=dict(kendt_epj),
            by_cpr=dict(kendt_cpr),
            unresolved_ifdb_ids=resolution.unknown_ifdb_ids,
        )
        result.existing = len(set(kendt_epj.values()) | set(kendt_cpr.values()))
        _log_conflicts(context, resolution.conflicts)
        _log_superseded(context, resolution.superseded_numbers)
        result.conflicts = resolution.conflicts
        pending = [p for p in resolution.persons if not p.resource_is_complete]
        epj = epj_contributions(pending, context)
        registered = register.persons(pending)
        kept = stub_sources_on_resources(fhir_database, pending)
        for person in resolution.persons:
            if person.resource_is_complete:
                result.existing += 1
                result.record(person, cast(str, person.fhir_id))
                continue
            patient = build_person(
                person,
                context,
                epj=epj.get(person.epj_ids[0]) if person.epj_ids else None,
                stub_source=stub_source,
                bom_rows=bom_rows.get(person.current_cpr or ""),
                register=registered.get(person.key),
                kept_sources=kept.get(person.fhir_id or "", ()),
            )
            if patient is None:
                if person.fhir_id:
                    # Exists, but cannot be rebuilt from here (no row): leave it.
                    result.existing += 1
                    result.record(person, person.fhir_id)
                else:
                    unresolved_cprs.extend(person.cprs)
                continue
            if person.key in registered:
                result.from_register += 1
            if person.fhir_id:
                # A resource lacking one of the person's EPJ ids is rebuilt.
                result.rebuilt += 1
            to_write.append((person, patient))
            create_only.append(not person.rows and person.fhir_id is None)

    # Unknown numbers: neither EPJ, register nor resource — a stub or nothing.
    for cpr in resolution.unknown_cprs:
        person = Person(key=cpr, cprs=(cpr,), epj_ids=(), rows=(), fhir_id=None)
        patient = build_person(person, context, stub_source=stub_source, bom_rows=bom_rows.get(cpr))
        if patient is None:
            unresolved_cprs.append(cpr)
            continue
        to_write.append((person, patient))
        create_only.append(True)

    result.unresolved_cprs = tuple(sorted(unresolved_cprs))
    if to_write:
        written = write_patients(
            context,
            fhir_database,
            [p for _, p in to_write],
            source_name=source_name,
            create_only=create_only,
        )
        result.written = written.written
        result.existing += written.existing
        for (person, _), patient in zip(to_write, written, strict=True):
            result.record(person, patient.id)

    context.log.info(
        "ensure_patients (%s): %d existed, %d written (of which %d rebuilt, %d with the "
        "register's contribution), %d EPJ ids and %d personal numbers without a basis",
        source_name,
        result.existing,
        result.written,
        result.rebuilt,
        result.from_register,
        len(result.unresolved_ifdb_ids),
        len(result.unresolved_cprs),
    )
    return result


def write_rows(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    rows: Iterable[dict],
    *,
    source_name: str,
) -> list[Patient]:
    """Producer path: build and write the archive rows' persons with the register's contribution."""
    rows = list(rows)
    if not rows:
        return []
    with open_register(mssql) as register:
        resolution = resolve(
            IdentityRequest.of(),
            mssql,
            fhir_database,
            rows=rows,
            members=register.members if register.available else None,
        )
        _log_conflicts(context, resolution.conflicts)
        _log_superseded(context, resolution.superseded_numbers)
        epj = epj_contributions(resolution.persons, context)
        registered = register.persons(resolution.persons)
        kept = stub_sources_on_resources(fhir_database, resolution.persons)
        patients: list[Patient] = []
        for person in resolution.persons:
            patient = build_person(
                person,
                context,
                epj=epj.get(person.epj_ids[0]) if person.epj_ids else None,
                register=registered.get(person.key),
                kept_sources=kept.get(person.fhir_id or "", ()),
            )
            if patient is not None:
                patients.append(patient)
    if registered:
        context.log.info(
            "Patient: the register contributed to %d of %d persons", len(registered), len(patients)
        )
    return write_patients(context, fhir_database, patients, source_name=source_name)


def write_register_persons(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    cprs: Iterable[str],
    *,
    scope: str = SCOPE_KNOWN,
    source_name: str = "cpr_register",
) -> RegisterWriteResult:
    """Register path: build and write the persons a delivery has changed, within ``scope``.

    ``known`` builds only persons with an EPJ row or a resource; ``register`` also creates the
    others. Fails if the register is not published.
    """
    if scope not in SCOPES:
        raise ValueError(f"scope must be one of {SCOPES}")
    numbers = sorted({str(c).strip() for c in cprs if c is not None and str(c).strip()})
    result = RegisterWriteResult()
    if not numbers:
        return result
    to_write: list[Patient] = []
    create_only: list[bool] = []
    with open_register(mssql) as register:
        if not register.available:
            raise dg.Failure(description="The CPR register is not published in this environment")
        resolution = resolve(
            IdentityRequest.of(cprs=numbers), mssql, fhir_database, members=register.members
        )
        _log_conflicts(context, resolution.conflicts)
        _log_superseded(context, resolution.superseded_numbers)
        result.conflicts = resolution.conflicts
        result.missing_in_register = len(resolution.unknown_cprs)
        chosen: list[Person] = []
        for person in resolution.persons:
            known = bool(person.rows) or person.fhir_id is not None
            if scope == SCOPE_KNOWN and not known:
                result.skipped_unknown += 1
                continue
            chosen.append(person)
        epj = epj_contributions(chosen, context)
        registered = register.persons(chosen)
        kept = stub_sources_on_resources(fhir_database, chosen)
        for person in chosen:
            patient = build_person(
                person,
                context,
                epj=epj.get(person.epj_ids[0]) if person.epj_ids else None,
                register=registered.get(person.key),
                kept_sources=kept.get(person.fhir_id or "", ()),
            )
            if patient is None:
                result.untouched += 1
                continue
            if person.fhir_id is None:
                result.created += 1
            else:
                result.rebuilt += 1
            to_write.append(patient)
            create_only.append(person.fhir_id is None and not person.rows)
    if to_write:
        written = write_patients(
            context, fhir_database, to_write, source_name=source_name, create_only=create_only
        )
        result.written = written.written
        result.existing = written.existing
        result.withheld = written.withheld
        result.uninterpreted = written.uninterpreted
    context.log.info(
        "write_register_persons (%s, scope=%s): %d written (%d rebuilt, %d created), "
        "%d outside the person scope, %d not in the register",
        source_name,
        scope,
        result.written,
        result.rebuilt,
        result.created,
        result.skipped_unknown,
        result.missing_in_register,
    )
    return result


@aggregate_errors
def mark_deleted(context, fhir_database, fhir_ids, *, missing_epj_ids):
    """Mark only persons with no surviving EPJ alias and no register support.

    Recheck identifiers while holding the same transaction lock as writes.
    A disappearing historical EPJ row must not delete the surviving person.
    """
    updated, skipped = mark_resources_deleted(
        fhir_database,
        resource_type="Patient",
        row_ids=fhir_ids,
        missing_identifiers={(SYSTEM_IFDB_PATIENT_ID, str(i)) for i in missing_epj_ids},
        protected_sources=((SYSTEM_SOURCE_TAG, SOURCE_CPR),),
    )
    context.log.info("Patient deletion: %d marked, %d retained", updated, skipped)
    return updated, skipped


__all__ = [
    "SCOPE_KNOWN",
    "SCOPE_REGISTER",
    "EnsureResult",
    "RegisterWriteResult",
    "build_person",
    "ensure_patients",
    "mark_deleted",
    "epj_contributions",
    "write_patients",
    "write_register_persons",
    "write_rows",
]
