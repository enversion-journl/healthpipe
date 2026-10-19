# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Identity resolution for FHIR Patient: from identities to persons.

A. EPJ ids with the same personal number in ``arc.ifdb_patient`` are one person.
B. A historical personal number is led to the current one through ``ref.PatientIdentity``.
C. A number the CPR register knows is a person, even without an EPJ row.
D. Existing resources are found on all the person's identifiers. A person pointing to several
   resources is split per resource and counted; one value on two resources is a hard failure.

``resolve_identities`` is pure; ``fetch_identity_inputs`` fetches what it needs.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field

import dagster as dg
from core.fhir.alias_lookup import lookup_values
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

# Node types of the identities in the linking.
EPJ = "epj"
CPR = "cpr"


class IdentityConflict(ValueError):
    """Sources in conflict: a historical number with two current, an identifier on two resources."""


@dataclass(frozen=True)
class IdentityRequest:
    """What a caller knows: EPJ patient ids and/or personal numbers."""

    ifdb_patient_ids: frozenset[str] = frozenset()
    cprs: frozenset[str] = frozenset()

    @classmethod
    def of(
        cls, *, ifdb_patient_ids: Iterable[str] = (), cprs: Iterable[str] = ()
    ) -> IdentityRequest:
        return cls(
            ifdb_patient_ids=frozenset(_norm(x) for x in ifdb_patient_ids if _norm(x)),
            cprs=frozenset(_norm(x) for x in cprs if _norm(x)),
        )


@dataclass
class IdentityInputs:
    """What the adapters have fetched. Pure data — no connections."""

    # All rows for the EPJ ids and the personal numbers, including deleted ones.
    archive_rows: list[dict] = field(default_factory=list)
    # 022: historical number -> current number.
    current_by_historical: dict[str, str] = field(default_factory=dict)
    # The FHIR server's answer: identity -> resource id.
    fhir_ids_by_epj_id: dict[str, str] = field(default_factory=dict)
    fhir_ids_by_cpr: dict[str, str] = field(default_factory=dict)
    # Rule C: the personal numbers the register knows (live row in arc.cpr_001_person).
    register_cprs: set[str] = field(default_factory=set)


@dataclass
class Person:
    """One person the builder can build: her identities, rows and resource, if any."""

    key: str  # current personal number, or "epj:<id>" for a person without one
    cprs: tuple[str, ...]  # current first, then previous numbers
    epj_ids: tuple[str, ...]  # primary first (same rule as the producer's dedup), then the rest
    rows: tuple[dict, ...]  # archive rows in the same order as epj_ids
    fhir_id: str | None
    # EPJ ids the resource already carries.
    known_epj_ids: tuple[str, ...] = ()
    # Rule C: the person's numbers are known to the register.
    in_register: bool = False

    @property
    def primary_row(self) -> dict | None:
        return self.rows[0] if self.rows else None

    @property
    def current_cpr(self) -> str | None:
        return self.cprs[0] if self.cprs else None

    @property
    def has_epj(self) -> bool:
        return bool(self.rows)

    @property
    def resource_is_complete(self) -> bool:
        """Does the resource exist, and does it carry all the person's EPJ ids?"""
        return self.fhir_id is not None and set(self.epj_ids) <= set(self.known_epj_ids)


@dataclass
class IdentityResolution:
    persons: list[Person]
    by_ifdb_id: dict[str, Person]  # all EPJ ids, including those the archive found
    by_cpr: dict[str, Person]  # all numbers, including previous ones
    unknown_ifdb_ids: tuple[str, ...]  # requested, but neither in the archive nor in FHIR
    unknown_cprs: tuple[str, ...]  # requested, but not in the archive, in 022 or in FHIR
    # Persons with more than one resource, split per resource.
    conflicts: int = 0
    # Numbers unknown to the register that gave way to the person's registered number.
    superseded_numbers: int = 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _norm(value: object) -> str:
    return str(value).strip() if value is not None else ""


def row_patient_id(row: dict) -> str:
    return _norm(row.get("PATIENT_ID") or row.get("patient_id"))


def row_cpr(row: dict) -> str:
    return _norm(row.get("CPR") or row.get("cpr"))


def row_is_deleted(row: dict) -> bool:
    """SLETTET is 0 or missing on a live row; anything else, also empty text, is deleted."""
    slettet = row["SLETTET"] if "SLETTET" in row else row.get("slettet")
    if slettet is None:
        return False
    return str(slettet).strip() not in ("0", "0.0")


def order_rows(rows: Iterable[dict]) -> list[dict]:
    """Primary row first, as in the producer's SQL: live, latest updated, lowest patient id.

    A row without OPDATERET_TID comes last among its equals.
    """
    by_id = sorted(rows, key=row_patient_id)
    by_time = sorted(
        by_id, key=lambda r: _norm(r.get("OPDATERET_TID") or r.get("opdateret_tid")), reverse=True
    )
    return sorted(by_time, key=row_is_deleted)


def add_history(history: dict[str, str], historical: object, current: object) -> None:
    """One more 022 relation. A historical number with two current ones is irreconcilable."""
    h, c = _norm(historical), _norm(current)
    if not h or not c or h == c:
        return
    previous = history.get(h)
    if previous is not None and previous != c:
        raise IdentityConflict(
            "one historical personal number points to two different current numbers"
        )
    history[h] = c


def canonical_history(history: dict[str, str]) -> dict[str, str]:
    """Follow the chains to the end (H → C → D gives H → D); a cycle is a hard failure."""
    result: dict[str, str] = {}
    for start in history:
        seen: set[str] = set()
        current = start
        while current in history:
            if current in seen:
                raise IdentityConflict(
                    f"the personal number history contains a cycle of {len(seen)} numbers"
                )
            seen.add(current)
            current = history[current]
        result[start] = current
    return result


class _Union:
    """Union-find over identity nodes: (EPJ, id) and (CPR, number)."""

    def __init__(self) -> None:
        self._parent: dict[tuple[str, str], tuple[str, str]] = {}

    def add(self, node: tuple[str, str]) -> None:
        self._parent.setdefault(node, node)

    def find(self, node: tuple[str, str]) -> tuple[str, str]:
        self.add(node)
        while self._parent[node] != node:
            self._parent[node] = self._parent[self._parent[node]]
            node = self._parent[node]
        return node

    def union(self, a: tuple[str, str], b: tuple[str, str]) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra

    def groups(self) -> list[list[tuple[str, str]]]:
        by_root: dict[tuple[str, str], list[tuple[str, str]]] = {}
        for node in list(self._parent):
            by_root.setdefault(self.find(node), []).append(node)
        return list(by_root.values())


# ---------------------------------------------------------------------------
# The core: pure function
# ---------------------------------------------------------------------------


def resolve_identities(request: IdentityRequest, inputs: IdentityInputs) -> IdentityResolution:
    """Join identities into persons by rules A, B and D."""
    graf = _Union()
    rows_by_epj: dict[str, dict] = {}

    for epj_id in request.ifdb_patient_ids:
        graf.add((EPJ, epj_id))
    for cpr in request.cprs:
        graf.add((CPR, cpr))

    # A: archive rows link EPJ id and personal number.
    for row in inputs.archive_rows:
        epj_id = row_patient_id(row)
        if not epj_id:
            continue
        rows_by_epj[epj_id] = row
        graf.add((EPJ, epj_id))
        cpr = row_cpr(row)
        if cpr:
            graf.union((EPJ, epj_id), (CPR, cpr))

    # B: previous numbers link to the current one — through the whole chain.
    history = canonical_history(inputs.current_by_historical)
    for h, c in history.items():
        graf.union((CPR, h), (CPR, c))

    # D: one resource with several identities links them.
    nodes_by_fhir: dict[str, list[tuple[str, str]]] = {}
    for epj_id, fhir_id in inputs.fhir_ids_by_epj_id.items():
        nodes_by_fhir.setdefault(fhir_id, []).append((EPJ, _norm(epj_id)))
    for cpr, fhir_id in inputs.fhir_ids_by_cpr.items():
        nodes_by_fhir.setdefault(fhir_id, []).append((CPR, _norm(cpr)))
    for nodes in nodes_by_fhir.values():
        for node in nodes[1:]:
            graf.union(nodes[0], node)

    persons: list[Person] = []
    by_ifdb_id: dict[str, Person] = {}
    by_cpr: dict[str, Person] = {}
    unknown_epj: list[str] = []
    unknown_cpr: list[str] = []
    conflicts = 0
    superseded = 0

    def fhir_of(node: tuple[str, str]) -> str | None:
        return (inputs.fhir_ids_by_epj_id if node[0] == EPJ else inputs.fhir_ids_by_cpr).get(
            node[1]
        )

    for group in graf.groups():
        parts = _split_by_resource(group, fhir_of, rows_by_epj, history)
        if len(parts) > 1:
            conflicts += 1
        for nodes in parts:
            epj_ids = sorted(n[1] for n in nodes if n[0] == EPJ)
            cprs = sorted(n[1] for n in nodes if n[0] == CPR)
            rows = order_rows(rows_by_epj[e] for e in epj_ids if e in rows_by_epj)
            ordered_epj = [row_patient_id(r) for r in rows] + [
                e for e in epj_ids if e not in rows_by_epj
            ]
            fhir_ids = {fid for fid in (fhir_of(n) for n in nodes) if fid}

            currents = [c for c in cprs if c not in history]
            if len(currents) > 1:
                registered = [c for c in currents if c in inputs.register_cprs]
                if len(registered) != 1:
                    raise IdentityConflict(
                        f"one person, several current personal numbers ({len(currents)})"
                    )
                # The register's number is the current one; a number the register
                # does not know (a replacement number) becomes a previous number.
                superseded += len(currents) - 1
                currents = registered + [c for c in currents if c not in registered]
            ordered_cprs = currents + [c for c in cprs if c in history]

            in_register = any(c in inputs.register_cprs for c in cprs)

            # An identity no source knows does not become a person here.
            if not rows and not fhir_ids and len(nodes) == 1 and not in_register:
                node_type, value = nodes[0]
                (unknown_epj if node_type == EPJ else unknown_cpr).append(value)
                continue

            person = Person(
                key=ordered_cprs[0] if ordered_cprs else f"epj:{ordered_epj[0]}",
                cprs=tuple(ordered_cprs),
                epj_ids=tuple(ordered_epj),
                rows=tuple(rows),
                fhir_id=next(iter(fhir_ids)) if fhir_ids else None,
                known_epj_ids=tuple(e for e in ordered_epj if inputs.fhir_ids_by_epj_id.get(e)),
                in_register=in_register,
            )
            persons.append(person)
            for e in ordered_epj:
                by_ifdb_id[e] = person
            for c in ordered_cprs:
                by_cpr[c] = person

    persons.sort(key=lambda p: p.key)
    return IdentityResolution(
        persons=persons,
        by_ifdb_id=by_ifdb_id,
        by_cpr=by_cpr,
        unknown_ifdb_ids=tuple(sorted(unknown_epj)),
        unknown_cprs=tuple(sorted(unknown_cpr)),
        conflicts=conflicts,
        superseded_numbers=superseded,
    )


def _split_by_resource(
    group: list[tuple[str, str]],
    fhir_of: Callable[[tuple[str, str]], str | None],
    rows_by_epj: dict[str, dict],
    history: dict[str, str],
) -> list[list[tuple[str, str]]]:
    """One group -> one list per resource when the group points to several resources.

    Identities without a resource follow their nearest: an EPJ row its number, a historical
    number the current one.
    """
    by_fhir: dict[str, list[tuple[str, str]]] = {}
    rest: list[tuple[str, str]] = []
    for node in group:
        fid = fhir_of(node)
        (by_fhir.setdefault(fid, []) if fid else rest).append(node)
    if len(by_fhir) <= 1:
        return [list(group)]

    cpr_fid = {n[1]: fhir_of(n) for n in group if n[0] == CPR and fhir_of(n)}
    epj_fid = {n[1]: fhir_of(n) for n in group if n[0] == EPJ and fhir_of(n)}
    cpr_of_rows: dict[str, str] = {}
    for e, fid in epj_fid.items():
        c = row_cpr(rows_by_epj.get(e, {}))
        if c and fid is not None:
            cpr_of_rows.setdefault(c, fid)
    current = next((n[1] for n in group if n[0] == CPR and n[1] not in history), None)
    default = cpr_fid.get(current or "") or cpr_of_rows.get(current or "") or sorted(by_fhir)[0]

    for node in rest:
        fid = None
        if node[0] == EPJ:
            c = row_cpr(rows_by_epj.get(node[1], {}))
            fid = cpr_fid.get(c) or cpr_of_rows.get(c)
        else:
            fid = cpr_of_rows.get(node[1]) or cpr_fid.get(history.get(node[1], ""))
        by_fhir[fid or default].append(node)
    return [by_fhir[k] for k in sorted(by_fhir)]


# ---------------------------------------------------------------------------
# Adapter: fetch what the rules need
# ---------------------------------------------------------------------------

ARCHIVE_TABLE = "arc.ifdb_patient"
HISTORY_TABLE = "arc.cpr_022_historisk_personnummer"
LOOKUP_CHUNK = 5_000


def _archive_columns() -> tuple[str, ...]:
    from DSI.oracle.ifdb_extract import PATIENT_COLUMNS

    return PATIENT_COLUMNS


def dedupe_rows(rows: Iterable[dict]) -> list[dict]:
    """One row per EPJ id; rows without an id are kept."""
    seen: set[str] = set()
    ud: list[dict] = []
    for row in rows:
        pid = row_patient_id(row)
        if pid and pid in seen:
            continue
        if pid:
            seen.add(pid)
        ud.append(row)
    return ud


def fetch_fhir_ids(
    fhir_database: FHIRDatabaseResource, system: str, values: Iterable[str]
) -> dict[str, str]:
    """FHIR ids per value, on any identifier in the system; a value on two resources fails."""
    wanted = sorted({_norm(v) for v in values if _norm(v)})
    if not wanted:
        return {}
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            found = lookup_values(cur, resource_type="Patient", system=system, values=wanted)
    result: dict[str, str] = {}
    for value, fhir_ids in found.items():
        if len(fhir_ids) > 1:
            raise IdentityConflict("one identifier points to two different resources")
        result[value] = next(iter(fhir_ids))
    return result


def fetch_fhir_aliases(fhir_database, resource_ids):
    """Read all domain identifiers on the found resources, not only the lookup values."""
    by_epj, by_cpr = {}, {}
    ids = sorted(set(resource_ids))
    if not ids:
        return by_epj, by_cpr
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cursor:
            for start in range(0, len(ids), LOOKUP_CHUNK):
                cursor.execute(
                    "SELECT id, resource->'identifier' FROM patient WHERE id = ANY(%s)",
                    (ids[start : start + LOOKUP_CHUNK],),
                )
                for fid, identifiers in cursor.fetchall():
                    for identifier in identifiers or []:
                        system, value = identifier.get("system"), _norm(identifier.get("value"))
                        if not value or system not in (SYSTEM_IFDB_PATIENT_ID, SYSTEM_CPR):
                            continue
                        target = by_epj if system == SYSTEM_IFDB_PATIENT_ID else by_cpr
                        _merge_fhir_ids(target, {value: str(fid)})
    return by_epj, by_cpr


def _merge_fhir_ids(target, discovered):
    for value, fid in discovered.items():
        if value in target and target[value] != fid:
            raise IdentityConflict("one identifier points to two different resources")
        target[value] = fid


def _fetch_archive(mssql: MSSQLResource, column: str, values: set[str]) -> list[dict]:
    if not values:
        return []
    return chunked_in_fetch(
        sorted(values),
        table=ARCHIVE_TABLE,
        columns=_archive_columns(),
        where_column=column,
        mssql=mssql,
        include_deleted=True,
    )


@contextmanager
def _maal(regnskab: dict[str, float], navn: str):
    """Add the lookup's duration to the tally. Seconds only — never values."""
    start = time.perf_counter()
    try:
        yield
    finally:
        regnskab[navn] = regnskab.get(navn, 0.0) + (time.perf_counter() - start)


def _log_opslag(regnskab: dict[str, float], runder: int, personer: int) -> None:
    """Where the time went in the resolution."""
    if not regnskab:
        return
    dg.get_dagster_logger().info(
        "Patient identity: %d numbers in %d round(s) — %s (%.1f s in total)",
        personer,
        runder,
        ", ".join(f"{navn} {sek:.1f} s" for navn, sek in sorted(regnskab.items())),
        sum(regnskab.values()),
    )


# ---------------------------------------------------------------------------
# The union, looked up instead of computed
# ---------------------------------------------------------------------------

IDENTITY_TABLE = "[ref].[PatientIdentity]"


def fetch_union(mssql: MSSQLResource, epj_ids: Iterable[str], cprs: Iterable[str]):
    """The identities of the found persons, looked up in ``ref.PatientIdentity``.

    Returns (history, epj_ids, cprs): previous number -> current, and the identities of the
    found persons.
    """
    par = [(SYSTEM_IFDB_PATIENT_ID, _norm(v)) for v in epj_ids if _norm(v)]
    par += [(SYSTEM_CPR, _norm(v)) for v in cprs if _norm(v)]
    if not par:
        return {}, set(), set()
    fundne_epj: set[str] = set()
    fundne_cpr: set[str] = set()
    gaeldende: dict[str, str] = {}
    tidligere: dict[str, list[str]] = {}
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Without the table an empty answer would look like "nobody belongs together".
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = 'ref' AND TABLE_NAME = 'PatientIdentity'"
            )
            if not cursor.fetchone()[0]:
                raise IdentityConflict(
                    "ref.PatientIdentity is missing; run ref_patient_identity, or ask "
                    "explicitly for the old path (union=False)"
                )
            cursor.execute(
                "CREATE TABLE #Identiteter (identity_system NVARCHAR(128) NOT NULL, "
                "identity_value NVARCHAR(64) NOT NULL, "
                "PRIMARY KEY (identity_system, identity_value))"
            )
            for start in range(0, len(par), LOOKUP_CHUNK):
                cursor.executemany(
                    "INSERT INTO #Identiteter VALUES (?, ?)", par[start : start + LOOKUP_CHUNK]
                )
            # One join over a temporary table.
            cursor.execute(
                f"SELECT alle.identity_system, alle.identity_value, alle.person_key, "
                f"alle.is_current FROM {IDENTITY_TABLE} alle "
                f"WHERE alle.person_key IN ("
                f"  SELECT i.person_key FROM {IDENTITY_TABLE} i "
                f"  JOIN #Identiteter t ON t.identity_system = i.identity_system "
                f"   AND t.identity_value = i.identity_value)"
            )
            for system, vaerdi, person, er_gaeldende in cursor.fetchall():
                vaerdi = _norm(vaerdi)
                person = _norm(person)
                if system == SYSTEM_IFDB_PATIENT_ID:
                    fundne_epj.add(vaerdi)
                    continue
                fundne_cpr.add(vaerdi)
                if er_gaeldende:
                    gaeldende[person] = vaerdi
                else:
                    tidligere.setdefault(person, []).append(vaerdi)
        finally:
            cursor.close()
    historik = {
        gammelt: gaeldende[person]
        for person, numre in tidligere.items()
        if person in gaeldende
        for gammelt in numre
    }
    return historik, fundne_epj, fundne_cpr


def fetch_identity_inputs(
    request: IdentityRequest,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    rows: Iterable[dict] = (),
    members: Callable[[set[str]], set[str]] | None = None,
    union: bool = True,
) -> IdentityInputs:
    """Fetch the union, the archive rows and the server's ids for the rules.

    ``rows`` are rows the caller already has. ``members`` answers rule C and is given by the
    caller, which holds the register's publication lock.
    """
    rows_by_id: dict[str, dict] = {row_patient_id(r): r for r in rows if row_patient_id(r)}
    epj_ids: set[str] = set(request.ifdb_patient_ids) | set(rows_by_id)
    cprs: set[str] = set(request.cprs) | {row_cpr(r) for r in rows_by_id.values() if row_cpr(r)}
    history: dict[str, str] = {}
    fhir_by_epj: dict[str, str] = {}
    fhir_by_cpr: dict[str, str] = {}
    regnskab: dict[str, float] = {}
    runder = 0

    if union:
        with _maal(regnskab, "forening"):
            history, flere_epj, flere_cpr = fetch_union(mssql, epj_ids, cprs)
        epj_ids |= flere_epj
        cprs |= flere_cpr
        # The rows for the person's other patient ids, except the caller's own.
        mangler = epj_ids - set(rows_by_id)
        if mangler:
            with _maal(regnskab, "arkiv"):
                for r in _fetch_archive(mssql, "PATIENT_ID", mangler):
                    rows_by_id.setdefault(row_patient_id(r), r)
            cprs |= {row_cpr(r) for r in rows_by_id.values() if row_cpr(r)}
        with _maal(regnskab, "server"):
            _merge_fhir_ids(
                fhir_by_epj, fetch_fhir_ids(fhir_database, SYSTEM_IFDB_PATIENT_ID, epj_ids)
            )
            _merge_fhir_ids(fhir_by_cpr, fetch_fhir_ids(fhir_database, SYSTEM_CPR, cprs))
        # Identities on the resource that the union does not know are read without a new round.
        ressourcer = set(fhir_by_epj.values()) | set(fhir_by_cpr.values())
        if ressourcer:
            with _maal(regnskab, "aliaser"):
                aliases_epj, aliases_cpr = fetch_fhir_aliases(fhir_database, ressourcer)
            _merge_fhir_ids(fhir_by_epj, aliases_epj)
            _merge_fhir_ids(fhir_by_cpr, aliases_cpr)
        _log_opslag(regnskab, 1, len(cprs))
        return IdentityInputs(
            archive_rows=list(rows_by_id.values()),
            current_by_historical=history,
            fhir_ids_by_epj_id=fhir_by_epj,
            fhir_ids_by_cpr=fhir_by_cpr,
            register_cprs=(
                set(members(set(cprs) | set(fhir_by_cpr)))
                if members is not None and (cprs or fhir_by_cpr)
                else set()
            ),
        )

    done_epj: set[str] = set(rows_by_id)  # the caller's rows are not fetched again
    done_cpr: set[str] = set()
    done_fhir_epj: set[str] = set()
    done_fhir_cpr: set[str] = set()
    expanded_resources: set[str] = set()

    while True:
        runder += 1
        new_epj = epj_ids - done_epj
        new_cpr = cprs - done_cpr
        new_fhir_epj = epj_ids - done_fhir_epj
        new_fhir_cpr = cprs - done_fhir_cpr
        if not (new_epj or new_cpr or new_fhir_epj or new_fhir_cpr):
            break
        with _maal(regnskab, "arkiv"):
            arkivraekker = _fetch_archive(mssql, "PATIENT_ID", new_epj) + _fetch_archive(
                mssql, "CPR", new_cpr
            )
        for r in arkivraekker:
            rows_by_id.setdefault(row_patient_id(r), r)
        done_epj |= new_epj
        done_cpr |= new_cpr
        if new_cpr:
            with _maal(regnskab, "historik"):
                for where in ("pnr", "pnrgaeld"):
                    for r in chunked_in_fetch(
                        sorted(new_cpr),
                        table=HISTORY_TABLE,
                        columns=("pnr", "pnrgaeld"),
                        where_column=where,
                        mssql=mssql,
                    ):
                        add_history(history, r.get("pnr"), r.get("pnrgaeld"))
        with _maal(regnskab, "server"):
            if new_fhir_epj:
                _merge_fhir_ids(
                    fhir_by_epj, fetch_fhir_ids(fhir_database, SYSTEM_IFDB_PATIENT_ID, new_fhir_epj)
                )
                done_fhir_epj |= new_fhir_epj
            if new_fhir_cpr:
                _merge_fhir_ids(
                    fhir_by_cpr, fetch_fhir_ids(fhir_database, SYSTEM_CPR, new_fhir_cpr)
                )
                done_fhir_cpr |= new_fhir_cpr
        new_resources = (set(fhir_by_epj.values()) | set(fhir_by_cpr.values())) - expanded_resources
        if new_resources:
            with _maal(regnskab, "aliaser"):
                aliases_epj, aliases_cpr = fetch_fhir_aliases(fhir_database, new_resources)
            _merge_fhir_ids(fhir_by_epj, aliases_epj)
            _merge_fhir_ids(fhir_by_cpr, aliases_cpr)
            epj_ids.update(aliases_epj)
            cprs.update(aliases_cpr)
            expanded_resources.update(new_resources)
        # What the round found must itself be looked up in the next round.
        epj_ids |= {row_patient_id(r) for r in rows_by_id.values() if row_patient_id(r)}
        cprs |= {row_cpr(r) for r in rows_by_id.values() if row_cpr(r)}
        cprs |= set(history) | set(history.values())

    _log_opslag(regnskab, runder, len(cprs))
    return IdentityInputs(
        archive_rows=dedupe_rows(rows_by_id.values()),
        current_by_historical=history,
        fhir_ids_by_epj_id=fhir_by_epj,
        fhir_ids_by_cpr=fhir_by_cpr,
        register_cprs=set(members(set(cprs))) if members is not None and cprs else set(),
    )


def resolve(
    request: IdentityRequest,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    rows: Iterable[dict] = (),
    members: Callable[[set[str]], set[str]] | None = None,
    union: bool = True,
) -> IdentityResolution:
    """Fetch and resolve in one call. ``union`` looks the union up instead of closing it."""
    return resolve_identities(
        request, fetch_identity_inputs(request, mssql, fhir_database, rows, members, union)
    )


__all__ = [
    "CPR",
    "EPJ",
    "IdentityConflict",
    "IdentityInputs",
    "IdentityRequest",
    "IdentityResolution",
    "Person",
    "add_history",
    "canonical_history",
    "dedupe_rows",
    "fetch_fhir_ids",
    "fetch_identity_inputs",
    "fetch_union",
    "order_rows",
    "resolve",
    "resolve_identities",
    "row_cpr",
    "row_is_deleted",
    "row_patient_id",
]
