# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Register contribution to the builder: CPR archive (``arc.cpr_*``) as one Patient contribution.

The contribution carries exactly the fields ``field_ownership`` gives the register. ``active``,
``deceased`` and address protection are also set when the answer is no, so a stale yes from EPJ
does not win the field. Previous numbers come from the identity resolution, not raw 022 rows.
A date the register marks as uncertain (``*_umrk``) is carried as delivered; the marker stays in
the archive.
``open_register`` fetches under the publication lock; with no register in the environment there
is no contribution.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime

from core.resources import MSSQLResource
from DSA.FHIR.Patient.field_ownership import SOURCE_CPR, USE_OFFICIAL
from DSA.FHIR.Patient.patient_builder import Contribution
from DSA.FHIR.Patient.patient_contributions import USE_OLD
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from DSI.csv_files.cpr_publication import published_cpr
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from utils.fhir.cpr_status import is_active, is_deceased, normalize_cpr_status
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_KVHX,
    EXTENSION_ADDRESS_OFFICIAL,
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_CPR_UPDATED,
    EXTENSION_DK_MUNICIPALITY_CODES,
    EXTENSION_FATHER_IDENTIFIER,
    EXTENSION_MOTHER_IDENTIFIER,
    EXTENSION_PATIENT_CITIZENSHIP,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_CPR_LANDEKODE,
    SYSTEM_CPR_PERSON_ID,
    SYSTEM_DK_MARITAL_STATUS,
    SYSTEM_DK_MUNICIPALITY_CODES,
    SYSTEM_V3_MARITAL_STATUS,
)

# Type 0001 is name and address protection; the other protection types are not.
NAME_ADDRESS_PROTECTION = "0001"

# Correction codes on 023 where the row does not apply: K is corrected, A is withdrawn.
UGYLDIGE_ADRESSEKODER = frozenset({"K", "A"})

# CPR's marital status (record type 012) per dk-core: HL7 v3 plus dk-core's P and O.
# D is not a marital status; the death is in Patient.deceased.
CIVILSTAND: dict[str, tuple[str, str, str, str]] = {
    "U": (SYSTEM_V3_MARITAL_STATUS, "U", "unmarried", "ugift"),
    "G": (SYSTEM_V3_MARITAL_STATUS, "M", "Married", "gift"),
    "F": (SYSTEM_V3_MARITAL_STATUS, "D", "Divorced", "fraskilt"),
    "E": (SYSTEM_V3_MARITAL_STATUS, "W", "Widowed", "enke eller enkemand"),
    "P": (SYSTEM_DK_MARITAL_STATUS, "P", "Registreret partnerskab", "registreret partnerskab"),
    "O": (SYSTEM_DK_MARITAL_STATUS, "O", "Ophævet partnerskab", "ophævet partnerskab"),
    "L": (SYSTEM_V3_MARITAL_STATUS, "W", "Widowed", "længstlevende partner"),
}

# The column names are CPR's own field names in lower case.
REGISTER_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "person": ("arc.cpr_001_person", ("pnr", "leverance_dato", "status", "koen", "foed_dt")),
    "address": (
        "arc.cpr_002_adresse",
        (
            "pnr",
            "leverance_dato",
            "komkod",
            "husnr",
            "etage",
            "sidedoer",
            "tilflydto",
            "vejnvn",
            "adresse_uuid",
        ),
    ),
    "plain_address": (
        "arc.cpr_003_klarskriftadresse",
        ("pnr", "leverance_dato", "adrnvn", "standardadr", "bynavn", "postnr", "postdisttxt"),
    ),
    "protections": (
        "arc.cpr_004_beskyttelse",
        ("pnr", "leverance_dato", "beskyttype", "start_dt_beskyttelse", "slet_dt_beskyttelse"),
    ),
    "name": (
        "arc.cpr_008_navn",
        ("pnr", "leverance_dato", "fornvn", "melnvn", "efternvn", "adrnvn"),
    ),
    "citizenship": ("arc.cpr_010_statsborgerskab", ("pnr", "leverance_dato", "landekode")),
    "marital": ("arc.cpr_012_civilstand", ("pnr", "leverance_dato", "civst")),
    "parents": ("arc.cpr_015_foraeldre", ("pnr", "leverance_dato", "pnrmor", "pnrfar")),
    "history_addresses": (
        "arc.cpr_023_historisk_adresse",
        ("pnr", "leverance_dato", "annkor", "komkod", "tilflydto", "fraflydto", "adresse_uuid"),
    ),
    "person_id": ("arc.cpr_037_personid", ("pnr", "leverance_dato", "person_id")),
}
PERSON_TABLE = REGISTER_TABLES["person"][0]
_MULTI_ROW = ("protections", "history_addresses")


@dataclass(frozen=True)
class RegisterPerson:
    """What the register knows about one person: the person record and the other record types."""

    person: dict
    address: dict | None = None
    plain_address: dict | None = None
    name: dict | None = None
    citizenship: dict | None = None
    marital: dict | None = None
    parents: dict | None = None
    person_id: dict | None = None
    protections: tuple[dict, ...] = ()
    history_addresses: tuple[dict, ...] = ()


@dataclass
class RegisterRows:
    """Raw rows per table, keyed on personal number — one batch at a time."""

    single: dict[str, dict[str, dict]] = field(default_factory=dict)
    multi: dict[str, dict[str, list[dict]]] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Field helpers: the register's text fields to FHIR's types
# ---------------------------------------------------------------------------


# Values the register sent that could not be interpreted — count per field only.
_UFORTOLKEDE: Counter = Counter()


def ufortolkede_vaerdier() -> dict[str, int]:
    """Number of uninterpreted values per field since the last call; the counter is reset."""
    tal = dict(_UFORTOLKEDE)
    _UFORTOLKEDE.clear()
    return tal


def _text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _date(value: object) -> str | None:
    """'YYYY-MM-DD' from 'YYYY-MM-DD' or 'YYYYMMDDHHMM'; zero dates and invalid dates give None."""
    text = _text(value)
    if text is None:
        return None
    digits = text.replace("-", "")[:8]
    if digits == "00000000" or set(text) <= {"0", "-"}:
        return None
    try:
        if len(digits) != 8 or not digits.isdigit():
            raise ValueError(text)
        return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError:
        _UFORTOLKEDE["dato"] += 1
        return None


def _cpr(value: object) -> str | None:
    """A personal number that is filled in: ten digits, not zero."""
    text = _text(value)
    if text is None or len(text) != 10 or not text.isdigit() or set(text) == {"0"}:
        return None
    return text


def _code(value: object) -> str | None:
    """A code that is filled in: not empty and not only zeros."""
    text = _text(value)
    if text is None or set(text) == {"0"}:
        return None
    return text


def _words(value: object) -> list[str]:
    return [w for w in (_text(value) or "").split(" ") if w]


def _address_extensions(
    komkod: object, adresse_uuid: object, *, official: bool
) -> list[Extension] | None:
    extensions: list[Extension] = []
    if official:
        extensions.append(Extension(url=EXTENSION_ADDRESS_OFFICIAL, valueBoolean=True))
    code = _code(komkod)
    if code:
        extensions.append(
            Extension(
                url=EXTENSION_DK_MUNICIPALITY_CODES,
                valueCodeableConcept=CodeableConcept(
                    coding=[Coding(system=SYSTEM_DK_MUNICIPALITY_CODES, code=code)]
                ),
            )
        )
    uuid = _text(adresse_uuid)
    if uuid:
        extensions.append(Extension(url=EXTENSION_ADDRESS_KVHX, valueString=uuid))
    return extensions or None


# ---------------------------------------------------------------------------
# The fields, one at a time
# ---------------------------------------------------------------------------


def _identifiers(
    reg: RegisterPerson, current_cpr: str, historical_cprs: Iterable[str]
) -> list[Identifier]:
    """Current number first, previous numbers as ``use='old'``, the register's PersonID last."""
    identifiers = [Identifier(system=SYSTEM_CPR, value=current_cpr)]
    seen = {current_cpr}
    for number in historical_cprs:
        number = str(number).strip()
        if number and number not in seen:
            seen.add(number)
            identifiers.append(Identifier(system=SYSTEM_CPR, value=number, use=USE_OLD))
    person_id = _text((reg.person_id or {}).get("person_id"))
    if person_id:
        identifiers.append(Identifier(system=SYSTEM_CPR_PERSON_ID, value=person_id))
    return identifiers


def _name(reg: RegisterPerson) -> HumanName | None:
    """Name as ``use='official'`` for the projection: given, family, addressing name as text."""
    row = reg.name or {}
    given = _words(row.get("fornvn")) + _words(row.get("melnvn"))
    family = _text(row.get("efternvn"))
    if not given and not family:
        return None
    text = _text(row.get("adrnvn")) or _text((reg.plain_address or {}).get("adrnvn"))
    return HumanName(use=USE_OFFICIAL, given=given or None, family=family, text=text)


def _gender(reg: RegisterPerson) -> str | None:
    """The register's gender: M and K. Nothing else is asserted."""
    koen = (_text(reg.person.get("koen")) or "").upper()
    if koen and koen not in ("M", "K"):
        _UFORTOLKEDE["koen"] += 1
    return {"M": "male", "K": "female"}.get(koen)


def _address_line(reg: RegisterPerson) -> str | None:
    """Plain-text address (003), otherwise street name, house number, floor and side from 002."""
    standard = _text((reg.plain_address or {}).get("standardadr"))
    if standard:
        return standard
    row = reg.address or {}
    husnr = (_text(row.get("husnr")) or "").lstrip("0")
    parts = [_text(row.get("vejnvn")), husnr, _text(row.get("etage")), _text(row.get("sidedoer"))]
    line = " ".join(p for p in parts if p)
    return line or None


def _current_address(reg: RegisterPerson) -> Address | None:
    row = reg.address
    if not row:
        return None
    plain = reg.plain_address or {}
    line = _address_line(reg)
    postal = _code(plain.get("postnr"))
    city = _text(plain.get("bynavn")) or _text(plain.get("postdisttxt"))
    start = _date(row.get("tilflydto"))
    extensions = _address_extensions(row.get("komkod"), row.get("adresse_uuid"), official=True)
    if not any([line, postal, city, start, extensions]):
        return None
    return Address(
        use="home",
        line=[line] if line else None,
        postalCode=postal,
        city=city,
        country="DK",
        period=Period(start=start) if start else None,
        extension=extensions,
    )


def _historical_addresses(reg: RegisterPerson, current_start: str | None) -> list[Address]:
    """One address period per start from 023, without corrected (K) and withdrawn (A) rows.

    On the same start the latest move-out wins, and the current address wins over a historical one.
    """
    chosen: dict[str, tuple[str, Address]] = {}
    for row in reg.history_addresses:
        if (_text(row.get("annkor")) or "").upper() in UGYLDIGE_ADRESSEKODER:
            continue
        start = _date(row.get("tilflydto"))
        if start is None or start == current_start:
            continue
        end = _date(row.get("fraflydto"))
        if end is None:
            # A historical period without an end would overlap all later periods.
            _UFORTOLKEDE["historisk_adresse_uden_slut"] += 1
            continue
        candidate = Address(
            use="old",
            country="DK",
            period=Period(start=start, end=end),
            extension=_address_extensions(
                row.get("komkod"), row.get("adresse_uuid"), official=False
            ),
        )
        previous = chosen.get(start)
        if previous is None or (end or "9999-12-31") > (previous[0] or "9999-12-31"):
            chosen[start] = (end or "", candidate)
    return [chosen[start][1] for start in sorted(chosen)]


def _addresses(reg: RegisterPerson) -> list[Address]:
    current = _current_address(reg)
    current_start = current.period.start if current is not None and current.period else None
    addresses = _historical_addresses(reg, str(current_start) if current_start else None)
    if current is not None:
        addresses.append(current)
    return addresses


def _address_protected(reg: RegisterPerson, idag: date | None = None) -> bool:
    """Name and address protection (type 0001, record type 004) valid today: start to deletion."""
    i_dag = (idag or date.today()).isoformat()
    return any(
        _text(row.get("beskyttype")) == NAME_ADDRESS_PROTECTION
        and (_date(row.get("start_dt_beskyttelse")) or i_dag) <= i_dag
        and (_date(row.get("slet_dt_beskyttelse")) or "9999-12-31") > i_dag
        for row in reg.protections
    )


def _marital_status(reg: RegisterPerson) -> CodeableConcept | None:
    code = (_text((reg.marital or {}).get("civst")) or "").upper()
    mapped = CIVILSTAND.get(code)
    if mapped is None:
        if code and code != "D":
            _UFORTOLKEDE["civilstand"] += 1
        return None
    system, fhir_code, display, betegnelse = mapped
    return CodeableConcept(
        coding=[Coding(system=system, code=fhir_code, display=display)],
        text=betegnelse,
    )


def _cpr_updated(reg: RegisterPerson) -> Extension | None:
    """The latest delivery date on the rows the contribution is built from."""
    rows = [
        reg.person,
        reg.address,
        reg.plain_address,
        reg.name,
        reg.citizenship,
        reg.marital,
        reg.parents,
        reg.person_id,
        *reg.protections,
        *reg.history_addresses,
    ]
    dates = [d for d in (_date(r.get("leverance_dato")) for r in rows if r) if d]
    return Extension(url=EXTENSION_CPR_UPDATED, valueDate=max(dates)) if dates else None


def _citizenship(reg: RegisterPerson) -> Extension | None:
    code = _code((reg.citizenship or {}).get("landekode"))
    if code is None:
        return None
    return Extension(
        url=EXTENSION_PATIENT_CITIZENSHIP,
        extension=[
            Extension(
                url="code",
                valueCodeableConcept=CodeableConcept(
                    coding=[Coding(system=SYSTEM_CPR_LANDEKODE, code=code)]
                ),
            )
        ],
    )


def _parent(url: str, value: object) -> Extension | None:
    number = _cpr(value)
    if number is None:
        return None
    return Extension(url=url, valueIdentifier=Identifier(system=SYSTEM_CPR, value=number))


# ---------------------------------------------------------------------------
# The contribution
# ---------------------------------------------------------------------------


def contribution_from_register(
    reg: RegisterPerson, *, current_cpr: str, historical_cprs: Iterable[str] = ()
) -> Contribution:
    """The register's contribution for one person. Only the register's fields, always its answer."""
    status = reg.person.get("status")
    if _text(status) and normalize_cpr_status(status) is None:
        _UFORTOLKEDE["status"] += 1
    parents = reg.parents or {}
    extensions = [
        Extension(url=EXTENSION_ADDRESS_PROTECTION, valueBoolean=_address_protected(reg)),
        _citizenship(reg),
        _parent(EXTENSION_MOTHER_IDENTIFIER, parents.get("pnrmor")),
        _parent(EXTENSION_FATHER_IDENTIFIER, parents.get("pnrfar")),
        _cpr_updated(reg),
    ]
    name = _name(reg)
    addresses = _addresses(reg)
    patient = Patient(
        identifier=_identifiers(reg, current_cpr, historical_cprs),
        active=is_active(status),
        deceasedBoolean=is_deceased(status),
        name=[name] if name else None,
        gender=_gender(reg),
        birthDate=_date(reg.person.get("foed_dt")),
        address=addresses or None,
        maritalStatus=_marital_status(reg),
        extension=[e for e in extensions if e is not None],
    )
    return Contribution(kilde=SOURCE_CPR, patient=patient)


# ---------------------------------------------------------------------------
# Adapter: fetch under the publication lock
# ---------------------------------------------------------------------------


def register_members(mssql: MSSQLResource, cprs: Iterable[str]) -> set[str]:
    """The numbers the register has a live person record for (rule C)."""
    rows = chunked_in_fetch(
        cprs, table=PERSON_TABLE, columns=("pnr",), where_column="pnr", mssql=mssql
    )
    return {str(r["pnr"]).strip() for r in rows if r.get("pnr")}


def fetch_register_rows(mssql: MSSQLResource, cprs: Iterable[str]) -> RegisterRows:
    """All the register's rows for these numbers, table by table, keyed on number."""
    numbers = sorted({str(c).strip() for c in cprs if c is not None and str(c).strip()})
    rows = RegisterRows()
    if not numbers:
        return rows
    for name, (table, columns) in REGISTER_TABLES.items():
        fetched = chunked_in_fetch(
            numbers, table=table, columns=columns, where_column="pnr", mssql=mssql
        )
        if name in _MULTI_ROW:
            grouped: dict[str, list[dict]] = {}
            for row in fetched:
                grouped.setdefault(str(row["pnr"]).strip(), []).append(row)
            rows.multi[name] = grouped
        else:
            rows.single[name] = {str(row["pnr"]).strip(): row for row in fetched}
    return rows


def register_person(rows: RegisterRows, cprs: Iterable[str]) -> RegisterPerson | None:
    """The person's register view from the fetched rows; None without a person record.

    The person record is taken under the current number, otherwise the first number the register
    knows. Protections and historical addresses are gathered across all the person's numbers.
    """
    numbers = [str(c).strip() for c in cprs if c is not None and str(c).strip()]
    persons = rows.single.get("person", {})
    record_number = next((n for n in numbers if n in persons), None)
    if record_number is None:
        return None

    def single(name: str) -> dict | None:
        table = rows.single.get(name, {})
        if record_number in table:
            return table[record_number]
        return next((table[n] for n in numbers if n in table), None)

    def multi(name: str) -> tuple[dict, ...]:
        table = rows.multi.get(name, {})
        return tuple(row for n in numbers for row in table.get(n, []))

    # The address belongs to the current number only; a previous number's address is not current.
    return RegisterPerson(
        person=persons[record_number],
        address=rows.single.get("address", {}).get(record_number),
        plain_address=rows.single.get("plain_address", {}).get(record_number),
        name=single("name"),
        citizenship=single("citizenship"),
        marital=single("marital"),
        parents=single("parents"),
        person_id=single("person_id"),
        protections=multi("protections"),
        history_addresses=multi("history_addresses"),
    )


class RegisterReader:
    """One run's read window on the register. ``available`` is False without a register."""

    def __init__(self, mssql: MSSQLResource, available: bool):
        self.mssql = mssql
        self.available = available

    def members(self, cprs: set[str]) -> set[str]:
        return register_members(self.mssql, cprs) if self.available and cprs else set()

    def persons(self, persons: Iterable) -> dict[str, RegisterPerson]:
        """Register view per person key for the persons that have a number."""
        persons = [p for p in persons if p.cprs]
        if not self.available or not persons:
            return {}
        rows = fetch_register_rows(self.mssql, (c for p in persons for c in p.cprs))
        found = {}
        for person in persons:
            reg = register_person(rows, person.cprs)
            if reg is not None:
                found[person.key] = reg
        return found


@contextmanager
def open_register(mssql: MSSQLResource):
    """Hold the register's publication lock while reading. No register = no contribution."""
    with published_cpr(mssql) as available:
        yield RegisterReader(mssql, bool(available))


__all__ = [
    "CIVILSTAND",
    "NAME_ADDRESS_PROTECTION",
    "REGISTER_TABLES",
    "RegisterPerson",
    "RegisterReader",
    "RegisterRows",
    "contribution_from_register",
    "fetch_register_rows",
    "open_register",
    "register_members",
    "register_person",
]
