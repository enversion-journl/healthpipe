# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""One SOR-keyed Location from the BOM_IFDB and IFDB contributions, merged by the table."""

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

import dagster as dg
from DSA.FHIR.Location.location_ownership import (
    LOCATION_OWNERSHIP,
    SOURCE_BOM_IFDB,
    SOURCE_IFDB,
)
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.field_ownership import Contribution, apply_provenance, assemble
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id, parse_clob_datetime
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import EXTENSION_LOCATION_VALIDITY_PERIOD
from utils.fhir.identifier_systems import SYSTEM_BOM_IFDB_ORG_TYPE, SYSTEM_SOR


def _managing_organization(sor_code: str) -> Reference:
    return Reference(type="Organization", identifier=Identifier(system=SYSTEM_SOR, value=sor_code))


def _rank(row: Mapping[str, Any]) -> tuple:
    return (not row.get("dw_is_deleted"), row.get("dw_timestamp_update"), str(row.get("OID")))


def current_sor_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    """One SOR archive row per SOR code: a live row before a deleted one, then the newest."""
    current: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        code = normalize_oracle_id(row.get("SORKODE"))
        if not code:
            continue
        held = current.get(code)
        if held is None or _rank(row) > _rank(held):
            current[code] = row
    return current


def sor_contribution(row: Mapping[str, Any], parent_sor_map: Mapping[str, str]) -> Contribution:
    """The SOR archive row (ORGENHED+SORENHED) as SOR's Location contribution."""
    sor_value = str(normalize_oracle_id(row.get("SORKODE")))

    aktiv = row.get("AKTIV")
    try:
        aktiv_int = int(aktiv) if aktiv is not None else 0
    except (TypeError, ValueError):
        dg.get_dagster_logger().warning(
            f"SOR code {sor_value}: unparseable AKTIV {aktiv!r}; status set to inactive"
        )
        aktiv_int = 0
    status = "active" if aktiv_int == 1 else "inactive"

    org_type = row.get("ORG_TYPE")
    location_type = None
    if org_type:
        location_type = [
            CodeableConcept(
                coding=[Coding(system=SYSTEM_BOM_IFDB_ORG_TYPE, code=str(org_type).strip())]
            )
        ]

    part_of_ref = None
    parent_oid = normalize_oracle_id(row.get("PARENT_ORGENHEDOID"))
    if parent_oid and parent_oid in parent_sor_map:
        part_of_ref = Reference(
            type="Location",
            identifier=Identifier(system=SYSTEM_SOR, value=parent_sor_map[parent_oid]),
        )

    period_start = None
    period_end = None
    if row.get("GYLDIG_FRA_DATOTID"):
        parsed = parse_clob_datetime(row["GYLDIG_FRA_DATOTID"]) or row["GYLDIG_FRA_DATOTID"]
        if parsed:
            period_start = fhir_datetime(parsed)
    if row.get("GYLDIG_TIL_DATOTID"):
        parsed = parse_clob_datetime(row["GYLDIG_TIL_DATOTID"]) or row["GYLDIG_TIL_DATOTID"]
        if parsed:
            period_end = fhir_datetime(parsed)

    # R4B Location has no native validity-period field; carry it on an extension.
    extensions: list[Extension] = []
    if period_start or period_end:
        extensions.append(
            Extension(
                url=EXTENSION_LOCATION_VALIDITY_PERIOD,
                valuePeriod=Period(start=period_start, end=period_end),
            )
        )

    return Contribution(
        source=SOURCE_BOM_IFDB,
        resource=Location(
            identifier=[Identifier(system=SYSTEM_SOR, value=sor_value)],
            status=status,
            type=location_type,
            managingOrganization=_managing_organization(sor_value),
            partOf=part_of_ref,
            extension=extensions or None,
        ),
    )


def ifdb_contribution(sor_code: str) -> Contribution:
    """IFDB's contact location: only the SOR code, its organisation and status active."""
    return Contribution(
        source=SOURCE_IFDB,
        resource=Location(
            identifier=[Identifier(system=SYSTEM_SOR, value=sor_code)],
            managingOrganization=_managing_organization(sor_code),
            status="active",
        ),
    )


def build(contributions: Sequence[Contribution]) -> Location:
    """Assemble and apply provenance. What is to be written — still without id."""
    location = assemble(LOCATION_OWNERSHIP, contributions)
    return apply_provenance(LOCATION_OWNERSHIP, location, [c.source for c in contributions])


__all__ = ["build", "current_sor_rows", "ifdb_contribution", "sor_contribution"]
