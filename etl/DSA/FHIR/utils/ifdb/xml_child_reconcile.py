# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""XML-child identifier families and call-site helpers.

Keeps Aidbox SQL in ``core.fhir.child_resource_reconcile``. This module only
knows IFDB identifier conventions and which parents had a usable XML parse.
"""

from __future__ import annotations

from typing import Any

from core.fhir.child_resource_reconcile import (
    ChildFamily,
    expected_child_identifiers,
    reconcile_children,
)
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import ris_xml_is_malformed
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_RIS_YDELSE,
)

FAMILY_DELOP = ChildFamily(
    resource_type="Procedure",
    identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    infix="-delop-",
)
FAMILY_SFI_OBS = ChildFamily(
    resource_type="Observation",
    identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    infix="#sfi#",
)
FAMILY_XML_CONDITION = ChildFamily(
    resource_type="Condition",
    identifier_system=SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    infix="/",
    status_kind="verificationStatus",
)
FAMILY_RIS_PROCEDURE = ChildFamily(
    resource_type="Procedure",
    identifier_system=SYSTEM_IFDB_RIS_YDELSE,
    infix=":",
)

_SFI_FAMILIES: tuple[tuple[ChildFamily, str], ...] = (
    (FAMILY_SFI_OBS, "observation_dicts"),
    (FAMILY_DELOP, "procedure_dicts"),
    (FAMILY_XML_CONDITION, "condition_dicts"),
)


def sfi_parent_oids_for_reconcile(
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None],
) -> list[str]:
    """Parents whose XML parsed or is empty; malformed and crashed parses excluded."""
    oids: list[str] = []
    for row_dict, parsed_rows in zip(rows, parsed_data_by_index, strict=True):
        oid = normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        if oid is None:
            continue
        if parsed_rows is None:
            # The parse stage skips empty payloads, so None + empty raw XML
            # means a cleared CLOB: every child must be retired.
            if _nonempty_text(row_dict.get("XML")) is None:
                oids.append(oid)
            continue
        if not parsed_rows:
            # [] only occurs when the parse worker crashed; never retire on that.
            continue
        if _aktivitetsresultat_xml_is_malformed(parsed_rows, row_dict.get("XML")):
            continue
        oids.append(oid)
    return oids


def ris_parent_oids_for_reconcile(rows: list[dict[str, Any]]) -> list[str]:
    """Every RIS row with an oid except unparseable XML."""
    oids: list[str] = []
    for row_dict in rows:
        oid = normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        if oid is None:
            continue
        if ris_xml_is_malformed(row_dict.get("XML")):
            continue
        oids.append(oid)
    return oids


def reconcile_aktivitetsresultat_xml_children(
    *,
    fhir_database: Any,
    rows: list[dict[str, Any]],
    parsed_data_by_index: list[list[AktivitetsresultatRow] | None],
    observation_dicts: list[Any],
    procedure_dicts: list[Any],
    condition_dicts: list[Any],
    build_errors: int = 0,
    logger: Any | None = None,
) -> dict[str, int]:
    """Retire SFI Observation / delop Procedure / XML-Condition orphans for the chunk."""
    if build_errors:
        # A skipped row still looks eligible but posted no children; retiring
        # against that expected-set would wipe the row's live children.
        if logger is not None:
            logger.warning(
                "XML child reconcile skipped: %d row build error(s) in chunk",
                build_errors,
            )
        return {}
    if len(rows) != len(parsed_data_by_index):
        # Reconcile kører EFTER posten, hvor en ValueError er dyrest: data er
        # skrevet, oprydningen udebliver, kørslen vælter. En længde-mismatch
        # behandles derfor som build-fejlen ovenfor — log og spring over.
        if logger is not None:
            logger.warning(
                "XML child reconcile skipped: %d rows vs %d parsed entries in chunk",
                len(rows),
                len(parsed_data_by_index),
            )
        return {}
    parent_oids = sfi_parent_oids_for_reconcile(rows, parsed_data_by_index)
    posted = {
        "observation_dicts": observation_dicts,
        "procedure_dicts": procedure_dicts,
        "condition_dicts": condition_dicts,
    }
    retired: dict[str, int] = {}
    for family, key in _SFI_FAMILIES:
        retired[family.resource_type] = reconcile_children(
            fhir_database=fhir_database,
            family=family,
            parent_oids=parent_oids,
            expected_identifiers=expected_child_identifiers(posted[key], family),
            logger=logger,
        )
    return retired


def reconcile_ris_xml_children(
    *,
    fhir_database: Any,
    rows: list[dict[str, Any]],
    procedures: list[Any],
    logger: Any | None = None,
) -> int:
    """Retire RIS Procedure services that disappeared from the current XML/STATE."""
    return reconcile_children(
        fhir_database=fhir_database,
        family=FAMILY_RIS_PROCEDURE,
        parent_oids=ris_parent_oids_for_reconcile(rows),
        expected_identifiers=expected_child_identifiers(procedures, FAMILY_RIS_PROCEDURE),
        logger=logger,
    )


def _aktivitetsresultat_xml_is_malformed(
    parsed_rows: list[AktivitetsresultatRow],
    xml_raw: Any,
) -> bool:
    """True only when non-empty XML failed to parse (xml_status == -1)."""
    if not any(row.xml_status == -1 for row in parsed_rows):
        return False
    return bool(_nonempty_text(xml_raw))


def _nonempty_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
