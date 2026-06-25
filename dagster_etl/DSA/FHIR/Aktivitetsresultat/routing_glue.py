# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Routing helpers for AKTIVITETSRESULTAT."""

from typing import Any

from core.resources import MSSQLResource
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import (
    _SFI_MODE,
    SFI_MODE_LEGACY,
)
from DSA.FHIR.utils.ifdb.routing import RoutingResolver, build_routing_resolver
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow


def _get_routing_resolver(mssql_target_staging: MSSQLResource | None = None) -> RoutingResolver:
    """Return a RoutingResolver.

    The MSSQL routing table is the single operational source of truth. When a
    connection is provided the resolver loads from it and raises
    ``RoutingTableMissingError`` if the table is absent — there is no silent
    YAML fallback at runtime. The YAML seed is only used for the offline path
    (no connection), i.e. workstation/test runs.
    """
    if mssql_target_staging is None:
        return build_routing_resolver()

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            return build_routing_resolver(cursor=cursor)
        finally:
            cursor.close()


def _sfi_mode_enabled() -> bool:
    return _SFI_MODE != SFI_MODE_LEGACY


def _audit_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _has_value_bearing_xml_rows(
    xml_rows: list[AktivitetsresultatRow] | None,
) -> bool:
    """True when XML contains SFI rows that can become QR answers/Observations."""
    if not xml_rows:
        return False

    for xml_row in xml_rows:
        if xml_row.xml_status == -1:
            continue
        if xml_row.raekke_type == "Element":
            if _audit_text(xml_row.element_vaerdi) is not None:
                return True
        elif xml_row.raekke_type == "Selektion":
            if _audit_text(xml_row.klynge_valg_ledetekst) is not None:
                return True
    return False
