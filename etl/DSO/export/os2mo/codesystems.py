# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The codesystems delivered to OS2mo, one spec each.

Adding a codesystem is adding a spec here: the assets, the job selection, the
delivery sensor's run config and the export table all follow from this tuple.
"""

from DSA.CodeSystem.sor.sor import ACTIVE_OWN_PROPERTY_KEY, SOR_ROOT
from utils.os2mo.codesystem_export import CodesystemExportSpec, MappingSource

__all__ = ["CODESYSTEM_EXPORTS", "OKOSTED_EXPORT", "SOR_EXPORT"]

# Example Region's own SOR organisation: the codes cs_sor flags as
# activeOwnOrganization (open, in effect, and reachable from the RSD root
# through open units), walked from that root.
SOR_EXPORT = CodesystemExportSpec(
    name="SOR",
    asset_prefix="sor",
    source_deps=("cs_sor",),
    from_property="openingDate",
    to_property="closingDate",
    type_property="entityType",
    active_property=ACTIVE_OWN_PROPERTY_KEY,
    active_value="true",
    status_active_property=ACTIVE_OWN_PROPERTY_KEY,
    root_code=SOR_ROOT,
)

# The whole cost-centre codesystem, active or not. Its dates are always
# 1900-01-01 → 9999-12-31, so the status is in the active/deleted columns
# (SydAX's Aktiv and Slettet). It has no single root. Each code carries its SOR
# mapping from
# conceptmap.OkostedToSOR, relationship included: a code with no direct match
# falls back to its parent's SOR code as "narrower", and Magenta must be able
# to tell the two apart.
OKOSTED_EXPORT = CodesystemExportSpec(
    name="Okosted",
    asset_prefix="okosted",
    source_deps=("cs_okosted", "cm_okosted_to_sor"),
    from_property="startsdate",
    to_property="enddate",
    status_active_property="aktiv",
    status_deleted_property="slettet",
    mappings=(MappingSource(table="OkostedToSOR", system="SOR"),),
    min_map_slots=1,
)

CODESYSTEM_EXPORTS: tuple[CodesystemExportSpec, ...] = (SOR_EXPORT, OKOSTED_EXPORT)
