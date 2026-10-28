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
from utils.os2mo.codesystem_export import CodesystemExportSpec

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
# (SydAX's Aktiv and Slettet). It has no single root. Its type comes from the
# codesystem's own enhedstype property (juridisk enhed/virksomhedsenhed/
# afdeling/omkostningssted/Ukendt). The file carries no SOR mapping: a code's
# ConceptMap target is not part of this delivery — it is maintained by hand in
# OS2mo instead. Each code is composite, "<nummer>_NIVEAU_<niveau>": the same
# nummer recurring at a different level is a distinct code.
OKOSTED_EXPORT = CodesystemExportSpec(
    name="Okosted",
    asset_prefix="okosted",
    source_deps=("cs_okosted",),
    from_property="startsdate",
    to_property="enddate",
    type_property="enhedstype",
    status_active_property="aktiv",
    status_deleted_property="slettet",
)

CODESYSTEM_EXPORTS: tuple[CodesystemExportSpec, ...] = (SOR_EXPORT, OKOSTED_EXPORT)
