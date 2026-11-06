# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The codesystems delivered to OS2mo, one spec each.

Adding a codesystem is adding a spec here: the assets, the job selection, the
delivery sensor's run config and the export table all follow from this tuple.
"""

from DSA.CodeSystem.lager.lager import LAGER_ROOT
from DSA.CodeSystem.sor.sor import ACTIVE_OWN_PROPERTY_KEY, REGION_PROPERTY_KEY, SOR_ROOT
from utils.os2mo.codesystem_export import CodeSource, CodesystemExportSpec

__all__ = ["CODESYSTEM_EXPORTS", "EAN_EXPORT", "LAGER_EXPORT", "OKOSTED_EXPORT", "SOR_EXPORT"]

# Example Region's SOR tree: every code cs_sor flags as regionOrganization,
# walked from SOR_ROOT — closed and not-yet-opened units included.
SOR_EXPORT = CodesystemExportSpec(
    name="SOR",
    asset_prefix="sor",
    source_deps=("cs_sor",),
    from_property="openingDate",
    to_property="closingDate",
    type_property="entityType",
    scope_property=REGION_PROPERTY_KEY,
    scope_value="true",
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

# Example Region's own EANs: every invoice receiver (RegningsModtagerID) in
# the MySupply invoice-header archive — the same set as dim.EAN — not the whole
# national NemHandel list. The archive keeps every header, so a code never
# leaves the file. navn is the code's participantName in codesystem.EAN*
# (empty when NemHandel has none). Flat, every code a root at niveau 1, and no
# type or status flags, so those columns are empty. No dates either, so every
# code is delivered valid 1900-01-01 → 9999-12-31.
EAN_EXPORT = CodesystemExportSpec(
    name="EAN",
    asset_prefix="ean",
    source_deps=("cs_ean", "arc_mysupply_statistic_invoice_header"),
    from_property=None,
    to_property=None,
    code_source=CodeSource(
        schema="arc", table="mysupply_statistic_invoice_header", column="RegningsModtagerID"
    ),
    display_property="participantName",
)

# ILS's warehouse organisation, whole: Example Region -> sygehus (dbo.sgh) ->
# lager (dbo.lgr), so a lager is at niveau 3 and its kode is SGH_<sgh>_LGR_<lgr>.
# type is the codesystem's entityType (region/sygehus/lager). aktiv is each
# unit's own flag in ILS — dbo.sgh.aktiv or dbo.lgr.aktiv — and empty for the
# region and for any unit whose flag ILS leaves NULL. ILS has no validity dates,
# so every unit is delivered valid 1900-01-01 → 9999-12-31.
# The ILS archive does not detect deletes, so a unit deleted in ILS never leaves
# the file: it is delivered with the last aktiv the archive holds for it.
LAGER_EXPORT = CodesystemExportSpec(
    name="Lager",
    asset_prefix="lager",
    source_deps=("cs_lager",),
    from_property=None,
    to_property=None,
    type_property="entityType",
    status_active_property="aktiv",
    root_code=LAGER_ROOT,
)

CODESYSTEM_EXPORTS: tuple[CodesystemExportSpec, ...] = (
    SOR_EXPORT,
    OKOSTED_EXPORT,
    EAN_EXPORT,
    LAGER_EXPORT,
)
