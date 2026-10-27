# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
XML parser for IFDB AKTIVITETSRESULTAT.

Ported from the reference implementation's C# XML unpacking routine.
Parses the XML column from AKTIVITETSRESULTAT into two flat structures:
  - AktivitetsresultatRow: one row per Element/Selektion in the XML tree
  - AktivitetskontekstRow: one row per AKTIVITETSRESULTAT with context metadata

The XML structure is a tree of:
  Afdelingsydelsesresultat
    -> Kontekst (Oprettet, Aendret, Besluttet, Status)
    -> Element (leaf values)
    -> Klynge (cluster, type=sekvens|selektion) -> recursive children
    -> Repetition -> recursive children
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime

try:
    from lxml import etree as _lxml_etree

    _USE_LXML = True
except ImportError:
    _USE_LXML = False

_XML_END_TAG = "</Afdelingsydelsesresultat>"


def _trim_xml(xml_string: str) -> str:
    """Trim trailing garbage after the root end tag.

    Oracle CLOB fields occasionally contain trailing whitespace or partial data
    after the closing </Afdelingsydelsesresultat>. SQL Server's XML parser
    silently ignores this; Python's ElementTree does not.
    """
    idx = xml_string.find(_XML_END_TAG)
    if idx >= 0:
        return xml_string[: idx + len(_XML_END_TAG)]
    return xml_string


def _tr(value: str) -> str | None:
    """Trim whitespace and strip CR/LF. Returns None for empty results."""
    if "\n" not in value and "\r" not in value:
        trimmed = value.strip()
        return trimmed or None
    trimmed = value.replace("\n", "").replace("\r", "").strip()
    return trimmed or None


_HASH = "#"
_GOD_XML = "God XML"
_XML_FEJL = "XML Fejl"
_INGEN_ELEM = "Ingen Elementer"
_VISIT = 0
_POP_KLY = 1
_POP_REP = 2


@dataclass(slots=True)
class AktivitetskontekstRow:
    aktivitets_id: int
    opdateret_tid: datetime | None
    dw_id_audit_update: int
    dw_is_deleted: int
    oprettet_bruger: str | None = None
    oprettet_pers_id: str | None = None
    oprettet_rolle: str | None = None
    oprettet_adm_enhed: str | None = None
    oprettet_adm_enhed_id: str | None = None
    oprettet_tidspunkt: datetime | None = None
    oprettet_tidsstempel: int = 0
    aendret_bruger: str | None = None
    aendret_pers_id: str | None = None
    aendret_rolle: str | None = None
    aendret_adm_enhed: str | None = None
    aendret_adm_enhed_id: str | None = None
    aendret_tidspunkt: datetime | None = None
    aendret_tidsstempel: int = 0
    besluttet_bruger: str | None = None
    besluttet_pers_id: str | None = None
    besluttet_rolle: str | None = None
    besluttet_adm_enhed: str | None = None
    besluttet_adm_enhed_id: str | None = None
    besluttet_tidspunkt: datetime | None = None
    besluttet_tidsstempel: int = 0
    status: str | None = None


@dataclass(slots=True)
class AktivitetsresultatRow:
    aktivitets_id: int
    opdateret_tid: datetime | None
    dw_id_audit_update: int
    dw_is_deleted: int
    element_nummer: int | None = None
    klynge_id: int | None = None
    klynge_id_hierarki: str | None = None
    klynge_niveau: int = 0
    klynge_navne: str | None = None
    klynge_ledetekster: str | None = None
    klynge_parent_id: int | None = None
    repetition_id: int | None = None
    repetition_id_hierarki: str | None = None
    repetition_niveau: int = 0
    hierarki: str | None = None
    overskrift: str | None = None
    ledetekst: str | None = None
    type: str | None = None
    kladde: str | None = None
    klynge_navn: str | None = None
    klynge_ledetekst: str | None = None
    klynge_valg_ledetekst: str | None = None
    klynge_type: str | None = None
    element_navn: str | None = None
    element_ledetekst: str | None = None
    element_vaerdi: str | None = None
    element_element_id: str | None = None
    element_kode: str | None = None
    element_original_kode: str | None = None
    element_administrativ_enhed_id: str | None = None
    element_pers_id: str | None = None
    element_type: str | None = None
    element_moid: str | None = None
    raekke_type: str | None = None
    xml_status: int = 0
    xml_status_text: str | None = None


def _parse_kontekst(ctx_node: ET.Element | None, row: AktivitetskontekstRow) -> None:
    """Parse the Kontekst element (Oprettet, Aendret, Besluttet, Status) into row."""
    if ctx_node is None:
        return

    _fromisoformat = datetime.fromisoformat

    for child in ctx_node:
        ct: dict[str, str] = {}
        for gc in child:
            gt = gc.tag
            if gt not in ct and gc.text is not None:
                ct[gt] = gc.text

        tag = child.tag
        if tag == "Oprettet":
            v = ct.get("bruger")
            row.oprettet_bruger = _tr(v) if v is not None else None
            v = ct.get("PersId")
            row.oprettet_pers_id = _tr(v) if v is not None else None
            v = ct.get("Rolle")
            row.oprettet_rolle = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhed")
            row.oprettet_adm_enhed = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhedId")
            row.oprettet_adm_enhed_id = _tr(v) if v is not None else None
            ts = ct.get("Tidspunkt")
            if ts:
                try:
                    row.oprettet_tidspunkt = _fromisoformat(ts)
                except ValueError:
                    pass
            tss = ct.get("Tidsstempel")
            if tss:
                try:
                    row.oprettet_tidsstempel = int(tss)
                except ValueError:
                    pass
        elif tag == "Aendret":
            v = ct.get("bruger")
            row.aendret_bruger = _tr(v) if v is not None else None
            v = ct.get("PersId")
            row.aendret_pers_id = _tr(v) if v is not None else None
            v = ct.get("Rolle")
            row.aendret_rolle = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhed")
            row.aendret_adm_enhed = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhedId")
            row.aendret_adm_enhed_id = _tr(v) if v is not None else None
            ts = ct.get("Tidspunkt")
            if ts:
                try:
                    row.aendret_tidspunkt = _fromisoformat(ts)
                except ValueError:
                    pass
            tss = ct.get("Tidsstempel")
            if tss:
                try:
                    row.aendret_tidsstempel = int(tss)
                except ValueError:
                    pass
        elif tag == "Besluttet":
            v = ct.get("bruger")
            row.besluttet_bruger = _tr(v) if v is not None else None
            v = ct.get("PersId")
            row.besluttet_pers_id = _tr(v) if v is not None else None
            v = ct.get("Rolle")
            row.besluttet_rolle = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhed")
            row.besluttet_adm_enhed = _tr(v) if v is not None else None
            v = ct.get("AdministrativEnhedId")
            row.besluttet_adm_enhed_id = _tr(v) if v is not None else None
            ts = ct.get("Tidspunkt")
            if ts:
                try:
                    row.besluttet_tidspunkt = _fromisoformat(ts)
                except ValueError:
                    pass
            tss = ct.get("Tidsstempel")
            if tss:
                try:
                    row.besluttet_tidsstempel = int(tss)
                except ValueError:
                    pass

        status = ct.get("Status")
        if status:
            row.status = _tr(status)


def parse_aktivitetsresultat_xml(
    xml_string: str | None,
    aktivitets_id: int,
    opdateret_tid: datetime | None = None,
    dw_id_audit_update: int = 0,
    dw_is_deleted: int = 0,
) -> tuple[list[AktivitetsresultatRow], AktivitetskontekstRow]:
    """Parse the XML column from AKTIVITETSRESULTAT into flat row structures.

    This is the Python equivalent of the C# ScriptMain.Input0_ProcessInputRow()
    from the reference implementation's XML unpacking routine.

    Returns:
        Tuple of (aktivitetsresultat_rows, kontekst_row).
        aktivitetsresultat_rows may be empty if the XML is null/empty.
    """
    error_kontekst = AktivitetskontekstRow(
        aktivitets_id=aktivitets_id,
        opdateret_tid=opdateret_tid,
        dw_id_audit_update=dw_id_audit_update,
        dw_is_deleted=dw_is_deleted,
    )

    if not xml_string or not xml_string.strip():
        return [
            AktivitetsresultatRow(
                aktivitets_id=aktivitets_id,
                opdateret_tid=opdateret_tid,
                dw_id_audit_update=dw_id_audit_update,
                dw_is_deleted=dw_is_deleted,
                raekke_type=None,
                xml_status=-1,
                xml_status_text=_XML_FEJL,
            )
        ], error_kontekst

    xml_string = _trim_xml(xml_string)

    try:
        root = ET.fromstring(xml_string)
    except ET.ParseError:
        return [
            AktivitetsresultatRow(
                aktivitets_id=aktivitets_id,
                opdateret_tid=opdateret_tid,
                dw_id_audit_update=dw_id_audit_update,
                dw_is_deleted=dw_is_deleted,
                raekke_type=None,
                xml_status=-1,
                xml_status_text=_XML_FEJL,
            )
        ], error_kontekst

    kontekst_row = AktivitetskontekstRow(
        aktivitets_id=aktivitets_id,
        opdateret_tid=opdateret_tid,
        dw_id_audit_update=dw_id_audit_update,
        dw_is_deleted=dw_is_deleted,
    )

    # Extract root metadata and Kontekst node in one pass
    overskrift_raw = ledetekst_raw = type_raw = kladde_raw = None
    kontekst_node: ET.Element | None = None
    root_children = list(root)
    for rc in root_children:
        rt = rc.tag
        if rt == "Overskrift":
            if overskrift_raw is None:
                overskrift_raw = rc.text
        elif rt == "Ledetekst":
            if ledetekst_raw is None:
                ledetekst_raw = rc.text
        elif rt == "Type":
            if type_raw is None:
                type_raw = rc.text
        elif rt == "Kladde":
            if kladde_raw is None:
                kladde_raw = rc.text
        elif rt == "Kontekst":
            if kontekst_node is None:
                kontekst_node = rc

    _parse_kontekst(kontekst_node, kontekst_row)

    overskrift = _tr(overskrift_raw) if overskrift_raw is not None else None
    ledetekst_val = _tr(ledetekst_raw) if ledetekst_raw is not None else None
    type_val = _tr(type_raw) if type_raw is not None else None
    kladde_val = _tr(kladde_raw) if kladde_raw is not None else None

    # --- Iterative stack-based traversal ---
    results: list[AktivitetsresultatRow] = []
    _append = results.append

    rep_counter = 0
    kly_counter = 0
    elem_counter = 0

    for root_child in root_children:
        rtag = root_child.tag
        if (
            rtag != "Element"
            and rtag != "Klynge"
            and rtag != "Repetition"
            and rtag != "Sekvens"
            and rtag != "Selektion"
        ):
            continue

        # Per-top-level-child state (scalar vars instead of lists)
        cur_kly_id: int | None = None
        par_kly_id: int | None = None
        cur_kly_navn_tr: str | None = None
        cur_kly_ldt_tr: str | None = None
        cur_kly_type_tr: str | None = None
        kly_id_s = ""
        kly_navne_s = ""
        kly_ledetekster_s = ""
        rep_id_s = ""
        hier_s = ""
        pers_rolle: str | None = None

        stack: list = [(_VISIT, root_child, 0, 0)]

        while stack:
            item = stack.pop()
            action = item[0]

            if action == _VISIT:
                node = item[1]
                rep_depth: int = item[2]
                kly_depth: int = item[3]
                tag = node.tag

                if tag == "Element":
                    # Direct local var extraction (no dict allocation)
                    _name = _vaerdi = _ldt = _eid = _kode = None
                    _orgkode = _admeid = _persid = _moid = None
                    for gc in node:
                        txt = gc.text
                        if txt is None:
                            continue
                        gt = gc.tag
                        if gt == "Navn":
                            _name = txt
                        elif gt == "Vaerdi":
                            _vaerdi = txt
                        elif gt == "Ledetekst":
                            _ldt = txt
                        elif gt == "ElementId":
                            _eid = txt
                        elif gt == "Kode":
                            _kode = txt
                        elif gt == "Originalkode":
                            _orgkode = txt
                        elif gt == "AdministrativEnhedId":
                            _admeid = txt
                        elif gt == "PersId":
                            _persid = txt
                        elif gt == "MOID":
                            _moid = txt

                    if _name == "PersonaleRolle" and pers_rolle is None:
                        pers_rolle = _vaerdi
                        continue

                    elem_counter += 1

                    if kly_depth > 0:
                        e_kly_id = cur_kly_id
                        e_kn_tr = cur_kly_navn_tr
                        e_kl_tr = cur_kly_ldt_tr
                        e_kt_tr = cur_kly_type_tr
                    else:
                        e_kly_id = None
                        e_kn_tr = None
                        e_kl_tr = None
                        e_kt_tr = None

                    e_kly_par = par_kly_id if kly_depth > 1 else None

                    if pers_rolle is not None and _name == "Personale":
                        el_ldt: str | None = pers_rolle
                    else:
                        el_ldt = _ldt
                    pers_rolle = None

                    na = node.get("type")

                    _append(
                        AktivitetsresultatRow(
                            aktivitets_id=aktivitets_id,
                            opdateret_tid=opdateret_tid,
                            dw_id_audit_update=dw_id_audit_update,
                            dw_is_deleted=dw_is_deleted,
                            element_nummer=elem_counter,
                            klynge_id=e_kly_id,
                            klynge_id_hierarki=kly_id_s or None,
                            klynge_niveau=kly_depth,
                            klynge_navne=kly_navne_s or None,
                            klynge_ledetekster=kly_ledetekster_s or None,
                            klynge_parent_id=e_kly_par,
                            repetition_id=rep_counter if rep_depth > 0 else None,
                            repetition_id_hierarki=rep_id_s or None,
                            repetition_niveau=rep_depth,
                            hierarki=hier_s or None,
                            overskrift=overskrift,
                            ledetekst=ledetekst_val,
                            type=type_val,
                            kladde=kladde_val,
                            klynge_navn=e_kn_tr,
                            klynge_ledetekst=e_kl_tr,
                            klynge_valg_ledetekst=None,
                            klynge_type=e_kt_tr,
                            element_navn=(_tr(_name) if _name is not None else None),
                            element_ledetekst=(_tr(el_ldt) if el_ldt is not None else None),
                            element_vaerdi=_vaerdi or "",
                            element_element_id=(_tr(_eid) if _eid is not None else None),
                            element_kode=(_tr(_kode) if _kode is not None else None),
                            element_original_kode=(_tr(_orgkode) if _orgkode is not None else None),
                            element_administrativ_enhed_id=(
                                _tr(_admeid) if _admeid is not None else None
                            ),
                            element_pers_id=(_tr(_persid) if _persid is not None else None),
                            element_type=_tr(na) if na is not None else None,
                            element_moid=(_tr(_moid) if _moid is not None else None),
                            raekke_type="Element",
                            xml_status=1,
                            xml_status_text=_GOD_XML,
                        )
                    )

                elif tag == "Klynge":
                    node_type = node.get("type")
                    kly_counter += 1
                    kly_depth_new = kly_depth + 1

                    # Extract Klynge metadata (dict only for Klynge, not Element)
                    _kn = _kl = _vl = None
                    for gc in node:
                        txt = gc.text
                        if txt is None:
                            continue
                        gt = gc.tag
                        if gt == "Navn" and _kn is None:
                            _kn = txt
                        elif gt == "Ledetekst" and _kl is None:
                            _kl = txt
                        elif gt == "ValgLedetekst" and _vl is None:
                            _vl = txt

                    kn = _kn or ""
                    kl = _kl or ""

                    # Save old state for pop marker
                    old_cur_kly_id = cur_kly_id
                    old_par_kly_id = par_kly_id
                    old_kn_tr = cur_kly_navn_tr
                    old_kl_tr = cur_kly_ldt_tr
                    old_kt_tr = cur_kly_type_tr
                    old_kly_id_s = kly_id_s
                    old_kly_navne_s = kly_navne_s
                    old_kly_ledetekster_s = kly_ledetekster_s
                    old_hier_s = hier_s

                    # Push new state with pre-trimmed klynge metadata
                    par_kly_id = cur_kly_id
                    cur_kly_id = kly_counter
                    cur_kly_navn_tr = _tr(_kn) if _kn else None
                    cur_kly_ldt_tr = _tr(_kl) if _kl else None
                    cur_kly_type_tr = _tr(node_type) if node_type is not None else None

                    ks = str(kly_counter)
                    he = "Kly" + ks
                    kly_id_s = (kly_id_s + _HASH + ks) if kly_id_s else ks
                    kly_navne_s = (kly_navne_s + _HASH + kn) if kly_navne_s else kn
                    kly_ledetekster_s = (
                        (kly_ledetekster_s + _HASH + kl) if kly_ledetekster_s else kl
                    )
                    hier_s = (hier_s + _HASH + he) if hier_s else he

                    if node_type == "selektion":
                        elem_counter += 1
                        s_kly_par = par_kly_id if kly_depth_new > 1 else None

                        _append(
                            AktivitetsresultatRow(
                                aktivitets_id=aktivitets_id,
                                opdateret_tid=opdateret_tid,
                                dw_id_audit_update=dw_id_audit_update,
                                dw_is_deleted=dw_is_deleted,
                                element_nummer=elem_counter,
                                klynge_id=(kly_counter if kly_depth_new > 0 else None),
                                klynge_id_hierarki=kly_id_s or None,
                                klynge_niveau=kly_depth_new,
                                klynge_navne=kly_navne_s or None,
                                klynge_ledetekster=kly_ledetekster_s or None,
                                klynge_parent_id=s_kly_par,
                                repetition_id=(rep_counter if rep_depth > 0 else None),
                                repetition_id_hierarki=rep_id_s or None,
                                repetition_niveau=rep_depth,
                                hierarki=hier_s or None,
                                overskrift=overskrift,
                                ledetekst=ledetekst_val,
                                type=type_val,
                                kladde=kladde_val,
                                klynge_navn=cur_kly_navn_tr,
                                klynge_ledetekst=cur_kly_ldt_tr,
                                klynge_valg_ledetekst=(_tr(_vl) if _vl is not None else None),
                                klynge_type=cur_kly_type_tr,
                                element_navn=None,
                                element_ledetekst=None,
                                element_vaerdi="",
                                element_element_id=None,
                                element_kode=None,
                                element_original_kode=None,
                                element_administrativ_enhed_id=None,
                                element_pers_id=None,
                                element_type=None,
                                element_moid=None,
                                raekke_type="Selektion",
                                xml_status=1,
                                xml_status_text=_GOD_XML,
                            )
                        )

                    # Schedule: pop marker first (LIFO), then children in reverse
                    stack.append(
                        (
                            _POP_KLY,
                            old_cur_kly_id,
                            old_par_kly_id,
                            old_kn_tr,
                            old_kl_tr,
                            old_kt_tr,
                            old_kly_id_s,
                            old_kly_navne_s,
                            old_kly_ledetekster_s,
                            old_hier_s,
                        )
                    )
                    for child in reversed(list(node)):
                        stack.append((_VISIT, child, rep_depth, kly_depth_new))

                elif tag == "Repetition":
                    rep_counter += 1
                    rep_depth_new = rep_depth + 1

                    rs = str(rep_counter)
                    he = "Rep" + rs

                    old_rep_id_s = rep_id_s
                    old_hier_s = hier_s

                    rep_id_s = (rep_id_s + _HASH + rs) if rep_id_s else rs
                    hier_s = (hier_s + _HASH + he) if hier_s else he

                    stack.append((_POP_REP, old_rep_id_s, old_hier_s))
                    for child in reversed(list(node)):
                        stack.append((_VISIT, child, rep_depth_new, kly_depth))

                elif tag == "Sekvens" or tag == "Selektion":
                    # Transparent passthrough: IFDB wraps Klynge children in
                    # <Sekvens>/<Selektion> containers. They group items but
                    # don't represent answers themselves — descend with
                    # unchanged depth so nested Element/Repetition/Klynge are
                    # reached.
                    for child in reversed(list(node)):
                        stack.append((_VISIT, child, rep_depth, kly_depth))

            elif action == _POP_KLY:
                cur_kly_id = item[1]
                par_kly_id = item[2]
                cur_kly_navn_tr = item[3]
                cur_kly_ldt_tr = item[4]
                cur_kly_type_tr = item[5]
                kly_id_s = item[6]
                kly_navne_s = item[7]
                kly_ledetekster_s = item[8]
                hier_s = item[9]

            elif action == _POP_REP:
                rep_id_s = item[1]
                hier_s = item[2]

    if not results:
        return [
            AktivitetsresultatRow(
                aktivitets_id=aktivitets_id,
                opdateret_tid=opdateret_tid,
                dw_id_audit_update=dw_id_audit_update,
                dw_is_deleted=dw_is_deleted,
                overskrift=overskrift,
                ledetekst=ledetekst_val,
                type=type_val,
                kladde=kladde_val,
                element_vaerdi="",
                raekke_type="Tom",
                xml_status=0,
                xml_status_text=_INGEN_ELEM,
            )
        ], kontekst_row

    return results, kontekst_row


def parse_xml_components_fast(
    xml_string: str | None,
) -> tuple[list[tuple[str | None, str | None]], int]:
    """Fast XML component extraction for FHIR Observation.component[] pipeline.

    Lightweight alternative to parse_aktivitetsresultat_xml() that extracts only
    the (code_text, value_text) pairs needed for Observation components.

    Performance design vs the full parser:
      - Uses lxml (C-level libxml2) when available, ~1.9x faster XML parsing
      - No AktivitetsresultatRow / AktivitetskontekstRow construction (35 fields)
      - No hierarchy string tracking (kly_id_s, kly_navne_s, hier_s ...)
      - No counter management (elem_counter, kly_counter, rep_counter)
      - No Kontekst parsing
      - No root metadata extraction (Overskrift, Ledetekst, Type, Kladde)
      - No PersonaleRolle value propagation (only skips the element)
      - Inlined _tr to avoid function call overhead on ~75k calls per 10k batch
      - Early exit in Element children loop once both Navn and Vaerdi found
      - Uses C-level iter() instead of Python-level stack

    Returns:
        (components, xml_error_count)
        components: list of (code_text, value_text) tuples in document order.
        xml_error_count: 1 if XML was invalid/empty, 0 otherwise.

    Explicitly omitted reference-compatible fields in fast mode:
      - Root metadata:
          Overskrift, Ledetekst, Type, Kladde
      - Kontekst block:
          Oprettet.*, Aendret.*, Besluttet.*, Status
      - Element-level metadata:
          ElementId, Kode, Ledetekst (only Navn/Vaerdi are used)
      - Hierarchy bookkeeping:
          Klynge/Repetition IDs, names, and hierarchical paths

    Why:
      The fast path is a deliberate projection for Observation.component[] only.
      Full semantic unpacking remains available via parse_aktivitetsresultat_xml().
    """
    if not xml_string or not xml_string.strip():
        return [], 1

    xml_string = _trim_xml(xml_string)

    try:
        if _USE_LXML:
            xml_bytes = xml_string.encode() if isinstance(xml_string, str) else xml_string
            root = _lxml_etree.fromstring(xml_bytes)
        else:
            root = ET.fromstring(xml_string)
    except Exception:
        return [], 1

    results: list[tuple[str | None, str | None]] = []
    _append = results.append

    for node in root.iter():
        tag = node.tag

        if tag == "Element":
            navn = vaerdi = None
            for gc in node:
                txt = gc.text
                if txt is None:
                    continue
                gt = gc.tag
                if gt == "Navn":
                    navn = txt
                    if vaerdi is not None:
                        break
                elif gt == "Vaerdi":
                    vaerdi = txt
                    if navn is not None:
                        break

            if navn == "PersonaleRolle":
                continue

            if navn is not None:
                if "\n" not in navn and "\r" not in navn:
                    s = navn.strip()
                    code = s if s else None
                else:
                    s = navn.replace("\n", "").replace("\r", "").strip()
                    code = s if s else None
            else:
                code = None
            _append((code, vaerdi if vaerdi else None))

        elif tag == "Klynge" and node.get("type") == "selektion":
            kn = None
            for gc in node:
                if gc.tag == "Navn" and gc.text is not None:
                    kn = gc.text
                    break
            if kn is not None:
                if "\n" not in kn and "\r" not in kn:
                    s = kn.strip()
                    code = s if s else None
                else:
                    s = kn.replace("\n", "").replace("\r", "").strip()
                    code = s if s else None
            else:
                code = None
            _append((code, None))

    return results, 0
