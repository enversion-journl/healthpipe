# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""XML child reconcile — retire Aidbox children missing from the current XML.

Bugs these tests catch:
- retiring a child we just posted
- treating the parent identifier as a child
- prefix matching a different parent's children
- retiring on malformed XML (would wipe children when parse failed)
- skipping empty XML (would leave orphans when the CLOB was cleared)
- retiring when the parse worker crashed or a row failed to build
- mass-retiring past the ceiling (a future skip condition emptying expected sets)
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from core.fhir import child_resource_reconcile as crr
from core.fhir.child_resource_reconcile import (
    DEFAULT_RETIRE_CEILING,
    ChildFamily,
    RetireCeiling,
    expected_child_identifiers,
    fhir_ids_to_retire,
    identifier_values_for_system,
    reconcile_children,
)
from DSA.FHIR.utils.ifdb.posting_config import retire_ceiling_from_context
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import ris_xml_is_malformed
from DSA.FHIR.utils.ifdb.xml_child_reconcile import (
    FAMILY_DELOP,
    FAMILY_RIS_PROCEDURE,
    FAMILY_SFI_OBS,
    FAMILY_XML_CONDITION,
    reconcile_aktivitetsresultat_xml_children,
    sfi_parent_oids_for_reconcile,
)
from DSA.FHIR.utils.ifdb.xml_parsing_workers import _parse_xml_lossless_for_rows
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_RIS_YDELSE,
)

pytestmark = pytest.mark.unit


def test_delop_prefix_does_not_match_parent_or_other_oid():
    assert FAMILY_DELOP.is_child_of("100-delop-1", "100")
    assert FAMILY_DELOP.is_child_of("100-delop-10", "100")
    assert not FAMILY_DELOP.is_child_of("100", "100")
    assert not FAMILY_DELOP.is_child_of("1000-delop-1", "100")
    assert not FAMILY_DELOP.is_child_of("10-delop-1", "1")


def test_sfi_and_ris_infixes_do_not_cross_parents():
    assert FAMILY_SFI_OBS.is_child_of("100#sfi#3", "100")
    assert not FAMILY_SFI_OBS.is_child_of("1000#sfi#3", "100")
    assert FAMILY_RIS_PROCEDURE.is_child_of("100:2", "100")
    assert not FAMILY_RIS_PROCEDURE.is_child_of("1000:2", "100")
    assert FAMILY_XML_CONDITION.is_child_of("100/4", "100")
    assert not FAMILY_XML_CONDITION.is_child_of("1000/4", "100")


def test_parent_procedure_is_not_in_delop_expected_set():
    parent = {
        "identifier": [
            {"system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "value": "100"},
        ]
    }
    delop = {
        "identifier": [
            {"system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "value": "100-delop-1"},
        ]
    }
    assert expected_child_identifiers([parent, delop], FAMILY_DELOP) == {"100-delop-1"}


def test_retire_only_identifiers_missing_from_current_xml():
    live = [("uuid-keep", "100-delop-1"), ("uuid-gone", "100-delop-2")]
    assert fhir_ids_to_retire(live, {"100-delop-1"}) == ["uuid-gone"]


def test_empty_expected_retires_all_live_children():
    live = [("uuid-a", "100-delop-1"), ("uuid-b", "100-delop-2")]
    assert fhir_ids_to_retire(live, set()) == ["uuid-a", "uuid-b"]


def test_already_expected_is_not_retired():
    live = [("uuid-keep", "100-delop-1")]
    assert fhir_ids_to_retire(live, {"100-delop-1"}) == []


def test_identifier_values_from_pydantic_like_and_tuple():
    class _Ident:
        system = SYSTEM_IFDB_RIS_YDELSE
        value = "55:1"

    class _Proc:
        identifier = [_Ident()]

    assert identifier_values_for_system(_Proc(), SYSTEM_IFDB_RIS_YDELSE) == ["55:1"]
    assert identifier_values_for_system(
        (
            "rid",
            {"identifier": [{"system": SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "value": "9#sfi#2"}]},
        ),
        SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    ) == ["9#sfi#2"]


def test_eligibility_follows_the_production_parser():
    # Whitespace-XML (500) behandles som den tomme streng: parseren stripper
    # før parse, så begge ender som «ryddet CLOB» — berettiget til reconcile,
    # og alle levende børn pensioneres.
    rows = [
        {"OBJEKT_ID": 100, "XML": None},
        {"OBJEKT_ID": 200, "XML": ""},
        {"OBJEKT_ID": 300, "XML": "<Afdelingsydelsesresultat/>"},
        {"OBJEKT_ID": 400, "XML": "<broken"},
        {"OBJEKT_ID": 500, "XML": "     "},
    ]
    parsed, _, _ = _parse_xml_lossless_for_rows(rows, logging.getLogger("test"))
    assert sfi_parent_oids_for_reconcile(rows, parsed) == ["100", "200", "300", "500"]


def test_crashed_parse_worker_output_is_not_eligible():
    rows = [{"OBJEKT_ID": 100, "XML": "<Afdelingsydelsesresultat/>"}]
    assert sfi_parent_oids_for_reconcile(rows, [[]]) == []


def test_build_errors_skip_the_chunk_reconcile():
    retired = reconcile_aktivitetsresultat_xml_children(
        fhir_database=None,
        rows=[{"OBJEKT_ID": 100, "XML": None}],
        parsed_data_by_index=[None],
        observation_dicts=[],
        procedure_dicts=[],
        condition_dicts=[],
        build_errors=1,
    )
    assert retired == {}


def test_length_mismatch_skips_the_chunk_reconcile():
    # Reconcile kører efter posten; en mismatch må aldrig vælte kørslen dér.
    # fhir_database=None beviser, at der hverken slås op eller pensioneres.
    retired = reconcile_aktivitetsresultat_xml_children(
        fhir_database=None,
        rows=[{"OBJEKT_ID": 100, "XML": None}, {"OBJEKT_ID": 200, "XML": None}],
        parsed_data_by_index=[None],
        observation_dicts=[],
        procedure_dicts=[],
        condition_dicts=[],
    )
    assert retired == {}


# ---------------------------------------------------------------------------
# Loft på retire — værnet mod masse-pensionering
# ---------------------------------------------------------------------------


def _live_boern(n: int) -> list[tuple[str, str]]:
    return [(f"fhir-{i}", f"1-delop-{i}") for i in range(n)]


def _reconcile_med_live(monkeypatch, n_live: int, **kwargs) -> tuple[int, list]:
    """Kør reconcile_children mod n_live attrap-børn; returnér (retired, mark-kald)."""
    marked: list = []
    monkeypatch.setattr(crr, "_fetch_live_children", lambda *a, **k: _live_boern(n_live))
    monkeypatch.setattr(
        crr, "_mark_entered_in_error", lambda db, fam, ids: marked.append(ids) or len(ids)
    )
    retired = reconcile_children(
        fhir_database=object(),
        family=FAMILY_DELOP,
        parent_oids=["1"],
        expected_identifiers=set(),
        **kwargs,
    )
    return retired, marked


def test_retire_ceiling_er_gulv_plus_andel():
    # Gulvet beskytter små chunks mod falske alarmer; andelen fanger
    # systematisk tømning på store kørsler. Bevidst u-kalibrerede værn.
    ceiling = RetireCeiling()
    assert ceiling.limit(10) == 1000
    assert ceiling.limit(10_000) == 5_000


def test_retire_over_loftet_springes_over(monkeypatch):
    # 2.500 at pensionere mod loftet max(1000, 50 % af 2.500) = 1.250:
    # intet må røres — en bevidst masseoprydning kræver hævet loft.
    retired, marked = _reconcile_med_live(monkeypatch, 2500)
    assert retired == 0
    assert marked == []


def test_retire_under_loftet_udfoeres(monkeypatch):
    retired, marked = _reconcile_med_live(monkeypatch, 100)
    assert retired == 100
    assert len(marked) == 1


def test_fraction_1_slaar_loftet_fra(monkeypatch):
    # retire kan aldrig overstige de levende, så fraction=1.0 er off-kontakten.
    retired, _ = _reconcile_med_live(monkeypatch, 5000, retire_ceiling=RetireCeiling(fraction=1.0))
    assert retired == 5000


def test_retire_ceiling_none_slaar_loftet_fra(monkeypatch):
    retired, _ = _reconcile_med_live(monkeypatch, 5000, retire_ceiling=None)
    assert retired == 5000


def test_retire_ceiling_laeses_fra_run_tags():
    ctx = SimpleNamespace(
        run=SimpleNamespace(
            tags={
                "fhir/retire_ceiling_floor": "50000",
                "fhir/retire_ceiling_fraction": "1.0",
            }
        )
    )
    assert retire_ceiling_from_context(ctx) == RetireCeiling(floor=50_000, fraction=1.0)


def test_retire_ceiling_default_uden_tags():
    assert retire_ceiling_from_context(SimpleNamespace(run=None)) == DEFAULT_RETIRE_CEILING


def test_ris_xml_malformed_vs_empty():
    assert ris_xml_is_malformed(None) is False
    assert ris_xml_is_malformed("  ") is False
    assert ris_xml_is_malformed("<rekvisition></rekvisition>") is False
    assert ris_xml_is_malformed("<rekvisition>") is True


def test_unsupported_resource_type_is_rejected():
    family = ChildFamily(
        resource_type="Patient",
        identifier_system="urn:x",
        infix="-",
    )
    with pytest.raises(ValueError, match="Patient"):
        family.table_name()


def test_prefix_bounds_rammer_praecis_foraelderens_boern():
    lo, hi = FAMILY_DELOP.prefix_bounds("12345")
    assert lo == "12345-delop-"
    # Intervallet er halvaabent i byte-orden: alle loebenumre ligger indenfor,
    # mens foraelderen selv og en anden foraelders boern ligger udenfor.
    for barn in ("12345-delop-1", "12345-delop-9", "12345-delop-42"):
        assert lo <= barn < hi
    for udenfor in ("12345", "12345-delo", "123456-delop-1", "12346-delop-1"):
        assert not (lo <= udenfor < hi)


def test_prefix_bounds_afviser_tom_foraelder():
    with pytest.raises(ValueError, match="empty parent oid"):
        FAMILY_DELOP.prefix_bounds("")


class _OptagendeCursor:
    """Cursor der husker den SQL, den fik, og kan levere faste rækker."""

    def __init__(self, opsamling, raekker=None):
        self._opsamling = opsamling
        self._raekker = raekker or []
        self._sidste = ""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self._opsamling.append((sql, params))
        self._sidste = sql

    def fetchall(self):
        if self._sidste.startswith("SELECT o.id"):
            return list(self._raekker)
        return []


class _Forbindelse:
    def __init__(self, opsamling, raekker=None):
        self._opsamling = opsamling
        self._raekker = raekker

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return _OptagendeCursor(self._opsamling, self._raekker)


class _OptagendeDatabase:
    def __init__(self, raekker=None):
        self.udfoert: list[tuple[str, object]] = []
        self.forbindelse = _Forbindelse(self.udfoert, raekker)

    def get_connection(self):
        return self.forbindelse


def test_et_interval_opslag_pr_foraelder():
    # Destillatet af fem efterprøvede forsøg: kun ét interval pr. forespørgsel
    # kan ikke fejlplanlægges, og rundturene fjernes af pipeline-tilstanden i
    # drift (attrappen har ingen pipeline og tager den sekventielle sti).
    db = _OptagendeDatabase()
    reconcile_children(
        fhir_database=db,
        family=FAMILY_DELOP,
        parent_oids=["12345", "67890"],
        expected_identifiers=set(),
    )
    assert "statement_timeout" in db.udfoert[0][0]
    assert db.udfoert[-1][0].startswith("RESET")
    opslag = [(s, p) for s, p in db.udfoert if s.startswith("SELECT o.id")]
    assert len(opslag) == 2, "ét opslag pr. forælder"
    for sql, _params in opslag:
        assert "~>=~" in sql and "~<~" in sql, "intervalgrænserne mangler"
        assert "VALUES" not in sql, "mængdebaseret form — fejlplanlægges ved statistikdrift"
        assert " OR " not in sql, "OR-kæde: eftervist katastrofal"
        assert "LIKE" not in sql.upper(), "faldet tilbage til mønstermatch (seq scan)"
        assert "jsonb_array_elements" not in sql, "udfolder stadig hele identifier-arrayet"
        assert "extract_identifier_value" in sql, "bruger ikke det indekserede udtryk"
    assert opslag[0][1][:2] == ["12345-delop-", "12345-delop."]
    assert opslag[1][1][:2] == ["67890-delop-", "67890-delop."]


def test_opslag_er_eksakte_pr_foraelder():
    # Hvert opslag er afgrænset til én forælders interval — der findes ingen
    # bred hentning, der kan fange naboers børn (det nøglerums-interval, der
    # kunne, er efterprøvet og fjernet: spredte forældre fik det til at læse
    # hele familien).
    from core.fhir.child_resource_reconcile import _fetch_live_children

    db = _OptagendeDatabase(raekker=[("id-a", "12345-delop-1")])
    found = _fetch_live_children(db, FAMILY_DELOP, ["12345", "67890"])
    assert ("id-a", "12345-delop-1") in found


def test_empty_parent_list_does_not_touch_the_database():
    retired = reconcile_children(
        fhir_database=None,
        family=FAMILY_DELOP,
        parent_oids=[],
        expected_identifiers=set(),
    )
    assert retired == 0


class TestChildValueErFormatetsEjer:
    """Formatparitet: child_value() danner PRÆCIS de literale formater, alle
    kaldesteder dannede før centraliseringen. Ændres et format her, skal det
    være en bevidst beslutning — ikke drift mellem dannelse og opslag."""

    def test_alle_fire_familier_matcher_de_historiske_formater(self):
        assert FAMILY_DELOP.child_value("12345", 2) == "12345-delop-2"
        assert FAMILY_SFI_OBS.child_value("12345", 7) == "12345#sfi#7"
        assert FAMILY_XML_CONDITION.child_value("12345", 3) == "12345/3"
        assert FAMILY_RIS_PROCEDURE.child_value("98765", 1) == "98765:1"

    def test_child_value_og_prefix_kan_ikke_drifte(self):
        for fam in (FAMILY_DELOP, FAMILY_SFI_OBS, FAMILY_XML_CONDITION, FAMILY_RIS_PROCEDURE):
            assert fam.child_value("P1", 9).startswith(fam.prefix("P1"))
            lav, hoej = fam.prefix_bounds("P1")
            assert lav <= fam.child_value("P1", 0) < hoej
