# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for AKTIVITETSRESULTAT → Procedure extraction.

Verifies:
- is_sks_procedure_code() classification logic
- _build_aktivitetsresultat_procedure_dict() produces valid Procedure dicts
- SFI flow treats Procedure as exclusive before Observation/QuestionnaireResponse
"""

import pytest
from DSA.FHIR.Procedure.procedure_aktivitetsresultat_builder import (
    _build_aktivitetsresultat_procedure_dict,
    build_aktivitetsresultat_procedure_dicts,
)
from DSA.FHIR.utils.code_normalization import (
    is_sks_procedure_code,
    load_ifdb_procedure_code_rules,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from DSA.FHIR.utils.sql_on_fhir.period_keys import parse_fhir_datetime
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED
from utils.fhir.resource_flags import FLAG_LPR3_IGNORED, SYSTEM_RESOURCE_FLAG

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# is_sks_procedure_code tests
# ---------------------------------------------------------------------------


class TestIsSKSProcedureCode:
    """Test SKS procedure code classification."""

    @pytest.mark.parametrize(
        "code",
        [
            "SKS.a.oprKABC10",
            "SKS.a.oprKFCA20",
            "SKS.a.proZXY01",
            "SKS.a.proKABD00",
        ],
    )
    def test_procedure_codes(self, code: str) -> None:
        assert is_sks_procedure_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "SKS.a.diaDA01",  # diagnosis
            "SKS.a.tilKZ12",  # supplementary
            "KABC10",  # bare K-code is not enabled by default
            "KFCA20",
            "KA",
            "KPYdelser.Afdelingsydelser.Afdelingsydelser.Maalinger.Saturation",
            "BomKS.a.BomYdelsesTyper.Ambulant",
            "ZZ3170",  # bare Z-code (not a procedure)
            "DA01",  # diagnosis bare code
            "A01",
            "",
            None,
        ],
    )
    def test_non_procedure_codes(self, code: str | None) -> None:
        assert is_sks_procedure_code(code) is False

    def test_procedure_rules_can_add_exact_bare_codes(self, tmp_path) -> None:
        rules_path = tmp_path / "ifdb_procedure_code_rules.yml"
        rules_path.write_text(
            """
procedure_code_rules:
  exact:
    - "KABC10"
  prefixes:
    - "SKS.a.pro"
  bare_regex: []
  exclude_exact: []
  exclude_prefixes:
    - "KPYdelser."
    - "BomKS."
""",
            encoding="utf-8",
        )

        load_ifdb_procedure_code_rules.cache_clear()
        try:
            assert is_sks_procedure_code("KABC10", rules_path=rules_path) is True
            assert is_sks_procedure_code("KPYdelser.X", rules_path=rules_path) is False
        finally:
            load_ifdb_procedure_code_rules.cache_clear()


# ---------------------------------------------------------------------------
# _build_aktivitetsresultat_procedure_dict tests
# ---------------------------------------------------------------------------


def _make_row(**overrides: object) -> dict:
    """Create a minimal AKTIVITETSRESULTAT row dict with sensible defaults."""
    row = {
        "OBJEKT_ID": "12345",
        "PATIENT_ID": "PAT-001",
        "ENCOUNTER_ID": "ENC-001",
        "EPISODE_OF_CARE_ID": "EOC-001",
        "AKTIVITETTYPE_KODE": "KABC10",
        "AKTIVITETTYPE_KODETEKST": "Appendektomi",
        "PERIOD_START": "2024-06-15T10:00:00",
        "PERIOD_END": "2024-06-15T11:30:00",
        "SLETTET": None,
        "KLADDE": None,
        "UDFOERT_PERSID": "PERS-001",
        "UDFOERT_PERSNAVN": "Dr. Hansen",
        "UDFOERT_BRUGERROLLE": "Læge",
        "UDFOERT_BRUGERROLLE_NAME": "Overlæge",
        "UDFOERT_TIDSPUNKT": "2024-06-15T10:00:00",
        "UDFOERT_ADMENHED_KODE": "4202011",
        "UDFOERT_ADMENHED_NAVN": "Kirurgisk Afd.",
        "UDFOERT_ADMENHED_ORGKODE": "ORG-001",
        "UDFOERT_ADMENHED_SORCODE": "SOR-001",
        "MODTIDSPUNKT": "2024-06-15T12:00:00",
    }
    row.update(overrides)
    return row


class TestBuildProcedureDict:
    """Test Procedure dict builder."""

    def test_basic_structure(self) -> None:
        row = _make_row()
        rid, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["resourceType"] == "Procedure"
        assert proc["status"] == "completed"
        assert proc["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE
        assert proc["identifier"][0]["value"] == "12345"

    def test_code_mapping(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.proKABC10")
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["code"]["coding"][0]["system"] == SYSTEM_SKS
        assert proc["code"]["coding"][0]["code"] == "KABC10"
        assert proc["code"]["coding"][0]["userSelected"] is True
        assert proc["code"]["coding"][0]["display"] == "Appendektomi"
        assert proc["code"]["text"] == "Appendektomi"

    def test_subject_reference(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["subject"]["identifier"]["system"] == SYSTEM_IFDB_PATIENT_ID
        assert proc["subject"]["identifier"]["value"] == "PAT-001"

    def test_encounter_reference(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["encounter"]["identifier"]["system"] == SYSTEM_IFDB_OBJECT_ID
        assert proc["encounter"]["identifier"]["value"] == "ENC-001"

    def test_performed_period(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert "performedPeriod" in proc
        assert proc["performedPeriod"]["start"] is not None
        assert proc["performedPeriod"]["end"] is not None

    def test_performed_datetime_when_no_end(self) -> None:
        row = _make_row(PERIOD_END=None)
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert "performedDateTime" in proc
        assert "performedPeriod" not in proc

    def test_deleted_status(self) -> None:
        row = _make_row(SLETTET="1")
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["status"] == "entered-in-error"
        is_deleted = [
            e for e in proc.get("extension", []) if e.get("url") == EXTENSION_FHIR_IS_DELETED
        ]
        assert len(is_deleted) == 1
        assert is_deleted[0].get("valueInteger") == 1

    def test_performer(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert len(proc["performer"]) == 2
        assert proc["performer"][0]["actor"]["type"] == "Organization"
        assert proc["performer"][0]["actor"]["identifier"]["value"] == "SOR-001"
        assert proc["performer"][1]["actor"]["display"] == "Dr. Hansen"
        assert proc["performer"][0]["actor"]["identifier"]["system"] == SYSTEM_SOR
        assert proc["performer"][0]["function"]["coding"][0]["code"] == "udfoerende"

    def test_source_meta_extension(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        source_ext = [e for e in proc["extension"] if "ProcedureSourceMeta" in e["url"]]
        assert len(source_ext) == 1
        parts = {s["url"]: s for s in source_ext[0]["extension"]}
        assert "udfoertBrugerrolle" in parts
        assert "modtidspunkt" in parts
        assert parts["udfoertTidspunkt"]["valueString"] == "2024-06-15T10:00:00"
        assert "lpr3Ignored" not in parts

    def test_lpr3_ignored_written_as_boolean(self) -> None:
        _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(LPR3_IGNORED=1))
        source_ext = [e for e in proc["extension"] if "ProcedureSourceMeta" in e["url"]]
        parts = {s["url"]: s for s in source_ext[0]["extension"]}
        assert parts["lpr3Ignored"]["valueBoolean"] is True

        _, proc_false = _build_aktivitetsresultat_procedure_dict(_make_row(LPR3_IGNORED=0))
        source_false = [e for e in proc_false["extension"] if "ProcedureSourceMeta" in e["url"]]
        parts_false = {s["url"]: s for s in source_false[0]["extension"]}
        assert parts_false["lpr3Ignored"]["valueBoolean"] is False

    def test_no_encounter_when_none(self) -> None:
        row = _make_row(ENCOUNTER_ID=None)
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert "encounter" not in proc

    def test_no_performer_when_missing(self) -> None:
        row = _make_row(
            UDFOERT_PERSID=None,
            UDFOERT_PERSNAVN=None,
            UDFOERT_ADMENHED_SORCODE=None,
        )
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert "performer" not in proc

    def test_episode_of_care_extension(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        eoc_ext = [e for e in proc["extension"] if "episodeOfCare" in e["url"]]
        assert len(eoc_ext) == 1

    def test_oracle_number_identifiers_are_normalized(self) -> None:
        row = _make_row(
            OBJEKT_ID=12345.0,
            PATIENT_ID=100.0,
            ENCOUNTER_ID=200.0,
            EPISODE_OF_CARE_ID=300.0,
            UDFOERT_PERSID=400.0,
        )
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        assert proc["identifier"][0]["value"] == "12345"
        assert proc["subject"]["identifier"]["value"] == "100"
        assert proc["encounter"]["identifier"]["value"] == "200"
        assert proc["performer"][1]["actor"]["identifier"]["value"] == "400"
        eoc_ext = [e for e in proc["extension"] if "episodeOfCare" in e["url"]]
        assert eoc_ext[0]["valueReference"]["identifier"]["value"] == "300"


# ---------------------------------------------------------------------------
# build_aktivitetsresultat_procedure_dicts: klyngebundne tillægskoder,
# deloperationer, knivtid og operationspersonale fra XML
# ---------------------------------------------------------------------------


def _xml_row(**overrides: object) -> AktivitetsresultatRow:
    """Minimal parser-række med de felter, procedure-udtrækket bruger."""
    defaults: dict = {
        "aktivitets_id": 12345,
        "opdateret_tid": None,
        "dw_id_audit_update": 0,
        "dw_is_deleted": 0,
    }
    defaults.update(overrides)
    return AktivitetsresultatRow(**defaults)


def _bilateral_xml_rows() -> list[AktivitetsresultatRow]:
    """Struktur fra prod-beviscasen: samme K-kode som primær og deloperation,
    med hver sin sideangivelse i hver sin klynge."""
    return [
        _xml_row(
            element_navn="Operation",
            element_original_kode="SKS.a.oprKCBB10",
            element_vaerdi="Excision af slap hud på øjenlåg",
            klynge_navn="PrimaerOperation",
            klynge_navne="PrimaerOperation",
            klynge_id=1,
        ),
        _xml_row(
            element_navn="Tillaegskoder",
            element_original_kode="SKS.a.tilTUL1",
            element_vaerdi="højresidig",
            klynge_navn="PrimaerOperation",
            klynge_navne="PrimaerOperation",
            klynge_id=1,
        ),
        _xml_row(
            element_navn="TillaegskodeKommatal",
            element_vaerdi="1,45",
            klynge_navn="PrimaerOperation",
            klynge_navne="PrimaerOperation",
            klynge_id=1,
        ),
        _xml_row(
            element_navn="Operation",
            element_original_kode="SKS.a.oprKCBB10",
            element_vaerdi="Excision af slap hud på øjenlåg",
            klynge_navn="DelOperationer",
            klynge_navne="DelOperationer",
            klynge_id=2,
        ),
        _xml_row(
            element_navn="Tillaegskoder",
            element_original_kode="SKS.a.tilTUL2",
            element_vaerdi="venstresidig",
            klynge_navn="DelOperationer",
            klynge_navne="DelOperationer",
            klynge_id=2,
        ),
        _xml_row(
            element_navn="Operation",
            element_original_kode="SKS.a.oprKUDB22",
            element_vaerdi="Otomikroskopi",
            klynge_navn="DelOperationer",
            klynge_navne="DelOperationer",
            klynge_id=3,
        ),
        _xml_row(
            element_navn="KnivtidStart",
            element_vaerdi="2099-01-01T08:00:00",
            klynge_navn="Operationsinformation",
            klynge_navne="Operationsinformation",
            klynge_id=4,
        ),
        _xml_row(
            element_navn="KnivtidSlut",
            element_vaerdi="2099-01-01T09:30:00",
            klynge_navn="Operationsinformation",
            klynge_navne="Operationsinformation",
            klynge_id=4,
        ),
        # Parserens faktiske form: PersonaleRolle emitteres IKKE som egen
        # række — rollen ligger i Personale-rækkens element_ledetekst.
        _xml_row(
            element_navn="Personale",
            element_ledetekst="Operatør",
            element_vaerdi="Test Person 010101",
            klynge_navn="Operationspersonale",
            klynge_navne="Operationsinformation#Operationspersonale",
            klynge_id=5,
        ),
    ]


def _codes(proc: dict) -> list[str]:
    return [c["code"] for c in proc.get("code", {}).get("coding", [])]


def _source_meta_parts(proc: dict) -> dict[str, dict]:
    """Kilde-metadata-delene som opslag på url."""
    for ext in proc.get("extension", []):
        if ext.get("url") == EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META:
            return {part.get("url"): part for part in ext.get("extension", [])}
    return {}


def _source_meta_urls(proc: dict) -> list[str]:
    for ext in proc.get("extension", []):
        if ext.get("url") == EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META:
            return [part.get("url") for part in ext.get("extension", [])]
    return []


class TestBuildProcedureDictsFromXml:
    def test_uden_xml_bygges_kun_hovedproceduren(self) -> None:
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), None)
        assert len(results) == 1
        _, proc = results[0]
        assert _codes(proc) == ["KABC10"]

    def test_bilateral_case_giver_egne_delop_ressourcer(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.oprKCBB10", AKTIVITETTYPE_KODETEKST=None)
        results = build_aktivitetsresultat_procedure_dicts(row, _bilateral_xml_rows())
        assert len(results) == 3
        main_rid, main = results[0]
        _, delop1 = results[1]
        _, delop2 = results[2]

        # Hovedproceduren: typekoden + KUN klyngens egne tillægskoder.
        # XML-operationselementet dedupes mod typekoden; TUL2 hører til
        # deloperationen og må ALDRIG lande på hovedproceduren.
        assert _codes(main) == ["KCBB10", "TUL1", "VPK01K45"]
        assert main["code"]["coding"][0].get("userSelected") is True
        assert all("userSelected" not in c for c in main["code"]["coding"][1:])

        # Deloperation 1: samme K-kode med SIN side — beviset for egne ressourcer.
        assert _codes(delop1) == ["KCBB10", "TUL2"]
        assert delop1["identifier"][0]["value"] == "12345-delop-1"
        assert delop1["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE
        # Aidbox-formen, saa det flade view kan projicere partOf.id
        assert delop1["partOf"][0]["id"] == main_rid
        assert delop1["partOf"][0]["resourceType"] == "Procedure"
        assert delop1["partOf"][0]["identifier"]["value"] == "12345"
        assert delop1["subject"] == main["subject"]
        assert delop1["status"] == main["status"]

        # Deloperation 2: egen kode, eget sekvensnummer.
        assert _codes(delop2) == ["KUDB22"]
        assert delop2["identifier"][0]["value"] == "12345-delop-2"

        # Knivtid ligger på hovedproceduren — ikke på deloperationerne.
        assert "knivtidStart" in _source_meta_urls(main)
        assert "knivtidSlut" in _source_meta_urls(main)
        assert "knivtidStart" not in _source_meta_urls(delop1)
        assert "delOperationSekvens" in _source_meta_urls(delop1)

        # Operationspersonalet bliver performer med rolle på hovedproceduren.
        xml_performers = [
            p for p in main.get("performer", []) if p.get("function", {}).get("text") == "Operatør"
        ]
        assert len(xml_performers) == 1
        assert xml_performers[0]["actor"]["display"] == "Test Person 010101"
        assert not any(
            p.get("function", {}).get("text") == "Operatør" for p in delop1.get("performer", [])
        )

    def test_hver_procedure_baerer_praecis_et_identifier(self) -> None:
        # Aftagerfladen og DQA joiner på (source_system, source_identifier_value)
        # som to uafhængige first() over identifier-listen. Parret er kun
        # stabilt, fordi hver Procedure bærer præcis ét identifier — et nr. 2
        # ville gøre dækningstjekkene stille forkerte i stedet for røde.
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.oprKCBB10", AKTIVITETTYPE_KODETEKST=None)
        results = build_aktivitetsresultat_procedure_dicts(row, _bilateral_xml_rows())
        assert len(results) == 3
        for _, proc in results:
            assert len(proc["identifier"]) == 1

    def test_knivtid_start_uden_slut_tolereres(self) -> None:
        rows = [
            _xml_row(
                element_navn="KnivtidStart",
                element_vaerdi="2099-01-02T10:00:00",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            )
        ]
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), rows)
        assert len(results) == 1
        urls = _source_meta_urls(results[0][1])
        assert "knivtidStart" in urls
        assert "knivtidSlut" not in urls

    def test_kladde_giver_status_i_gang(self) -> None:
        # Uden dette taeller en kladde som en gennemfoert procedure.
        _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(KLADDE=1))
        assert proc["status"] == "in-progress"

    def test_slettet_slaar_kladde(self) -> None:
        # Samme trappe som skemasvar og observationer: slettet vejer tungest.
        _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(KLADDE=1, SLETTET=1))
        assert proc["status"] == "entered-in-error"

    def test_uden_kladde_er_proceduren_gennemfoert(self) -> None:
        for vaerdi in (0, None):
            _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(KLADDE=vaerdi))
            assert proc["status"] == "completed", vaerdi

    def test_deloperation_arver_kladdestatus(self) -> None:
        results = build_aktivitetsresultat_procedure_dicts(
            _make_row(KLADDE=1), _bilateral_xml_rows()
        )
        assert len(results) > 1
        for _rid, proc in results:
            assert proc["status"] == "in-progress"

    def test_lpr3_flag_bliver_et_soegbart_maerke(self) -> None:
        # Flaget laa kun i en udvidelse og kunne derfor hverken soeges paa via
        # FHIR-serveren eller filtreres paa som kolonne.
        _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(LPR3_IGNORED=1))
        tags = proc.get("meta", {}).get("tag", [])
        assert {"system": SYSTEM_RESOURCE_FLAG, "code": FLAG_LPR3_IGNORED} in [
            {"system": t.get("system"), "code": t.get("code")} for t in tags
        ]

    def test_lpr3_maerke_udebliver_naar_flaget_er_falsk(self) -> None:
        # Maerket saettes kun naar flaget er sandt: fravaer betyder "ikke maerket",
        # og en kolonne bygget paa exists() bliver derfor aldrig NULL.
        for vaerdi in (0, None):
            _, proc = _build_aktivitetsresultat_procedure_dict(_make_row(LPR3_IGNORED=vaerdi))
            koder = [t.get("code") for t in proc.get("meta", {}).get("tag", [])]
            assert FLAG_LPR3_IGNORED not in koder, vaerdi

    def test_deloperation_arver_lpr3_maerket(self) -> None:
        # Ellers ville et filter paa flaget fange hovedoperationen men ikke dens dele.
        results = build_aktivitetsresultat_procedure_dicts(
            _make_row(LPR3_IGNORED=1), _bilateral_xml_rows()
        )
        assert len(results) > 1
        for _rid, proc in results:
            koder = [t.get("code") for t in proc.get("meta", {}).get("tag", [])]
            assert FLAG_LPR3_IGNORED in koder

    def test_knivtid_er_et_tidspunkt_og_ikke_en_streng(self) -> None:
        # Knivtid skal kunne regnes på. Som valueString kunne den ikke, og
        # ingenting validerede værdien.
        rows = [
            _xml_row(
                element_navn="KnivtidStart",
                element_vaerdi="2026-07-01T08:00:00",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            ),
            _xml_row(
                element_navn="KnivtidSlut",
                element_vaerdi="2026-07-01T09:30:00",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            ),
        ]
        parts = _source_meta_parts(
            build_aktivitetsresultat_procedure_dicts(_make_row(), rows)[0][1]
        )
        assert "valueString" not in parts["knivtidStart"]
        assert "valueDateTime" in parts["knivtidStart"]
        assert parts["knivtidMinutter"]["valueInteger"] == 90

    def test_knivtid_bevarer_det_registrerede_tidspunkt_gennem_indlaesningen(self) -> None:
        # Kilden skriver vægur-tid uden zone. Uden en eksplicit dansk zone på
        # ressourcen ville indlæsningen af de flade tabeller læse værdien som
        # UTC og flytte den 1-2 timer. Denne test er hele grunden til, at
        # tidspunktet bærer zoneangivelse.
        for raw, forventet_time in (("2026-07-01T08:00:00", 8), ("2026-01-15T08:00:00", 8)):
            rows = [
                _xml_row(
                    element_navn="KnivtidStart",
                    element_vaerdi=raw,
                    klynge_navn="Operationsinformation",
                    klynge_navne="Operationsinformation",
                    klynge_id=1,
                )
            ]
            parts = _source_meta_parts(
                build_aktivitetsresultat_procedure_dicts(_make_row(), rows)[0][1]
            )
            efter_load = parse_fhir_datetime(parts["knivtidStart"]["valueDateTime"])
            assert efter_load is not None
            assert efter_load.hour == forventet_time, raw

    def test_uparsbar_knivtid_udelades_frem_for_at_gemmes(self) -> None:
        rows = [
            _xml_row(
                element_navn="KnivtidStart",
                element_vaerdi="ikke et tidspunkt",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            )
        ]
        urls = _source_meta_urls(build_aktivitetsresultat_procedure_dicts(_make_row(), rows)[0][1])
        assert "knivtidStart" not in urls

    def test_negativt_spaend_giver_ingen_varighed(self) -> None:
        rows = [
            _xml_row(
                element_navn="KnivtidStart",
                element_vaerdi="2026-07-01T09:30:00",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            ),
            _xml_row(
                element_navn="KnivtidSlut",
                element_vaerdi="2026-07-01T08:00:00",
                klynge_navn="Operationsinformation",
                klynge_navne="Operationsinformation",
                klynge_id=1,
            ),
        ]
        urls = _source_meta_urls(build_aktivitetsresultat_procedure_dicts(_make_row(), rows)[0][1])
        assert "knivtidStart" in urls
        assert "knivtidMinutter" not in urls

    def test_delop_klynge_uden_operationskode_ignoreres(self) -> None:
        # Klyngebinding: en tillægskode i en (defekt) delop-klynge uden
        # operationselement må ikke falde tilbage på hovedproceduren.
        rows = [
            _xml_row(
                element_navn="Tillaegskoder",
                element_original_kode="SKS.a.tilTUL2",
                element_vaerdi="venstresidig",
                klynge_navn="DelOperationer",
                klynge_navne="DelOperationer",
                klynge_id=7,
            )
        ]
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), rows)
        assert len(results) == 1
        assert "TUL2" not in _codes(results[0][1])


class TestUndersoegelseskoderKlassifikation:
    """Aktivitetsfamilier danner Procedure; ikke-aktiviteter gør ikke.

    Linjen er: en SKS-familie, der beskriver noget, DER BLEV GJORT, danner
    Procedure. Familier der beskriver et fund, et stof, en organisation eller
    en modifikator gør ikke — de har enten deres eget spor i modellen eller er
    ikke en handling. Ukendte familier danner Procedure, så en ny familie ikke
    forsvinder lydløst."""

    @pytest.mark.parametrize(
        ("kode", "forventet"),
        [
            ("SKS.a.undUXUC80", True),  # transtorakal ekkokardiografi
            ("SKS.a.undUXRG80", True),  # osteodensitometri
            ("SKS.a.undUCGG00", True),  # ENG/EMG
            ("SKS.a.undZZ3170", True),  # hele und-familien, ikke kun undU
            ("SKS.a.admAAF22", True),  # administrativ aktivitet
            ("SKS.a.nyFamilie99", True),  # ukendt familie tælles med
            ("SKS.a.atcMB01AD02", False),  # lægemiddelstof, ikke handling
            ("SKS.a.diaDC509", False),  # diagnose — eget spor til Condition
            ("SKS.a.tilZP", False),  # tillægskode — modifikator
            ("SKS.a.afd4202011", False),  # organisationskode
        ],
    )
    def test_aktivitetsfamilier_er_procedurer(self, kode: str, forventet: bool) -> None:
        assert is_sks_procedure_code(kode) is forventet

    def test_personale_rolle_som_separat_raekke_haandteres_ogsaa(self) -> None:
        # Defensiv gren: håndbyggede/fremtidige former med eksplicit
        # PersonaleRolle-række skal også virke.
        rows = [
            _xml_row(
                element_navn="PersonaleRolle",
                element_vaerdi="Assistent",
                klynge_navn="Operationspersonale",
                klynge_navne="Operationspersonale",
                klynge_id=1,
            ),
            _xml_row(
                element_navn="Personale",
                element_ledetekst="Personale",
                element_vaerdi="Test Person 020202",
                klynge_navn="Operationspersonale",
                klynge_navne="Operationspersonale",
                klynge_id=1,
            ),
        ]
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), rows)
        perf = [
            p
            for p in results[0][1].get("performer", [])
            if p.get("function", {}).get("text") == "Assistent"
        ]
        assert len(perf) == 1
        assert perf[0]["actor"]["display"] == "Test Person 020202"

    def test_gamle_deloperationer_klynge_er_ikke_delop(self) -> None:
        rows = [
            _xml_row(
                element_navn="Operation",
                element_original_kode="SKS.a.oprKUDB22",
                element_vaerdi="Otomikroskopi",
                klynge_navn="GamleDelOperationer",
                klynge_navne="GamleDelOperationer",
                klynge_id=9,
            )
        ]
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), rows)
        assert len(results) == 1
        assert results[0][1]["identifier"][0]["value"] == "12345"

    def test_sks_vaerdier_til_prefix_strippes(self) -> None:
        rows = [
            _xml_row(
                element_navn="Tillaegskoder",
                element_original_kode="SKS_Vaerdier.a.tilTUL1",
                element_vaerdi="højresidig",
                klynge_navn="PrimaerOperation",
                klynge_navne="PrimaerOperation",
                klynge_id=1,
            ),
            _xml_row(
                element_navn="Tillaegskoder",
                element_original_kode="SKS_Vaerdier.a.VPK484K8",
                element_vaerdi="tabt",
                klynge_navn="PrimaerOperation",
                klynge_navne="PrimaerOperation",
                klynge_id=1,
            ),
        ]
        results = build_aktivitetsresultat_procedure_dicts(_make_row(), rows)
        assert "TUL1" in _codes(results[0][1])
        assert "VPK484K8" not in _codes(results[0][1])
        assert "a.VPK484K8" not in _codes(results[0][1])


class TestIngenSaerregelPaaSkematype:
    """Udvælgelsen ser kun på aktivitetstypen — som referencen."""

    def test_adm_er_procedure_uanset_skematype(self) -> None:
        assert is_sks_procedure_code("SKS.a.admAAF22") is True

    def test_opr_er_procedure(self) -> None:
        assert is_sks_procedure_code("SKS.a.oprKABC10") is True


_PARSER_DELOP_XML = """<Afdelingsydelsesresultat>
  <Klynge type="sekvens">
    <Navn>PrimaerOperation</Navn>
    <Element type="string">
      <Navn>Operation</Navn>
      <Vaerdi>Excision af slap hud på øjenlåg</Vaerdi>
      <Originalkode>SKS.a.oprKCBB10</Originalkode>
    </Element>
    <Element type="string">
      <Navn>Tillaegskoder</Navn>
      <Vaerdi>højresidig</Vaerdi>
      <Originalkode>SKS.a.tilTUL1</Originalkode>
    </Element>
  </Klynge>
  <Klynge type="sekvens">
    <Navn>DelOperationer</Navn>
    <Element type="string">
      <Navn>Operation</Navn>
      <Vaerdi>Excision af slap hud på øjenlåg</Vaerdi>
      <Originalkode>SKS.a.oprKCBB10</Originalkode>
    </Element>
    <Element type="string">
      <Navn>Tillaegskoder</Navn>
      <Vaerdi>venstresidig</Vaerdi>
      <Originalkode>SKS.a.tilTUL2</Originalkode>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>
"""


class TestParserToBuilder:
    def test_xml_parser_til_delop_procedurer(self) -> None:
        from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import parse_aktivitetsresultat_xml

        parsed, _ = parse_aktivitetsresultat_xml(_PARSER_DELOP_XML, aktivitets_id=12345)
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.oprKCBB10", AKTIVITETTYPE_KODETEKST=None)
        results = build_aktivitetsresultat_procedure_dicts(row, parsed)
        assert len(results) == 2
        _, main = results[0]
        _, delop = results[1]
        assert _codes(main) == ["KCBB10", "TUL1"]
        assert _codes(delop) == ["KCBB10", "TUL2"]
        assert delop["identifier"][0]["value"] == "12345-delop-1"
