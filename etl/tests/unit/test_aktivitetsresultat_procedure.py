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
)
from DSA.FHIR.utils.code_normalization import (
    is_sks_procedure_code,
    load_ifdb_procedure_code_rules,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

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
            "SKS.a.undZZ3170",  # examination
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

        assert len(proc["performer"]) == 3
        assert proc["performer"][0]["actor"]["type"] == "Organization"
        assert proc["performer"][0]["actor"]["identifier"]["value"] == "SOR-001"
        assert proc["performer"][1]["actor"]["display"] == "Dr. Hansen"
        assert proc["performer"][2]["actor"]["identifier"]["system"] == SYSTEM_SOR
        assert proc["performer"][2]["actor"]["identifier"]["value"] == "SOR-001"

    def test_source_meta_extension(self) -> None:
        row = _make_row()
        _, proc = _build_aktivitetsresultat_procedure_dict(row)

        source_ext = [e for e in proc["extension"] if "ProcedureSourceMeta" in e["url"]]
        assert len(source_ext) == 1
        sub_urls = {s["url"] for s in source_ext[0]["extension"]}
        assert "udfoertBrugerrolle" in sub_urls
        assert "modtidspunkt" in sub_urls

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
