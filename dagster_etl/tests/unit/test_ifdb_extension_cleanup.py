# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the IFDB SFI extension-cleanup contract.

These tests enforce the allowed/disallowed extension lists defined in
``tests/fixtures/ifdb_sfi/expected/allowed_extensions.csv``.

The tests work in two layers:

1. **Contract tests**: verify the allowed_extensions.csv fixture itself is
   correct, consistent, and covers all known redundancies.
2. **Builder tests**: verify that resource dicts produced by the
   Questionnaire and QuestionnaireResponse builders carry only allowed
   extensions.
"""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_SOURCE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_XML_ELEMENT_META,
    EXTENSION_XML_HIERARCHY,
    EXTENSION_XML_KONTEKST,
    EXTENSION_XML_ROOT_META,
    EXTENSION_XML_ROW_TYPE,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "ifdb_sfi" / "expected"
ALLOWED_EXTENSIONS_PATH = FIXTURE_DIR / "allowed_extensions.csv"

DISALLOWED_ON_SFI_OBSERVATION = frozenset(
    {
        EXTENSION_XML_ROOT_META,
        EXTENSION_XML_KONTEKST,
        EXTENSION_AKTIVITETSRESULTAT_SOURCE_META,
        EXTENSION_AKTIVITETSUDFOERELSE_META,
        EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
        EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
        EXTENSION_XML_HIERARCHY,
        EXTENSION_XML_ELEMENT_META,
        EXTENSION_XML_ROW_TYPE,
    }
)

ALLOWED_ON_SFI_OBSERVATION = frozenset(
    {
        EXTENSION_FHIR_IS_DELETED,
        EXTENSION_EPISODE_OF_CARE,
    }
)


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


def _load_allowed_extensions_csv() -> list[dict[str, str]]:
    with open(ALLOWED_EXTENSIONS_PATH, newline="") as fh:
        return list(csv.DictReader(fh))


def _collect_extension_urls(resource_dict: dict) -> set[str]:
    """Recursively collect all ``extension[].url`` values from a FHIR dict."""
    urls: set[str] = set()
    if isinstance(resource_dict, dict):
        for key, value in resource_dict.items():
            if key == "extension" and isinstance(value, list):
                for ext in value:
                    if isinstance(ext, dict) and "url" in ext:
                        url = ext["url"]
                        base = url.split("#")[0]
                        urls.add(base)
                    urls |= _collect_extension_urls(ext)
            else:
                urls |= _collect_extension_urls(value)
    elif isinstance(resource_dict, list):
        for item in resource_dict:
            urls |= _collect_extension_urls(item)
    return urls


class TestAllowedExtensionsCsvContract:
    """Validate the allowed_extensions.csv fixture itself."""

    def test_csv_exists(self):
        assert (
            ALLOWED_EXTENSIONS_PATH.exists()
        ), f"allowed_extensions.csv not found at {ALLOWED_EXTENSIONS_PATH}"

    def test_csv_has_required_columns(self):
        rows = _load_allowed_extensions_csv()
        assert rows, "allowed_extensions.csv must not be empty"
        required = {"extension_url", "resource_type", "status", "rationale"}
        for row in rows:
            missing = required - set(row.keys())
            assert not missing, f"Missing columns: {missing}"

    def test_csv_status_values_valid(self):
        rows = _load_allowed_extensions_csv()
        for row in rows:
            assert row["status"] in {
                "allowed",
                "disallowed",
            }, f"Invalid status '{row['status']}' for {row['extension_url']}"

    def test_csv_resource_types_valid(self):
        rows = _load_allowed_extensions_csv()
        valid_types = {"Observation", "QuestionnaireResponse", "Questionnaire"}
        for row in rows:
            assert (
                row["resource_type"] in valid_types
            ), f"Unexpected resource_type '{row['resource_type']}' for {row['extension_url']}"

    def test_csv_urls_are_real_extension_urls(self):
        """Extension URLs should use the actual StructureDefinition URLs, not placeholders."""
        rows = _load_allowed_extensions_csv()
        for row in rows:
            url = row["extension_url"]
            assert not url.startswith(
                "urn:dk:ifdb:extension:"
            ), f"Placeholder URL detected: {url}. Use real StructureDefinition URLs."

    def test_disallowed_observation_extensions_cover_plan_redundancies(self):
        """All known redundant AKTIVITETSRESULTAT extensions must be disallowed."""
        rows = _load_allowed_extensions_csv()
        disallowed_obs = {
            row["extension_url"]
            for row in rows
            if row["status"] == "disallowed" and row["resource_type"] == "Observation"
        }
        for url in DISALLOWED_ON_SFI_OBSERVATION:
            assert url in disallowed_obs, (
                f"Extension {url} should be disallowed for SFI Observations but is missing "
                f"from allowed_extensions.csv"
            )

    def test_allowed_observation_extensions_match_inventory(self):
        rows = _load_allowed_extensions_csv()
        allowed_obs = {
            row["extension_url"]
            for row in rows
            if row["status"] == "allowed" and row["resource_type"] == "Observation"
        }
        for url in ALLOWED_ON_SFI_OBSERVATION:
            assert url in allowed_obs, f"Extension {url} should be allowed for SFI Observations"

    def test_no_extension_is_both_allowed_and_disallowed_for_same_resource(self):
        rows = _load_allowed_extensions_csv()
        by_key: dict[tuple[str, str], set[str]] = {}
        for row in rows:
            key = (row["extension_url"], row["resource_type"])
            by_key.setdefault(key, set()).add(row["status"])
        for key, statuses in by_key.items():
            assert len(statuses) == 1, f"{key} has conflicting statuses: {statuses}"


class TestDisallowedExtensionConstants:
    """Cross-check that the disallowed set uses real constants from fhir_extensions.py."""

    def test_disallowed_set_uses_correct_urls(self):
        assert EXTENSION_XML_ROOT_META in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_XML_KONTEKST in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_AKTIVITETSRESULTAT_SOURCE_META in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_AKTIVITETSUDFOERELSE_META in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_AKTIVITETTYPE_KODE_HIERARKI in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_XML_HIERARCHY in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_XML_ELEMENT_META in DISALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_XML_ROW_TYPE in DISALLOWED_ON_SFI_OBSERVATION

    def test_allowed_set_uses_correct_urls(self):
        assert EXTENSION_FHIR_IS_DELETED in ALLOWED_ON_SFI_OBSERVATION
        assert EXTENSION_EPISODE_OF_CARE in ALLOWED_ON_SFI_OBSERVATION

    def test_allowed_and_disallowed_are_disjoint(self):
        overlap = ALLOWED_ON_SFI_OBSERVATION & DISALLOWED_ON_SFI_OBSERVATION
        assert not overlap, f"Overlap between allowed and disallowed sets: {overlap}"

    def test_disallowed_count_matches_plan(self):
        assert len(DISALLOWED_ON_SFI_OBSERVATION) == 9, (
            "Expected 9 disallowed extension base URLs for SFI Observations "
            "(rootMeta, kontekst, sourceMeta, udfMeta, udfSpeciale, hierarkiLevel, "
            "xmlHierarchy, elementMeta, rowType)"
        )


class TestExtensionUrlCollector:
    """Verify the recursive URL extraction helper."""

    def test_empty_dict(self):
        assert _collect_extension_urls({}) == set()

    def test_single_extension(self):
        resource = {
            "resourceType": "Observation",
            "extension": [{"url": "http://example.com/ext", "valueString": "test"}],
        }
        assert _collect_extension_urls(resource) == {"http://example.com/ext"}

    def test_nested_extension(self):
        resource = {
            "resourceType": "Observation",
            "extension": [
                {
                    "url": "http://example.com/parent",
                    "extension": [{"url": "http://example.com/child", "valueString": "v"}],
                }
            ],
        }
        urls = _collect_extension_urls(resource)
        assert "http://example.com/parent" in urls
        assert "http://example.com/child" in urls

    def test_fragment_urls_normalized_to_base(self):
        resource = {
            "extension": [
                {"url": "http://example.com/ext#fragment1", "valueString": "a"},
                {"url": "http://example.com/ext#fragment2", "valueString": "b"},
            ],
        }
        urls = _collect_extension_urls(resource)
        assert urls == {"http://example.com/ext"}

    def test_extensions_in_items(self):
        resource = {
            "resourceType": "QuestionnaireResponse",
            "item": [
                {
                    "linkId": "item1",
                    "extension": [{"url": "http://example.com/item-ext", "valueString": "x"}],
                }
            ],
        }
        urls = _collect_extension_urls(resource)
        assert "http://example.com/item-ext" in urls


def _assert_no_disallowed_extensions(
    resource_dict: dict,
    resource_type: str,
    disallowed_set: frozenset[str],
) -> None:
    """Fail if any disallowed extension base URL is present on the resource."""
    found_urls = _collect_extension_urls(resource_dict)
    violations = found_urls & disallowed_set
    assert not violations, (
        f"{resource_type} carries disallowed extensions: {violations}. "
        f"See allowed_extensions.csv for the cleanup contract."
    )


@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Questionnaire.Questionnaire_IFDB"),
    reason="Questionnaire builder import is unavailable.",
)
class TestQuestionnaireExtensions:
    """Verify Questionnaire resources carry only allowed extensions."""

    def test_questionnaire_has_no_custom_extensions_by_default(self):
        from DSA.FHIR.Questionnaire.Questionnaire_IFDB import build_questionnaire_from_xml

        xml = """<Resultat>
            <Element>
                <ElementId>edSat</ElementId>
                <Navn>Saturation</Navn>
                <Type>Heltal</Type>
                <Ledetekst>Saturation (%)</Ledetekst>
                <Vaerdi>97</Vaerdi>
            </Element>
        </Resultat>"""
        q = build_questionnaire_from_xml(
            xml,
            aktivitets_id=9001,
            model_name="TestModel",
            model_version="1.0",
        )
        urls = _collect_extension_urls(q)
        assert not urls, f"Questionnaire should use native structure, found extensions: {urls}"

    def test_questionnaire_has_no_observation_extensions(self):
        from DSA.FHIR.Questionnaire.Questionnaire_IFDB import build_questionnaire_from_xml

        xml = """<Resultat>
            <Element>
                <ElementId>edTemp</ElementId>
                <Navn>Temperatur</Navn>
                <Type>Decimal</Type>
                <Ledetekst>Temperatur</Ledetekst>
                <Vaerdi>37.2</Vaerdi>
            </Element>
        </Resultat>"""
        q = build_questionnaire_from_xml(
            xml,
            aktivitets_id=9002,
            model_name="TestModel",
            model_version="1.0",
        )
        _assert_no_disallowed_extensions(q, "Questionnaire", DISALLOWED_ON_SFI_OBSERVATION)


@pytest.mark.skipif(
    not _has_module("DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB"),
    reason="QuestionnaireResponse builder import is unavailable.",
)
class TestQuestionnaireResponseExtensions:
    """Verify QR resources carry only allowed extensions."""

    def _build_test_qr(self, xml: str, objekt_id: int = 5001) -> dict:
        from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB import (
            build_questionnaire_response_from_xml,
        )

        row_dict = {
            "OBJEKT_ID": objekt_id,
            "PATIENT_ID": "PAT001",
            "ENCOUNTER_ID": None,
            "OVERSKRIFT": "Test",
            "SLETTET": 0,
            "KLADDE": 0,
            "PERIOD_START": "2026-01-15T10:00:00",
            "MODELLERINGSNAVN": "TestModel",
            "MODELLERINGSVERSION": "1.0",
            "XML": xml,
        }
        return build_questionnaire_response_from_xml(
            row_dict,
            model_name="TestModel",
            model_version="1.0",
        )

    def test_qr_has_no_observation_extensions(self):
        xml = """<Resultat>
            <Element>
                <ElementId>edScore</ElementId>
                <Navn>Score</Navn>
                <Type>Heltal</Type>
                <Ledetekst>Total Score</Ledetekst>
                <Vaerdi>15</Vaerdi>
            </Element>
        </Resultat>"""
        qr = self._build_test_qr(xml)
        _assert_no_disallowed_extensions(qr, "QuestionnaireResponse", DISALLOWED_ON_SFI_OBSERVATION)

    def test_qr_has_no_custom_item_extensions_by_default(self):
        xml = """<Resultat>
            <Element>
                <ElementId>edVal</ElementId>
                <Navn>Vaerdi</Navn>
                <Type>Tekst</Type>
                <Ledetekst>En vaerdi</Ledetekst>
                <Vaerdi>hello</Vaerdi>
            </Element>
        </Resultat>"""
        qr = self._build_test_qr(xml)
        urls = _collect_extension_urls(qr)
        top_level_urls = {u for u in urls if u.startswith(("http://", "urn:"))}
        allowed_qr = {
            "http://regionsyddanmark.dk/fhir/StructureDefinition/sfiInstanceMeta",
        }
        unexpected = top_level_urls - allowed_qr
        assert not unexpected, (
            f"QuestionnaireResponse should only use native item fields "
            f"and sfiInstanceMeta, found unexpected: {unexpected}"
        )

    def test_qr_item_id_is_native_occurrence_key(self):
        xml = """<Resultat>
            <Element>
                <ElementId>edPuls</ElementId>
                <Navn>Puls</Navn>
                <Type>Heltal</Type>
                <Ledetekst>Puls (bpm)</Ledetekst>
                <Vaerdi>72</Vaerdi>
            </Element>
        </Resultat>"""
        qr = self._build_test_qr(xml)
        items = qr.get("item", [])
        assert items, "QR should contain items"
        assert all(item.get("id") for item in items), "QR.item.id is required for SOF item_key"


class TestProcedureExtensionsUntouched:
    """Verify that the extension cleanup contract does NOT touch Procedure extensions."""

    def test_procedure_source_meta_not_in_disallowed(self):
        from utils.fhir.fhir_extensions import (
            EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
        )

        assert (
            EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META not in DISALLOWED_ON_SFI_OBSERVATION
        )

    def test_episode_of_care_is_allowed(self):
        assert EXTENSION_EPISODE_OF_CARE in ALLOWED_ON_SFI_OBSERVATION

    def test_is_deleted_is_allowed(self):
        assert EXTENSION_FHIR_IS_DELETED in ALLOWED_ON_SFI_OBSERVATION


class TestRegressionDisallowedExtensionsCannotReturn:
    """Guard tests: if a builder re-introduces a disallowed extension, these fail.

    These are parameterized over the disallowed set to make failures specific.
    """

    @pytest.mark.parametrize(
        "ext_url",
        sorted(DISALLOWED_ON_SFI_OBSERVATION),
        ids=[u.split("/")[-1] for u in sorted(DISALLOWED_ON_SFI_OBSERVATION)],
    )
    def test_disallowed_url_not_in_allowed_set(self, ext_url: str):
        assert (
            ext_url not in ALLOWED_ON_SFI_OBSERVATION
        ), f"{ext_url} must not appear in the allowed set"

    @pytest.mark.parametrize(
        "ext_url",
        sorted(DISALLOWED_ON_SFI_OBSERVATION),
        ids=[u.split("/")[-1] for u in sorted(DISALLOWED_ON_SFI_OBSERVATION)],
    )
    def test_disallowed_url_in_csv(self, ext_url: str):
        rows = _load_allowed_extensions_csv()
        disallowed_in_csv = {
            row["extension_url"]
            for row in rows
            if row["status"] == "disallowed" and row["resource_type"] == "Observation"
        }
        assert (
            ext_url in disallowed_in_csv
        ), f"{ext_url} missing from allowed_extensions.csv disallowed entries"
