# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared fixtures for IFDB SFI integration tests.

These fixtures expose the deterministic XML fixtures, expected outputs and
routing seed defined under etl/tests/fixtures/ifdb_sfi/.

Production code is NOT imported here. The fixtures only describe inputs
and expected outputs. The actual builders and SQL-on-FHIR projections are
tested in their own unit/integration suites.

Real-resource tests are gated on USE_REAL_DB_FOR_TESTS=true and the
`real_resources` marker, matching the convention in etl/tests/conftest.py.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import pytest

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures" / "ifdb_sfi"
XML_DIR = FIXTURE_ROOT / "xml"
EXPECTED_DIR = FIXTURE_ROOT / "expected"


CASE_IDS = [
    "case_01_procedure_blodtryk",
    "case_02_observation_saturation",
    "case_03_qr_simpel_aktuelt",
    "case_04_qr_kompleks_ernaering",
    "case_05_qr_gcs_total",
    "case_06_fejl_datatype_mismatch",
]


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


@pytest.fixture(scope="session")
def sfi_fixture_root() -> Path:
    """Root directory for the SFI test fixtures."""
    return FIXTURE_ROOT


@pytest.fixture(scope="session")
def sfi_case_ids() -> list[str]:
    """Ordered list of canonical SFI test case ids."""
    return list(CASE_IDS)


@pytest.fixture(scope="session")
def sfi_xml_fixtures() -> dict[str, str]:
    """Map of case_id -> raw XML payload."""
    payloads: dict[str, str] = {}
    for case_id in CASE_IDS:
        xml_path = XML_DIR / f"{case_id}.xml"
        payloads[case_id] = xml_path.read_text(encoding="utf-8")
    return payloads


@pytest.fixture(scope="session")
def sfi_expected_cases() -> dict[str, dict]:
    """Map of case_id -> expected output JSON contract."""
    expected: dict[str, dict] = {}
    for case_id in CASE_IDS:
        expected[case_id] = _load_json(EXPECTED_DIR / f"{case_id}.json")
    return expected


@pytest.fixture(scope="session")
def sfi_expected_concepts() -> list[dict[str, str]]:
    """Expected SFI CodeSystem concepts joined across all test cases."""
    return _load_csv(FIXTURE_ROOT / "sfi_expected_concepts.csv")


@pytest.fixture(scope="session")
def sfi_routing_seed() -> list[dict[str, str]]:
    """Routing seed (positive list) used to drive the test cases."""
    return _load_csv(FIXTURE_ROOT / "routing_seed.csv")


@pytest.fixture(scope="session")
def sfi_expected_observation_ids() -> list[dict[str, str]]:
    """Deterministic Observation identifier values expected per case."""
    return _load_csv(EXPECTED_DIR / "observation_ids.csv")


@pytest.fixture(scope="session")
def sfi_expected_qr_linkids() -> list[dict[str, str]]:
    """Expected Questionnaire/QR linkId structure per case."""
    return _load_csv(EXPECTED_DIR / "qr_linkids.csv")


@pytest.fixture(scope="session")
def sfi_expected_datapoint_rows() -> list[dict[str, str]]:
    """Expected fhir.SFI_datapoint rows (Observation + QR-item union)."""
    return _load_csv(EXPECTED_DIR / "sfi_datapoint_rows.csv")


@pytest.fixture(scope="session")
def sfi_allowed_extensions() -> list[dict[str, str]]:
    """Contract over which extensions are allowed vs. disallowed on new resources."""
    return _load_csv(EXPECTED_DIR / "allowed_extensions.csv")


@pytest.fixture(scope="session")
def use_real_resources() -> bool:
    """Whether the test run is allowed to hit real Aidbox/MSSQL/IFDB."""
    return os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"


@pytest.fixture(autouse=True)
def _skip_when_real_resources_required(request, use_real_resources):
    """Skip real_resources-marked tests when real DB usage is disabled.

    Mirrors the gating convention from fhir_ifdb/fhir_sof integration tests.
    Non-real tests still execute so that fixture wiring and contract checks
    run in CI without external dependencies.
    """
    if request.node.get_closest_marker("real_resources") and not use_real_resources:
        pytest.skip("Real-resource test skipped: set USE_REAL_DB_FOR_TESTS=true to enable.")
