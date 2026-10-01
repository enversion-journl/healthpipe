# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end integration test for the SOR CodeSystem pipeline.

Downloads SOR data from sundhedsdatastyrelsen.dk → parses CSV → builds codesystem triple.
No database required — exercises the full transform logic on live data.

Run with:
    pytest tests/integration/test_sor_e2e.py -v
"""

import importlib.util
import sys
import types

import pytest

pytestmark = pytest.mark.integration


def _import_sor_register():
    """Import sor_register module without triggering the full DSI package init."""
    spec = importlib.util.spec_from_file_location(
        "sor_register",
        "DSI/csv_files/sor_register.py",
        submodule_search_locations=[],
    )
    if "core" not in sys.modules:
        core_stub = types.ModuleType("core")
        sys.modules["core"] = core_stub
    if "core.resources" not in sys.modules:
        res_stub = types.ModuleType("core.resources")
        res_stub.MSSQLResource = type("MSSQLResource", (), {})
        sys.modules["core.resources"] = res_stub

    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _import_sor_codesystem():
    """Import sor.py codesystem builder without triggering the full DSA package init."""
    for name in ["DSA", "DSA.CodeSystem", "DSA.CodeSystem.utils"]:
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    sys.modules["DSA.CodeSystem.utils"].merge_codesystem_tables = lambda *a, **kw: {}

    spec = importlib.util.spec_from_file_location(
        "sor_codesystem",
        "DSA/CodeSystem/sor/sor.py",
        submodule_search_locations=[],
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sor_reg = _import_sor_register()
sor_cs = _import_sor_codesystem()

build_sor_codesystem = sor_cs.build_sor_codesystem
KNOWN_PROPERTY_KEYS = sor_cs.KNOWN_PROPERTY_KEYS


@pytest.fixture(scope="module")
def sor_raw_data():
    """Download and parse SOR data (daily CSV)."""
    import csv
    import io
    from urllib.request import Request, urlopen

    url = "https://sor-filer.sundhedsdata.dk/sor2_produktion/V1_00/data/SorEntity_daily.csv"
    req = Request(url, headers={"User-Agent": "pytest-sor-e2e/1.0"})
    with urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(raw), delimiter=";")
    rows = list(reader)
    assert len(rows) > 0, "SOR daily CSV returned no rows"
    return rows


class _StubLog:
    """Minimal stub for dagster AssetExecutionContext.log used by sor_register helpers."""

    def info(self, msg, *args, **kwargs):
        pass

    def warning(self, msg, *args, **kwargs):
        pass


class _StubContext:
    log = _StubLog()


@pytest.fixture(scope="module")
def sor_wide_rows(sor_raw_data):
    """Convert raw SOR rows to the wide format cs_sor feeds build_sor_codesystem.

    Drives raw CSV rows through the REAL sor_register._transform_to_ext_format
    (version grain), then applies the current-version-per-code selection that
    cs_sor's _CURRENT_VERSION_QUERY performs in SQL: highest from_date wins
    (lexicographic on the ISO strings, matching NVARCHAR ordering), to_date is
    projected as the closingDate property column.
    """
    _transform_to_ext_format = sor_reg._transform_to_ext_format

    ctx = _StubContext()
    ext_rows = _transform_to_ext_format(sor_raw_data, ctx)

    current: dict[str, dict] = {}
    for row in ext_rows:
        prev = current.get(row["code"])
        if prev is None or row["from_date"] > prev["from_date"]:
            current[row["code"]] = row

    wide_rows = []
    for row in current.values():
        wide = dict(row)
        wide["closingDate"] = wide.pop("to_date")
        wide.pop("from_date")
        wide_rows.append(wide)
    return wide_rows


@pytest.fixture(scope="module")
def sor_triple(sor_wide_rows):
    """Build codesystem triple from wide rows."""
    concepts, properties, hierarchy = build_sor_codesystem(sor_wide_rows)
    return concepts, properties, hierarchy


class TestSorDownload:
    def test_has_rows(self, sor_raw_data):
        assert len(sor_raw_data) > 100, f"Expected >100 rows, got {len(sor_raw_data)}"

    def test_has_expected_columns(self, sor_raw_data):
        row = sor_raw_data[0]
        assert "SorId" in row
        assert "EntityName" in row
        assert "ParentSorId" in row


class TestSorCodesystemBuild:
    def test_concepts_not_empty(self, sor_triple):
        concepts, _, _ = sor_triple
        assert len(concepts) > 100

    def test_concepts_have_code_and_display(self, sor_triple):
        concepts, _, _ = sor_triple
        for c in concepts[:10]:
            assert "CODE" in c
            assert "DISPLAY" in c
            assert c["CODE"].strip() != ""

    def test_properties_have_expected_keys(self, sor_triple):
        _, properties, _ = sor_triple
        assert len(properties) > 0
        prop_keys = {p["KEY"] for p in properties}
        assert "entityType" in prop_keys
        assert (
            "municipality" in prop_keys
        ), "Expected 'municipality' property; source column PostalAddressMunicipalityCode missing from CSV_TO_EXT_MAP or CSV"
        assert (
            "region" in prop_keys
        ), "Expected 'region' property; source column PostalAddressRegionCode missing from CSV_TO_EXT_MAP or CSV"

    def test_hierarchy_has_parent_child(self, sor_triple):
        _, _, hierarchy = sor_triple
        assert len(hierarchy) > 0
        for h in hierarchy[:10]:
            assert "CODE" in h
            assert "PARENT_CODE" in h
            assert h["CODE"] != h["PARENT_CODE"]

    def test_property_types(self, sor_triple):
        _, properties, _ = sor_triple
        types_seen = {p["TYPE"] for p in properties}
        assert "String" in types_seen or "Date" in types_seen
