# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end integration test for the SKS CodeSystem pipeline.

Downloads SKScomplete.txt from Sundhedsdatastyrelsen → parses fixed-width
records → deduplicates to newest versions → builds hierarchy → builds
codesystem triple. No database required — exercises the full transform logic
on live data.

Run with:
    pytest tests/integration/test_sks_e2e.py -v
"""

import gzip

# ---------------------------------------------------------------------------
# We import the pure-logic helpers directly to avoid pulling in Dagster/MSSQL
# through the package __init__.py chain (which requires oracledb at import).
# If the project environment has all deps, you can also use:
#   from DSI.csv_files.sks_register import _parse_sks_lines, ...
# ---------------------------------------------------------------------------
import sys
import types
from datetime import date
from urllib.request import Request, urlopen

import pytest


def _import_sks_register():
    """Import sks_register module without triggering the full DSI package init."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "sks_register",
        "DSI/csv_files/sks_register.py",
        submodule_search_locations=[],
    )
    # Provide a stub for core.resources so the import doesn't fail
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


def _import_sks_codesystem():
    """Import sks.py codesystem builder without triggering the full DSA package init."""
    import importlib.util

    # Stub DSA.CodeSystem.utils so we can import the builder
    for name in ["DSA", "DSA.CodeSystem", "DSA.CodeSystem.utils"]:
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    # Provide a dummy merge_codesystem_tables (we won't call it in e2e)
    sys.modules["DSA.CodeSystem.utils"].merge_codesystem_tables = lambda *a, **kw: {}

    spec = importlib.util.spec_from_file_location(
        "sks_codesystem",
        "DSA/CodeSystem/sks/sks.py",
        submodule_search_locations=[],
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sks_reg = _import_sks_register()
sks_cs = _import_sks_codesystem()

KNOWN_PREFIXES = sks_reg.KNOWN_PREFIXES
SKS_COMPLETE_URL = sks_reg.SKS_COMPLETE_URL
EXT_COLUMNS = sks_reg.EXT_COLUMNS
_parse_sks_lines = sks_reg._parse_sks_lines
_deduplicate_newest = sks_reg._deduplicate_newest
_build_hierarchy = sks_reg._build_hierarchy
_build_ext_rows = sks_reg._build_ext_rows
build_sks_codesystem = sks_cs.build_sks_codesystem


# -- Minimal context stub for _build_ext_rows -------------------------------


class _Ctx:
    class log:
        @staticmethod
        def info(msg):
            pass

        @staticmethod
        def warning(msg):
            pass


# -- Raw HTTP fetch (no Dagster context needed) ------------------------------


def _fetch_raw() -> str:
    """Download SKScomplete.txt and return decoded text."""
    req = Request(
        SKS_COMPLETE_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; Dagster-SKS-Loader/2.0)"},
    )
    with urlopen(req, timeout=300) as response:
        data = response.read()
    if len(data) > 2 and data[0] == 0x1F and data[1] == 0x8B:
        data = gzip.decompress(data)
    return data.decode("latin-1")


# ===========================================================================
# Tests
# ===========================================================================

pytestmark = [pytest.mark.integration]

TODAY = date.today()


@pytest.fixture(scope="module")
def sks_text():
    """Download SKScomplete.txt once for the whole module."""
    return _fetch_raw()


@pytest.fixture(scope="module")
def sks_records(sks_text):
    return _parse_sks_lines(sks_text)


@pytest.fixture(scope="module")
def sks_deduped(sks_records):
    return _deduplicate_newest(sks_records)


class TestSksCompleteFetch:
    """Verify that SKScomplete.txt is downloadable and parseable."""

    def test_download_size(self, sks_text):
        assert len(sks_text) > 10_000_000, f"Expected >10MB of text, got {len(sks_text)} chars"

    def test_parse_returns_records(self, sks_records):
        assert len(sks_records) > 100_000, f"Expected >100k version records, got {len(sks_records)}"

    def test_record_format(self, sks_records):
        for rec in sks_records[:50]:
            assert rec["code"], "Code must not be empty"
            assert rec["classification"] in KNOWN_PREFIXES
            assert isinstance(rec["valid_from"], date)
            assert isinstance(rec["valid_to"], date)

    def test_all_classifications_present(self, sks_records):
        present = {r["classification"] for r in sks_records}
        assert present == set(
            KNOWN_PREFIXES
        ), f"Missing classifications: {set(KNOWN_PREFIXES) - present}"

    def test_codes_have_no_prefix(self, sks_records):
        """Clean codes must not retain the lowercase classification prefix."""
        for rec in sks_records[:1000]:
            assert not rec["code"][:3].islower() or rec["code"][:3] not in KNOWN_PREFIXES


class TestDeduplicationOnRealData:
    """Deduplicate real data and verify newest-version semantics."""

    def test_unique_codes_count(self, sks_deduped):
        newest_by_code, _ = sks_deduped
        assert (
            len(newest_by_code) > 60_000
        ), f"Expected >60k unique codes, got {len(newest_by_code)}"

    def test_active_majority(self, sks_deduped):
        newest_by_code, _ = sks_deduped
        active = sum(1 for r in newest_by_code.values() if r["valid_to"] >= TODAY)
        assert active > 50_000, f"Expected >50k active codes, got {active}"

    def test_known_code_is_active_and_current(self, sks_deduped):
        """DA01 (Tyfus og paratyfus) has a closed 1994-version and an open
        2012-version — dedup must pick the open one."""
        newest_by_code, _ = sks_deduped
        rec = newest_by_code.get("DA01")
        assert rec is not None, "DA01 missing from deduped codes"
        assert rec["valid_to"] >= TODAY, "DA01 should be active"

    def test_multi_classification_codes_exist(self, sks_deduped):
        _, classifications_by_code = sks_deduped
        multi = [c for c, s in classifications_by_code.items() if len(s) > 1]
        assert len(multi) > 50, f"Expected >50 codes in multiple classifications, got {len(multi)}"


class TestHierarchyOnRealData:
    """Build hierarchy from real data and verify structure."""

    def test_hierarchy_has_parents(self, sks_deduped):
        newest_by_code, _ = sks_deduped
        parent_map = _build_hierarchy(set(newest_by_code))
        assert (
            len(parent_map) > 50_000
        ), f"Expected >50k parent relationships, got {len(parent_map)}"

    def test_every_parent_exists_in_code_set(self, sks_deduped):
        newest_by_code, _ = sks_deduped
        code_set = set(newest_by_code)
        parent_map = _build_hierarchy(code_set)
        for child, parent in parent_map.items():
            assert parent in code_set, f"Parent '{parent}' of '{child}' not in code set"

    def test_no_self_references(self, sks_deduped):
        newest_by_code, _ = sks_deduped
        parent_map = _build_hierarchy(set(newest_by_code))
        for child, parent in parent_map.items():
            assert child != parent, f"Self-reference: {child}"


class TestEndToEndPipeline:
    """Full pipeline: fetch → dedup → ext rows → codesystem triple (no DB)."""

    def test_full_transform(self, sks_deduped):
        newest_by_code, classifications_by_code = sks_deduped
        ext_rows = _build_ext_rows(newest_by_code, classifications_by_code, TODAY, _Ctx())

        # One row per (code, classification) — at least one per code
        assert len(ext_rows) >= len(newest_by_code)

        # Simulate DB round-trip: ext_rows → tuples matching EXT_COLUMNS
        rows_as_tuples = [tuple(row[col] for col in EXT_COLUMNS) for row in ext_rows]

        concepts, properties, hierarchy = build_sks_codesystem(rows_as_tuples, EXT_COLUMNS)

        # Concepts: one entry per ext row, deduplicating to unique codes
        unique_codes = {c["CODE"] for c in concepts}
        assert unique_codes == set(newest_by_code)

        # Properties: classification per row + inactive for expired codes
        prop_keys = {p["KEY"] for p in properties}
        assert prop_keys == {"classification", "inactive"}

        classification_values = {p["VALUE"] for p in properties if p["KEY"] == "classification"}
        assert classification_values == set(KNOWN_PREFIXES)

        inactive_codes = {p["CODE"] for p in properties if p["KEY"] == "inactive"}
        expected_inactive = {c for c, r in newest_by_code.items() if r["valid_to"] < TODAY}
        assert inactive_codes == expected_inactive

        # Hierarchy references resolve within the concept set
        for h in hierarchy[:1000]:
            assert h["CODE"] in unique_codes
            assert h["PARENT_CODE"] in unique_codes

    def test_known_hierarchy_chain(self, sks_deduped):
        """DA01 should chain up through DA0/DA to D via prefix matching."""
        newest_by_code, _ = sks_deduped
        parent_map = _build_hierarchy(set(newest_by_code))

        assert "DA01" in parent_map
        # Walk up the chain from DA01 — must terminate at a root
        seen = set()
        code = "DA01"
        while code in parent_map:
            assert code not in seen, f"Cycle detected at {code}"
            seen.add(code)
            code = parent_map[code]
        assert len(code) < len("DA01"), "Chain should end at a shorter root code"
