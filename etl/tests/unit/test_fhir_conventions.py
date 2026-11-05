# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tripwires for FHIR conventions: every asset under DSA/FHIR must live in a
sanctioned group with a key that matches the group's naming grammar, and URI
literals (http/https/urn) must live in the constant modules
(utils/fhir/identifier_systems.py, utils/fhir/fhir_extensions.py) — not be
scattered through DSA/FHIR source files. Existing deviations are pinned in
explicit legacy whitelists below; do NOT extend a whitelist to make a new
asset or literal pass — name the asset per the grammar / move the URI to the
constant module instead.
"""

import ast
import re
from pathlib import Path

import dagster as dg
import pytest

pytestmark = pytest.mark.unit

FHIR_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "DSA" / "FHIR"

# Sanctioned groups and the key grammar per group.
SANCTIONED_GROUP_KEY_PATTERNS: dict[str, str] = {
    "fhir_ifdb_daily": r"fhir_[a-z0-9_]+_daily_ifdb",
    "fhir_ifdb_monthly": r"fhir_[a-z0-9_]+_monthly_ifdb",
    "fhir_ifdb_full_load": r"fhir_[a-z0-9_]+_full_load_ifdb",
    "fhir_ifdb_dfm": r"fhir_server_[a-z0-9_]+_dfm",
    # SilkeborgData is a separate source system, so its assets stay out of the
    # IFDB groups and cadence: practitioner_daily_job selects this group and
    # runs archive -> resources -> SOF as one dependency-ordered chain.
    "fhir_silkeborgdata_dfm": r"fhir_server_[a-z0-9_]+_dfm",
    # EPJI likewise, in the same practitioner_daily_job.
    "fhir_epji_dfm": r"fhir_server_[a-z0-9_]+_dfm",
    # Practitioner SOF, shared by SilkeborgData and EPJI, in the same job.
    "fhir_practitioner_sof": r"sof_[a-z0-9_]+_dfm",
    "fhir_sof_daily": r"sof_[a-z0-9_]+_daily",
    "fhir_sof_monthly": r"sof_[a-z0-9_]+_monthly",
    "fhir_sof_dfm": r"sof_[a-z0-9_]+_dfm",
    "fhir_organization_sor": r"fhir_organization_sor|sof_organization",
    "fhir_quality_dfm": r"fhir_quality_check_[a-z0-9_]+",
    "fhir_dw_keys": r"fhir_dw_keys_[a-z0-9_]+",
    "haiba_borger": r"fhir_[a-z0-9_]+_haiba",
    "drg_borger": r"fhir_[a-z0-9_]+_drg",
    "cpr": r"fhir_server_[a-z0-9_]+_cpr_dfm",
    "udredningsret_borger": r"fhir_[a-z0-9_]+_udredningsret",
}

URI_PREFIXES = ("http://", "https://", "urn:")
HL7_TERMINOLOGY_PREFIX = "http://terminology.hl7.org/"

# Pre-existing module-level URI constants in DSA/FHIR, pinned per (file, value).
# New URIs belong in utils/fhir/identifier_systems.py or utils/fhir/fhir_extensions.py.
LEGACY_MODULE_URI_LITERALS = frozenset(
    {
        ("Observation/observation_dict_builder.py", "urn:source:ifdb:aktivitettype_category"),
        (
            "Procedure/Procedure_IFDB_RisYdelse_dfm.py",
            "http://example.org/fhir/CodeSystem/procedure-category",
        ),
        ("Questionnaire/Questionnaire_IFDB.py", "urn:dk:ifdb:questionnaire"),
        ("ServiceRequest/ServiceRequest_IFDB_dfm.py", "urn:source:ifdb:henvisning:type"),
        (
            "ServiceRequest/ServiceRequest_IFDB_dfm.py",
            "http://example.org/fhir/CodeSystem/service-request-category",
        ),
    }
)


def _group_and_key_violations(assets: list) -> list[str]:
    violations = []
    for assets_def in assets:
        if not isinstance(assets_def, dg.AssetsDefinition):
            continue
        for key in assets_def.keys:
            name = key.to_user_string()
            group = assets_def.group_names_by_key[key]
            if group not in SANCTIONED_GROUP_KEY_PATTERNS:
                violations.append(f"{name}: group {group!r} is not a sanctioned FHIR group")
            elif not re.fullmatch(SANCTIONED_GROUP_KEY_PATTERNS[group], name):
                violations.append(f"{name}: key does not match the grammar for group {group!r}")
    return violations


def _uri_literal_violations(source: str, relpath: str) -> list[str]:
    tree = ast.parse(source)
    module_level_ids = set()
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign | ast.AnnAssign):
            for node in ast.walk(stmt):
                if isinstance(node, ast.Constant):
                    module_level_ids.add(id(node))
    violations = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        if not node.value.startswith(URI_PREFIXES):
            continue
        at_module_level = id(node) in module_level_ids
        if at_module_level and node.value.startswith(HL7_TERMINOLOGY_PREFIX):
            continue
        if at_module_level and (relpath, node.value) in LEGACY_MODULE_URI_LITERALS:
            continue
        violations.append(f"{relpath}:{node.lineno}: hardcoded URI literal {node.value!r}")
    return violations


class TestFhirAssetNamingConventions:
    def test_all_fhir_assets_in_sanctioned_groups_with_conforming_keys(self):
        from DSA.FHIR.assets import all_assets

        violations = _group_and_key_violations(all_assets)
        assert not violations, "FHIR asset naming/group violations:\n" + "\n".join(violations)

    def test_guard_flags_unknown_group(self):
        @dg.asset(name="fhir_seeded_bulk_asset", group_name="fhir_ifdb_bulk")
        def seeded():
            return None

        violations = _group_and_key_violations([seeded])
        assert violations and "not a sanctioned FHIR group" in violations[0]

    def test_guard_flags_key_that_breaks_group_grammar(self):
        @dg.asset(name="fhir_seeded_weekly_ifdb", group_name="fhir_ifdb_daily")
        def seeded():
            return None

        violations = _group_and_key_violations([seeded])
        assert violations and "does not match the grammar" in violations[0]

    def test_guard_accepts_conforming_key(self):
        @dg.asset(name="fhir_seeded_daily_ifdb", group_name="fhir_ifdb_daily")
        def seeded():
            return None

        assert _group_and_key_violations([seeded]) == []


class TestNoStrayUriLiterals:
    def test_no_stray_uri_literals_under_dsa_fhir(self):
        violations = []
        for path in sorted(FHIR_PACKAGE_ROOT.rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            relpath = path.relative_to(FHIR_PACKAGE_ROOT).as_posix()
            violations.extend(_uri_literal_violations(path.read_text(encoding="utf-8"), relpath))
        assert not violations, (
            "URI literals outside the constant modules "
            "(move to utils/fhir/identifier_systems.py or fhir_extensions.py):\n"
            + "\n".join(violations)
        )

    def test_scan_flags_inline_uri_literal(self):
        source = 'def build():\n    return {"system": "urn:source:ifdb:seeded"}\n'
        assert _uri_literal_violations(source, "Seeded/Seeded_IFDB.py")

    def test_scan_flags_new_module_level_uri_constant(self):
        source = 'SYSTEM_SEEDED = "http://example.org/fhir/CodeSystem/seeded"\n'
        assert _uri_literal_violations(source, "Seeded/Seeded_IFDB.py")

    def test_scan_allows_module_level_hl7_terminology_constant(self):
        source = '_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ActCode"\n'
        assert _uri_literal_violations(source, "Seeded/Seeded_IFDB.py") == []

    def test_scan_flags_inline_hl7_terminology_literal(self):
        source = 'def f():\n    return "http://terminology.hl7.org/CodeSystem/v3-ActCode"\n'
        assert _uri_literal_violations(source, "Seeded/Seeded_IFDB.py")


class TestPatientAssetsAreRegistered:
    """An asset missing from ``all_assets`` does not exist on the code server."""

    def test_every_verify_asset_in_the_patient_package_is_registered(self):
        import importlib
        import pkgutil

        import dagster as dg

        package = importlib.import_module("DSA.FHIR.Patient")
        registered = {
            a.key.to_user_string() for a in package.all_assets if isinstance(a, dg.AssetsDefinition)
        }
        found: dict[str, str] = {}
        for module in pkgutil.iter_modules(package.__path__):
            m = importlib.import_module(f"DSA.FHIR.Patient.{module.name}")
            for name in dir(m):
                if not name.startswith("verify_"):
                    continue
                obj = getattr(m, name)
                if isinstance(obj, dg.AssetsDefinition):
                    found[obj.key.to_user_string()] = module.name
        missing = {k: v for k, v in found.items() if k not in registered}
        assert not missing, (
            "these verify assets are defined but not in DSA.FHIR.Patient.all_assets, "
            f"so they do not exist on the code server: {missing}"
        )
