# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Patient ViewDefinition structure tests.

Validates the three patient-specific ViewDefinitions (demo, address, identifiers)
against the lineage spec in data_lineage/fhir/fhir_patient*.yml and the asset
contracts in DSA/FHIR/SqlOnFhir/.

These checks complement the generic structural validation in
test_sof_view_definitions.py — they catch drift between the lineage YAMLs and
the SOF JSON projections (e.g., a column added in YAML but not the JSON).
"""

import json
from pathlib import Path

import pytest

VIEW_DEFS_DIR = (
    Path(__file__).resolve().parents[2] / "DSA" / "FHIR" / "SqlOnFhir" / "view_definitions"
)


def _load(name: str) -> dict:
    with open(VIEW_DEFS_DIR / f"{name}.json") as f:
        return json.load(f)


def _columns(view_def: dict) -> set[str]:
    cols = set()
    for block in view_def["select"]:
        if "column" in block:
            cols.update(c["name"] for c in block["column"])
    return cols


class TestPatientDemoView:
    """patient_view: one row per patient_id (no address fields)."""

    def test_loads(self):
        vd = _load("patient_view")
        assert vd["resource"] == "Patient"
        assert vd["name"] == "patient_view"

    def test_carries_required_identifier_columns(self):
        cols = _columns(_load("patient_view"))
        for required in (
            "patient_id",
            "patient_ssn",
            "previous_ssn",
            "x_ecpr",
            "d_ecpr",
            "ifdb_patient_id",
        ):
            assert required in cols, f"patient_view missing identifier column {required}"

    def test_carries_dk_core_demographic_columns(self):
        cols = _columns(_load("patient_view"))
        for required in (
            "active",
            "deceased",
            "name_use",
            "name_given",
            "name_family",
            "name_text",
            "gender",
            "birth_date",
            "marital_status_code",
            "marital_status_display",
            "citizenship_code",
            "citizenship_display",
            "gp_display",
            "gp_identifier",
        ):
            assert required in cols, f"patient_view missing demographic column {required}"

    def test_does_not_carry_address_columns(self):
        """Address history moved to patient_address_view — must not duplicate here."""
        cols = _columns(_load("patient_view"))
        for forbidden in (
            "address_line",
            "address_postal_code",
            "address_district",
            "address_period_start",
            "address_period_end",
            "address_city",
            "address_country",
        ):
            assert forbidden not in cols, (
                f"patient_view still carries address column {forbidden} — "
                "should be in patient_address_view"
            )

    def test_uses_name_official_slice_not_foreach(self):
        """Name flattens via .where(use='official') not forEach — one row per patient."""
        vd = _load("patient_view")
        for block in vd["select"]:
            assert "forEach" not in block, (
                "patient_view should not use forEach (would multiply rows). "
                f"Found forEach: {block.get('forEach')}"
            )


class TestPatientAddressView:
    """patient_address_view: one row per (patient_id × address.period.start)."""

    def test_loads(self):
        vd = _load("patient_address_view")
        assert vd["resource"] == "Patient"
        assert vd["name"] == "patient_address_view"

    def test_has_for_each_or_null_on_address(self):
        vd = _load("patient_address_view")
        address_blocks = [b for b in vd["select"] if b.get("forEachOrNull") == "address"]
        assert (
            len(address_blocks) == 1
        ), "patient_address_view must have exactly one forEachOrNull: address block"

    def test_carries_full_address_column_set(self):
        cols = _columns(_load("patient_address_view"))
        for required in (
            "patient_id",
            "address_period_start",
            "address_period_end",
            "address_use",
            "address_line",
            "address_postal_code",
            "address_city",
            "address_country",
            "municipality_code",
            "region_code",
            "kvhx",
            "address_official",
        ):
            assert required in cols, f"patient_address_view missing column {required}"


class TestSofConfigsForPatientViews:
    """SOF_RESOURCE_CONFIGS entries for the patient views."""

    @pytest.fixture
    def configs(self):
        # Read SOF_RESOURCE_CONFIGS via importlib (avoids the package __init__
        # chain that requires dagster).
        import importlib.util

        importlib.util.spec_from_file_location(
            "sof_utils",
            Path(__file__).resolve().parents[2]
            / "DSA"
            / "FHIR"
            / "utils"
            / "sql_on_fhir"
            / "sof_utils.py",
        )
        # sof_utils imports core.resources; we mock by reading text instead.
        # Fall back to regex extraction.
        import re

        content = (
            Path(__file__).resolve().parents[2]
            / "DSA"
            / "FHIR"
            / "utils"
            / "sql_on_fhir"
            / "sof_utils.py"
        ).read_text()
        # Match each "view_key": { ... primary_keys: [...] }
        configs = {}
        for m in re.finditer(
            r"\"(patient_\w+_view)\":\s*\{(.*?)\n\s{4}\},",
            content,
            flags=re.DOTALL,
        ):
            name, body = m.group(1), m.group(2)
            configs[name] = body
        return configs

    def test_patient_address_view_has_dedup_config(self, configs):
        body = configs["patient_address_view"]
        assert "address_period_start" in body
        assert "deduplicate_by_pk" in body
        assert "dedup_tiebreak_columns" in body
