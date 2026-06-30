# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for SQL on FHIR ViewDefinition JSON files.

Validates that each ViewDefinition is well-formed and matches the expected
MSSQL table schema.
"""

import json
from pathlib import Path

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    VIEW_DEFINITIONS_DIR,
    load_view_definitions,
)


def _column_names(select_blocks: list[dict]) -> set[str]:
    names: set[str] = set()
    for block in select_blocks:
        for col in block.get("column", []):
            names.add(col["name"])
        names.update(_column_names(block.get("select", [])))
    return names


@pytest.mark.unit
class TestViewDefinitionFiles:
    """Validate ViewDefinition JSON files are well-formed."""

    def test_view_definitions_directory_exists(self):
        """The view_definitions directory should exist and contain JSON files."""
        assert VIEW_DEFINITIONS_DIR.exists(), f"Missing directory: {VIEW_DEFINITIONS_DIR}"
        json_files = list(VIEW_DEFINITIONS_DIR.glob("*.json"))
        assert len(json_files) >= 1, "Expected at least 1 JSON file"
        assert len(json_files) == len(SOF_RESOURCE_CONFIGS), (
            f"JSON file count ({len(json_files)}) does not match "
            f"SOF_RESOURCE_CONFIGS entries ({len(SOF_RESOURCE_CONFIGS)})"
        )

    def test_all_view_definitions_load_successfully(self):
        """All JSON files should parse without errors."""
        view_defs = load_view_definitions()
        assert len(view_defs) == len(SOF_RESOURCE_CONFIGS)

    @pytest.mark.parametrize(
        "json_file",
        sorted(VIEW_DEFINITIONS_DIR.glob("*.json")),
        ids=lambda f: f.stem,
    )
    def test_view_definition_has_required_fields(self, json_file: Path):
        """Each ViewDefinition must have required SQL on FHIR fields."""
        with open(json_file) as f:
            view_def = json.load(f)

        assert (
            view_def.get("resourceType") == "ViewDefinition"
        ), f"{json_file.name}: resourceType must be 'ViewDefinition'"
        assert "name" in view_def, f"{json_file.name}: missing 'name'"
        assert "resource" in view_def, f"{json_file.name}: missing 'resource'"
        assert "status" in view_def, f"{json_file.name}: missing 'status'"
        assert "select" in view_def, f"{json_file.name}: missing 'select'"
        assert len(view_def["select"]) > 0, f"{json_file.name}: 'select' is empty"

    @pytest.mark.parametrize(
        "json_file",
        sorted(VIEW_DEFINITIONS_DIR.glob("*.json")),
        ids=lambda f: f.stem,
    )
    def test_view_definition_columns_have_name_and_path(self, json_file: Path):
        """Each column in every select block must have 'name' and 'path'."""
        with open(json_file) as f:
            view_def = json.load(f)

        for i, select_block in enumerate(view_def["select"]):
            columns = select_block.get("column", [])
            for j, col in enumerate(columns):
                assert "name" in col, f"{json_file.name}: select[{i}].column[{j}] missing 'name'"
                assert "path" in col, f"{json_file.name}: select[{i}].column[{j}] missing 'path'"

    @pytest.mark.parametrize(
        "json_file",
        sorted(VIEW_DEFINITIONS_DIR.glob("*.json")),
        ids=lambda f: f.stem,
    )
    def test_no_duplicate_column_names(self, json_file: Path):
        """Column names must be unique within a ViewDefinition."""
        with open(json_file) as f:
            view_def = json.load(f)

        all_names = []
        for select_block in view_def["select"]:
            for col in select_block.get("column", []):
                all_names.append(col["name"])

        duplicates = [n for n in all_names if all_names.count(n) > 1]
        assert (
            len(set(duplicates)) == 0
        ), f"{json_file.name}: duplicate column names: {set(duplicates)}"

    def test_each_view_definition_has_resource_config(self):
        """Each ViewDefinition should have a corresponding entry in SOF_RESOURCE_CONFIGS."""
        view_defs = load_view_definitions()
        for view_def in view_defs:
            name = view_def["name"]
            assert (
                name in SOF_RESOURCE_CONFIGS
            ), f"ViewDefinition '{name}' has no entry in SOF_RESOURCE_CONFIGS"

    def test_resource_config_has_required_fields(self):
        """Each resource config entry must have all required fields."""
        required_fields = {
            "view_name",
            "resource_table",
            "id_column",
            "mssql_table",
            "primary_keys",
        }
        for name, config in SOF_RESOURCE_CONFIGS.items():
            missing = required_fields - set(config.keys())
            assert not missing, f"Config '{name}' missing fields: {missing}"

    def test_primary_keys_are_emitted_by_view_definition(self):
        """MERGE PK columns must exist in the SOF view output."""
        view_defs = {view_def["name"]: view_def for view_def in load_view_definitions()}
        for name, config in SOF_RESOURCE_CONFIGS.items():
            view_def = view_defs[name]
            view_cols = _column_names(view_def.get("select", []))
            missing = set(config["primary_keys"]) - view_cols
            assert (
                not missing
            ), f"Config '{name}' primary_keys are not emitted by its ViewDefinition: {missing}"


@pytest.mark.unit
class TestIsDeletedExtensionProjected:
    """isDeleted (urn:fhir:isDeleted) must be projected into the DWH for the
    IFDB resources that carry the deletion marker, using the canonical URL.

    Regression guards:
      - patient_view previously read a stale URL
        (http://example.org/.../isDeleted) that nothing writes, so
        fhir.Patient.is_deleted was always NULL.
      - encounter_view / episode_of_care_view previously had no is_deleted
        column, so the soft-delete only surfaced via status=entered-in-error.
    """

    _CANONICAL_URL = "urn:fhir:isDeleted"
    _STALE_URL = "example.org/fhir/StructureDefinition/isDeleted"

    def _load(self, name: str) -> dict:
        with open(VIEW_DEFINITIONS_DIR / f"{name}.json") as f:
            return json.load(f)

    @pytest.mark.parametrize(
        "view_name",
        ["patient_view", "encounter_view", "episode_of_care_view"],
    )
    def test_view_projects_is_deleted_with_canonical_url(self, view_name: str):
        view_def = self._load(view_name)
        cols = {
            c["name"]: c["path"] for block in view_def["select"] for c in block.get("column", [])
        }
        assert "is_deleted" in cols, f"{view_name} missing is_deleted column"
        assert self._CANONICAL_URL in cols["is_deleted"], (
            f"{view_name} is_deleted must read {self._CANONICAL_URL!r}, "
            f"got: {cols['is_deleted']}"
        )

    @pytest.mark.parametrize(
        "json_file",
        sorted(VIEW_DEFINITIONS_DIR.glob("*.json")),
        ids=lambda f: f.stem,
    )
    def test_no_view_uses_stale_isdeleted_url(self, json_file: Path):
        """No ViewDefinition may reference the stale isDeleted URL that nothing
        writes (would silently produce NULL)."""
        assert self._STALE_URL not in json_file.read_text(), (
            f"{json_file.name} references the stale isDeleted URL "
            f"({self._STALE_URL}); use {self._CANONICAL_URL}"
        )
