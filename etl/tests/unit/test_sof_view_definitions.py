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

    def test_normalized_datetime_columns_are_emitted_by_view_definition(self):
        """A column normalised at load must actually be produced by the view.

        A typo here fails silently in the worst way: the column is never
        rewritten, so a raw FHIR dateTime string is staged into a DATETIME2
        target column and the load dies on a conversion error -- or worse, for a
        key column, the DWH ends up keying on the string form again.
        """
        view_defs = {view_def["name"]: view_def for view_def in load_view_definitions()}
        for name, config in SOF_RESOURCE_CONFIGS.items():
            normalized = set(config.get("normalized_datetime_columns") or [])
            if not normalized:
                continue
            view_cols = _column_names(view_defs[name].get("select", []))
            missing = normalized - view_cols
            assert not missing, (
                f"Config '{name}' normalizes column(s) its ViewDefinition does "
                f"not emit: {missing}"
            )


@pytest.mark.unit
class TestIsDeletedExtensionProjected:
    """isDeleted (urn:fhir:isDeleted) is projected for IFDB resources that
    carry the deletion marker, using the canonical URL.
    """

    _CANONICAL_URL = "urn:fhir:isDeleted"
    _STALE_URL = "regionsyddanmark.dk/fhir/StructureDefinition/isDeleted"

    def _load(self, name: str) -> dict:
        with open(VIEW_DEFINITIONS_DIR / f"{name}.json") as f:
            return json.load(f)

    @pytest.mark.parametrize(
        "view_name",
        [
            "patient_view",
            "encounter_view",
            "episode_of_care_view",
            "condition_view",
            "servicerequest_view",
            "appointment_view",
            "documentreference_view",
            "observation_view",
            "procedure_view",
        ],
    )
    def test_view_projects_is_deleted_with_canonical_url(self, view_name: str):
        view_def = self._load(view_name)
        cols = {
            c["name"]: c["path"] for block in view_def["select"] for c in block.get("column", [])
        }
        assert "dw_is_deleted" in cols, f"{view_name} missing dw_is_deleted column"
        assert self._CANONICAL_URL in cols["dw_is_deleted"], (
            f"{view_name} dw_is_deleted must read {self._CANONICAL_URL!r}, "
            f"got: {cols['dw_is_deleted']}"
        )

    @pytest.mark.parametrize(
        "view_name",
        sorted(SOF_RESOURCE_CONFIGS),
    )
    def test_projected_is_deleted_has_absent_extension_fallback(self, view_name: str):
        """A view that projects dw_is_deleted must also declare the coalesce-to-0
        fallback, or the column lands NULL whenever the resource carries no
        isDeleted extension -- i.e. for every row that was never deleted.

        The flag is NOT NULL on every fhir.* table, so a missing
        fallback fails the MERGE outright rather than degrading quietly.
        """
        view_def = self._load(view_name)
        if "dw_is_deleted" not in _column_names(view_def["select"]):
            pytest.skip(f"{view_name} does not project dw_is_deleted")

        fallback = SOF_RESOURCE_CONFIGS[view_name].get("boolean_false_default_columns") or []
        assert "dw_is_deleted" in fallback, (
            f"{view_name} projects dw_is_deleted but its SOF_RESOURCE_CONFIGS entry "
            f"does not list it in boolean_false_default_columns; an absent "
            f"isDeleted extension would write NULL into a NOT NULL column."
        )

    def test_every_flag_bearing_table_projects_or_resurrects(self):
        """Each fhir.* table must reach dw_is_deleted = 0 by exactly one route.

        Either the view projects the flag and the load coalesces an absent
        extension to 0 (boolean_false_default_columns), or the view does not
        project it and the MERGE clears it on a match (resurrect_soft_deleted).

        Neither route means a row that dqa_fhir_server_cleanup retired can never
        come back: the MERGE never writes the column, so dw_is_deleted stays 1
        and the row is invisible to every `= 0` consumer forever. Both routes at
        once means the flag is written twice with different semantics.
        """
        offenders = {}
        for view_name, cfg in SOF_RESOURCE_CONFIGS.items():
            # Read the ViewDefinition, not the config: a view that does not emit
            # the column but lists it in boolean_false_default_columns would
            # satisfy a config-only XOR while writing nothing at all.
            emits = "dw_is_deleted" in _column_names(self._load(view_name)["select"])
            declared = "dw_is_deleted" in (cfg.get("boolean_false_default_columns") or [])
            resurrects = bool(cfg.get("resurrect_soft_deleted"))

            if declared and not emits:
                offenders[view_name] = "declares fallback but view emits no dw_is_deleted"
            elif emits == resurrects:
                offenders[view_name] = "both" if emits else "neither"
            elif emits and not declared:
                offenders[view_name] = "projects dw_is_deleted without the coalesce fallback"

        assert not offenders, (
            "each view must either emit dw_is_deleted AND declare it in "
            "boolean_false_default_columns, or not emit it and set "
            f"resurrect_soft_deleted: {offenders}"
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
