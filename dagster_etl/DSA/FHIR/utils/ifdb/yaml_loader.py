# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
YAML loader utility for parsing FHIR IFDB model definitions.

Loads and caches the fhir.ifdb.yml file to extract column mappings and resource definitions.
"""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def _get_yaml_path() -> Path:
    """Get the path to fhir.ifdb.yml relative to this file."""
    # File structure: DSA/FHIR/utils/ifdb/yaml_loader.py
    # YAML location: model/fhir.ifdb.yml
    current_file = Path(__file__).resolve()
    # Go up: utils -> IFDB -> FHIR -> DSA -> dagster_etl
    project_root = current_file.parents[4]
    return project_root / "model" / "fhir.ifdb.yml"


@lru_cache(maxsize=1)
def load_fhir_ifdb_yaml() -> dict[str, Any]:
    """
    Load and parse the fhir.ifdb.yml file.
    Results are cached after first load.
    """
    yaml_path = _get_yaml_path()
    if not yaml_path.exists():
        raise FileNotFoundError(
            f"fhir.ifdb.yml not found at {yaml_path}. " "Ensure the model file exists in model/"
        )

    with open(yaml_path, encoding="utf-8") as f:
        content = yaml.safe_load(f)

    return content.get("fhir", {})


def get_adapter_definition(adapter_name: str) -> dict[str, Any] | None:
    """Get adapter definition by name from YAML."""
    yaml_data = load_fhir_ifdb_yaml()
    adapters = yaml_data.get("adapters", {})
    return adapters.get(adapter_name)


def get_canonical_definition(resource_name: str) -> dict[str, Any] | None:
    """Get canonical resource definition by name from YAML."""
    yaml_data = load_fhir_ifdb_yaml()
    canonical = yaml_data.get("canonical", {})
    return canonical.get(resource_name)


def get_source_tables(adapter_name: str) -> list[str]:
    """Get source table names for an adapter."""
    adapter = get_adapter_definition(adapter_name)
    if not adapter:
        return []
    return adapter.get("source_tables", [])


def get_column_mappings(resource_name: str) -> dict[str, dict[str, Any]]:
    """Get column mappings for a canonical resource."""
    canonical = get_canonical_definition(resource_name)
    if not canonical:
        return {}
    return canonical.get("columns", {})


def parse_column_from_path(column_from: str) -> tuple[str, str]:
    """
    Parse a column 'from' path like "IFDB.ENCOUNTER.OBJECT_ID" into (schema.table, column).

    Returns:
        (table_path, column_name) where table_path may include schema prefix
    """
    parts = column_from.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid column path format: {column_from}")

    column_name = parts[-1]
    table_path = ".".join(parts[:-1])
    return (table_path, column_name)


def is_reference_column(column_def: dict[str, Any]) -> bool:
    """Check if a column definition is a reference type."""
    return "ref" in column_def


def get_reference_type(column_def: dict[str, Any]) -> str | None:
    """Get the reference type (e.g., 'Patient', 'EpisodeOfCare') from a column definition."""
    return column_def.get("ref")


def is_derived_column(column_def: dict[str, Any]) -> bool:
    """Check if a column is derived (from: "Derived")."""
    column_from = column_def.get("from", "")
    return isinstance(column_from, str) and column_from.strip().lower() == "derived"


def get_partition_column(resource_name: str) -> str | None:
    """
    Get the partition column name (usually 'last_updated') for a canonical resource.
    Looks for columns with type 'timestamp' that contain 'update' or 'modification' in name.
    """
    columns = get_column_mappings(resource_name)

    # Look for common partition column names
    for _col_name, col_def in columns.items():
        col_from = col_def.get("from", "")
        col_type = col_def.get("type", "")

        # Check if it's a timestamp and contains update/modification keywords
        if (
            col_type == "timestamp"
            and isinstance(col_from, str)
            and ("update" in col_from.lower() or "modification" in col_from.lower())
        ):
            # Extract just the column name from the path
            _, column = parse_column_from_path(col_from)
            return column

    return None


__all__ = [
    "load_fhir_ifdb_yaml",
    "get_adapter_definition",
    "get_canonical_definition",
    "get_source_tables",
    "get_column_mappings",
    "parse_column_from_path",
    "is_reference_column",
    "get_reference_type",
    "is_derived_column",
    "get_partition_column",
]
