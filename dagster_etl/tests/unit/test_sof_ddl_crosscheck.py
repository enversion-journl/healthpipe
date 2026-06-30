# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Cross-check: SOF ViewDefinition JSON columns vs MSSQL DDL columns.

Ensures every column exposed by a SOF view has a matching column in the
MSSQL DDL, preventing MERGE failures during DSO sync. Covers all FHIR
resource types including Observation, Condition, and Procedure.
"""

import json
import re
from pathlib import Path

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS, VIEW_DEFINITIONS_DIR

DDL_PATH = (
    Path(__file__).parents[2] / "infrastructure" / "ddl" / "tables" / "fhir" / "01_tables.sql"
)


def _extract_ddl_columns(ddl_text: str, table_name: str) -> set[str]:
    """Parse DDL text and extract column names for a given [fhir].[TableName] table."""
    pattern = (
        r"CREATE\s+TABLE\s+\[fhir\]\.\["
        + re.escape(table_name)
        + r"\]\s*\((.*?)\)\s*ON\s*\[PRIMARY\]"
    )
    match = re.search(pattern, ddl_text, re.DOTALL | re.IGNORECASE)
    if not match:
        return set()
    block = match.group(1)
    cols: set[str] = set()
    for line in block.splitlines():
        line = line.strip()
        col_match = re.match(r"\[(\w+)\]", line)
        if col_match:
            cols.add(col_match.group(1))
    return cols


def _extract_view_columns(view_name: str) -> set[str]:
    """Load a SOF ViewDefinition JSON and return all column names."""
    path = VIEW_DEFINITIONS_DIR / f"{view_name}.json"
    with open(path) as f:
        view_def = json.load(f)

    def collect(select_blocks: list[dict]) -> set[str]:
        columns: set[str] = set()
        for block in select_blocks:
            for col in block.get("column", []):
                columns.add(col["name"])
            columns.update(collect(block.get("select", [])))
        return columns

    return collect(view_def.get("select", []))


@pytest.fixture(scope="module")
def ddl_text() -> str:
    with open(DDL_PATH) as f:
        return f.read()


AUDIT_COLUMNS = {"last_updated", "created_at", "DW_Timestamp_Insert", "DW_Timestamp_Update"}

VIEW_TABLE_PAIRS = [
    (view_name, cfg["mssql_table"]) for view_name, cfg in SOF_RESOURCE_CONFIGS.items()
]


@pytest.mark.unit
class TestSofDdlCrossCheck:
    """Every SOF view column must have a corresponding DDL column."""

    @pytest.mark.parametrize(
        "view_name,mssql_table", VIEW_TABLE_PAIRS, ids=[v for v, _ in VIEW_TABLE_PAIRS]
    )
    def test_view_columns_subset_of_ddl(self, view_name: str, mssql_table: str, ddl_text: str):
        view_cols = _extract_view_columns(view_name)
        ddl_cols = _extract_ddl_columns(ddl_text, mssql_table)
        assert ddl_cols, f"No DDL columns found for [fhir].[{mssql_table}]"

        ddl_data_cols = ddl_cols - AUDIT_COLUMNS
        missing = view_cols - ddl_data_cols - AUDIT_COLUMNS
        assert (
            not missing
        ), f"SOF view '{view_name}' exposes columns not in DDL [fhir].[{mssql_table}]: {missing}"

    @pytest.mark.parametrize(
        "view_name,mssql_table", VIEW_TABLE_PAIRS, ids=[v for v, _ in VIEW_TABLE_PAIRS]
    )
    def test_ddl_has_audit_columns(self, view_name: str, mssql_table: str, ddl_text: str):
        ddl_cols = _extract_ddl_columns(ddl_text, mssql_table)
        if not ddl_cols:
            pytest.skip(f"No DDL found for [fhir].[{mssql_table}]")
        missing_audit = AUDIT_COLUMNS - ddl_cols
        assert not missing_audit, f"[fhir].[{mssql_table}] missing audit columns: {missing_audit}"

    def test_observation_view_columns_match_ddl(self, ddl_text: str):
        view_cols = _extract_view_columns("observation_view")
        ddl_cols = _extract_ddl_columns(ddl_text, "Observation")
        missing = view_cols - ddl_cols
        assert not missing, f"observation_view columns missing from DDL: {missing}"

    def test_condition_view_columns_match_ddl(self, ddl_text: str):
        view_cols = _extract_view_columns("condition_view")
        ddl_cols = _extract_ddl_columns(ddl_text, "Condition")
        missing = view_cols - ddl_cols
        assert not missing, f"condition_view columns missing from DDL: {missing}"

    def test_condition_view_exposes_verification_status(self):
        path = VIEW_DEFINITIONS_DIR / "condition_view.json"
        with open(path) as f:
            view_def = json.load(f)

        path_by_col = {
            col["name"]: col["path"]
            for block in view_def.get("select", [])
            for col in block.get("column", [])
        }
        assert path_by_col["verification_status"] == "verificationStatus.coding.code.first()"

    def test_procedure_view_columns_match_ddl(self, ddl_text: str):
        view_cols = _extract_view_columns("procedure_view")
        ddl_cols = _extract_ddl_columns(ddl_text, "Procedure")
        missing = view_cols - ddl_cols
        assert not missing, f"procedure_view columns missing from DDL: {missing}"
