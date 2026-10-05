# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import textwrap

import pytest
from data_lineage.tools.lineage_ddl_check.checker import ColumnFinding, check
from data_lineage.tools.lineage_ddl_check.ddl_parser import DdlTable
from data_lineage.tools.lineage_ddl_check.lineage_reader import LineageTable, read_lineage

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Adversarial edge cases for lineage_reader.read_lineage and checker.check.
#
# All cases assert the verifier's real (correct, by-design) behaviour. Two
# behaviours are explicitly LIMITATIONS rather than bugs and are documented as
# such inline:
#   * duplicate YAML column keys collapse silently (a YAML mapping property),
#   * an explicit empty `Columns: {}` is accepted (but the checker then flags
#     every DDL column, so the gate still fails — just via a different category).
# ---------------------------------------------------------------------------

SCHEMAS = ("dim", "fct", "fhir")


def _write(tmp_path, schema, filename, text, *, dedent=True, encoding="utf-8"):
    d = tmp_path / schema
    d.mkdir(parents=True, exist_ok=True)
    body = textwrap.dedent(text) if dedent else text
    (d / filename).write_text(body, encoding=encoding)


def _ddl(name, cols):
    return DdlTable(name=name, columns=tuple(cols), source_file="x.sql")


def _lin(name, cols):
    return LineageTable(name=name, columns=tuple(cols), source_file="x.yml")


# --------------------------------------------------------------------------- #
# reader edge cases
# --------------------------------------------------------------------------- #


def test_duplicate_column_keys_yaml_keeps_last_count_drops(tmp_path):
    # LIMITATION: PyYAML keeps the last value for a repeated mapping key, so the
    # reader silently collapses A/B/A into two columns with no malformed report.
    _write(
        tmp_path,
        "dim",
        "dup.yml",
        """
        dso_lineage:
          TableName: dim.Dup
          Columns:
            A: { DataType: INT }
            B: { DataType: INT }
            A: { DataType: NVARCHAR(10) }
        """,
    )
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert malformed == []
    assert tables["dim.Dup"].columns == ("A", "B")
    assert len(tables["dim.Dup"].columns) == 2


def test_columns_null_is_malformed(tmp_path):
    _write(tmp_path, "dim", "n.yml", "dso_lineage:\n  TableName: dim.N\n  Columns:\n")
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert tables == {}
    assert any("Columns" in m for m in malformed)


def test_columns_empty_mapping_is_accepted_with_no_columns(tmp_path):
    # LIMITATION: an explicit empty mapping is a valid dict -> NOT malformed.
    _write(tmp_path, "dim", "e.yml", "dso_lineage:\n  TableName: dim.E\n  Columns: {}\n")
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert malformed == []
    assert tables["dim.E"].columns == ()


def test_columns_empty_mapping_still_fails_gate_via_checker(tmp_path):
    # The empty-Columns:{} limitation is not swallowed end-to-end: the checker
    # reports every DDL column under columns_ddl_not_lineage, so the gate fails.
    _write(tmp_path, "dim", "e.yml", "dso_lineage:\n  TableName: dim.E\n  Columns: {}\n")
    lineage, malformed = read_lineage(tmp_path, SCHEMAS)
    report = check({"dim.E": _ddl("dim.E", ["K", "V"])}, lineage, malformed=malformed)
    assert report.columns_ddl_not_lineage == [
        ColumnFinding("dim.E", "K"),
        ColumnFinding("dim.E", "V"),
    ]
    assert report.has_findings


def test_columns_as_list_is_malformed(tmp_path):
    _write(
        tmp_path,
        "dim",
        "l.yml",
        """
        dso_lineage:
          TableName: dim.L
          Columns:
            - A
            - B
        """,
    )
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert tables == {}
    assert any("Columns" in m for m in malformed)


def test_missing_dso_lineage_is_malformed(tmp_path):
    _write(tmp_path, "dim", "x.yml", "something_else:\n  foo: bar\n")
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert tables == {}
    assert any("dso_lineage" in m for m in malformed)


def test_extra_unknown_top_level_keys_are_ignored(tmp_path):
    _write(
        tmp_path,
        "dim",
        "extra.yml",
        """
        version: 7
        owner: data-platform
        dso_lineage:
          TableName: dim.Extra
          Columns:
            A: {}
        notes: anything
        """,
    )
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert malformed == []
    assert tables["dim.Extra"].columns == ("A",)


def test_utf8_bom_at_file_start_is_handled(tmp_path):
    raw = "﻿dso_lineage:\n  TableName: dim.Bom\n  Columns:\n    A: {}\n"
    _write(tmp_path, "dim", "bom.yml", raw, dedent=False)
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert malformed == []
    assert "dim.Bom" in tables
    assert tables["dim.Bom"].columns == ("A",)


def test_tablename_schema_outside_targets_uses_declared_name(tmp_path):
    # The directory only selects the glob; TableName is trusted verbatim. The
    # checker (not the reader) decides whether the declared name matches DDL.
    _write(
        tmp_path,
        "dim",
        "weird.yml",
        """
        dso_lineage:
          TableName: staging.Weird
          Columns:
            A: {}
        """,
    )
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert malformed == []
    assert "staging.Weird" in tables


def test_two_files_same_tablename_is_malformed_duplicate(tmp_path):
    _write(
        tmp_path,
        "dim",
        "a_first.yml",
        """
        dso_lineage:
          TableName: dim.Twin
          Columns:
            A: {}
        """,
    )
    _write(
        tmp_path,
        "dim",
        "b_second.yml",
        """
        dso_lineage:
          TableName: dim.Twin
          Columns:
            B: {}
        """,
    )
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert tables["dim.Twin"].columns == ("A",)  # first (sorted) wins
    assert any("duplicate" in m.lower() and "dim.Twin" in m for m in malformed)


def test_empty_file_is_malformed_missing_dso_lineage(tmp_path):
    _write(tmp_path, "dim", "empty.yml", "", dedent=False)
    tables, malformed = read_lineage(tmp_path, SCHEMAS)
    assert tables == {}
    assert any("dso_lineage" in m for m in malformed)


# --------------------------------------------------------------------------- #
# checker edge cases
# --------------------------------------------------------------------------- #


def test_report_lists_are_sorted_and_deterministic():
    ddl = {
        "dim.C": _ddl("dim.C", ["K"]),
        "dim.A": _ddl("dim.A", ["K"]),
        "dim.B": _ddl("dim.B", ["K"]),
    }
    report = check(ddl, {})
    assert report.tables_missing_lineage == ["dim.A", "dim.B", "dim.C"]


def test_column_findings_sorted_within_table():
    report = check(
        {"dim.A": _ddl("dim.A", ["Zeta", "Alpha", "Mu"])},
        {"dim.A": _lin("dim.A", [])},
    )
    assert [f.column for f in report.columns_ddl_not_lineage] == ["Alpha", "Mu", "Zeta"]


def test_lineage_missing_ddl_sorted():
    report = check(
        {},
        {"dim.Z": _lin("dim.Z", ["K"]), "dim.A": _lin("dim.A", ["K"])},
    )
    assert report.lineage_missing_ddl == ["dim.A", "dim.Z"]


def test_system_columns_apply_only_to_ddl_not_lineage_direction():
    # A system column present in lineage-but-not-DDL is STILL flagged.
    report = check(
        {"dim.A": _ddl("dim.A", ["K"])},
        {"dim.A": _lin("dim.A", ["K", "dw_timestamp_insert"])},
        system_columns=frozenset({"dw_timestamp_insert"}),
    )
    assert report.columns_ddl_not_lineage == []
    assert report.columns_lineage_not_ddl == [ColumnFinding("dim.A", "dw_timestamp_insert")]
    assert report.has_findings


def test_ignored_tables_apply_only_to_tables_missing_lineage():
    # An ignored table that DOES have lineage still has its columns compared.
    report = check(
        {"fhir.Store": _ddl("fhir.Store", ["K", "Extra"])},
        {"fhir.Store": _lin("fhir.Store", ["K"])},
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert report.tables_missing_lineage == []
    assert report.columns_ddl_not_lineage == [ColumnFinding("fhir.Store", "Extra")]
    assert report.has_findings


def test_ignored_tables_does_not_suppress_lineage_missing_ddl():
    report = check(
        {},
        {"fhir.Store": _lin("fhir.Store", ["K"])},
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert report.lineage_missing_ddl == ["fhir.Store"]
    assert report.has_findings


def test_ignored_table_with_lineage_file_clean_match_is_fine():
    # Redundant ignore entry on a fully-consistent table is harmless.
    report = check(
        {"fhir.Store": _ddl("fhir.Store", ["K", "V"])},
        {"fhir.Store": _lin("fhir.Store", ["K", "V"])},
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert not report.has_findings


def test_malformed_makes_has_findings_true_even_with_clean_tables():
    report = check(
        {"dim.A": _ddl("dim.A", ["K"])},
        {"dim.A": _lin("dim.A", ["K"])},
        malformed=["dim/bad.yml: missing or invalid 'Columns' mapping"],
    )
    assert report.malformed_lineage == ["dim/bad.yml: missing or invalid 'Columns' mapping"]
    assert report.has_findings


def test_case_only_column_difference_flagged_both_directions():
    report = check(
        {"dim.A": _ddl("dim.A", ["Key"])},
        {"dim.A": _lin("dim.A", ["key"])},
    )
    assert report.columns_ddl_not_lineage == [ColumnFinding("dim.A", "Key")]
    assert report.columns_lineage_not_ddl == [ColumnFinding("dim.A", "key")]
    assert report.has_findings


def test_check_does_not_mutate_caller_malformed_list():
    src = ["dim/bad.yml: boom"]
    report = check({}, {}, malformed=src)
    report.malformed_lineage.append("mutated")
    assert src == ["dim/bad.yml: boom"]
