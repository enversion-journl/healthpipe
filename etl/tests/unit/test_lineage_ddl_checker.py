# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import pytest
from data_lineage.tools.lineage_ddl_check.checker import ColumnFinding, Report, check
from data_lineage.tools.lineage_ddl_check.ddl_parser import DdlTable
from data_lineage.tools.lineage_ddl_check.lineage_reader import LineageTable

pytestmark = pytest.mark.unit


def _ddl(name, cols):
    return DdlTable(name=name, columns=tuple(cols), source_file="x.sql")


def _lin(name, cols):
    return LineageTable(name=name, columns=tuple(cols), source_file="x.yml")


def _lin_specs(name, specs):
    """LineageTable carrying per-column annotation dicts (for the FK-annotation check)."""
    return LineageTable(name=name, columns=tuple(specs), source_file="x.yml", column_specs=specs)


def test_clean_match_has_no_findings():
    report = check({"dim.A": _ddl("dim.A", ["K", "V"])}, {"dim.A": _lin("dim.A", ["K", "V"])})
    assert isinstance(report, Report)
    assert not report.has_findings


def test_table_missing_lineage():
    report = check({"dim.A": _ddl("dim.A", ["K"])}, {})
    assert report.tables_missing_lineage == ["dim.A"]
    assert report.has_findings


def test_ignored_table_not_flagged():
    report = check(
        {"fhir.Store": _ddl("fhir.Store", ["K"])},
        {},
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert report.tables_missing_lineage == []
    assert not report.has_findings


def test_lineage_missing_ddl():
    report = check({}, {"dim.Ghost": _lin("dim.Ghost", ["K"])})
    assert report.lineage_missing_ddl == ["dim.Ghost"]
    assert report.has_findings


def test_column_in_ddl_not_lineage():
    report = check(
        {"dim.A": _ddl("dim.A", ["K", "Extra"])},
        {"dim.A": _lin("dim.A", ["K"])},
    )
    assert report.columns_ddl_not_lineage == [ColumnFinding("dim.A", "Extra")]


def test_system_column_not_flagged():
    report = check(
        {"dim.A": _ddl("dim.A", ["K", "dw_timestamp_insert"])},
        {"dim.A": _lin("dim.A", ["K"])},
        system_columns=frozenset({"dw_timestamp_insert"}),
    )
    assert report.columns_ddl_not_lineage == []
    assert not report.has_findings


def test_column_in_lineage_not_ddl():
    report = check(
        {"dim.A": _ddl("dim.A", ["K"])},
        {"dim.A": _lin("dim.A", ["K", "Phantom"])},
    )
    assert report.columns_lineage_not_ddl == [ColumnFinding("dim.A", "Phantom")]


def test_case_sensitive_column_mismatch_is_flagged():
    report = check(
        {"dim.A": _ddl("dim.A", ["Key"])},
        {"dim.A": _lin("dim.A", ["key"])},
    )
    assert report.columns_ddl_not_lineage == [ColumnFinding("dim.A", "Key")]
    assert report.columns_lineage_not_ddl == [ColumnFinding("dim.A", "key")]


def test_malformed_counts_as_finding():
    report = check({}, {}, malformed=["x.yml: missing 'Columns' mapping"])
    assert report.has_findings


def test_fhir_id_without_fk_annotation_is_flagged():
    lin = {
        "fhir.T": _lin_specs(
            "fhir.T", {"t_id": {"Type": "PK"}, "patient_id": {"DataType": "NVARCHAR(255)"}}
        )
    }
    report = check({"fhir.T": _ddl("fhir.T", ["t_id", "patient_id"])}, lin)
    assert report.columns_missing_fk_annotation == [ColumnFinding("fhir.T", "patient_id")]
    assert report.has_findings


def test_fhir_id_with_pk_fk_target_or_fk_none_not_flagged():
    specs = {
        "t_id": {"Type": "PK"},  # own id
        "patient_id": {"FK-Target": "fhir.Patient"},  # resolved reference
        "ifdb_x_id": {"FK-None": True},  # explicit opt-out
    }
    lin = {"fhir.T": _lin_specs("fhir.T", specs)}
    report = check({"fhir.T": _ddl("fhir.T", list(specs))}, lin)
    assert report.columns_missing_fk_annotation == []


def test_dwh_dw_ek_self_is_pk_and_non_pk_requires_annotation():
    specs = {
        "dw_ek_Faktura": {},  # dw_ek_<self> = the fact PK -> exempt
        "dw_ek_Dato_faktura": {"Type": "FK"},  # role FK, naming resolves -> ok
        "dw_ek_Kreditor": {},  # non-PK dw_ek_, unannotated -> flagged
    }
    lin = {"fct.Faktura": _lin_specs("fct.Faktura", specs)}
    report = check({"fct.Faktura": _ddl("fct.Faktura", list(specs))}, lin)
    assert report.columns_missing_fk_annotation == [ColumnFinding("fct.Faktura", "dw_ek_Kreditor")]


def test_relationship_check_only_applies_to_fhir_and_dwh_schemas():
    # A *_id in some other schema (not fhir/dim/fct) is not a relationship column here.
    lin = {"stg.T": _lin_specs("stg.T", {"foo_id": {"DataType": "INT"}})}
    report = check({"stg.T": _ddl("stg.T", ["foo_id"])}, lin)
    assert report.columns_missing_fk_annotation == []


def test_relationship_check_skips_ignored_tables():
    lin = {"fhir.Store": _lin_specs("fhir.Store", {"patient_id": {"DataType": "NVARCHAR(255)"}})}
    report = check(
        {"fhir.Store": _ddl("fhir.Store", ["patient_id"])},
        lin,
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert report.columns_missing_fk_annotation == []


def test_fk_target_must_match_a_lineage_table_case_insensitively():
    specs = {
        "id": {"Type": "PK"},
        "part_of_id": {"FK-Target": "fhir.organization"},  # self-reference, other case
        "location_id": {"FK-Target": "fhir.Location"},
        "bed_id": {"FK-Target": "fhir.Bed"},
        "dato_id": {"FK-Target": "dim.Dato"},
        "empty_id": {"FK-Target": None},
    }
    tables = {
        "fhir.Organization": _lin_specs("fhir.Organization", specs),
        "fhir.Location": _lin("fhir.Location", ["id"]),
        "dim.Dato": _lin("dim.Dato", ["id"]),
    }
    ddl = {name: _ddl(name, table.columns) for name, table in tables.items()}
    report = check(ddl, tables)
    assert report.unresolved_fk_targets == [
        "fhir.Organization.bed_id: 'fhir.Bed'",
        "fhir.Organization.dato_id: 'dim.Dato'",
        "fhir.Organization.empty_id: 'None'",
    ]
    assert report.has_findings


def test_render_returns_str_with_result_line():
    report = check({"dim.A": _ddl("dim.A", ["K"])}, {})
    text = report.render()
    assert isinstance(text, str)
    assert "RESULT: FAIL" in text
    assert "dim.A" in text


# ---------------------------------------------------------------------------
# Source-reference validation
# ---------------------------------------------------------------------------

_KREDITOR_SYSTEMS = {
    "sydax": ("Kreditor.Kreditor", "Finans.Finanspostering"),
    "sor": ("SOR_registry",),
}


def _lin_sources(name, specs, source_systems):
    """LineageTable carrying column specs AND declared SourceSystems."""
    return LineageTable(
        name=name,
        columns=tuple(specs),
        source_file="x.yml",
        column_specs=specs,
        source_systems=source_systems,
    )


def _source_report(specs, source_systems=_KREDITOR_SYSTEMS, name="dim.Kreditor"):
    lin = {name: _lin_sources(name, specs, source_systems)}
    return check({name: _ddl(name, list(specs))}, lin)


def test_declared_four_part_source_ref_passes():
    report = _source_report(
        {"cvrnr": {"Source": ["sydax.Kreditor.Kreditor.JuridiskEnhed"]}},
    )
    assert report.invalid_source_refs == []
    assert not report.has_findings


def test_schemaless_registry_source_ref_passes():
    # Registry-style sources (SOR_registry) are declared without a schema and
    # give 3-part refs — still resolvable against SourceSystems.
    report = _source_report({"code_sor": {"Source": ["sor.SOR_registry.CodeSOR"]}})
    assert report.invalid_source_refs == []


def test_hierarchy_range_shorthand_passes():
    report = _source_report(
        {"s1": {"Source": ["sydax.Finans.OmkostningsstedHierarki.nummer1..nummer20"]}},
        source_systems={"sydax": ("Finans.OmkostningsstedHierarki",)},
    )
    assert report.invalid_source_refs == []


def test_dwh_layer_prefix_is_rejected():
    # Seeded defect: Source pointing at a DWH-computed table instead of the source.
    for ref in (
        "map.Leverandoervare.bk_kreditor",
        "dim.Kreditor.cvrnr",
        "codesystem.LagerHierarchy.parent_code",
    ):
        report = _source_report({"c": {"Source": [ref]}})
        assert len(report.invalid_source_refs) == 1, ref
        assert "DWH-computed layer" in report.invalid_source_refs[0]
        assert report.has_findings


def test_undeclared_system_is_rejected():
    report = _source_report({"c": {"Source": ["mysupply.dbo.metadata.LeverandorCVR"]}})
    assert len(report.invalid_source_refs) == 1
    assert "not declared in SourceSystems" in report.invalid_source_refs[0]


def test_undeclared_table_is_rejected():
    # Seeded defect from fct_postering: table Finans.Kreditor never existed in sydax.
    report = _source_report({"c": {"Source": ["sydax.Finans.Kreditor.JuridiskEnhed"]}})
    assert len(report.invalid_source_refs) == 1
    assert "not declared under 'sydax'" in report.invalid_source_refs[0]


def test_table_casing_drift_is_rejected():
    # Declared 'Finans.Art' but referenced as 'Finans.art' — matching is case-sensitive.
    report = _source_report(
        {"a1": {"Source": ["sydax.Finans.art.A1"]}},
        source_systems={"sydax": ("Finans.Art",)},
    )
    assert len(report.invalid_source_refs) == 1


def test_missing_column_part_is_rejected():
    report = _source_report({"c": {"Source": ["sydax.Kreditor.Kreditor"]}})
    assert len(report.invalid_source_refs) == 1
    assert "missing column" in report.invalid_source_refs[0]


def test_legacy_coalesce_key_is_rejected():
    report = _source_report({"cvrnr": {"coalesce": ["sydax.Kreditor.Kreditor.JuridiskEnhed"]}})
    assert len(report.invalid_source_refs) == 1
    assert "coalesce" in report.invalid_source_refs[0]


def test_hyphenated_source_column_passes():
    # Real MySupply columns contain hyphens (e.g. KundeReferenceE-Mail).
    report = _source_report(
        {"c": {"Source": ["mysupply.dbo.statistic_invoice_header.KundeReferenceE-Mail"]}},
        source_systems={"mysupply": ("dbo.statistic_invoice_header",)},
    )
    assert report.invalid_source_refs == []


def test_source_check_skips_ignored_tables():
    lin = {
        "fhir.Store": _lin_sources("fhir.Store", {"c": {"Source": ["map.X.y"]}}, {}),
    }
    report = check(
        {"fhir.Store": _ddl("fhir.Store", ["c"])},
        lin,
        ignored_tables=frozenset({"fhir.Store"}),
    )
    assert report.invalid_source_refs == []
