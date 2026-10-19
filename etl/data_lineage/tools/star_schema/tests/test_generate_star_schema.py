# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the simplified-format star schema generator.

All fixtures use the NEW lineage format:
  - Capitalized metadata keys: Description, Type, Domain, Grain, SourceSystems, Columns
  - Columns dict: key = column name, value = dict with optional Type (PK/FK/Measure),
    Description, Source, Expr, DataType, FK-Target, FhirPath
  - FK inferred from naming convention: DW_EK_{Dim} -> dim.{Dim}
  - FK-Target overrides naming convention
"""

import json
import sys
import textwrap
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generate_star_schema import (
    _normalize_notes,
    _normalize_privacy,
    _resolve_fhir_target,
    _resolve_fk_target,
    build_data,
    classify_dim_models,
    extract_relationships,
    generate_html,
    parse_lineage_file,
    scan_lineage_directory,
)

# ---------------------------------------------------------------------------
# YAML fixture content (new simplified format)
# ---------------------------------------------------------------------------

_FIXTURES = {
    "dim/dim_Kreditor.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Kreditor
          Domain: ERP
          Description: "Supplier/vendor dimension"
          SourceSystems:
            - sydax: [Kreditor.Kreditor]
            - ils: [dbo.lev]
            - mysupply: [dbo.metadata]
          Columns:
            dw_ek_Kreditor:
              Type: PK
            cvrnr:
              Source: sydax.Kreditor.Kreditor.juridisk_enhed
            kreditornavn:
              Source: sydax.Kreditor.Kreditor.kreditornavn
            dw_ek_Aftale:
              Type: FK
              FK-Target: dim.Aftale
    """
    ),
    "dim/dim_Organization.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Organization
          Domain: Organization
          Description: "Organizational hierarchy — self-referencing"
          SourceSystems: [SOR, OKOSTED]
          Columns:
            dw_ek_Organization:
              Type: PK
            name:
              Source: stg.name
            dw_ek_Org_ParentSOR:
              Type: FK
              FK-Target: dim.Organization
            dw_ek_Org_ParentSource:
              Type: FK
              FK-Target: dim.Organization
    """
    ),
    "dim/dim_Leverandoervare.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Leverandoervare
          Domain: ERP
          Description: "Product/item dimension"
          SourceSystems: [MySupply, ILS]
          Columns:
            dw_ek_Leverandoervare:
              Type: PK
            varenavn:
              Source: mysupply.item_name
            dw_ek_Aftale:
              Type: FK
              FK-Target: dim.Aftale
    """
    ),
    "dim/dim_Borger.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Borger
          Domain: Shared
          Description: "Patient/citizen dimension — SCD Type 2"
          Grain: Borger_CleanCode
          SCD: type2
          SourceSystems: [IFDB]
          Columns:
            dw_sk_Borger:
              Type: PK
              Description: "Version surrogate key"
            dw_ek_Borger:
              Type: PK
              Description: "Durable key"
            CPR:
              Source: ifdb.CPR
            fornavne:
              Source: ifdb.FORNAVNE
            ValidFrom:
              Expr: "Temporal validity start"
            ValidTo:
              Expr: "Temporal validity end"
            IsCurrent:
              Expr: "1 if latest, else 0"
    """
    ),
    "dim/dim_Dato.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Dato
          Domain: Shared
          Description: "Shared date dimension"
          SourceSystems: [Generated]
          Columns:
            dw_ek_Dato:
              Type: PK
            DateKey: {}
            FullDate: {}
            Year: {}
            Month: {}
    """
    ),
    "dim/dim_Aftale.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Aftale
          Domain: ERP
          Description: "Agreement dimension"
          SCD: type2
          SourceSystems: [ILS]
          Columns:
            dw_ek_Aftale:
              Type: PK
            aftale_nummer:
              Source: ils.lafth.aftale
    """
    ),
    "dim/dim_Kontoplan.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Kontoplan
          Domain: ERP
          Description: "Chart of accounts dimension"
          SourceSystems: [SydAX]
          Columns:
            dw_ek_Kontoplan:
              Type: PK
            kontonummer:
              Source: sydax.Finans.Finanskonto.kontonummer
    """
    ),
    "dim/dim_Faktura.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Faktura
          Domain: ERP
          Description: "Invoice metadata dimension"
          SourceSystems: [MySupply]
          Columns:
            dw_ek_Faktura:
              Type: PK
            faktura_nr:
              Source: mysupply.header.faktura_nr
    """
    ),
    "fct/fct_Fakturalinje.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fact
          TableName: fct.Fakturalinje
          Domain: ERP
          Description: "Invoice line fact"
          Grain: clean_code_Fakturalinje
          SourceSystems:
            - mysupply: [dbo.invoice_lines]
          Columns:
            clean_code_Fakturalinje:
              Type: PK
              Expr: "composite key"
            dw_ek_Kreditor:
              Type: FK
            dw_ek_Leverandoervare:
              Type: FK
            dw_ek_Faktura:
              Type: FK
            dw_ek_Organization:
              Type: FK
            dw_ek_Kontoplan:
              Type: FK
              FK-Target: fct.Postering
              Description: "Lookup via fct.Postering subquery"
            dw_ek_Dato_faktura:
              Type: FK
              Description: "Invoice date"
            dw_ek_Dato_ordre:
              Type: FK
              Description: "Order date"
            linje_beloeb:
              Type: Measure
              DataType: DECIMAL(18,4)
              Source: mysupply.line.linje_beloeb
            enheds_pris:
              Type: Measure
              DataType: DECIMAL(18,4)
    """
    ),
    "fct/fct_Postering.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fact
          TableName: fct.Postering
          Domain: ERP
          Description: "Financial posting fact"
          Grain: ref_ledgertrans_id
          SourceSystems: [SydAX]
          Columns:
            dw_ek_Postering:
              Type: PK
            dw_ek_Kreditor:
              Type: FK
            dw_ek_Kontoplan:
              Type: FK
            dw_ek_Organization:
              Type: FK
            dw_ek_Dato_Bogfoerings:
              Type: FK
            posteringsbeloeb:
              Type: Measure
              DataType: DECIMAL(30,12)
    """
    ),
    "fhir/fhir_patient.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fhir
          TableName: fhir.Patient
          Domain: FHIR
          Description: "FHIR Patient resource"
          FhirResource: Patient
          SourceSystems: [IFDB]
          Columns:
            patient_id:
              Type: PK
              FhirPath: "id"
              Source: ifdb.patient_id_ifdb
            patient_ssn:
              FhirPath: "identifier.where(system='urn:dk:cpr').value"
              Source: ifdb.CPR
            gender:
              FhirPath: "gender"
              Source: ifdb.KOEN_KODE
            dob:
              FhirPath: "birthDate"
              Source: ifdb.FOEDSELSDATO
    """
    ),
    "fhir/fhir_encounter.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fhir
          TableName: fhir.Encounter
          Domain: FHIR
          Description: "FHIR Encounter resource"
          FhirResource: Encounter
          Columns:
            encounter_id:
              Type: PK
              FhirPath: "id"
            patient_id:
              FhirPath: "subject.id"
            episode_of_care_id:
              FhirPath: "episodeOfCare.id"
    """
    ),
    "fhir/fhir_episode_of_care.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fhir
          TableName: fhir.EpisodeOfCare
          Domain: FHIR
          Description: "FHIR EpisodeOfCare resource"
          FhirResource: EpisodeOfCare
          Columns:
            episode_of_care_id:
              Type: PK
              FhirPath: "id"
            patient_id:
              FhirPath: "patient.id"
            managing_organization_id:
              FhirPath: "managingOrganization.id"
            previous_episode_of_care_id:
              FhirPath: "extension.previousEpisodeOfCare"
    """
    ),
    "fhir/fhir_organization.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fhir
          TableName: fhir.Organization
          Domain: FHIR
          Description: "FHIR Organization resource"
          FhirResource: Organization
          Columns:
            organization_id:
              Type: PK
              FhirPath: "id"
            part_of_id:
              FhirPath: "partOf.id"
    """
    ),
    "bridge/bridge_LeverandoervareILSVare.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: bridge
          TableName: bridge.LeverandoervareILSVare
          Domain: ERP
          Description: "Many-to-many bridge dim.Leverandoervare <-> dim.Kreditor (fixture)"
          Grain: "One row per pair"
          Columns:
            dw_ek_Leverandoervare:
              Type: FK
              FK-Target: dim.Leverandoervare
            dw_ek_Kreditor:
              Type: FK
              FK-Target: dim.Kreditor
            dw_is_deleted:
              DataType: BIT
    """
    ),
    "dim/dim_budget_egenskaber.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.BudgetEgenskaber
          Domain: ERP
          Description: "Budget property dimension"
          Columns:
            DW_EK_BudgetEgenskaber:
              Type: PK
              DataType: INT
    """
    ),
    "fct/fct_budget.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fact
          TableName: fct.Budget
          Domain: ERP
          Description: "Budget fact with property dimension FK"
          Grain: dw_ek_Budget
          Columns:
            DW_EK_Budget:
              Type: PK
              DataType: INT
            DW_EK_BudgetEgenskaber:
              Type: FK
              DataType: INT
    """
    ),
}


_TRANSACTION_DIM_FIXTURES = {
    "dim/dim_fakturalinje.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Fakturalinje
          Domain: ERP
          Description: "Invoice line transaction dimension"
          DimModel: transaction
          TransactionDimFor: fct.Fakturalinje
          Columns:
            dw_ek_Fakturalinje:
              Type: PK
              DataType: INT
    """
    ),
    "fct/fct_fakturalinje.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: fact
          TableName: fct.Fakturalinje
          Domain: ERP
          Description: "Invoice line fact — transaction dim pattern"
          Grain: CleanCode_Fakturalinje
          Columns:
            dw_ek_Fakturalinje:
              Type: PK
              DataType: INT
              FK-Target: dim.Fakturalinje
              Role: pk_and_fk
            CleanCode_Fakturalinje:
              DataType: NVARCHAR(500)
            DW_EK_Kreditor:
              Type: FK
    """
    ),
    "dim/dim_kreditor.yml": textwrap.dedent(
        """\
        dso_lineage:
          Type: dimension
          TableName: dim.Kreditor
          Domain: ERP
          Description: "Supplier dimension"
          Columns:
            DW_EK_Kreditor:
              Type: PK
    """
    ),
}


@pytest.fixture
def transaction_lineage_dir(tmp_path):
    """Minimal lineage dir for transaction-dimension tests."""
    for rel_path, content in _TRANSACTION_DIM_FIXTURES.items():
        p = tmp_path / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def transaction_tables(transaction_lineage_dir):
    tables = scan_lineage_directory(transaction_lineage_dir)
    classify_dim_models(tables)
    return tables


@pytest.fixture
def lineage_dir(tmp_path):
    """Write all YAML fixtures to tmp_path and return the path."""
    for rel_path, content in _FIXTURES.items():
        p = tmp_path / rel_path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    return tmp_path


@pytest.fixture
def all_tables(lineage_dir):
    """Parse all fixture files and return the list of table dicts."""
    return scan_lineage_directory(lineage_dir)


# =========================================================================
# 1. Parser tests
# =========================================================================


class TestParserDimension:
    def test_table_name(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["name"] == "dim.Kreditor"

    def test_table_type(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["type"] == "dim"

    def test_domain(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["domain"] == ["ERP"]

    def test_description(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["description"] == "Supplier/vendor dimension"

    def test_source_systems(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["source_systems"] == [
            {"sydax": ["Kreditor.Kreditor"]},
            {"ils": ["dbo.lev"]},
            {"mysupply": ["dbo.metadata"]},
        ]

    def test_source_systems_structured_format(self, lineage_dir):
        """SourceSystems should be a list of {name: [tables]} dicts."""
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        ss = t["source_systems"]
        assert isinstance(ss, list)
        for entry in ss:
            assert isinstance(entry, dict), f"Expected dict, got {type(entry)}: {entry}"
            assert len(entry) == 1, f"Expected single-key dict, got {entry}"
            for key, val in entry.items():
                assert isinstance(key, str), f"Key should be str, got {type(key)}"
                assert isinstance(val, list), f"Value should be list, got {type(val)}"

    def test_pk_detected(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        pk_cols = [c for c in t["columns"] if c["pk"]]
        assert any(c["name"] == "dw_ek_Kreditor" for c in pk_cols)

    def test_fk_detected(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        fk_cols = [c for c in t["columns"] if c["fk"]]
        assert any(c["name"] == "dw_ek_Aftale" for c in fk_cols)

    def test_regular_column(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        names = [c["name"] for c in t["columns"]]
        assert "cvrnr" in names
        assert "kreditornavn" in names

    def test_no_measures(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert not any(c["measure"] for c in t["columns"])


class TestParserFact:
    def test_table_name(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        assert t["name"] == "fct.Fakturalinje"

    def test_type(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        assert t["type"] == "fact"

    def test_grain(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        assert t["grain"] == "clean_code_Fakturalinje"

    def test_fk_columns(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        fk_cols = [c for c in t["columns"] if c["fk"]]
        fk_names = {c["name"] for c in fk_cols}
        assert "dw_ek_Kreditor" in fk_names
        assert "dw_ek_Leverandoervare" in fk_names
        assert "dw_ek_Faktura" in fk_names
        assert "dw_ek_Organization" in fk_names
        assert "dw_ek_Kontoplan" in fk_names
        assert "dw_ek_Dato_faktura" in fk_names
        assert "dw_ek_Dato_ordre" in fk_names

    def test_measures(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        measures = [c for c in t["columns"] if c["measure"]]
        assert len(measures) == 2
        names = {c["name"] for c in measures}
        assert "linje_beloeb" in names
        assert "enheds_pris" in names

    def test_datatype(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fct" / "fct_Fakturalinje.yml")
        col = next(c for c in t["columns"] if c["name"] == "linje_beloeb")
        assert col["type"] == "DECIMAL(18,4)"


class TestParserFhir:
    def test_table_name(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fhir" / "fhir_patient.yml")
        assert t["name"] == "fhir.Patient"

    def test_type(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fhir" / "fhir_patient.yml")
        assert t["type"] == "fhir"

    def test_fhir_resource(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fhir" / "fhir_patient.yml")
        assert t["fhir_resource"] == "Patient"

    def test_fhir_path_on_column(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fhir" / "fhir_patient.yml")
        col = next(c for c in t["columns"] if c["name"] == "patient_id")
        assert col["fhir_path"] == "id"

    def test_domain(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "fhir" / "fhir_patient.yml")
        assert t["domain"] == ["FHIR"]


class TestParserSCD:
    def test_scd_type2(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Borger.yml")
        assert t["scd"] == "type2"

    def test_scd_none(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "dim" / "dim_Kreditor.yml")
        assert t["scd"] is None


class TestMissingTableName:
    def test_missing_tablename_returns_none(self, tmp_path):
        p = tmp_path / "dim" / "dim_test.yml"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("dso_lineage:\n  Type: dimension\n  Domain: Test\n  Columns:\n    col1: {}\n")
        assert parse_lineage_file(p) is None


class TestParsePrivacyAndNotes:
    def test_privacy_folded_string(self, tmp_path):
        yml = tmp_path / "dim.yml"
        yml.write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: dimension
                  TableName: dim.Leverandoervare
                  Domain: [ERP]
                  Description: x
                  Privacy: >
                    Fritekst kan indeholde CPR.
                    Adgang styres via vw_dim.Leverandoervare.
                  Columns:
                    DW_EK_Vare:
                      Type: PK
            """
            ),
            encoding="utf-8",
        )
        table = parse_lineage_file(yml)
        assert "CPR" in table["privacy"]
        assert "vw_dim.Leverandoervare" in table["privacy"]

    def test_notes_list(self, tmp_path):
        yml = tmp_path / "fct.yml"
        yml.write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: fact
                  TableName: fct.Faktura
                  Domain: [ERP]
                  Description: x
                  Notes:
                    - "Første note"
                    - "Anden note"
                  Columns:
                    DW_EK_Faktura:
                      Type: PK
            """
            ),
            encoding="utf-8",
        )
        table = parse_lineage_file(yml)
        assert table["notes"] == ["Første note", "Anden note"]

    def test_notes_multiline_string(self, tmp_path):
        yml = tmp_path / "dim.yml"
        yml.write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: dimension
                  TableName: dim.Organization
                  Domain: [ERP]
                  Description: x
                  Notes: |
                    Linje 1
                    Linje 2
                  Columns:
                    DW_EK_Organization:
                      Type: PK
            """
            ),
            encoding="utf-8",
        )
        table = parse_lineage_file(yml)
        assert table["notes"] == ["Linje 1", "Linje 2"]

    def test_missing_privacy_and_notes(self, lineage_dir):
        table = parse_lineage_file(lineage_dir / "dim" / "dim_kreditor.yml")
        assert table["privacy"] is None
        assert table["notes"] == []

    def test_normalize_helpers(self):
        assert _normalize_notes(["a", "  b  "]) == ["a", "b"]
        assert _normalize_notes("one\ntwo\n") == ["one", "two"]
        assert _normalize_privacy("  tekst  ") == "tekst"
        assert _normalize_privacy("") is None


class TestDetailPanelPrivacyAndNotes:
    @pytest.fixture
    def rendered_html(self, tmp_path):
        (tmp_path / "dim").mkdir()
        (tmp_path / "dim" / "dim_test.yml").write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: dimension
                  TableName: dim.Test
                  Domain: [ERP]
                  Description: Test table
                  Privacy: >
                    Personfølsomme data.
                  Notes:
                    - "Vigtig note"
                  Columns:
                    DW_EK_Test:
                      Type: PK
            """
            ),
            encoding="utf-8",
        )
        data = build_data(tmp_path)
        template = Path(__file__).resolve().parent.parent / "star_schema_template.html"
        output = tmp_path / "star_schema.html"
        generate_html(data, template, output)
        return output.read_text(encoding="utf-8"), data

    def test_privacy_and_notes_in_data(self, rendered_html):
        _, data = rendered_html
        table = data["tables"][0]
        assert table["privacy"] == "Personfølsomme data."
        assert table["notes"] == ["Vigtig note"]

    def test_privacy_and_notes_in_html(self, rendered_html):
        html, _ = rendered_html
        assert "detail-privacy" in html
        assert "detail-notes" in html
        assert "Personfølsomme data." in html
        assert "Vigtig note" in html


# =========================================================================
# 2. FK resolution tests
# =========================================================================


class TestParseAuditColumns:
    def test_audit_type_detected(self, tmp_path):
        yml = tmp_path / "fct.yml"
        yml.write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: fact
                  TableName: fct.Test
                  Domain: [ERP]
                  Description: x
                  Columns:
                    DW_EK_Test:
                      Type: PK
                    DW_Timestamp_Insert:
                      Type: Audit
                      DataType: DATETIME2(0)
                    DW_Timestamp_Update:
                      Type: Audit
                      DataType: DATETIME2(0)
            """
            ),
            encoding="utf-8",
        )
        table = parse_lineage_file(yml)
        audit_cols = [c for c in table["columns"] if c["audit"]]
        assert {c["name"] for c in audit_cols} == {
            "DW_Timestamp_Insert",
            "DW_Timestamp_Update",
        }
        assert not audit_cols[0]["pk"]
        assert not audit_cols[0]["fk"]

    def test_audit_in_rendered_html(self, tmp_path):
        (tmp_path / "fct").mkdir()
        (tmp_path / "fct" / "fct_test.yml").write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: fact
                  TableName: fct.Test
                  Domain: [ERP]
                  Description: x
                  Columns:
                    DW_Timestamp_Insert:
                      Type: Audit
                      DataType: DATETIME2(0)
            """
            ),
            encoding="utf-8",
        )
        data = build_data(tmp_path)
        template = Path(__file__).resolve().parent.parent / "star_schema_template.html"
        output = tmp_path / "out.html"
        generate_html(data, template, output)
        html = output.read_text(encoding="utf-8")
        assert "audit-row" in html
        assert "column-legend-swatch audit" in html
        assert 'col-audit">audit</span>' in html

    def test_production_fakturalinje_has_audit_columns(self):
        lineage_dir = Path(__file__).resolve().parent.parent.parent.parent
        table = parse_lineage_file(lineage_dir / "fct" / "fct_fakturalinje.yml")
        audit_cols = [c for c in table["columns"] if c["audit"]]
        assert len(audit_cols) == 2


class TestResolveFkTarget:
    """Test _resolve_fk_target(col_name, fk_target, all_table_names)."""

    def _table_names(self):
        return {
            "dim.Kreditor",
            "dim.Leverandoervare",
            "dim.Aftale",
            "dim.Organization",
            "dim.Kontoplan",
            "dim.Faktura",
            "dim.Dato",
            "dim.Borger",
            "fct.Fakturalinje",
            "fct.Postering",
        }

    def test_naming_convention_simple(self):
        result = _resolve_fk_target("dw_ek_Kreditor", None, self._table_names())
        assert result == "dim.Kreditor"

    def test_naming_convention_role_playing(self):
        """dw_ek_Dato_faktura -> dim.Dato (first segment after DW_EK_)."""
        result = _resolve_fk_target("dw_ek_Dato_faktura", None, self._table_names())
        assert result == "dim.Dato"

    def test_fk_target_override(self):
        result = _resolve_fk_target("dw_ek_Kontoplan", "fct.Postering", self._table_names())
        assert result == "fct.Postering"

    def test_fk_target_override_dim(self):
        result = _resolve_fk_target("dw_ek_Aftale", "dim.Aftale", self._table_names())
        assert result == "dim.Aftale"

    def test_fallback_to_dim_when_not_in_all_tables(self):
        """If naming convention yields a name not in all_table_names, still return dim.{Name}."""
        result = _resolve_fk_target("dw_ek_UnknownDim", None, self._table_names())
        assert result == "dim.UnknownDim"

    def test_fk_target_self_reference(self):
        result = _resolve_fk_target("dw_ek_Org_ParentSOR", "dim.Organization", self._table_names())
        assert result == "dim.Organization"

    def test_canonicalizes_casing_to_match_known_table(self):
        names = self._table_names() | {"dim.ILSVare"}
        # FK column suggests "dim.ILSVare"; even if a user wrote "dim.ilsvare"
        # in TableName, the resolver should snap to the canonical casing.
        assert _resolve_fk_target("dw_ek_ILSVare", None, names) == "dim.ILSVare"
        assert _resolve_fk_target("dw_ek_X", "dim.ILSVARE", names) == "dim.ILSVare"


class TestResolveFkTargetEgenskaber:
    def _table_names(self):
        return {
            "dim.Budget",
            "dim.BudgetEgenskaber",
            "dim.Dato",
            "dim.LagerbeholdningEgenskaber",
        }

    def test_full_remainder_match_egenskaber(self):
        result = _resolve_fk_target("DW_EK_BudgetEgenskaber", None, self._table_names())
        assert result == "dim.BudgetEgenskaber"

    def test_full_remainder_match_lagerbeholdning_egenskaber(self):
        names = self._table_names() | {"dim.LagerbeholdningEgenskaber"}
        result = _resolve_fk_target("DW_EK_LagerbeholdningEgenskaber", None, names)
        assert result == "dim.LagerbeholdningEgenskaber"

    def test_role_playing_date_still_works(self):
        result = _resolve_fk_target("DW_EK_Dato_Faktura", None, self._table_names())
        assert result == "dim.Dato"

    def test_unresolved_egenskaber_when_dim_missing(self):
        result = _resolve_fk_target("DW_EK_BudgetEgenskaber", None, {"dim.Budget"})
        assert result == "dim.BudgetEgenskaber"


# =========================================================================
# 3. Relationship classification tests
# =========================================================================


class TestTransactionDimClassification:
    def test_explicit_role_sets_pk_and_fk(self, transaction_tables):
        fct = next(t for t in transaction_tables if t["name"] == "fct.Fakturalinje")
        pk_col = next(c for c in fct["columns"] if c["name"] == "dw_ek_Fakturalinje")
        assert pk_col["pk_and_fk"] is True
        assert pk_col["role"] == "pk_and_fk"

    def test_dim_model_transaction(self, transaction_tables):
        dim = next(t for t in transaction_tables if t["name"] == "dim.Fakturalinje")
        assert dim["dim_model"] == "transaction"
        assert dim["transaction_dim_for"] == "fct.Fakturalinje"

    def test_inference_without_explicit_role(self, tmp_path):
        (tmp_path / "fct").mkdir()
        (tmp_path / "dim").mkdir()
        (tmp_path / "fct" / "fct_vareudlevering.yml").write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: fact
                  TableName: fct.Vareudlevering
                  Domain: [ERP]
                  Description: x
                  Columns:
                    dw_ek_Vareudlevering:
                      Type: PK
                      FK-Target: dim.Vareudlevering
            """
            ),
            encoding="utf-8",
        )
        (tmp_path / "dim" / "dim_vareudlevering.yml").write_text(
            textwrap.dedent(
                """\
                dso_lineage:
                  Type: dimension
                  TableName: dim.Vareudlevering
                  Domain: [ERP]
                  Description: x
                  Columns:
                    dw_ek_Vareudlevering:
                      Type: PK
            """
            ),
            encoding="utf-8",
        )
        tables = scan_lineage_directory(tmp_path)
        classify_dim_models(tables)
        fct = next(t for t in tables if t["name"] == "fct.Vareudlevering")
        dim = next(t for t in tables if t["name"] == "dim.Vareudlevering")
        pk_col = next(c for c in fct["columns"] if c["pk"])
        assert pk_col["pk_and_fk"] is True
        assert dim["dim_model"] == "transaction"


class TestTransactionDimRelationships:
    def test_dual_key_bus_matrix_relationship(self, transaction_tables):
        fct = next(t for t in transaction_tables if t["name"] == "fct.Fakturalinje")
        rels = extract_relationships(fct, transaction_tables)
        tx_rel = next(
            r
            for r in rels
            if r["target"] == "dim.Fakturalinje" and r["key"] == "dw_ek_Fakturalinje"
        )
        assert tx_rel["type"] == "bus_matrix"
        assert tx_rel["dual_key"] is True

    def test_no_duplicate_fk_edge_for_dual_key(self, transaction_tables):
        fct = next(t for t in transaction_tables if t["name"] == "fct.Fakturalinje")
        rels = extract_relationships(fct, transaction_tables)
        dual_rels = [r for r in rels if r["key"] == "dw_ek_Fakturalinje"]
        assert len(dual_rels) == 1

    def test_build_data_includes_dual_key(self, transaction_lineage_dir):
        data = build_data(transaction_lineage_dir)
        dual = [
            r
            for r in data["relationships"]
            if r.get("dual_key") and r["source"] == "fct.Fakturalinje"
        ]
        assert len(dual) == 1
        assert dual[0]["target"] == "dim.Fakturalinje"


class TestEgenskabsdimRelationships:
    def test_budget_egenskaber_resolves_correctly(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Budget")
        rels = extract_relationships(fct, all_tables)
        egenskaber = next(r for r in rels if r["key"] == "DW_EK_BudgetEgenskaber")
        assert egenskaber["target"] == "dim.BudgetEgenskaber"
        assert egenskaber.get("dual_key") is not True

    def test_budget_pk_not_marked_dual_key(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Budget")
        pk_col = next(c for c in fct["columns"] if c["name"] == "DW_EK_Budget")
        assert pk_col.get("pk_and_fk") is not True


class TestRegressionExistingFixtures:
    def test_fakturalinje_legacy_fixture_unchanged(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Fakturalinje")
        pk_cols = [c for c in fct["columns"] if c["pk"]]
        assert any(c["name"] == "clean_code_Fakturalinje" for c in pk_cols)
        assert not any(c.get("pk_and_fk") for c in fct["columns"])

    def test_bus_matrix_counts_unchanged(self, lineage_dir):
        data = build_data(lineage_dir)
        bus = [r for r in data["relationships"] if r["type"] == "bus_matrix"]
        fct_linje = [r for r in bus if r["source"] == "fct.Fakturalinje"]
        assert len(fct_linje) == 6
        assert not any(r.get("dual_key") for r in fct_linje)
        fct_budget = [r for r in bus if r["source"] == "fct.Budget"]
        assert len(fct_budget) == 1
        assert fct_budget[0]["target"] == "dim.BudgetEgenskaber"


class TestProductionLineageSmoke:
    @pytest.fixture
    def production_data(self):
        lineage_dir = Path(__file__).resolve().parent.parent.parent.parent
        return build_data(lineage_dir)

    def test_fakturalinje_transaction_dim_dual_key(self, production_data):
        dual = [
            r
            for r in production_data["relationships"]
            if r["source"] == "fct.Fakturalinje"
            and r["target"] == "dim.Fakturalinje"
            and r.get("dual_key")
        ]
        assert len(dual) == 1

    def test_budget_egenskaber_fk_target(self, production_data):
        rels = [
            r
            for r in production_data["relationships"]
            if r["source"] == "fct.Budget" and r["key"] == "dw_ek_BudgetEgenskaber"
        ]
        assert len(rels) == 1
        assert rels[0]["target"] == "dim.BudgetEgenskaber"


class TestRelationshipBusMatrix:
    """fact -> dim = bus_matrix."""

    def test_fact_to_dim_via_naming(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Fakturalinje")
        rels = extract_relationships(fct, all_tables)
        bus = [r for r in rels if r["type"] == "bus_matrix"]
        targets = {r["target"] for r in bus}
        assert "dim.Kreditor" in targets
        assert "dim.Leverandoervare" in targets
        assert "dim.Faktura" in targets
        assert "dim.Organization" in targets

    def test_role_playing_dates(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Fakturalinje")
        rels = extract_relationships(fct, all_tables)
        bus = [r for r in rels if r["type"] == "bus_matrix"]
        dato_rels = [r for r in bus if r["target"] == "dim.Dato"]
        assert len(dato_rels) == 2
        keys = {r["key"] for r in dato_rels}
        assert "dw_ek_Dato_faktura" in keys
        assert "dw_ek_Dato_ordre" in keys

    def test_fct_postering_bus_matrix(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Postering")
        rels = extract_relationships(fct, all_tables)
        bus = [r for r in rels if r["type"] == "bus_matrix"]
        targets = {r["target"] for r in bus}
        assert "dim.Kreditor" in targets
        assert "dim.Kontoplan" in targets
        assert "dim.Organization" in targets
        assert "dim.Dato" in targets


class TestRelationshipFactToFact:
    """fact -> fact via FK-Target = fact_to_fact."""

    def test_fk_target_to_fact(self, all_tables):
        fct = next(t for t in all_tables if t["name"] == "fct.Fakturalinje")
        rels = extract_relationships(fct, all_tables)
        f2f = [r for r in rels if r["type"] == "fact_to_fact"]
        assert len(f2f) == 1
        assert f2f[0]["target"] == "fct.Postering"
        assert f2f[0]["key"] == "dw_ek_Kontoplan"


class TestRelationshipDimToDim:
    """dim -> different dim = dim_to_dim."""

    def test_dim_leverandoervare_to_dim_aftale(self, all_tables):
        dim = next(t for t in all_tables if t["name"] == "dim.Leverandoervare")
        rels = extract_relationships(dim, all_tables)
        d2d = [r for r in rels if r["type"] == "dim_to_dim"]
        assert len(d2d) == 1
        assert d2d[0]["target"] == "dim.Aftale"

    def test_dim_kreditor_to_dim_aftale(self, all_tables):
        dim = next(t for t in all_tables if t["name"] == "dim.Kreditor")
        rels = extract_relationships(dim, all_tables)
        d2d = [r for r in rels if r["type"] == "dim_to_dim"]
        assert len(d2d) == 1
        assert d2d[0]["target"] == "dim.Aftale"


class TestBridgeRendersLikeFact:
    """Type: bridge renders like a fact: fact type + bus_matrix edges to dims.

    Without the TYPE_MAP entry, bridge tables parse with an unknown type,
    extract_relationships drops all their FK edges, and the table becomes
    invisible in every view of the visualization.
    """

    def test_bridge_type_maps_to_fact(self, lineage_dir):
        t = parse_lineage_file(lineage_dir / "bridge" / "bridge_LeverandoervareILSVare.yml")
        assert t["type"] == "fact"

    def test_bridge_gets_bus_matrix_edges(self, all_tables):
        bridge = next(t for t in all_tables if t["name"] == "bridge.LeverandoervareILSVare")
        rels = extract_relationships(bridge, all_tables)
        assert {(r["target"], r["type"]) for r in rels} == {
            ("dim.Leverandoervare", "bus_matrix"),
            ("dim.Kreditor", "bus_matrix"),
        }


class TestRelationshipHierarchy:
    """dim -> itself via FK-Target = hierarchy."""

    def test_organization_hierarchy(self, all_tables):
        dim = next(t for t in all_tables if t["name"] == "dim.Organization")
        rels = extract_relationships(dim, all_tables)
        hier = [r for r in rels if r["type"] == "hierarchy"]
        assert len(hier) == 2  # ParentSOR + ParentSource
        for r in hier:
            assert r["source"] == "dim.Organization"
            assert r["target"] == "dim.Organization"


# =========================================================================
# 4. Data builder tests
# =========================================================================


class TestBuildData:
    def test_top_level_keys(self, lineage_dir):
        data = build_data(lineage_dir)
        assert "tables" in data
        assert "relationships" in data
        assert "domains" in data
        assert "fhirTables" in data

    def test_fhir_excluded_from_tables(self, lineage_dir):
        data = build_data(lineage_dir)
        table_types = {t["type"] for t in data["tables"]}
        assert "fhir" not in table_types

    def test_fhir_in_fhir_tables(self, lineage_dir):
        data = build_data(lineage_dir)
        assert len(data["fhirTables"]) == 4
        assert all(t["type"] == "fhir" for t in data["fhirTables"])

    def test_no_duplicate_relationships(self, lineage_dir):
        data = build_data(lineage_dir)
        seen = set()
        for rel in data["relationships"]:
            key = (rel["source"], rel["target"], rel["key"])
            assert key not in seen, f"Duplicate relationship: {key}"
            seen.add(key)

    def test_domains_is_split_dict(self, lineage_dir):
        data = build_data(lineage_dir)
        assert isinstance(data["domains"], dict)
        assert set(data["domains"].keys()) == {"matrix", "fhir"}
        assert isinstance(data["domains"]["matrix"], list)
        assert isinstance(data["domains"]["fhir"], list)

    def test_matrix_domains_from_fact_and_dim_files(self, lineage_dir):
        data = build_data(lineage_dir)
        matrix = data["domains"]["matrix"]
        assert "ERP" in matrix
        assert "Shared" in matrix
        assert "Organization" in matrix
        assert "FHIR" not in matrix

    def test_fhir_domains_from_fhir_files(self, lineage_dir):
        data = build_data(lineage_dir)
        fhir = data["domains"]["fhir"]
        assert "FHIR" in fhir
        assert "ERP" not in fhir
        assert "Shared" not in fhir

    def test_domains_sorted(self, lineage_dir):
        data = build_data(lineage_dir)
        assert data["domains"]["matrix"] == sorted(data["domains"]["matrix"])
        assert data["domains"]["fhir"] == sorted(data["domains"]["fhir"])

    def test_unknown_domain_excluded(self, tmp_path):
        (tmp_path / "dim").mkdir()
        (tmp_path / "dim" / "dim_x.yml").write_text(
            "dso_lineage:\n"
            "  Type: dimension\n"
            "  TableName: dim.X\n"
            "  Domain: [Unknown]\n"
            "  Description: x\n"
            "  Columns:\n"
            "    dw_ek_X:\n"
            "      Type: PK\n",
            encoding="utf-8",
        )
        (tmp_path / "fhir").mkdir()
        (tmp_path / "fhir" / "fhir_y.yml").write_text(
            "dso_lineage:\n"
            "  Type: fhir\n"
            "  TableName: fhir.Y\n"
            "  Domain: [Unknown]\n"
            "  Description: y\n"
            "  Columns:\n"
            "    id:\n"
            "      Type: PK\n",
            encoding="utf-8",
        )
        data = build_data(tmp_path)
        assert "Unknown" not in data["domains"]["matrix"]
        assert "Unknown" not in data["domains"]["fhir"]

    def test_relationship_types_present(self, lineage_dir):
        data = build_data(lineage_dir)
        rel_types = {r["type"] for r in data["relationships"]}
        assert "bus_matrix" in rel_types
        assert "fact_to_fact" in rel_types
        assert "dim_to_dim" in rel_types
        assert "hierarchy" in rel_types

    def test_internal_keys_stripped(self, lineage_dir):
        """Tables should not leak internal parser keys."""
        data = build_data(lineage_dir)
        for t in data["tables"] + data["fhirTables"]:
            assert "_raw_lookups" not in t
            assert "_raw_dates" not in t


class TestGenerateHtml:
    def test_html_injection(self, tmp_path):
        data = {
            "tables": [{"name": "<script>alert(1)</script>", "type": "dim"}],
            "relationships": [],
            "domains": {"matrix": [], "fhir": []},
            "fhirTables": [],
        }
        template = tmp_path / "template.html"
        output = tmp_path / "output.html"
        template.write_text("<!DOCTYPE html><script>const DATA = {{DATA}};</script>")
        generate_html(data, template, output)
        html = output.read_text()
        # The script tag in the name should be JSON-escaped (no raw <script>)
        assert "{{DATA}}" not in html
        assert "const DATA = {" in html
        # json.dumps escapes < as part of the string, so no injection
        parsed = json.loads(html.split("const DATA = ", 1)[1].rstrip(";</script>"))
        assert parsed["tables"][0]["name"] == "<script>alert(1)</script>"

    def test_data_placeholder_replaced(self, tmp_path):
        data = {
            "tables": [],
            "relationships": [],
            "domains": {"matrix": [], "fhir": []},
            "fhirTables": [],
        }
        template = tmp_path / "tpl.html"
        output = tmp_path / "out.html"
        template.write_text("<html>{{DATA}}</html>")
        generate_html(data, template, output)
        html = output.read_text()
        assert "{{DATA}}" not in html


class TestScanLineageDirectory:
    def test_finds_all_fixtures(self, lineage_dir):
        tables = scan_lineage_directory(lineage_dir)
        names = {t["name"] for t in tables}
        assert "dim.Kreditor" in names
        assert "fct.Fakturalinje" in names
        assert "fhir.Patient" in names

    def test_count(self, lineage_dir):
        tables = scan_lineage_directory(lineage_dir)
        assert len(tables) == len(_FIXTURES)

    def test_skips_invalid_yaml(self, lineage_dir):
        bad = lineage_dir / "dim" / "dim_bad.yml"
        bad.write_text("not: valid\n  yaml: [")
        tables = scan_lineage_directory(lineage_dir)
        # Should skip the bad file, but still get the rest
        names = {t["name"] for t in tables}
        assert "dim.Kreditor" in names


# =========================================================================
# 6. FHIR relationship inference tests
# =========================================================================


class TestResolveFhirTarget:
    """Test _resolve_fhir_target(col_name, table_id, all_fhir_names, fk_target)."""

    _fhir_names = {
        "fhir.Patient",
        "fhir.Encounter",
        "fhir.EpisodeOfCare",
        "fhir.Organization",
        "fhir.Location",
        "fhir.CarePlan",
    }

    def test_simple_reference(self):
        assert (
            _resolve_fhir_target("patient_id", "fhir.Encounter", self._fhir_names) == "fhir.Patient"
        )

    def test_fk_target_overrides_the_name(self):
        assert (
            _resolve_fhir_target(
                "performing_location_id", "fhir.Appointment", self._fhir_names, "fhir.Location"
            )
            == "fhir.Location"
        )
        assert (
            _resolve_fhir_target("careplan_id", "fhir.Encounter", self._fhir_names, "fhir.CarePlan")
            == "fhir.CarePlan"
        )

    def test_fk_target_self_reference_skipped(self):
        assert (
            _resolve_fhir_target(
                "part_of_id", "fhir.Organization", self._fhir_names, "fhir.Organization"
            )
            is None
        )

    def test_fk_target_case_insensitive_and_unknown_warns(self, caplog):
        assert (
            _resolve_fhir_target("location_id", "fhir.Encounter", self._fhir_names, "fhir.location")
            == "fhir.Location"
        )
        assert not caplog.records
        assert (
            _resolve_fhir_target("bed_id", "fhir.Encounter", self._fhir_names, "fhir.Bed") is None
        )
        assert "fhir.Encounter.bed_id: FK-Target fhir.Bed matches no FHIR table" in caplog.text

    def test_compound_name(self):
        assert (
            _resolve_fhir_target("episode_of_care_id", "fhir.Encounter", self._fhir_names)
            == "fhir.EpisodeOfCare"
        )

    def test_managing_prefix_stripped(self):
        assert (
            _resolve_fhir_target("managing_organization_id", "fhir.EpisodeOfCare", self._fhir_names)
            == "fhir.Organization"
        )

    def test_previous_prefix_stripped(self):
        """previous_episode_of_care_id in EpisodeOfCare → fhir.EpisodeOfCare (self-ref skipped)."""
        assert (
            _resolve_fhir_target(
                "previous_episode_of_care_id", "fhir.EpisodeOfCare", self._fhir_names
            )
            is None
        )

    def test_self_reference_skipped(self):
        assert _resolve_fhir_target("patient_id", "fhir.Patient", self._fhir_names) is None

    def test_ifdb_prefix_skipped(self):
        assert (
            _resolve_fhir_target("ifdb_eventmarker_id", "fhir.CarePlan_Activity", self._fhir_names)
            is None
        )

    def test_nonexistent_target_skipped(self):
        assert (
            _resolve_fhir_target("practitioner_id", "fhir.CarePlan_Activity", self._fhir_names)
            is None
        )

    def test_non_id_column_skipped(self):
        assert _resolve_fhir_target("status", "fhir.Encounter", self._fhir_names) is None

    def test_part_of_id_no_match(self):
        """part_of_id → fhir.PartOf doesn't exist, so None."""
        assert _resolve_fhir_target("part_of_id", "fhir.Organization", self._fhir_names) is None


class TestFhirRelationships:
    """FHIR tables produce fhir_ref relationships from naming convention."""

    def test_encounter_references_patient(self, all_tables):
        encounter = next(t for t in all_tables if t["name"] == "fhir.Encounter")
        rels = extract_relationships(encounter, all_tables)
        fhir_rels = [r for r in rels if r["type"] == "fhir_ref"]
        targets = {r["target"] for r in fhir_rels}
        assert "fhir.Patient" in targets

    def test_encounter_references_episode_of_care(self, all_tables):
        encounter = next(t for t in all_tables if t["name"] == "fhir.Encounter")
        rels = extract_relationships(encounter, all_tables)
        fhir_rels = [r for r in rels if r["type"] == "fhir_ref"]
        targets = {r["target"] for r in fhir_rels}
        assert "fhir.EpisodeOfCare" in targets

    def test_episode_of_care_references_patient_and_org(self, all_tables):
        eoc = next(t for t in all_tables if t["name"] == "fhir.EpisodeOfCare")
        rels = extract_relationships(eoc, all_tables)
        fhir_rels = [r for r in rels if r["type"] == "fhir_ref"]
        targets = {r["target"] for r in fhir_rels}
        assert "fhir.Patient" in targets
        assert "fhir.Organization" in targets

    def test_patient_has_no_fhir_refs(self, all_tables):
        """Patient only has patient_id (self-ref) and ifdb_* columns — no outgoing refs."""
        patient = next(t for t in all_tables if t["name"] == "fhir.Patient")
        rels = extract_relationships(patient, all_tables)
        assert len(rels) == 0

    def test_fhir_ref_in_build_data(self, lineage_dir):
        data = build_data(lineage_dir)
        rel_types = {r["type"] for r in data["relationships"]}
        assert "fhir_ref" in rel_types

    def test_fhir_columns_marked_as_fk(self, all_tables):
        """After extract_relationships, inferred FK columns should have fk=True and target set."""
        encounter = next(t for t in all_tables if t["name"] == "fhir.Encounter")
        extract_relationships(encounter, all_tables)
        patient_col = next(c for c in encounter["columns"] if c["name"] == "patient_id")
        assert patient_col["fk"] is True
        assert patient_col["target"] == "fhir.Patient"


class TestDetailPanelRendering:
    """Verify that the HTML template renders SourceSystems and column-level fields."""

    @pytest.fixture
    def rendered_html(self, lineage_dir, tmp_path):
        """Generate the HTML from fixtures and return the content."""
        data = build_data(lineage_dir)
        template = Path(__file__).resolve().parent.parent / "star_schema_template.html"
        output = tmp_path / "star_schema.html"
        generate_html(data, template, output)
        return output.read_text(encoding="utf-8")

    @pytest.fixture
    def rendered_data(self, lineage_dir):
        """Build the JSON data from fixtures."""
        return build_data(lineage_dir)

    def test_source_systems_in_data(self, rendered_data):
        """source_systems should be present in the JSON data for dim.Kreditor."""
        kreditor = next(t for t in rendered_data["tables"] if t["name"] == "dim.Kreditor")
        assert len(kreditor["source_systems"]) == 3

    def test_source_systems_string_format_in_data(self, rendered_data):
        """SourceSystems as plain string list (dim.Organization) should be preserved."""
        org = next(t for t in rendered_data["tables"] if t["name"] == "dim.Organization")
        assert org["source_systems"] == ["SOR", "OKOSTED"]

    def test_column_source_in_data(self, rendered_data):
        """Column-level Source field should be in the JSON data."""
        kreditor = next(t for t in rendered_data["tables"] if t["name"] == "dim.Kreditor")
        cvrnr = next(c for c in kreditor["columns"] if c["name"] == "cvrnr")
        assert cvrnr["source"] == "sydax.Kreditor.Kreditor.juridisk_enhed"

    def test_column_expr_in_data(self, rendered_data):
        """Column-level Expr field should be in the JSON data."""
        borger = next(t for t in rendered_data["tables"] if t["name"] == "dim.Borger")
        valid_from = next(c for c in borger["columns"] if c["name"] == "ValidFrom")
        assert valid_from["expr"] == "Temporal validity start"

    def test_column_description_in_data(self, rendered_data):
        """Column-level Description field should be in the JSON data."""
        borger = next(t for t in rendered_data["tables"] if t["name"] == "dim.Borger")
        sk = next(c for c in borger["columns"] if c["name"] == "dw_sk_Borger")
        assert sk["description"] == "Version surrogate key"

    def test_html_contains_source_column_header(self, rendered_html):
        """The columns table should have Source, Expr, Description headers."""
        assert "<th>Source</th>" in rendered_html
        assert "<th>Expr</th>" in rendered_html
        assert "<th>Description</th>" in rendered_html

    def test_html_contains_source_systems_label(self, rendered_html):
        """The detail panel template should include the Source Systems label."""
        assert "Source Systems</dt>" in rendered_html
