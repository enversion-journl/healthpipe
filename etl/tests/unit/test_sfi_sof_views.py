# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SFI-specific assertions on SOF ViewDefinitions and fhir.* DDL.

These tests do not parametrize over SOF_RESOURCE_CONFIGS (that is the existing
test_sof_view_definitions/test_sof_ddl_crosscheck job). They
make targeted assertions for the SFI feature so a regression on the SFI
projection contract is caught early.

Scope:
- observation_view exposes element_system/element_code via the SFI coding.
- observation_view exposes typed value_* and ifdb_aktivitet_id.
- questionnaire_item_view exposes element_system/element_code from item.code.
- questionnaire_response_item_view intentionally does NOT expose
  element_system/element_code -- they come from the Questionnaire_item join.
- DDL crosscheck for the four new tables (Questionnaire, Questionnaire_item,
  QuestionnaireResponse, QuestionnaireResponse_item) AND for the SFI columns
  added to fhir.Observation.
- fhir.SFI_datapoint view exists and selects from Observation and QR only.
  AKTIVITETSRESULTAT-derived Procedures stay in their own event view.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
VIEW_DEFS_DIR = REPO_ROOT / "DSA" / "FHIR" / "SqlOnFhir" / "view_definitions"
DDL_PATH = REPO_ROOT / "infrastructure" / "ddl" / "tables" / "fhir" / "01_tables.sql"
VIEWS_PATH = REPO_ROOT / "infrastructure" / "ddl" / "views" / "fhir" / "01_vw_fhir.sql"
STORE_DDL_PATH = (
    REPO_ROOT / "infrastructure" / "ddl" / "tables" / "fhir" / "02_sfi_datapoint_store.sql"
)
ETL_DDL_PATH = (
    REPO_ROOT / "infrastructure" / "ddl" / "tables" / "etl" / "01_ifdb_aktivitetstype_routing.sql"
)
CODESYSTEM_DDL_PATH = (
    REPO_ROOT / "infrastructure" / "ddl" / "tables" / "codesystem" / "01_codesystem_tables.sql"
)
CODESYSTEM_VIEWS_PATH = (
    REPO_ROOT / "infrastructure" / "ddl" / "views" / "codesystem" / "01_vw_codesystem.sql"
)
DSO_INFRA_PATH = REPO_ROOT / "infrastructure" / "assets" / "dso_infrastructure.py"

SFI_SYSTEM = "urn:dk:ifdb:sfi"

NEW_VIEWS = [
    "questionnaire_view",
    "questionnaire_item_view",
    "questionnaire_response_view",
    "questionnaire_response_item_view",
]

NEW_TABLES = [
    "Questionnaire",
    "Questionnaire_item",
    "QuestionnaireResponse",
    "QuestionnaireResponse_item",
]

SFI_OBSERVATION_COLUMNS = {
    "organization_id",
    "practitioner_id",
    "element_system",
    "element_code",
    "element_display",
    "value_type",
    "value_int",
    "value_dec",
    "value_string",
    "value_datetime",
    "value_bool",
    "value_quantity",
    "value_unit",
    "value_coding_system",
    "value_coding_code",
    "value_coding_display",
    "ifdb_aktivitet_id",
    "ifdb_aktivitet_sfi_id",
}

SFI_INSTANCE_COLUMNS = {
    "element_nummer",
    "klynge_id",
    "klynge_id_hierarki",
    "klynge_valg_ledetekst",
    "klynge_parent_id",
    "repetition_id",
    "element_kode",
    "element_original_kode",
    "element_administrativ_enhed_id",
}

QR_ITEM_TYPED_VALUE_COLUMNS = {
    "value_type",
    "value_int",
    "value_dec",
    "value_string",
    "value_datetime",
    "value_bool",
    "value_quantity",
    "value_unit",
    "value_coding_system",
    "value_coding_code",
    "value_coding_display",
}

SFI_DATAPOINT_CONSUMER_COLUMNS = {
    "datapoint_key",
    "source_resource_id",
    "source_link_id",
    "source_item_key",
    "source_answer_index",
    "source_last_updated",
    "ifdb_aktivitet_id",
    "ifdb_aktivitet_sfi_id",
    "patient_id",
    "encounter_id",
    "source_resource",
    "organization_id",
    "practitioner_id",
    "occurred_at",
    "source_model_name",
    "source_model_version",
    "source_element_id",
    "source_element_name",
    "source_element_label",
    "sfi_path",
    "source_hierarchy_path",
    "source_group_path",
    "source_repetition_path",
    "klynge_niveau",
    "repetition_niveau",
    "raekke_type",
    "klynge_type",
    "klynge_valg_ledetekst",
    "element_nummer",
    "klynge_id",
    "klynge_id_hierarki",
    "klynge_parent_id",
    "repetition_id",
    "element_kode",
    "element_original_kode",
    "element_administrativ_enhed_id",
    "element_code",
    "element_display",
    "value_type",
    "has_value",
    "value_int",
    "value_dec",
    "value_text",
    "value_datetime",
    "value_bool",
    "value_quantity",
    "value_unit",
    "value_coding_code",
    "value_coding_display",
}


SFI_DATAPOINT_STORE_ONLY_COLUMNS = {
    "occurred_at_dt",
}


def _extract_fhir_view_body(views_text: str, view_name: str) -> str:
    match = re.search(
        rf"CREATE\s+OR\s+ALTER\s+VIEW\s+\[fhir\]\.\[{re.escape(view_name)}\]\s+AS(.+?)\bGO\b",
        views_text,
        re.DOTALL | re.IGNORECASE,
    )
    assert match, f"Could not locate fhir.{view_name} view definition block"
    return match.group(1)


def _load_view(name: str) -> dict:
    with open(VIEW_DEFS_DIR / f"{name}.json") as f:
        return json.load(f)


def _columns(view: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for block in view.get("select", []):
        for col in block.get("column", []):
            out[col["name"]] = col["path"]
        for nested in block.get("select", []):
            for col in nested.get("column", []):
                out[col["name"]] = col["path"]
    return out


def _extract_ddl_columns(ddl_text: str, table_name: str) -> set[str]:
    pattern = (
        r"CREATE\s+TABLE\s+\[fhir\]\.\["
        + re.escape(table_name)
        + r"\]\s*\((.*?)\)\s*ON\s*\[PRIMARY\]"
    )
    match = re.search(pattern, ddl_text, re.DOTALL | re.IGNORECASE)
    if not match:
        return set()
    cols: set[str] = set()
    for line in match.group(1).splitlines():
        col_match = re.match(r"\s*\[(\w+)\]", line)
        if col_match:
            cols.add(col_match.group(1))
    return cols


def _extract_alter_added_columns(ddl_text: str, table_name: str) -> set[str]:
    pattern = (
        r"ALTER\s+TABLE\s+\[fhir\]\.\[" + re.escape(table_name) + r"\]\s*\n?\s*ADD\s*\[(\w+)\]"
    )
    return set(re.findall(pattern, ddl_text, re.IGNORECASE))


@pytest.fixture(scope="module")
def ddl_text() -> str:
    return DDL_PATH.read_text()


@pytest.fixture(scope="module")
def views_text() -> str:
    return VIEWS_PATH.read_text()


@pytest.fixture(scope="module")
def etl_ddl_text() -> str:
    return ETL_DDL_PATH.read_text()


@pytest.fixture(scope="module")
def codesystem_ddl_text() -> str:
    return CODESYSTEM_DDL_PATH.read_text()


@pytest.fixture(scope="module")
def dso_infra_text() -> str:
    return DSO_INFRA_PATH.read_text()


@pytest.mark.unit
class TestObservationViewSfiContract:
    def test_observation_view_has_sfi_columns(self):
        cols = _columns(_load_view("observation_view"))
        missing = (SFI_OBSERVATION_COLUMNS | SFI_INSTANCE_COLUMNS) - set(cols)
        assert not missing, f"observation_view missing SFI/typed columns: {missing}"

    def test_observation_element_paths_filter_by_sfi_system(self):
        cols = _columns(_load_view("observation_view"))
        for c in ("element_system", "element_code", "element_display"):
            assert SFI_SYSTEM in cols[c], (
                f"observation_view.{c} must filter on system='{SFI_SYSTEM}' "
                f"so non-SFI codings do not leak into element_*; got: {cols[c]}"
            )

    def test_observation_ifdb_aktivitet_reads_parent_from_extension(self):
        cols = _columns(_load_view("observation_view"))
        path = cols["ifdb_aktivitet_id"]
        # Parent grouping id is read from the sfiInstanceMeta extension, not from
        # an identifier — the parent identifier is intentionally absent on SFI
        # Observations (1:N) to preserve the UNIQUE identifier index.
        assert "sfiInstanceMeta" in path and "aktivitetId" in path, (
            "ifdb_aktivitet_id must be derived from the sfiInstanceMeta "
            "'aktivitetId' extension so all Observations from the same XML can "
            "be regrouped without re-adding the unique-indexed parent identifier."
        )

    def test_observation_ifdb_sfi_id_uses_per_element_identifier_system(self):
        cols = _columns(_load_view("observation_view"))
        path = cols["ifdb_aktivitet_sfi_id"]
        assert "urn:source:ifdb:aktivitetsresultat_sfi" in path, (
            "ifdb_aktivitet_sfi_id must carry the per-SFI-element identifier so "
            "repeated source occurrences can be distinguished without using the "
            "random FHIR Observation id."
        )

    def test_observation_value_type_is_constant(self):
        cols = _columns(_load_view("observation_view"))
        assert cols["value_type"] == "'fhir_value'", (
            "value_type should be the constant 'fhir_value' in SOF; actual "
            "type derivation happens in the MSSQL view layer."
        )


@pytest.mark.unit
class TestQuestionnaireItemViewSfiContract:
    def test_questionnaire_item_view_has_sfi_columns(self):
        cols = _columns(_load_view("questionnaire_item_view"))
        for c in ("item_key", "element_system", "element_code", "element_display"):
            assert c in cols, f"questionnaire_item_view missing {c}"

    def test_questionnaire_item_uses_native_id_as_item_key(self):
        cols = _columns(_load_view("questionnaire_item_view"))
        assert cols["item_key"] == "id"

    def test_questionnaire_item_sfi_filters_on_code_system(self):
        cols = _columns(_load_view("questionnaire_item_view"))
        for c in ("element_system", "element_code", "element_display"):
            assert "code" in cols[c] and SFI_SYSTEM in cols[c], (
                f"questionnaire_item_view.{c} must read from item.code "
                f"filtered on system='{SFI_SYSTEM}'; got: {cols[c]}"
            )

    def test_questionnaire_item_forEach_iterates_flat_items(self):
        view = _load_view("questionnaire_item_view")
        for_each_values = [block["forEach"] for block in view["select"] if "forEach" in block]
        assert (
            "item" in for_each_values
        ), "questionnaire_item_view must forEach over 'item' (flat structure)."
        for val in for_each_values:
            assert (
                "descendants" not in val
            ), "questionnaire_item_view must NOT use descendants() — items are flat."


@pytest.mark.unit
class TestQuestionnaireResponseItemViewContract:
    def test_qr_item_does_not_project_native_code(self):
        """QR.item has no native code in FHIR R4. We must NOT pretend it does."""
        view = _load_view("questionnaire_response_item_view")
        paths_text = json.dumps(view)
        assert "item.code" not in paths_text and ".item.code" not in paths_text, (
            "questionnaire_response_item_view must not read QR.item.code -- "
            "SFI is resolved via Questionnaire_item join, not native QR fields."
        )

    def test_qr_item_does_not_expose_element_columns(self):
        cols = _columns(_load_view("questionnaire_response_item_view"))
        assert "element_system" not in cols, (
            "element_system on QR-item is misleading: it must come from the "
            "Questionnaire_item join in fhir.SFI_datapoint, not from QR."
        )
        assert "element_code" not in cols

    def test_qr_item_has_typed_value_columns(self):
        cols = _columns(_load_view("questionnaire_response_item_view"))
        missing = QR_ITEM_TYPED_VALUE_COLUMNS - set(cols)
        assert not missing, f"questionnaire_response_item_view missing typed value cols: {missing}"

    def test_qr_item_has_sfi_instance_columns(self):
        cols = _columns(_load_view("questionnaire_response_item_view"))
        missing = SFI_INSTANCE_COLUMNS - set(cols)
        assert not missing, (
            "questionnaire_response_item_view missing SFI instance columns: " f"{missing}"
        )

    def test_qr_item_has_answer_index_for_merge_pk(self):
        cols = _columns(_load_view("questionnaire_response_item_view"))
        assert cols["answer_index"] == "'0'"

    def test_qr_item_uses_native_id_as_item_key(self):
        cols = _columns(_load_view("questionnaire_response_item_view"))
        assert "item_key" in cols
        paths_text = json.dumps(_load_view("questionnaire_response_item_view"))
        assert '"path": "id"' in paths_text, (
            "QR-item item_key must come from native QuestionnaireResponse.item.id, "
            "not custom hierarchy extensions."
        )
        for c in ("klynge_niveau", "repetition_niveau", "hierarki", "raekke_type"):
            assert c not in cols


@pytest.mark.unit
class TestQuestionnaireResponseViewContract:
    def test_qr_subject_and_encounter_use_resolved_reference_ids(self):
        cols = _columns(_load_view("questionnaire_response_view"))
        assert cols["patient_id"] == "subject.id", (
            "questionnaire_response_view.patient_id must use subject.id so it aligns "
            "with Observation and carries resolved Aidbox ids."
        )
        assert cols["encounter_id"] == "encounter.id", (
            "questionnaire_response_view.encounter_id must use encounter.id so it "
            "aligns with Observation and carries resolved Aidbox ids."
        )
        assert (
            cols["organization_id"] == "author.getReferenceKey(Organization)"
        ), "questionnaire_response_view.organization_id should use author Organization reference."
        assert cols["practitioner_id"] == (
            "author.where(type='Practitioner').identifier.value.first()"
        ), (
            "questionnaire_response_view.practitioner_id should use the "
            "identifier carried on the author Practitioner reference."
        )


@pytest.mark.unit
class TestSfiDdlAdditions:
    @pytest.mark.parametrize("table_name", NEW_TABLES)
    def test_new_table_exists_in_ddl(self, table_name: str, ddl_text: str):
        cols = _extract_ddl_columns(ddl_text, table_name)
        assert cols, f"DDL block for [fhir].[{table_name}] not found"

    def test_observation_ddl_has_sfi_columns(self, ddl_text: str):
        ddl_cols = _extract_ddl_columns(ddl_text, "Observation")
        alter_cols = _extract_alter_added_columns(ddl_text, "Observation")
        all_cols = ddl_cols | alter_cols
        missing = (SFI_OBSERVATION_COLUMNS | SFI_INSTANCE_COLUMNS) - all_cols
        assert not missing, (
            f"fhir.Observation missing SFI columns in both CREATE and ALTER blocks: " f"{missing}"
        )

    def test_observation_view_columns_subset_of_ddl(self, ddl_text: str):
        view_cols = set(_columns(_load_view("observation_view")))
        ddl_cols = _extract_ddl_columns(ddl_text, "Observation") | _extract_alter_added_columns(
            ddl_text, "Observation"
        )
        missing = view_cols - ddl_cols
        assert not missing, f"observation_view columns missing from fhir.Observation DDL: {missing}"

    @pytest.mark.parametrize(
        "view_name,table_name",
        [
            ("questionnaire_view", "Questionnaire"),
            ("questionnaire_item_view", "Questionnaire_item"),
            ("questionnaire_response_view", "QuestionnaireResponse"),
            ("questionnaire_response_item_view", "QuestionnaireResponse_item"),
        ],
    )
    def test_new_view_columns_subset_of_ddl(self, view_name: str, table_name: str, ddl_text: str):
        view_cols = set(_columns(_load_view(view_name)))
        ddl_cols = _extract_ddl_columns(ddl_text, table_name)
        missing = view_cols - ddl_cols
        assert not missing, f"{view_name} columns missing from [fhir].[{table_name}] DDL: {missing}"

    def test_qr_item_existing_table_migrates_answer_index(self, ddl_text: str):
        assert "COL_LENGTH('fhir.QuestionnaireResponse_item', 'answer_index') IS NULL" in ddl_text
        assert (
            "ADD [answer_index] NVARCHAR(10) NOT NULL "
            "CONSTRAINT [DF_QuestionnaireResponse_item_answer_index] DEFAULT N''"
        ) in ddl_text
        assert "c.[name] = N'answer_index'" in ddl_text
        assert "PRIMARY KEY CLUSTERED ([qr_id], [link_id], [item_key], [answer_index])" in ddl_text

    def test_questionnaire_item_pk_includes_item_key(self, ddl_text: str):
        assert "PRIMARY KEY CLUSTERED ([questionnaire_id], [link_id], [item_key])" in ddl_text

    def test_procedure_view_has_ifdb_aktivitet_id(self, ddl_text: str):
        cols = _columns(_load_view("procedure_view"))
        assert cols["ifdb_aktivitet_id"] == (
            "identifier.where(system='urn:source:ifdb:aktivitetsresultat_procedure').value.first()"
        )
        ddl_cols = _extract_ddl_columns(ddl_text, "Procedure") | _extract_alter_added_columns(
            ddl_text, "Procedure"
        )
        assert "ifdb_aktivitet_id" in ddl_cols

    def test_procedure_tillaegskode_view_excludes_user_selected_primary(self):
        view = _load_view("procedure_tillaegskode_view")
        for_each = [block.get("forEach") for block in view["select"] if block.get("forEach")]
        assert for_each == ["code.coding.where(userSelected.empty() or userSelected = false)"]

    def test_routing_decision_audit_table_exists(self, etl_ddl_text: str):
        assert "[meta].[IFDBAktivitetsresultatRoutingDecision]" in etl_ddl_text
        assert "[routing_source]" in etl_ddl_text
        assert "xml_fallback" in etl_ddl_text

    def test_sfi_alias_curation_tables_exist(self, codesystem_ddl_text: str):
        assert "[codesystem].[SFIAlias]" in codesystem_ddl_text
        assert "[codesystem].[SFIAliasMember]" in codesystem_ddl_text

    def test_dso_fhir_views_have_source_metadata_dependencies(self, dso_infra_text: str):
        assert "dso_infra_codesystem" in dso_infra_text
        assert "dso_infra_etl_routing" in dso_infra_text
        assert (
            'deps=["dso_infra_fhir", "dso_infra_codesystem", ' '"dso_infra_etl_routing"]'
        ) in dso_infra_text


@pytest.mark.unit
class TestSfiDatapointView:
    def test_sfi_datapoint_views_are_defined(self, views_text: str):
        assert "[fhir].[SFI_datapoint_build]" in views_text
        assert "[fhir].[SFI_datapoint]" in views_text

    def test_sfi_datapoint_has_observation_column_compatibility_guard(self, ddl_text: str):
        assert "name = N'ifdb_aktivitet_sfi_id'" in ddl_text
        assert "ALTER TABLE [fhir].[Observation]" in ddl_text
        assert "ADD [ifdb_aktivitet_sfi_id] NVARCHAR(255) NULL" in ddl_text

    def test_sfi_datapoint_unions_resources(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert (
            "UNION ALL" in body.upper()
        ), "fhir.SFI_datapoint_build must UNION ALL Observation and QR-item rows."
        assert "[fhir].[Observation]" in body
        assert "[fhir].[QuestionnaireResponse_item]" in body
        assert "[fhir].[QuestionnaireResponse]" in body
        assert "[fhir].[Questionnaire_item]" not in body, (
            "QR-item branch must NOT join fhir.Questionnaire_item: that template "
            "is assembled from observed activities and may be incomplete, so "
            "gating on it silently drops clinical answers. element_code is the QR "
            "item's own SFI link_id."
        )
        assert "CAST(N'Observation' AS NVARCHAR(50)) AS source_resource" in body
        assert "CAST(N'QuestionnaireResponse' AS NVARCHAR(50)) AS source_resource" in body
        assert "[fhir].[Procedure]" not in body
        assert "urn:source:ifdb:aktivitetsresultat_procedure" not in body

    def test_sfi_datapoint_build_qr_leg_derives_code_from_link_id(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        # The QR leg must NOT gate on the Questionnaire_item template.
        assert "qi.[questionnaire_url] = qref.[questionnaire_canonical]" not in body
        assert "qi.[item_key] = qri.[item_key]" not in body
        # element_code IS the QR item's own SFI link_id; enrichment is a LEFT JOIN.
        assert "qri.[link_id] AS element_code" in body
        assert "ON sc.[code] = qri.[link_id]" in body
        # qref canonical/version are still parsed for source_model_name attribution.
        assert "CHARINDEX(N'|', qr.[questionnaire])" in body
        assert "COALESCE(" in body

    def test_sfi_datapoint_consumer_reads_from_store(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint")
        assert "FROM [fhir].[SFI_datapoint_store]" in body
        assert "UNION ALL" not in body.upper()

    def test_sfi_datapoint_store_table_defined(self):
        store_ddl = STORE_DDL_PATH.read_text(encoding="utf-8")
        assert "[fhir].[SFI_datapoint_store]" in store_ddl
        assert "[datapoint_key] NVARCHAR(64) NOT NULL" in store_ddl
        assert "UX_FHIR_SFI_datapoint_store_DatapointKey" in store_ddl
        assert "CREATE CLUSTERED COLUMNSTORE INDEX [CCI_FHIR_SFI_datapoint_store]" in store_ddl
        assert "occurred_at_dt" in store_ddl
        assert "IX_FHIR_SFI_datapoint_store_PatientOccurredDt" in store_ddl
        # Storen er månedspartitioneret (SWITCH-indlæsning af backfill-vinduer); fritekst
        # over elementtekster ligger i codesystem-søgedokumentet, ikke her.
        assert "ps_SFI_datapoint_store_Maaned" in store_ddl
        assert "FULLTEXT" not in store_ddl.upper()

    def test_sfi_datapoint_store_phase3_migration_defined(self):
        migration_path = (
            REPO_ROOT
            / "infrastructure"
            / "ddl"
            / "migrations"
            / "20260604_fhir_sfi_datapoint_store_phase3.sql"
        )
        migration_sql = migration_path.read_text(encoding="utf-8")
        assert "occurred_at_dt" in migration_sql
        assert "FULLTEXT INDEX" in migration_sql.upper()

    def test_sfi_datapoint_consumer_exposes_occurred_at_dt(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint")
        assert "[occurred_at_dt]" in body
        build_body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert (
            "occurred_at_dt" not in build_body
        ), "occurred_at_dt is computed on SFI_datapoint_store, not in build view"

    def test_sfi_alias_member_view_defined(self):
        views_sql = CODESYSTEM_VIEWS_PATH.read_text(encoding="utf-8")
        assert "[codesystem].[vw_sfi_alias_member]" in views_sql
        assert "[codesystem].[SFIAlias]" in views_sql
        assert "[codesystem].[SFIAliasMember]" in views_sql
        assert "[codesystem].[SFIConcept]" in views_sql

    def test_sfi_search_document_tables_and_view_defined(self, codesystem_ddl_text: str):
        views_sql = CODESYSTEM_VIEWS_PATH.read_text(encoding="utf-8")
        assert "[codesystem].[SFISearchDocument]" in codesystem_ddl_text
        assert "[codesystem].[SFISearchOverride]" in codesystem_ddl_text
        assert "FULLTEXT INDEX ON [codesystem].[SFISearchDocument]" in codesystem_ddl_text
        assert "[codesystem].[vw_sfi_search_document]" in views_sql
        assert "[search_text]" in views_sql

    def test_dso_codesystem_views_asset_registered(self, dso_infra_text: str):
        assert "dso_infra_views_codesystem" in dso_infra_text
        assert 'deps=["dso_infra_codesystem"]' in dso_infra_text
        assert "views/codesystem" in dso_infra_text

    def test_ifdb_procedure_event_view_is_separate_from_sfi_datapoint(self, views_text: str):
        assert "[fhir].[IFDB_procedure_event]" in views_text
        assert "[vw_fhir].[IFDB_procedure_event]" in views_text
        m = re.search(
            r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[fhir\]\.\[IFDB_procedure_event\]\s+AS(.+?)\bGO\b",
            views_text,
            re.DOTALL | re.IGNORECASE,
        )
        assert m
        body = m.group(1)
        assert "[fhir].[Procedure]" in body
        assert "urn:source:ifdb:aktivitetsresultat_procedure" in body
        assert "clinical_event_id" in body

    def test_sfi_datapoint_has_consumer_contract_columns(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        missing = {col for col in SFI_DATAPOINT_CONSUMER_COLUMNS if col not in body}
        assert not missing, f"SFI_datapoint_build missing consumer columns: {missing}"

    def test_sfi_datapoint_lean_contract_excludes_fhir_internal_columns(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        excluded = (
            "datapoint_kind",
            "datapoint_id",
            "resource_id",
            "concept_system",
            "concept_code",
            "concept_display",
            "concept_source",
            "activity_type_code",
            "activity_type_display",
            "clinical_instance_id",
            "value_status",
            "value_coding_system",
            "last_updated",
        )
        for col in excluded:
            pattern = re.compile(rf"(?<![a-zA-Z_]){re.escape(col)}(?![a-zA-Z_])")
            assert not pattern.search(
                body
            ), f"SFI_datapoint should not expose {col} in lean contract"

    def test_sfi_datapoint_observation_source_model_has_element_code_fallback(
        self, views_text: str
    ):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert "COALESCE(" in body
        assert "op.[source_model_name]" in body
        assert "CHARINDEX(N'.', o.[element_code]) > 1" in body
        assert "LEFT(o.[element_code], CHARINDEX(N'.', o.[element_code]) - 1)" in body

    def test_sfi_datapoint_uses_value_text_column_name(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert "o.[value_string] AS value_text" in body
        assert "qri.[value_string] AS value_text" in body

    def test_sfi_datapoint_build_has_stable_source_grain(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert "HASHBYTES('SHA2_256'" in body
        assert "o.[observation_id] AS source_resource_id" in body
        assert "qri.[link_id] AS source_link_id" in body
        assert "qri.[item_key] AS source_item_key" in body
        assert "qri.[answer_index] AS source_answer_index" in body

    def test_sfi_datapoint_value_columns_keep_typed_contract(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        for col in (
            "value_type",
            "has_value",
            "value_int",
            "value_dec",
            "value_text",
            "value_datetime",
            "value_bool",
            "value_quantity",
            "value_unit",
            "value_coding_code",
            "value_coding_display",
        ):
            assert col in body

    def test_sfi_datapoint_filters_on_sfi_system(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        # Observation leg scopes to SFI via element_system. The QR leg has no
        # element_system column on QuestionnaireResponse_item, so it scopes via
        # the QR item's own SFI link_id (every QR here is SFI-sourced).
        assert f"o.[element_system] = N'{SFI_SYSTEM}'" in body, (
            "Observation branch of SFI_datapoint_build must filter on "
            "element_system = 'urn:dk:ifdb:sfi'."
        )
        assert (
            "WHERE qri.[link_id] <> N''" in body
        ), "QR-item branch must scope to coded SFI items via a non-empty link_id."

    def test_sfi_datapoint_does_not_filter_empty_observation_values(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        assert re.search(
            r"WHERE\s+o\.\[element_system\]\s*=\s*N'urn:dk:ifdb:sfi'\s*UNION\s+ALL",
            body,
            re.IGNORECASE | re.DOTALL,
        ), "Observation branch must not apply a value_* WHERE filter."

    def test_sfi_datapoint_excludes_routing_audit_columns(self, views_text: str):
        body = _extract_fhir_view_body(views_text, "SFI_datapoint_build")
        for col in ("routing_target", "routing_source", "routing_confidence", "routing_reason"):
            assert col not in body

    def test_vw_fhir_sfi_datapoint_passthrough_exists(self, views_text: str):
        assert (
            "[vw_fhir].[SFI_datapoint]" in views_text
        ), "vw_fhir.SFI_datapoint pass-through must exist for the DSO layer."
