# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Contract tests for fhir.SFI_datapoint_store ↔ consumer view ↔ refresh asset.

Verifies column parity, index coverage for documented user filter patterns,
and that internal store columns stay hidden from the consumer view.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
STORE_DDL = REPO_ROOT / "infrastructure" / "ddl" / "tables" / "fhir" / "02_sfi_datapoint_store.sql"
VIEWS_DDL = REPO_ROOT / "infrastructure" / "ddl" / "views" / "fhir" / "01_vw_fhir.sql"
REFRESH_PY = REPO_ROOT / "DSO" / "load" / "refresh_sfi_datapoint.py"
PHASE3_MIGRATION = (
    REPO_ROOT
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260604_fhir_sfi_datapoint_store_phase3.sql"
)
GRAIN_MIGRATION = (
    REPO_ROOT / "infrastructure" / "ddl" / "migrations" / "20260606_sfi_datapoint_store_grain.sql"
)
INCREMENTAL_REFRESH_MIGRATION = (
    REPO_ROOT
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260607_sfi_datapoint_incremental_refresh.sql"
)

STORE_INTERNAL_COLUMNS = frozenset({"sfi_datapoint_id", "refreshed_at"})
STORE_COMPUTED_COLUMNS = frozenset()

# Aftagernes filtre: punktopslag pr. patient via B-træet; perioder, elementkoder og
# modeller aggregeres fra columnstoren (segment-eliminering på partitionen).
USER_FILTER_INDEXES = {
    "patient_id": ["IX_FHIR_SFI_datapoint_store_PatientOccurredDt"],
    "element_code": ["CCI_FHIR_SFI_datapoint_store"],
    "occurred_at_dt": ["IX_FHIR_SFI_datapoint_store_PatientOccurredDt"],
    "source_model_name": ["CCI_FHIR_SFI_datapoint_store"],
}

# Fritekstsøgning i elementtekster hører hjemme i codesystem-søgedokumentet (få
# tusinde distinkte elementer med fuldtekst), ikke i storen: et fuldtekstindeks kræver
# et unikt én-kolonne-nøgleindeks, og det findes ikke på en partitioneret tabel, hvor
# enhver unik nøgle skal indeholde partitionskolonnen (SWITCH-kravet).
CODESYSTEM_SEARCH_COLUMNS = frozenset({"element_display", "source_element_label"})
CODESYSTEM_SEARCH_DDL = (
    REPO_ROOT / "infrastructure" / "ddl" / "tables" / "codesystem" / "01_codesystem_tables.sql"
)

# occurred_at findes kun som INCLUDE-kolonne: patientindekset nøgler på den
# typede occurred_at_dt, så filtre på strengformen er residual-prædikater og
# ikke seek-nøgler.
USER_FILTER_INCLUDE_ONLY = frozenset({"has_value", "source_resource", "value_type", "occurred_at"})

SOURCE_GRAIN_COLUMNS = frozenset(
    {
        "datapoint_key",
        "source_resource_id",
        "source_link_id",
        "source_item_key",
        "source_answer_index",
        "source_last_updated",
    }
)


def _extract_bracket_columns(sql_block: str) -> list[str]:
    return re.findall(r"\[(\w+)\]", sql_block)


def _extract_store_table_columns(store_ddl: str) -> set[str]:
    # Tabellen ligger på et partitionsskema (ON [ps_...]([occurred_at_dt])); arbejdstabellen
    # i samme fil har sit eget CREATE TABLE og rammes ikke af det navnebundne mønster.
    match = re.search(
        r"CREATE TABLE \[fhir\]\.\[SFI_datapoint_store\]\s*\((.*?)\)\s*ON \[",
        store_ddl,
        re.DOTALL | re.IGNORECASE,
    )
    assert match, "Could not parse SFI_datapoint_store CREATE TABLE"
    cols: set[str] = set()
    for line in match.group(1).splitlines():
        col_match = re.match(r"\s*\[(\w+)\]", line)
        if col_match:
            cols.add(col_match.group(1))
    return cols


def _extract_consumer_view_columns(views_ddl: str) -> list[str]:
    match = re.search(
        r"CREATE OR ALTER VIEW \[fhir\]\.\[SFI_datapoint\]\s+AS\s+SELECT(.+?)FROM \[fhir\]\.\[SFI_datapoint_store\]",
        views_ddl,
        re.DOTALL | re.IGNORECASE,
    )
    assert match, "Could not parse fhir.SFI_datapoint consumer view"
    return _extract_bracket_columns(match.group(1))


def _extract_refresh_insert_columns(refresh_py: str) -> list[str]:
    match = re.search(
        r'_REFRESH_INSERT_COLUMNS = """(.+?)"""',
        refresh_py,
        re.DOTALL,
    )
    assert match, "Could not parse _REFRESH_INSERT_COLUMNS"
    return _extract_bracket_columns(match.group(1))


@pytest.fixture(scope="module")
def store_ddl() -> str:
    return STORE_DDL.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def views_ddl() -> str:
    return VIEWS_DDL.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def refresh_py() -> str:
    return REFRESH_PY.read_text(encoding="utf-8")


@pytest.mark.unit
class TestSfiDatapointStoreContract:
    def test_consumer_view_columns_match_store_minus_internal(self, store_ddl: str, views_ddl: str):
        store_cols = _extract_store_table_columns(store_ddl)
        consumer_cols = _extract_consumer_view_columns(views_ddl)
        expected = sorted(store_cols - STORE_INTERNAL_COLUMNS)
        assert sorted(consumer_cols) == expected, (
            "Consumer view must expose all store columns except internal ones "
            f"(missing={set(expected) - set(consumer_cols)}, "
            f"extra={set(consumer_cols) - set(expected)})"
        )

    def test_consumer_view_hides_internal_columns(self, views_ddl: str):
        consumer_cols = set(_extract_consumer_view_columns(views_ddl))
        assert not consumer_cols & STORE_INTERNAL_COLUMNS

    def test_refresh_insert_columns_match_store_minus_identity_and_computed(
        self, store_ddl: str, refresh_py: str
    ):
        store_cols = _extract_store_table_columns(store_ddl)
        insert_cols = _extract_refresh_insert_columns(refresh_py)
        expected = sorted(store_cols - {"sfi_datapoint_id"} - STORE_COMPUTED_COLUMNS)
        assert sorted(insert_cols) == expected, (
            "Refresh INSERT must list all writable store columns "
            f"(missing={set(expected) - set(insert_cols)}, "
            f"extra={set(insert_cols) - set(expected)})"
        )

    def test_occurred_at_dt_is_written_by_refresh(self, refresh_py: str):
        insert_cols = set(_extract_refresh_insert_columns(refresh_py))
        assert "occurred_at_dt" in insert_cols

    def test_store_has_stable_source_grain_columns(self, store_ddl: str):
        store_cols = _extract_store_table_columns(store_ddl)
        assert SOURCE_GRAIN_COLUMNS <= store_cols
        assert "UX_FHIR_SFI_datapoint_store_DatapointKey" in store_ddl

    def test_build_view_populates_qr_grain(self, views_ddl: str):
        assert "qri.[link_id] AS source_link_id" in views_ddl
        assert "qri.[item_key] AS source_item_key" in views_ddl
        assert "qri.[answer_index] AS source_answer_index" in views_ddl
        assert "HASHBYTES('SHA2_256'" in views_ddl
        assert "COALESCE(qri.[answer_index], N'')" in views_ddl

    def test_grain_migration_is_defined(self):
        migration_sql = GRAIN_MIGRATION.read_text(encoding="utf-8")
        for col in SOURCE_GRAIN_COLUMNS:
            assert col in migration_sql
        assert "UX_FHIR_SFI_datapoint_store_DatapointKey" in migration_sql

    def test_incremental_refresh_support_index_is_defined(self):
        migration_sql = INCREMENTAL_REFRESH_MIGRATION.read_text(encoding="utf-8")
        assert "IX_FHIR_SFI_datapoint_store_SourceResource" in migration_sql
        assert "[source_resource]" in migration_sql
        assert "[source_resource_id]" in migration_sql

    def test_user_filter_indexes_exist(self, store_ddl: str):
        phase3 = PHASE3_MIGRATION.read_text(encoding="utf-8")
        grain = GRAIN_MIGRATION.read_text(encoding="utf-8")
        combined = store_ddl + phase3 + grain
        for filter_col, index_names in USER_FILTER_INDEXES.items():
            for index_name in index_names:
                assert (
                    index_name in combined
                ), f"Expected index {index_name} for user filter on {filter_col}"

    def test_store_has_no_fulltext_but_codesystem_search_has(self, store_ddl: str):
        """Fritekst over elementtekster ligger i codesystem-søgedokumentet, ikke i storen."""
        assert "FULLTEXT" not in store_ddl.upper()
        search_ddl = CODESYSTEM_SEARCH_DDL.read_text(encoding="utf-8")
        assert "FULLTEXT INDEX" in search_ddl.upper()
        for column in CODESYSTEM_SEARCH_COLUMNS:
            assert column in store_ddl, f"{column} skal stadig være en kolonne i storen"

    def test_store_is_month_partitioned_and_sequence_backed(self, store_ddl: str):
        """Partitionering er forudsætningen for SWITCH-indlæsning af backfill-vinduer."""
        assert (
            "CREATE PARTITION FUNCTION [pf_SFI_datapoint_store_Maaned] (DATETIME2(0))" in store_ddl
        )
        assert "RANGE RIGHT" in store_ddl
        assert "CREATE PARTITION SCHEME [ps_SFI_datapoint_store_Maaned]" in store_ddl
        assert "CREATE SEQUENCE [fhir].[seq_SFI_datapoint_store]" in store_ddl
        assert (
            re.search(r"IDENTITY\s*\(", store_ddl, re.IGNORECASE) is None
        ), "id kommer fra sekvensen, ikke IDENTITY"
        assert "TEXTIMAGE_ON" not in store_ddl.upper(), "LOB følger partitionen"
        assert (
            "CREATE CLUSTERED COLUMNSTORE INDEX [CCI_FHIR_SFI_datapoint_store]" in store_ddl
        ), "storen er en clustered columnstore"
        assert (
            "([datapoint_key] ASC, [occurred_at_dt] ASC)" in store_ddl
        ), "partitionskolonnen skal indgå i enhver unik nøgle"
        assert "[fhir].[SFI_datapoint_store_backfill]" in store_ddl

    def test_documented_filter_columns_are_indexed_or_ft_or_include(self, store_ddl: str):
        phase3 = PHASE3_MIGRATION.read_text(encoding="utf-8")
        grain = GRAIN_MIGRATION.read_text(encoding="utf-8")
        combined = store_ddl + phase3 + grain
        for filter_col in USER_FILTER_INDEXES:
            assert filter_col in combined

        for filter_col in USER_FILTER_INCLUDE_ONLY:
            assert filter_col in combined
            assert re.search(
                rf"INCLUDE\s*\([^)]*\[{filter_col}\]",
                combined,
                re.IGNORECASE,
            ), f"{filter_col} should appear in at least one index INCLUDE list"

    def test_store_has_expected_nc_index_count(self, store_ddl: str):
        nc_indexes = re.findall(
            r"CREATE NONCLUSTERED INDEX \[IX_FHIR_SFI_datapoint_store_\w+\]",
            store_ddl,
            re.IGNORECASE,
        )
        # Keep in sync with infrastructure/ddl/tables/fhir/02_sfi_datapoint_store.sql.
        # Columnstore + det unikke UX-indeks + PatientOccurredDt. Arbejdstabellen i samme
        # fil har ingen indeks i DDL'en (de bygges af refreshen lige før et SWITCH).
        assert len(nc_indexes) == 1, f"Expected 1 NC index on store, found {nc_indexes}"

    def test_refresh_uses_singleton_lock_and_no_truncate(self, refresh_py: str):
        assert "TRUNCATE TABLE" not in refresh_py.upper()
        assert "sys.sp_getapplock" in refresh_py
        assert '_REFRESH_LOCK_NAME = "fhir.SFI_datapoint_store.refresh"' in refresh_py

    def test_incremental_refresh_uses_batched_inline_apply(self, refresh_py: str):
        assert "def _build_inline_batch_apply_sql" in refresh_py
        assert "def _build_number_changed_resources_sql" in refresh_py
        assert "DELETE tgt" in refresh_py
        assert "#sfi_props_pivot" in refresh_py

    def test_reconciliation_uses_batched_delete_insert(self, refresh_py: str):
        assert "def _build_reconciliation_setup_sql" in refresh_py
        assert "def _build_reconciliation_batch_sql" in refresh_py
        assert "#reconcile_resources" in refresh_py


BASE_TABLES_DDL = REPO_ROOT / "infrastructure" / "ddl" / "tables" / "fhir" / "01_tables.sql"
DELTA_DISCOVERY_MIGRATION = (
    REPO_ROOT
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260609_fhir_delta_discovery_effective_timestamp.sql"
)

_DELTA_DISCOVERY_INDEXES = (
    "IX_FHIR_Observation_SystemDWEffective",
    "IX_FHIR_QR_DWEffective",
    "IX_FHIR_QRI_DWEffective",
)

MIGRATIONS_DIR = REPO_ROOT / "infrastructure" / "ddl" / "migrations"

_DELTA_DISCOVERY_TABLES = (
    "Observation",
    "QuestionnaireResponse",
    "QuestionnaireResponse_item",
)


class TestDeltaDiscoveryDdlSync:
    """The delta-discovery migration and the base table DDL must stay in sync.

    Codebase convention: schema changes land BOTH as an idempotent migration
    (existing deployments) and in the base CREATE TABLE DDL (fresh deploys).
    The sargable delta-discovery SQL in refresh_sfi_datapoint.py depends on
    DW_Timestamp_Effective existing on all three source tables.
    """

    def test_effective_timestamp_column_in_base_ddl_for_all_three_tables(self):
        base_sql = BASE_TABLES_DDL.read_text(encoding="utf-8")
        occurrences = base_sql.count(
            "[dw_timestamp_effective] AS "
            "ISNULL([dw_timestamp_update], [dw_timestamp_insert]) PERSISTED"
        )
        assert occurrences == 3, (
            "Expected dw_timestamp_effective persisted computed column on "
            f"Observation, QuestionnaireResponse and QuestionnaireResponse_item; found {occurrences}"
        )

    def test_delta_discovery_indexes_in_both_base_ddl_and_migration(self):
        base_sql = BASE_TABLES_DDL.read_text(encoding="utf-8")
        migration_sql = DELTA_DISCOVERY_MIGRATION.read_text(encoding="utf-8")
        for index_name in _DELTA_DISCOVERY_INDEXES:
            assert index_name in base_sql, f"{index_name} missing from base 01_tables.sql"
            assert (
                index_name in migration_sql
            ), f"{index_name} missing from the delta-discovery migration"

    @pytest.mark.unit
    def test_audit_column_migrations_drop_and_readd_computed_column(self):
        """ALTER COLUMN on dw_timestamp_insert/update requires dropping dw_timestamp_effective first.

        The persisted computed column depends on both audit columns, so SQL Server
        rejects the ALTER with error 5074 ("The column 'dw_timestamp_effective' is
        dependent on column 'dw_timestamp_insert'"). Any migration retrofitting the
        audit columns must drop the computed column (and its index) first and
        recreate it afterwards.
        """
        for migration in sorted(MIGRATIONS_DIR.glob("*.sql")):
            sql = migration.read_text(encoding="utf-8")
            for table in _DELTA_DISCOVERY_TABLES:
                alter = re.search(
                    rf"ALTER TABLE \[fhir\]\.\[{table}\] ALTER COLUMN "
                    rf"\[dw_timestamp_(?:insert|update)\]",
                    sql,
                )
                if alter is None:
                    continue
                drop = re.search(
                    rf"ALTER TABLE \[fhir\]\.\[{table}\] DROP COLUMN "
                    rf"\[dw_timestamp_effective\]",
                    sql,
                )
                add = re.search(
                    rf"ALTER TABLE \[fhir\]\.\[{table}\] ADD \[dw_timestamp_effective\]",
                    sql,
                )
                assert drop is not None, (
                    f"{migration.name} alters fhir.{table} audit columns without dropping "
                    "the dependent dw_timestamp_effective computed column (SQL Server error 5074)"
                )
                assert add is not None, (
                    f"{migration.name} drops dw_timestamp_effective on fhir.{table} "
                    "without recreating it"
                )
                assert drop.start() < alter.start(), (
                    f"{migration.name} drops dw_timestamp_effective on fhir.{table} "
                    "after the ALTER COLUMN instead of before it"
                )
                assert add.start() > alter.start(), (
                    f"{migration.name} recreates dw_timestamp_effective on fhir.{table} "
                    "before the ALTER COLUMN instead of after it"
                )


@pytest.mark.unit
def test_condition_source_identifier_migration_synced_to_base_ddl():
    """The source_identifier_value migration (column + index) must be mirrored 1:1 in base 01_tables.sql."""
    base_sql = BASE_TABLES_DDL.read_text(encoding="utf-8")
    assert "[source_identifier_value]" in base_sql
    assert "IX_FHIR_Condition_SourceIdentifierValue" in base_sql
