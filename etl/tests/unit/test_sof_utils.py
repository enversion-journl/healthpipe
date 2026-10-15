# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for SQL on FHIR utility functions.

Covers:
- MERGE SQL generation (single PK, composite PK, 3-column PK)
- SOF resource config validation
- NULL sentinel handling
- Encounter_Location PK collision scenarios (end-to-end simulation)
"""

from contextlib import contextmanager

import dagster as dg
import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    SOF_STREAM_FETCH_CHUNK_SIZE,
    _build_changed_parent_ids_query,
    _build_payload_diff_predicate,
    _build_questionnaire_response_key_normalization_sql,
    _build_sof_duplicate_conflict_count_sql,
    _build_sof_duplicate_stats_sql,
    _reconcile_parents_create_sql,
    _semantic_duplicate_columns,
    _temp_table_col_defs,
    _validate_sof_columns,
    apply_null_sentinels,
    build_child_grain_mark_deleted_sql,
    build_fhir_merge_sql,
    build_resource_grain_mark_deleted_sql,
    extract_and_merge_sof_view,
)

# =============================================================================
# MERGE SQL Generation
# =============================================================================


@pytest.mark.unit
class TestBuildFhirMergeSql:
    """Test T-SQL MERGE statement generation."""

    def test_simple_primary_key(self):
        """MERGE with a single-column PK."""
        sql = build_fhir_merge_sql(
            table_name="Encounter",
            columns=["encounter_id", "patient_id", "status"],
            primary_keys=["encounter_id"],
        )
        assert "MERGE [fhir].[Encounter] AS tgt" in sql
        assert "USING #SofTemp AS src" in sql
        assert "tgt.[encounter_id] = src.[encounter_id]" in sql
        assert "tgt.[patient_id] = src.[patient_id]" in sql
        assert "tgt.[status] = src.[status]" in sql
        assert "dw_timestamp_update" in sql
        assert "dw_timestamp_insert" in sql
        assert "WHEN MATCHED AND" in sql
        assert "WHEN NOT MATCHED BY TARGET THEN" in sql

    def test_composite_primary_key(self):
        """MERGE with a composite PK should join on all PK columns."""
        sql = build_fhir_merge_sql(
            table_name="Patient",
            columns=["patient_id", "given", "family", "gender"],
            primary_keys=["patient_id", "given", "family"],
        )
        assert "tgt.[patient_id] = src.[patient_id]" in sql
        assert "tgt.[given] = src.[given]" in sql
        assert "tgt.[family] = src.[family]" in sql
        assert (
            "tgt.[patient_id] = src.[patient_id],"
            not in sql.split("UPDATE SET")[1].split("WHEN")[0]
        )
        update_section = sql.split("UPDATE SET")[1].split("WHEN")[0]
        assert "tgt.[gender] = src.[gender]" in update_section

    def test_insert_includes_all_columns(self):
        """INSERT clause should include all source columns plus both dw timestamps."""
        sql = build_fhir_merge_sql(
            table_name="Location",
            columns=["location_id", "name", "status"],
            primary_keys=["location_id"],
        )
        insert_section = sql.split("INSERT (")[1].split(")")[0]
        assert "[location_id]" in insert_section
        assert "[name]" in insert_section
        assert "[status]" in insert_section
        assert "[dw_timestamp_insert]" in insert_section
        assert "[dw_timestamp_update]" in insert_section

    def test_update_excludes_primary_key_columns(self):
        """UPDATE should not set PK columns."""
        sql = build_fhir_merge_sql(
            table_name="Encounter",
            columns=["encounter_id", "patient_id", "status"],
            primary_keys=["encounter_id"],
        )
        update_part = sql.split("UPDATE SET")[1].split("WHEN NOT")[0]
        assert "tgt.[encounter_id] = src.[encounter_id]" not in update_part
        assert "tgt.[patient_id] = src.[patient_id]" in update_part
        assert "tgt.[status] = src.[status]" in update_part

    def test_payload_diff_predicate_excludes_audit_and_technical_timestamps(self):
        predicate = _build_payload_diff_predicate(
            [
                "qr_id",
                "questionnaire",
                "status",
                "last_updated",
                "created_at",
                "dw_timestamp_insert",
                "dw_timestamp_update",
            ],
            ["qr_id"],
        )

        assert "src.[questionnaire]" in predicate
        assert "src.[status]" in predicate
        assert "src.[qr_id]" not in predicate
        assert "last_updated" not in predicate
        assert "created_at" not in predicate
        assert "dw_timestamp_insert" not in predicate
        assert "dw_timestamp_update" not in predicate

    def test_deduplicated_source_uses_row_number_partitioned_by_pk(self):
        """Streaming MERGE can deduplicate staged source rows before matching."""
        sql = build_fhir_merge_sql(
            table_name="Patient_Address",
            columns=[
                "patient_id",
                "address_period_start",
                "address_line",
                "last_updated",
                "created_at",
            ],
            primary_keys=["patient_id", "address_period_start"],
            source_table="##SofTemp_test",
            deduplicate_source_by_pk=True,
            dedup_tiebreak_columns=["last_updated", "created_at"],
        )

        assert "ROW_NUMBER() OVER" in sql
        assert "PARTITION BY [patient_id], [address_period_start]" in sql
        assert "ORDER BY [last_updated] DESC, [created_at] DESC" in sql
        assert "FROM ##SofTemp_test AS src_base" in sql
        assert "WHERE __sof_dedup_rn = 1" in sql
        assert "MERGE [fhir].[Patient_Address] AS tgt" in sql

    def test_duplicate_stats_sql_counts_pk_groups_and_extra_rows(self):
        sql = _build_sof_duplicate_stats_sql(
            "##SofTemp_test",
            ["patient_id", "address_period_start"],
        )

        assert "GROUP BY [patient_id], [address_period_start]" in sql
        assert "HAVING COUNT_BIG(*) > 1" in sql
        assert "duplicate_group_count" in sql
        assert "duplicate_extra_row_count" in sql

    def test_duplicate_conflict_sql_compares_payload_not_audit_columns(self):
        columns = [
            "patient_id",
            "address_period_start",
            "address_line",
            "address_city",
            "last_updated",
            "created_at",
        ]

        semantic_columns = _semantic_duplicate_columns(
            columns,
            ["patient_id", "address_period_start"],
            ["last_updated", "created_at"],
        )
        sql = _build_sof_duplicate_conflict_count_sql(
            "##SofTemp_test",
            columns,
            ["patient_id", "address_period_start"],
            ["last_updated", "created_at"],
        )

        assert semantic_columns == ["address_line", "address_city"]
        assert sql is not None
        assert "a.[address_line]" in sql
        assert "a.[address_city]" in sql
        assert "a.[last_updated]" not in sql
        assert "a.[created_at]" not in sql
        assert "conflict_group_count" in sql

    def test_encounter_location_three_column_pk(self):
        """MERGE for Encounter_Location must join on all 3 PK columns
        and keep location_status, location_period_end as updatable."""
        columns = [
            "encounter_id",
            "location_id",
            "location_period_start",
            "location_status",
            "location_period_end",
            "last_updated",
            "created_at",
        ]
        pk = ["encounter_id", "location_id", "location_period_start"]
        sql = build_fhir_merge_sql("Encounter_Location", columns, pk)

        assert "tgt.[encounter_id] = src.[encounter_id]" in sql
        assert "tgt.[location_id] = src.[location_id]" in sql
        assert "tgt.[location_period_start] = src.[location_period_start]" in sql

        update_part = sql.split("UPDATE SET")[1].split("WHEN NOT")[0]
        assert "tgt.[encounter_id] = src.[encounter_id]" not in update_part
        assert "tgt.[location_id] = src.[location_id]" not in update_part
        assert "tgt.[location_period_start] = src.[location_period_start]" not in update_part
        assert "tgt.[location_status] = src.[location_status]" in update_part
        assert "tgt.[location_period_end] = src.[location_period_end]" in update_part
        assert "tgt.[last_updated] = src.[last_updated]" in update_part


@pytest.mark.unit
def test_questionnaire_response_key_normalization_sql_is_scoped_and_version_aware():
    sql = _build_questionnaire_response_key_normalization_sql("##SofTemp_test")

    assert "INNER JOIN ##SofTemp_test AS src" in sql
    assert "src.[qr_id] = qr.[qr_id]" in sql
    assert "[questionnaire_canonical]" in sql
    assert "[questionnaire_version]" in sql
    assert "CHARINDEX(N'|', qr.[questionnaire]) > 0" in sql
    assert "ELSE NULL" in sql
    # An in-place content fixup must refresh the audit timestamp, otherwise the
    # change is invisible to the dw_timestamp_effective-based SFI delta discovery.
    assert "[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))" in sql


# =============================================================================
# SOF Resource Configs
# =============================================================================


@pytest.mark.unit
class TestSofResourceConfigs:
    """Test SQL on FHIR resource configuration mappings."""

    def test_encounter_uses_single_primary_key(self):
        """Encounter should be deduplicated to one row per encounter."""
        encounter_cfg = SOF_RESOURCE_CONFIGS["encounter_view"]
        assert encounter_cfg["mssql_table"] == "Encounter"
        assert encounter_cfg["primary_keys"] == ["encounter_id"]
        assert "null_sentinel_columns" not in encounter_cfg

    def test_encounter_location_pk_includes_period_start(self):
        """Encounter_Location PK must include location_period_start to allow
        the same location to appear multiple times per encounter (different stays)."""
        cfg = SOF_RESOURCE_CONFIGS["encounter_location_view"]
        assert cfg["resource_table"] == "encounter"
        assert cfg["id_column"] == "encounter_id"
        assert cfg["mssql_table"] == "Encounter_Location"
        assert cfg["primary_keys"] == ["encounter_id", "location_id", "location_period_start"]

    def test_encounter_location_null_sentinels_cover_all_pk_nullable_cols(self):
        """Both location_id and location_period_start must have NULL sentinels
        since they participate in the PK but can be NULL from forEachOrNull."""
        cfg = SOF_RESOURCE_CONFIGS["encounter_location_view"]
        assert "location_id" in cfg["null_sentinel_columns"]
        assert "location_period_start" in cfg["null_sentinel_columns"]

    def test_questionnaire_item_uses_occurrence_key_without_dedup(self):
        cfg = SOF_RESOURCE_CONFIGS["questionnaire_item_view"]
        assert cfg["primary_keys"] == ["questionnaire_id", "link_id", "item_key"]
        assert cfg["null_sentinel_columns"] == ["link_id", "item_key"]
        assert "deduplicate_by_pk" not in cfg

    def test_all_configs_have_required_keys(self):
        """Every SOF config must have the minimum required keys."""
        required = {"view_name", "resource_table", "id_column", "mssql_table", "primary_keys"}
        for name, cfg in SOF_RESOURCE_CONFIGS.items():
            missing = required - set(cfg.keys())
            assert not missing, f"{name} missing keys: {missing}"


@pytest.mark.unit
class TestValidateSofColumns:
    def test_allows_required_columns(self):
        _validate_sof_columns(
            "questionnaire_item_view",
            ["questionnaire_id", "link_id", "item_key", "text"],
            ["questionnaire_id", "link_id", "item_key"],
        )

    def test_reports_stale_view_definition_shape(self):
        with pytest.raises(RuntimeError) as exc:
            _validate_sof_columns(
                "questionnaire_item_view",
                ["questionnaire_id", "link_id", "text"],
                ["questionnaire_id", "link_id", "item_key"],
            )

        assert "sof.questionnaire_item_view did not return required column(s): item_key" in str(
            exc.value
        )
        assert "fhir_sof_infrastructure" in str(exc.value)


# =============================================================================
# NULL Sentinel Handling
# =============================================================================


@pytest.mark.unit
class TestApplyNullSentinels:
    """Test NULL -> sentinel replacement for PK columns."""

    def test_replaces_none_with_empty_string(self):
        rows = [
            {"encounter_id": "E-1", "location_id": None, "location_period_start": None},
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])
        assert rows[0]["location_id"] == ""
        assert rows[0]["location_period_start"] == ""

    def test_preserves_existing_values(self):
        rows = [
            {
                "encounter_id": "E-1",
                "location_id": "L-1",
                "location_period_start": "2026-02-10T08:00",
            },
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])
        assert rows[0]["location_id"] == "L-1"
        assert rows[0]["location_period_start"] == "2026-02-10T08:00"

    def test_handles_mixed_null_and_values(self):
        rows = [
            {"encounter_id": "E-1", "location_id": "L-1", "location_period_start": None},
            {"encounter_id": "E-2", "location_id": None, "location_period_start": "2026-02-10"},
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])
        assert rows[0]["location_id"] == "L-1"
        assert rows[0]["location_period_start"] == ""
        assert rows[1]["location_id"] == ""
        assert rows[1]["location_period_start"] == "2026-02-10"


# =============================================================================
# Pre-MERGE Validation & Dedup
# =============================================================================


@pytest.mark.unit
@pytest.mark.skip(
    reason="validate_required_columns and deduplicate_by_pk were removed from sof_utils"
)
class TestPreMergeGuards:
    """Test defensive checks before MERGE for Encounter_Location."""

    def test_validate_required_columns_raises_when_location_has_no_period_start(self):
        rows = [
            {
                "encounter_id": "E-1",
                "location_id": "LOC-1",
                "location_period_start": "",
            }
        ]

        with pytest.raises(
            ValueError, match="Data validation failed for \\[fhir\\]\\.\\[Encounter_Location\\]"
        ):
            validate_required_columns(  # noqa: F821
                rows,
                trigger_column="location_id",
                required_columns=["location_period_start"],
                table_name="Encounter_Location",
            )

    def test_validate_required_columns_allows_for_each_or_null_fallback_row(self):
        rows = [
            {
                "encounter_id": "E-1",
                "location_id": "",
                "location_period_start": "",
            }
        ]

        validate_required_columns(  # noqa: F821
            rows,
            trigger_column="location_id",
            required_columns=["location_period_start"],
            table_name="Encounter_Location",
        )

    def test_deduplicate_by_pk_keeps_row_with_latest_tiebreak(self):
        rows = [
            {
                "encounter_id": "E-1",
                "location_id": "L-1",
                "location_period_start": "2026-02-10T08:00:00",
                "location_period_end": "2026-02-10T10:00:00",
                "last_updated": "2026-02-10T10:01:00",
                "created_at": "2026-02-10T08:00:00",
            },
            {
                "encounter_id": "E-1",
                "location_id": "L-1",
                "location_period_start": "2026-02-10T08:00:00",
                "location_period_end": "2026-02-10T12:00:00",
                "last_updated": "2026-02-10T12:01:00",
                "created_at": "2026-02-10T08:00:00",
            },
            {
                "encounter_id": "E-1",
                "location_id": "L-2",
                "location_period_start": "2026-02-10T09:00:00",
                "location_period_end": "2026-02-10T11:00:00",
                "last_updated": "2026-02-10T11:01:00",
                "created_at": "2026-02-10T09:00:00",
            },
        ]

        deduped, dropped = deduplicate_by_pk(  # noqa: F821
            rows,
            primary_keys=["encounter_id", "location_id", "location_period_start"],
        )

        assert dropped == 1
        assert len(deduped) == 2
        kept = [
            r
            for r in deduped
            if r["location_id"] == "L-1" and r["location_period_start"] == "2026-02-10T08:00:00"
        ][0]
        assert kept["location_period_end"] == "2026-02-10T12:00:00"


# =============================================================================
# Encounter_Location PK Collision Simulation
# =============================================================================


@pytest.mark.unit
class TestEncounterLocationPkCollisions:
    """
    End-to-end simulation of PK collision scenarios.

    Data lineage per scenario:
      Oracle IFDB.LOCATION (OBJECT_ID, LOCATION_CODE, PERIOD_START, PERIOD_END)
      -> FHIR Encounter.location[] (one EncounterLocation per IFDB row)
      -> Reference resolution: LOCATION_CODE -> Location UUID (same code = same UUID)
      -> SOF ViewDefinition: forEachOrNull flattens to one row per location entry
      -> Rows arrive at the SoF staging MERGE path as rows with keys:
        encounter_id, location_id, location_period_start, location_period_end,
        location_status, last_updated, created_at

    These tests verify that the 3-column PK (encounter_id, location_id,
    location_period_start) produces unique keys where the old 2-column PK
    would have collided.
    """

    @staticmethod
    def _make_row(
        encounter_id: str,
        location_id: str,
        period_start: str | None,
        period_end: str | None = None,
        status: str = "completed",
    ) -> dict:
        return {
            "encounter_id": encounter_id,
            "location_id": location_id,
            "location_period_start": period_start,
            "location_period_end": period_end,
            "location_status": status,
            "last_updated": "2026-02-25T10:00:00",
            "created_at": "2026-02-25T09:00:00",
        }

    @staticmethod
    def _has_collisions(rows: list[dict], pk_cols: list[str]) -> bool:
        keys = [tuple(str(r.get(c, "")) for c in pk_cols) for r in rows]
        return len(keys) != len(set(keys))

    OLD_PK = ["encounter_id", "location_id"]
    NEW_PK = ["encounter_id", "location_id", "location_period_start"]

    def test_scenario_readmission_same_ward(self):
        """Patient moved: AfdA -> AfdB -> AfdA (back to same ward).

        Source trace (IFDB.LOCATION):
          Row 1: OBJECT_ID=E-001, LOCATION_CODE=AfdA, PERIOD_START=08:00, PERIOD_END=14:00
          Row 2: OBJECT_ID=E-001, LOCATION_CODE=AfdB, PERIOD_START=14:00, PERIOD_END=20:00
          Row 3: OBJECT_ID=E-001, LOCATION_CODE=AfdA, PERIOD_START=20:00, PERIOD_END=08:00

        After reference resolution: AfdA -> UUID-A, AfdB -> UUID-B

        Old PK (enc, loc): Row 1 and Row 3 collide (both E-001, UUID-A).
        New PK (enc, loc, start): All 3 rows unique.
        """
        rows = [
            self._make_row("E-001", "UUID-A", "2026-02-10T08:00:00", "2026-02-10T14:00:00"),
            self._make_row("E-001", "UUID-B", "2026-02-10T14:00:00", "2026-02-10T20:00:00"),
            self._make_row("E-001", "UUID-A", "2026-02-10T20:00:00", "2026-02-11T08:00:00"),
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])

        assert self._has_collisions(rows, self.OLD_PK), "Old PK should collide"
        assert not self._has_collisions(rows, self.NEW_PK), "New PK should NOT collide"

    def test_scenario_qa_error_reproduction(self):
        """Reproduces the exact QA error UUIDs.

        Source trace: Two IFDB.LOCATION rows for same encounter and location
        with different time periods.

        Old PK: collision (same encounter_id + location_id).
        New PK: no collision (different location_period_start).
        """
        enc = "005a4829-0102-4b3d-ac24-da64023fa8c2"
        loc = "9e45b12d-2b36-4253-b106-362d9ffc28ec"
        rows = [
            self._make_row(enc, loc, "2026-02-20T08:00:00", "2026-02-20T12:00:00"),
            self._make_row(enc, loc, "2026-02-20T14:00:00", "2026-02-20T18:00:00"),
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])

        assert self._has_collisions(rows, self.OLD_PK), "Old PK should collide (QA repro)"
        assert not self._has_collisions(rows, self.NEW_PK), "New PK should NOT collide"

    def test_scenario_two_encounters_same_location_no_collision(self):
        """Two different encounters at the same location.

        Source trace: IFDB.LOCATION has two rows with DIFFERENT OBJECT_IDs
        but same LOCATION_CODE.

        Neither old nor new PK should collide (encounter_id differs).
        """
        rows = [
            self._make_row("E-001", "UUID-A", "2026-02-10T08:00:00", "2026-02-10T14:00:00"),
            self._make_row("E-002", "UUID-A", "2026-02-10T08:00:00", "2026-02-10T14:00:00"),
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])

        assert not self._has_collisions(rows, self.OLD_PK)
        assert not self._has_collisions(rows, self.NEW_PK)

    def test_scenario_null_period_start_sentinel(self):
        """PERIOD_START is NOT NULL in Oracle, but if ETL parse fails,
        location_period_start could arrive as None.

        After sentinel application: None -> '' for both rows.
        This makes them collide on the new PK, but this is an ETL error
        scenario, not a legitimate data case.

        The sentinel prevents a NOT NULL constraint crash; the collision
        means one row wins (last-write-wins in MERGE), which is acceptable
        for broken data.
        """
        rows = [
            self._make_row("E-001", "UUID-A", None, None, "active"),
            self._make_row("E-001", "UUID-A", None, None, "planned"),
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])

        assert rows[0]["location_period_start"] == ""
        assert rows[1]["location_period_start"] == ""
        assert self._has_collisions(
            rows, self.NEW_PK
        ), "NULL sentinel rows collide -- expected for ETL error case"

    def test_scenario_encounter_without_locations(self):
        """Encounter with no locations gets one row from forEachOrNull
        where location_id is NULL (sentinel -> '').

        Source trace: IFDB.ENCOUNTER exists but IFDB.LOCATION has no rows
        for that OBJECT_ID. SOF forEachOrNull produces one row with all
        location columns NULL.
        """
        rows = [
            self._make_row("E-001", None, None),
        ]
        apply_null_sentinels(rows, ["location_id", "location_period_start"])

        assert rows[0]["location_id"] == ""
        assert rows[0]["location_period_start"] == ""
        assert not self._has_collisions(rows, self.NEW_PK)

    def test_merge_sql_update_semantics_with_new_pk(self):
        """When a location stay is updated (e.g. period_end changes from NULL
        to a date), the MERGE should UPDATE the existing row, not INSERT a new one.

        This works because location_period_start (part of PK) is stable,
        only location_period_end and location_status change.
        """
        cfg = SOF_RESOURCE_CONFIGS["encounter_location_view"]
        pk = cfg["primary_keys"]
        columns = [
            "encounter_id",
            "location_id",
            "location_period_start",
            "location_status",
            "location_period_end",
            "last_updated",
            "created_at",
        ]
        sql = build_fhir_merge_sql(cfg["mssql_table"], columns, pk)

        update_part = sql.split("UPDATE SET")[1].split("WHEN NOT")[0]
        assert "tgt.[location_period_end] = src.[location_period_end]" in update_part
        assert "tgt.[location_status] = src.[location_status]" in update_part

        assert "tgt.[encounter_id] = src.[encounter_id]" not in update_part
        assert "tgt.[location_id] = src.[location_id]" not in update_part
        assert "tgt.[location_period_start] = src.[location_period_start]" not in update_part


# =============================================================================
# Streaming Defaults
# =============================================================================


@pytest.mark.unit
class TestSofStreamingDefaults:
    """Test default chunk settings used by SoF streaming merge."""

    def test_stream_fetch_chunk_size_is_reasonable(self):
        """Chunk size should keep memory bounded while minimizing round-trips."""
        assert SOF_STREAM_FETCH_CHUNK_SIZE >= 1_000
        assert SOF_STREAM_FETCH_CHUNK_SIZE <= 100_000


# =============================================================================
# Streaming Extract + Merge
# =============================================================================


class _FakePgCursor:
    def __init__(self, chunks):
        self._chunks = list(chunks)
        self.description = [
            ("encounter_id",),
            ("location_id",),
            ("last_updated",),
            ("created_at",),
        ]
        self._last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _sql, _params=None):
        self._last_sql = _sql
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        rows = []
        for chunk in self._chunks:
            rows.extend(chunk)
        self._chunks.clear()
        return rows

    def fetchmany(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return []


class _FakePgConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, name=None, **kwargs):
        # Accept psycopg's server-side cursor kwarg `name` (and any future
        # kwargs) without changing fake behaviour — we always return the
        # same scripted cursor regardless of named/unnamed mode.
        return self._cursor


class _FakePgResource:
    def __init__(self, cursor):
        self._cursor = cursor

    @contextmanager
    def get_connection(self):
        yield _FakePgConn(self._cursor)


class _FakeMssqlConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        return None

    def rollback(self):
        return None


class _FakeMssqlResource:
    host = "fake-host"
    port = 0
    database = "fake-db"

    def __init__(self, cursor):
        self._cursor = cursor

    @contextmanager
    def get_connection(self, *args, **kwargs):
        yield _FakeMssqlConn(self._cursor)


@pytest.mark.unit
class TestExtractAndMergeSofView:
    def test_uses_bulkcopy_when_available(self):
        pg_cursor = _FakePgCursor(
            chunks=[
                [
                    ("enc-1", None, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
                    ("enc-2", "loc-2", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
                ]
            ]
        )

        class MssqlCursor:
            rowcount = 5

            def __init__(self):
                self._schema_fetched = False
                self.bulkcopy_calls = []
                self.executemany_calls = []

            def execute(self, _sql, _params=None):
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("encounter_id", "nvarchar", 255, None, None),
                    ("location_id", "nvarchar", 255, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                self.bulkcopy_calls.append((table, data, kwargs))
                return {"rows_copied": len(data)}

            def executemany(self, sql, rows):
                self.executemany_calls.append((sql, list(rows)))

            def close(self):
                return None

        mssql_cursor = MssqlCursor()
        source_rows, rows_affected = extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_FakeMssqlResource(mssql_cursor),
            view_name="encounter_location_view",
            resource_table="encounter",
            id_column="encounter_id",
            table_name="Encounter_Location",
            primary_keys=["encounter_id", "location_id"],
            null_sentinel_columns=["location_id"],
        )

        assert source_rows == 2
        assert rows_affected == 5
        assert len(mssql_cursor.bulkcopy_calls) == 1
        assert not mssql_cursor.executemany_calls
        bulkcopy_rows = mssql_cursor.bulkcopy_calls[0][1]
        assert bulkcopy_rows[0][1] == ""

    def test_streams_multiple_chunks_one_bulkcopy_each(self):
        """Each fetchmany chunk is staged immediately — never the whole set.

        Guards the OOM fix: peak memory is one chunk, so N source chunks map to
        N bulkcopy calls (no fetchall() of the full partition).
        """
        pg_cursor = _FakePgCursor(
            chunks=[
                [("enc-1", "loc-1", "t", "t"), ("enc-2", "loc-2", "t", "t")],
                [("enc-3", "loc-3", "t", "t")],
                [("enc-4", "loc-4", "t", "t"), ("enc-5", "loc-5", "t", "t")],
            ]
        )

        class MssqlCursor:
            rowcount = 5

            def __init__(self):
                self._schema_fetched = False
                self.bulkcopy_calls = []

            def execute(self, _sql, _params=None):
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("encounter_id", "nvarchar", 255, None, None),
                    ("location_id", "nvarchar", 255, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                self.bulkcopy_calls.append((table, list(data), kwargs))
                return {"rows_copied": len(data)}

            def close(self):
                return None

        mssql_cursor = MssqlCursor()
        source_rows, _ = extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_FakeMssqlResource(mssql_cursor),
            view_name="encounter_location_view",
            resource_table="encounter",
            id_column="encounter_id",
            table_name="Encounter_Location",
            primary_keys=["encounter_id", "location_id"],
        )

        assert source_rows == 5
        # One bulkcopy per source chunk → streaming, not a single fetchall buffer.
        assert len(mssql_cursor.bulkcopy_calls) == 3
        assert [len(c[1]) for c in mssql_cursor.bulkcopy_calls] == [2, 1, 2]

    def test_absent_boolean_flags_become_zero_not_null(self):
        """An absent FHIR extension projects NULL. address_protection would then
        be dropped by a `= 0` filter, and dw_is_deleted is NOT NULL on
        fhir.Patient, so a NULL fails the MERGE outright."""
        pg_cursor = _FakePgCursor(
            chunks=[[("p-1", None, True, None, "t", "t"), ("p-2", True, False, 1, "t", "t")]]
        )
        pg_cursor.description = [
            ("patient_id",),
            ("address_protection",),
            ("active",),
            ("dw_is_deleted",),
            ("last_updated",),
            ("created_at",),
        ]

        class MssqlCursor:
            rowcount = 2

            def __init__(self):
                self._schema_fetched = False
                self.bulkcopy_calls = []

            def execute(self, _sql, _params=None):
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("patient_id", "nvarchar", 255, None, None),
                    ("address_protection", "bit", None, None, None),
                    ("active", "bit", None, None, None),
                    ("dw_is_deleted", "bit", None, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                self.bulkcopy_calls.append((table, list(data), kwargs))
                return {"rows_copied": len(data)}

            def close(self):
                return None

        mssql_cursor = MssqlCursor()
        extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_FakeMssqlResource(mssql_cursor),
            view_name="patient_view",
            resource_table="patient",
            id_column="patient_id",
            table_name="Patient",
            primary_keys=["patient_id"],
        )

        rows = mssql_cursor.bulkcopy_calls[0][1]
        assert rows[0][1] == 0
        assert rows[0][3] == 0
        # A delivered 1 must survive; the default only fills an absent value.
        assert rows[1][3] == 1
        assert rows[1][1] is True

    @pytest.mark.skip(
        reason="bulkcopy->executemany fallback not implemented in extract_and_merge_sof_view"
    )
    def test_falls_back_to_executemany_when_bulkcopy_fails(self):
        pg_cursor = _FakePgCursor(
            chunks=[[("enc-1", "loc-1", "2026-01-01T00:00:00", "2026-01-01T00:00:00")]]
        )

        class MssqlCursor:
            rowcount = 1

            def __init__(self):
                self._schema_fetched = False
                self.executemany_calls = []

            def execute(self, _sql, _params=None):
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("encounter_id", "nvarchar", 255, None, None),
                    ("location_id", "nvarchar", 255, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def bulkcopy(self, *_args, **_kwargs):
                raise RuntimeError("Simulated bulkcopy failure")

            def executemany(self, sql, rows):
                self.executemany_calls.append((sql, list(rows)))

            def close(self):
                return None

        mssql_cursor = MssqlCursor()
        source_rows, rows_affected = extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_FakeMssqlResource(mssql_cursor),
            view_name="encounter_location_view",
            resource_table="encounter",
            id_column="encounter_id",
            table_name="Encounter_Location",
            primary_keys=["encounter_id", "location_id"],
        )

        assert source_rows == 1
        assert rows_affected == 1
        assert len(mssql_cursor.executemany_calls) == 1


@pytest.mark.unit
class TestTempTableColDefs:
    """The staging temp table must collate its CHAR columns to the target DB, not
    tempdb (the server-instance collation), or the MERGE against the Danish fhir.*
    target raises a collation conflict when the instance is still Latin1."""

    def test_char_columns_get_collate_database_default(self):
        col_defs = _temp_table_col_defs(
            ["encounter_id", "status", "note"],
            {"encounter_id": "INT", "status": "NVARCHAR(50)", "note": "VARCHAR(MAX)"},
        )
        # char types collated on both sides of the eventual MERGE join
        assert "[status] NVARCHAR(50) COLLATE DATABASE_DEFAULT NULL" in col_defs
        assert "[note] VARCHAR(MAX) COLLATE DATABASE_DEFAULT NULL" in col_defs
        # non-char type left untouched (COLLATE is illegal on INT)
        assert "[encounter_id] INT NULL" in col_defs
        assert "encounter_id] INT COLLATE" not in col_defs

    def test_unknown_column_falls_back_to_collated_nvarchar(self):
        # the NVARCHAR(500) fallback is a char type, so it must be collated too
        col_defs = _temp_table_col_defs(["mystery"], {})
        assert "[mystery] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL" in col_defs

    def test_non_char_types_never_collated(self):
        col_defs = _temp_table_col_defs(
            ["a", "b", "c", "d"],
            {"a": "DATETIME2", "b": "BIT", "c": "UNIQUEIDENTIFIER", "d": "DECIMAL(18,2)"},
        )
        assert "COLLATE" not in col_defs


@pytest.mark.unit
class TestChildGrainReconcileSql:
    def test_full_load_marks_entire_table(self):
        sql = build_child_grain_mark_deleted_sql("Patient_Address", "patient_id", full_load=True)
        assert "UPDATE [fhir].[Patient_Address]" in sql
        assert "SET [dw_is_deleted] = 1" in sql
        assert "WHERE [dw_is_deleted] = 0" in sql
        assert "INNER JOIN" not in sql

    def test_resource_grain_marks_only_ids_absent_from_stage(self):
        sql = build_resource_grain_mark_deleted_sql(
            "Organization", "organization_id", source_table="##SofTemp_abc"
        )
        assert "NOT EXISTS" in sql
        assert "##SofTemp_abc" in sql
        assert "s.[organization_id] = tgt.[organization_id]" in sql

    def test_delta_is_scoped_to_parent_ids(self):
        sql = build_child_grain_mark_deleted_sql(
            "Patient_Address", "patient_id", full_load=False, ids_table="#ReconcileParents"
        )
        assert "INNER JOIN #ReconcileParents AS ids ON ids.[id] = tgt.[patient_id]" in sql
        assert "WHERE tgt.[dw_is_deleted] = 0" in sql

    def test_parent_id_query_uses_the_extract_window(self):
        sql, params = _build_changed_parent_ids_query("patient", "2026-01-01", None)
        assert "SELECT r.id FROM patient r" in sql
        assert "WHERE r.ts >= %s" in sql
        assert params == ["2026-01-01"]

    def test_delta_parent_ids_table_collates_to_database_default(self):
        sql = _reconcile_parents_create_sql()
        assert "[id] NVARCHAR(255) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY" in sql


class _RecordingMssqlConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _RecordingMssqlResource:
    host = "fake-host"
    port = 0
    database = "fake-db"

    def __init__(self, conn):
        self._conn = conn

    @contextmanager
    def get_connection(self, *args, **kwargs):
        yield self._conn


class _PatientAddressPgCursor:
    def __init__(self, parent_ids, chunks):
        self._parent_ids = parent_ids
        self._chunks = list(chunks)
        self.description = [
            ("patient_id",),
            ("address_period_start",),
            ("last_updated",),
            ("created_at",),
        ]
        self._last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, _params=None):
        self._last_sql = sql
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        if "SELECT r.id FROM" in self._last_sql:
            return [(pid,) for pid in self._parent_ids]
        return []

    def fetchmany(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return []


@pytest.mark.unit
class TestChildGrainReconcileTransaction:
    def test_merge_failure_rolls_back_mark_all(self):
        pg_cursor = _PatientAddressPgCursor(
            parent_ids=["p-1"],
            chunks=[[("p-1", "2020-01-01", "t", "t")]],
        )

        class MssqlCursor:
            rowcount = 1

            def __init__(self):
                self._schema_fetched = False

            def execute(self, sql, _params=None):
                if "MERGE" in sql:
                    raise RuntimeError("merge failed")
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("patient_id", "nvarchar", 255, None, None),
                    ("address_period_start", "datetime2", None, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                return {"rows_copied": len(data)}

            def executemany(self, sql, rows):
                return None

            def close(self):
                return None

        mssql_cursor = MssqlCursor()
        mssql_conn = _RecordingMssqlConn(mssql_cursor)
        with pytest.raises(RuntimeError, match="merge failed"):
            extract_and_merge_sof_view(
                fhir_database=_FakePgResource(pg_cursor),
                mssql_target=_RecordingMssqlResource(mssql_conn),
                view_name="patient_address_view",
                resource_table="patient",
                id_column="patient_id",
                table_name="Patient_Address",
                primary_keys=["patient_id", "address_period_start"],
                partition_start="2026-01-01",
                normalized_datetime_columns=["address_period_start"],
            )

        assert mssql_conn.rollbacks == 1
        assert mssql_conn.commits >= 1

    def _zero_row_mssql(self):
        class MssqlCursor:
            rowcount = 0

            def __init__(self):
                self._schema_fetched = False
                self.executed = []

            def execute(self, sql, _params=None):
                self.executed.append(sql)
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("patient_id", "nvarchar", 255, None, None),
                    ("address_period_start", "datetime2", None, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                return {"rows_copied": len(data)}

            def executemany(self, sql, rows):
                return None

            def close(self):
                return None

        return MssqlCursor()

    def test_full_load_zero_rows_refuses_mark_deleted(self):
        pg_cursor = _PatientAddressPgCursor(parent_ids=[], chunks=[])
        mssql_cursor = self._zero_row_mssql()
        mssql_conn = _RecordingMssqlConn(mssql_cursor)

        with pytest.raises(dg.Failure, match="extract returned 0 rows"):
            extract_and_merge_sof_view(
                fhir_database=_FakePgResource(pg_cursor),
                mssql_target=_RecordingMssqlResource(mssql_conn),
                view_name="patient_address_view",
                resource_table="patient",
                id_column="patient_id",
                table_name="Patient_Address",
                primary_keys=["patient_id", "address_period_start"],
                partition_start=None,
                normalized_datetime_columns=["address_period_start"],
            )

        assert not any("dw_is_deleted" in sql for sql in mssql_cursor.executed)
        assert mssql_conn.rollbacks == 1

    def test_delta_zero_rows_with_parent_ids_marks_only_changed_parents_deleted(self):
        pg_cursor = _PatientAddressPgCursor(parent_ids=["p-1", "p-2"], chunks=[])
        mssql_cursor = self._zero_row_mssql()
        mssql_conn = _RecordingMssqlConn(mssql_cursor)

        extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_RecordingMssqlResource(mssql_conn),
            view_name="patient_address_view",
            resource_table="patient",
            id_column="patient_id",
            table_name="Patient_Address",
            primary_keys=["patient_id", "address_period_start"],
            partition_start="2026-01-01",
            normalized_datetime_columns=["address_period_start"],
        )

        assert any("#ReconcileParents" in sql for sql in mssql_cursor.executed)
        assert any("dw_is_deleted" in sql for sql in mssql_cursor.executed)
        assert mssql_conn.commits >= 1


class _CarePlanActivityPgCursor:
    """SoF cursor for careplan_activity_view: parent CarePlans, child activities."""

    def __init__(self, parent_ids, chunks):
        self._parent_ids = parent_ids
        self._chunks = list(chunks)
        self.description = [
            ("careplan_id",),
            ("ifdb_eventmarker_id",),
            ("last_updated",),
            ("created_at",),
        ]
        self._last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, _params=None):
        self._last_sql = sql
        return None

    def fetchone(self):
        return None

    def fetchall(self):
        if "SELECT r.id FROM" in self._last_sql:
            return [(pid,) for pid in self._parent_ids]
        return []

    def fetchmany(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return []


@pytest.mark.unit
class TestAllowEmptyChildExtract:
    """careplan_activity_view opts out of the empty-extract guard on deltas.

    A CarePlan whose markers were all retired is re-posted with zero activities
    on purpose — that is what puts it in the reconcile window. So a quiet night
    whose only change is such a patient produces a legitimately empty extract,
    and failing on it would block the very retirement the reconcile exists for.
    """

    def _zero_row_mssql(self):
        class MssqlCursor:
            rowcount = 0

            def __init__(self):
                self._schema_fetched = False
                self.executed = []

            def execute(self, sql, _params=None):
                self.executed.append(sql)
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("careplan_id", "nvarchar", 255, None, None),
                    ("ifdb_eventmarker_id", "nvarchar", 255, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (0, 0, 0)

            def bulkcopy(self, table, data, **kwargs):
                return {"rows_copied": len(data)}

            def executemany(self, sql, rows):
                return None

            def close(self):
                return None

        return MssqlCursor()

    def _run(self, *, parent_ids, partition_start):
        pg_cursor = _CarePlanActivityPgCursor(parent_ids=parent_ids, chunks=[])
        mssql_cursor = self._zero_row_mssql()
        mssql_conn = _RecordingMssqlConn(mssql_cursor)
        extract_and_merge_sof_view(
            fhir_database=_FakePgResource(pg_cursor),
            mssql_target=_RecordingMssqlResource(mssql_conn),
            view_name="careplan_activity_view",
            resource_table="careplan",
            id_column="careplan_id",
            table_name="CarePlan_Activity",
            primary_keys=["ifdb_eventmarker_id"],
            partition_start=partition_start,
        )
        return mssql_cursor, mssql_conn

    def test_delta_zero_rows_still_marks_the_emptied_parents_deleted(self):
        mssql_cursor, mssql_conn = self._run(parent_ids=["cp-1"], partition_start="2026-01-01")

        marked = [sql for sql in mssql_cursor.executed if "dw_is_deleted" in sql]
        assert marked, "the emptied parent's activity rows must still be retired"
        # Marked against the changed-parent temp table, not the whole table.
        assert any("#ReconcileParents" in sql for sql in marked)
        assert mssql_conn.rollbacks == 0
        assert mssql_conn.commits >= 1

    def test_full_load_zero_rows_still_refuses(self):
        # On a full load an empty extract always means the view or the connection
        # is broken — marking on it would retire the entire table.
        with pytest.raises(dg.Failure, match="extract returned 0 rows"):
            self._run(parent_ids=[], partition_start=None)


@pytest.mark.unit
class TestResourceGrainReconcile:
    def _org_pg_cursor(self):
        cursor = _FakePgCursor(chunks=[[("org-1", "true", "t", "t")]])
        cursor.description = [
            ("organization_id",),
            ("active",),
            ("last_updated",),
            ("created_at",),
        ]
        return cursor

    def _org_mssql_cursor(self, *, mark_rowcount=0, live_count=10_000):
        class MssqlCursor:
            def __init__(self):
                self.rowcount = 1
                self._schema_fetched = False
                self.executed = []
                self._live_count = live_count
                self._mark_rowcount = mark_rowcount

            def execute(self, sql, _params=None):
                self.executed.append(sql)
                if "NOT EXISTS" in sql and "dw_is_deleted" in sql:
                    self.rowcount = self._mark_rowcount
                return None

            def fetchall(self):
                if self._schema_fetched:
                    return []
                self._schema_fetched = True
                return [
                    ("organization_id", "nvarchar", 255, None, None),
                    ("active", "varchar", 10, None, None),
                    ("last_updated", "datetime2", None, None, None),
                    ("created_at", "datetime2", None, None, None),
                ]

            def fetchone(self):
                return (self._live_count,)

            def bulkcopy(self, table, data, **kwargs):
                return {"rows_copied": len(data)}

            def close(self):
                return None

        return MssqlCursor()

    def test_full_load_marks_only_ids_absent_from_stage(self):
        mssql_cursor = self._org_mssql_cursor()
        extract_and_merge_sof_view(
            fhir_database=_FakePgResource(self._org_pg_cursor()),
            mssql_target=_FakeMssqlResource(mssql_cursor),
            view_name="organization_view",
            resource_table="organization",
            id_column="organization_id",
            table_name="Organization",
            primary_keys=["organization_id"],
            partition_start=None,
        )
        mark_sql = [s for s in mssql_cursor.executed if "dw_is_deleted" in s and "NOT EXISTS" in s]
        assert mark_sql, "resource-grain reconcile must mark via NOT EXISTS against the stage table"
        assert "organization_id" in mark_sql[0]
        blanket = [
            s
            for s in mssql_cursor.executed
            if "SET [dw_is_deleted] = 1" in s and "NOT EXISTS" not in s
        ]
        assert not blanket

    def test_mark_count_above_threshold_fails(self):
        mssql_cursor = self._org_mssql_cursor(mark_rowcount=2000, live_count=10_000)
        with pytest.raises(dg.Failure, match="exceeds threshold"):
            extract_and_merge_sof_view(
                fhir_database=_FakePgResource(self._org_pg_cursor()),
                mssql_target=_FakeMssqlResource(mssql_cursor),
                view_name="organization_view",
                resource_table="organization",
                id_column="organization_id",
                table_name="Organization",
                primary_keys=["organization_id"],
                partition_start=None,
            )

    def test_composite_key_is_refused(self):
        """The reconcile anti-joins on id_column alone, so a composite MERGE key would
        match on a prefix and retire rows that are still in the extract."""
        mssql_cursor = self._org_mssql_cursor()
        with pytest.raises(dg.Failure, match="requires the MERGE key"):
            extract_and_merge_sof_view(
                fhir_database=_FakePgResource(self._org_pg_cursor()),
                mssql_target=_FakeMssqlResource(mssql_cursor),
                view_name="organization_view",
                resource_table="organization",
                id_column="organization_id",
                table_name="Organization",
                primary_keys=["organization_id", "active"],
                partition_start=None,
            )
