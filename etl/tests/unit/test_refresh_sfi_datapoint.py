# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SFI_datapoint store incremental refresh assets."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from dagster import AssetKey
from DSO.load.refresh_sfi_datapoint import (
    _DEFAULT_INCREMENTAL_BATCH_SIZE,
    _DEFAULT_QR_ZERO_WARN_THRESHOLD,
    _INCREMENTAL_BATCH_SIZE_ENV,
    _QR_PREFLIGHT_MODE_ENV,
    _QR_PREFLIGHT_MODE_FAIL,
    _QR_PREFLIGHT_MODE_SKIP,
    _QR_PREFLIGHT_MODE_WARN,
    _QR_ZERO_WARN_THRESHOLD_ENV,
    _SFI_PROPS_PIVOT_COLUMNS,
    _build_changed_resources_sql,
    _build_inline_batch_apply_sql,
    _build_number_changed_resources_sql,
    _build_qr_sfi_preflight_sql,
    _build_qr_zero_datapoint_count_sql,
    _build_reconciliation_batch_sql,
    _build_reconciliation_qr_gap_sql,
    _build_reconciliation_setup_sql,
    _build_sfi_props_pivot_sql,
    _execute_incremental_refresh,
    _incremental_batch_size,
    _is_store_missing_error,
    _next_checkpoint,
    _qr_preflight_mode,
    _qr_zero_warn_threshold,
    _refresh_columns,
    _refresh_sfi_datapoint_store,
    _validate_qr_preflight_stats,
    dso_refresh_sfi_datapoint_daily,
)


@pytest.mark.unit
def test_refresh_sfi_datapoint_daily_waits_for_fhir_and_codesystem_sync() -> None:
    refresh_key = AssetKey.from_user_string("dso_refresh_sfi_datapoint_daily")
    deps = dso_refresh_sfi_datapoint_daily.asset_deps[refresh_key]

    # The SFI DSO syncs are unpartitioned _dfm assets.
    assert AssetKey.from_user_string("dso_sync_fhir_observation_dfm") in deps
    assert AssetKey.from_user_string("dso_sync_fhir_questionnaire_response_dfm") in deps
    assert AssetKey.from_user_string("dso_sync_fhir_questionnaire_response_item_dfm") in deps
    assert AssetKey.from_user_string("dso_sync_fhir_questionnaire_item_dfm") in deps
    assert AssetKey.from_user_string("dso_sync_fhir_encounter_dfm") in deps
    assert AssetKey.from_user_string("dso_infra_views_fhir") in deps
    assert AssetKey.from_user_string("dso_sync_codesystem_sfi_concept") in deps
    assert AssetKey.from_user_string("dso_sync_codesystem_sfi_properties") in deps


@pytest.mark.unit
def test_store_missing_error_detector() -> None:
    assert _is_store_missing_error(
        RuntimeError(
            'Cannot find the object "SFI_datapoint_store" because it does not exist '
            "or you do not have permissions."
        )
    )
    assert not _is_store_missing_error(
        RuntimeError("Permission denied on fhir.SFI_datapoint_build")
    )


@pytest.mark.unit
def test_changed_resources_sql_with_watermark() -> None:
    """Incremental mode: watermark filter should appear in all three CTEs.

    The timestamp format passed here matches the canonical DSO watermark format
    ("%Y-%m-%d %H:%M:%S") used by sync_utils.
    """
    sql = _build_changed_resources_sql("2026-06-01 00:00:00")
    assert "2026-06-01 00:00:00" in sql
    assert "element_system" in sql
    assert "urn:dk:ifdb:sfi" in sql
    assert "#changed_resources" in sql
    assert "changed_observation" in sql
    assert "changed_qr" in sql
    assert "QuestionnaireResponse_item" in sql
    # Sargability contract: the watermark predicate must hit the persisted
    # computed column (08_delta_discovery_effective_timestamp.sql), never a
    # COALESCE over the raw DW_Timestamp_* columns (forces full scans).
    assert "[dw_timestamp_effective] >= '2026-06-01 00:00:00'" in sql
    assert "COALESCE" not in sql


@pytest.mark.unit
def test_changed_resources_sql_indexes_temp_table() -> None:
    sql = _build_changed_resources_sql("2026-06-01 00:00:00")

    assert "CREATE CLUSTERED INDEX [IX_changed_resources_SourceResourceId]" in sql
    assert "ON #changed_resources ([source_resource], [source_resource_id])" in sql


@pytest.mark.unit
def test_inline_batch_apply_no_merge() -> None:
    """Incremental batch apply must NOT use MERGE or NOT MATCHED BY SOURCE."""
    sql = _build_inline_batch_apply_sql(1, 100)
    assert "MERGE" not in sql
    assert "NOT MATCHED BY SOURCE" not in sql


@pytest.mark.unit
def test_reconciliation_setup_sql_builds_keyset() -> None:
    """Setup SQL should create #reconcile_resources with resource keyset."""
    sql = _build_reconciliation_setup_sql()
    assert "MERGE" not in sql
    assert "#reconcile_resources" in sql
    assert "current_observation" in sql
    assert "current_qr" in sql
    assert "existing_store" in sql
    assert "COUNT_BIG(*) AS total_resources" in sql


@pytest.mark.unit
def test_reconciliation_batch_sql_scoped_delete_insert() -> None:
    """Batch SQL should do scoped DELETE + INSERT per batch, not MERGE."""
    sql = _build_reconciliation_batch_sql(1, 1234)
    assert "MERGE" not in sql
    assert "NOT MATCHED BY SOURCE" not in sql
    assert "#batch_resources" in sql
    assert "#reconcile_resources" in sql
    assert "BETWEEN 1 AND 1234" in sql
    # Durability is owned by the per-batch conn.commit() in the Python loop
    # (autocommit=False); the batch SQL must NOT open its own nested transaction.
    assert "BEGIN TRANSACTION" not in sql
    assert "COMMIT TRANSACTION" not in sql
    assert "DELETE tgt" in sql
    assert "INSERT INTO" in sql


@pytest.mark.unit
def test_refresh_columns_complete() -> None:
    """Verify all expected columns are present in the column list."""
    columns = _refresh_columns()
    assert "datapoint_key" in columns
    assert "source_resource_id" in columns
    assert "source_resource" in columns
    assert "occurred_at_dt" in columns
    assert "refreshed_at" in columns
    assert "patient_id" in columns
    assert "element_code" in columns
    assert "value_type" in columns
    assert "source_last_updated" in columns
    # Must equal the fhir.SFI_datapoint_store DDL column count
    # (infrastructure/ddl/tables/fhir/02_sfi_datapoint_store.sql). Bump together.
    assert len(columns) == 60
    assert len(columns) == len(set(columns)), "duplicate column in _REFRESH_INSERT_COLUMNS"


@pytest.mark.unit
def test_qr_sfi_preflight_sql_checks_join_contract() -> None:
    sql = _build_qr_sfi_preflight_sql()

    assert "#changed_resources" in sql
    assert "QuestionnaireResponse" in sql
    assert "QuestionnaireResponse_item" in sql
    assert "Questionnaire_item" in sql
    assert "fhir].[Questionnaire]" in sql
    assert "urn:dk:ifdb:sfi" in sql
    assert "qr_missing_canonical" in sql
    assert "qr_missing_version_when_reference_is_versioned" in sql
    assert "qr_questionnaire_reference_without_questionnaire" in sql
    assert "qri_sfi_joinable_distinct_resources" in sql
    assert "qi.[item_key] = qri.[item_key]" in sql
    assert "SFI_datapoint_build" not in sql


@pytest.mark.unit
def test_reconciliation_qr_gap_sql_reports_template_completeness() -> None:
    """The full-load gap check is an ADVISORY template-completeness metric: a
    set-based LEFT JOIN over all QRI rows (no #changed_resources scope). The
    build itself no longer joins Questionnaire_item, so this gap is logged for
    visibility, not used to drop data."""
    sql = _build_reconciliation_qr_gap_sql()

    assert "#changed_resources" not in sql
    assert "[fhir].[QuestionnaireResponse_item]" in sql
    assert "LEFT JOIN [fhir].[Questionnaire_item]" in sql
    assert "qi.[link_id] = qri.[link_id]" in sql
    assert "qi.[item_key] = qri.[item_key]" in sql
    assert "urn:dk:ifdb:sfi" in sql
    assert "qri_total" in sql
    assert "qri_sfi_joinable" in sql
    assert "qri_without_link_id" in sql


@pytest.mark.unit
def test_qr_sfi_preflight_accepts_valid_unversioned_questionnaires() -> None:
    stats = {
        "qr_missing_canonical": 0,
        "qr_missing_version_when_reference_is_versioned": 0,
        "qr_questionnaire_reference_without_questionnaire": 0,
    }

    assert _validate_qr_preflight_stats(stats) == []


@pytest.mark.unit
def test_qr_sfi_preflight_fails_on_missing_questionnaire_reference() -> None:
    stats = {
        "qr_missing_canonical": 0,
        "qr_missing_version_when_reference_is_versioned": 0,
        "qr_questionnaire_reference_without_questionnaire": 2,
    }

    errors = _validate_qr_preflight_stats(stats)

    assert errors
    assert "do not exist in fhir.Questionnaire" in "; ".join(errors)


@pytest.mark.unit
def test_qr_sfi_preflight_mode_defaults_to_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    """Safe-by-default: no env override -> uncodeable items / unresolvable
    questionnaire references abort before the DELETE."""
    monkeypatch.delenv(_QR_PREFLIGHT_MODE_ENV, raising=False)

    assert _qr_preflight_mode() == _QR_PREFLIGHT_MODE_FAIL


@pytest.mark.unit
def test_qr_sfi_preflight_mode_can_be_set_to_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_QR_PREFLIGHT_MODE_ENV, _QR_PREFLIGHT_MODE_SKIP)

    assert _qr_preflight_mode() == _QR_PREFLIGHT_MODE_SKIP


@pytest.mark.unit
def test_qr_sfi_preflight_join_gap_alone_is_not_fatal() -> None:
    """A QR -> Questionnaire_item join gap is NO LONGER fatal: the build derives
    element_code from the QR item's own link_id and no longer gates on the
    template, so an incomplete Questionnaire drops nothing. The gap is an
    advisory completeness metric, not an error."""
    stats = {
        "qr_missing_canonical": 0,
        "qr_missing_version_when_reference_is_versioned": 0,
        "qr_questionnaire_reference_without_questionnaire": 0,
        "qri_total": 10,
        "qri_sfi_joinable": 7,
        "qri_without_link_id": 0,
    }

    assert _validate_qr_preflight_stats(stats) == []


@pytest.mark.unit
def test_qr_sfi_preflight_fails_on_uncodeable_items() -> None:
    """A QR item with an empty/NULL link_id has no SFI code, so the build
    excludes it (WHERE link_id <> '') — that would silently drop a clinical
    answer, so it must fail loudly."""
    stats = {
        "qr_missing_canonical": 0,
        "qr_missing_version_when_reference_is_versioned": 0,
        "qr_questionnaire_reference_without_questionnaire": 0,
        "qri_total": 10,
        "qri_sfi_joinable": 7,
        "qri_without_link_id": 2,
    }

    errors = _validate_qr_preflight_stats(stats)

    assert errors
    joined = "; ".join(errors)
    assert "2 changed QR items have no SFI link_id" in joined
    assert "silently drop" in joined


@pytest.mark.unit
def test_qr_sfi_preflight_accepts_fully_codeable_items() -> None:
    """No fatal error when every changed QR item has an SFI link_id, regardless
    of template completeness."""
    stats = {
        "qr_missing_canonical": 0,
        "qr_missing_version_when_reference_is_versioned": 0,
        "qr_questionnaire_reference_without_questionnaire": 0,
        "qri_total": 10,
        "qri_sfi_joinable": 10,
        "qri_without_link_id": 0,
    }

    assert _validate_qr_preflight_stats(stats) == []


@pytest.mark.unit
def test_qr_sfi_preflight_accepts_reconciliation_stats_shape() -> None:
    """Initial reconciliation only has QRI gap stats, not incremental QR stats."""
    stats = {
        "qri_total": 10,
        "qri_sfi_joinable": 10,
        "qri_without_link_id": 0,
    }

    assert _validate_qr_preflight_stats(stats) == []


@pytest.mark.unit
def test_qr_sfi_preflight_mode_can_be_set_to_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_QR_PREFLIGHT_MODE_ENV, _QR_PREFLIGHT_MODE_WARN)

    assert _qr_preflight_mode() == _QR_PREFLIGHT_MODE_WARN


@pytest.mark.unit
def test_qr_sfi_preflight_mode_can_be_set_to_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_QR_PREFLIGHT_MODE_ENV, _QR_PREFLIGHT_MODE_FAIL)

    assert _qr_preflight_mode() == _QR_PREFLIGHT_MODE_FAIL


@pytest.mark.unit
def test_changed_resources_sql_qr_item_triggers_qr_rebuild() -> None:
    """A changed QuestionnaireResponse_item should trigger rebuild of its parent qr_id."""
    sql = _build_changed_resources_sql("2026-06-01 00:00:00")
    assert "qri.[qr_id] AS source_resource_id" in sql


@pytest.mark.unit
def test_sfi_props_pivot_sql_pivots_all_property_columns() -> None:
    """Pivot SQL should materialize all SFI property columns into a temp table."""
    sql = _build_sfi_props_pivot_sql()
    assert "#sfi_props_pivot" in sql
    assert "codesystem].[SFIProperties]" in sql
    assert "GROUP BY sp.[code]" in sql
    # Fan-out guard: the inline legs LEFT JOIN this temp table on [code], so
    # uniqueness must be enforced on exactly that key — otherwise a duplicate
    # Code would multiply datapoint rows (PR review thread #56306).
    assert "CREATE UNIQUE CLUSTERED INDEX [IX_sfi_props_pivot_Code]" in sql
    assert "ON #sfi_props_pivot ([code])" in sql

    for col in _SFI_PROPS_PIVOT_COLUMNS:
        assert col in sql, f"Missing pivot column: {col}"


@pytest.mark.unit
def test_number_changed_resources_sql_structure() -> None:
    """Numbering SQL should create #changed_numbered with row_id for batched iteration."""
    sql = _build_number_changed_resources_sql()
    assert "#changed_numbered" in sql
    assert "#changed_resources" in sql
    assert "ROW_NUMBER()" in sql
    assert "IX_changed_numbered_RowId" in sql
    assert "IX_changed_numbered_SourceResourceId" in sql
    assert "total_resources" in sql


@pytest.mark.unit
def test_inline_batch_apply_sql_scoped_delete_insert() -> None:
    """Batch apply SQL should do scoped DELETE + INSERT per batch using inline build."""
    sql = _build_inline_batch_apply_sql(1, 50000)
    assert "#batch_resources" in sql
    assert "#changed_numbered" in sql
    assert "BETWEEN 1 AND 50000" in sql
    assert "#sfi_props_pivot" in sql
    assert "SFI_datapoint_build" not in sql
    # Durability is owned by the per-batch conn.commit() in the Python loop
    # (autocommit=False); the batch SQL must NOT open its own nested transaction.
    assert "BEGIN TRANSACTION" not in sql
    assert "COMMIT TRANSACTION" not in sql
    assert "DELETE tgt" in sql
    assert "INSERT INTO" in sql
    assert "UNION ALL" in sql
    assert "Observation" in sql
    assert "QuestionnaireResponse" in sql
    # The QR leg must NOT gate datapoints on fhir.Questionnaire_item: that would
    # silently drop answers whose (incomplete) template row is missing. element_code
    # is derived from the QR item's own link_id and enrichment is a LEFT JOIN.
    assert "INNER JOIN [fhir].[Questionnaire_item]" not in sql
    assert "qi.[item_key] = qri.[item_key]" not in sql
    assert "qri.[link_id] AS element_code" in sql
    assert "ON sc.[code] = qri.[link_id]" in sql
    assert "WHERE qri.[link_id] <> N''" in sql


@pytest.mark.unit
def test_inline_batch_apply_returns_statistics() -> None:
    """Batch apply SQL should return deleted/inserted row counts."""
    sql = _build_inline_batch_apply_sql(100, 200)
    assert "@deleted_rows AS deleted_rows" in sql
    assert "@inserted_rows AS inserted_rows" in sql


@pytest.mark.unit
def test_qr_zero_datapoint_count_sql_contract() -> None:
    """Post-apply under-emission signal: changed QRs with zero store rows."""
    sql = _build_qr_zero_datapoint_count_sql()
    assert "FROM #changed_resources" in sql
    assert "N'QuestionnaireResponse'" in sql
    assert "NOT EXISTS" in sql
    assert "[fhir].[SFI_datapoint_store]" in sql
    assert "qr_zero_datapoints" in sql


@pytest.mark.unit
def test_qr_zero_warn_threshold_default_and_custom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeContext:
        class log:
            @staticmethod
            def warning(*args: object) -> None:
                pass

    monkeypatch.delenv(_QR_ZERO_WARN_THRESHOLD_ENV, raising=False)
    assert _qr_zero_warn_threshold(FakeContext()) == _DEFAULT_QR_ZERO_WARN_THRESHOLD

    monkeypatch.setenv(_QR_ZERO_WARN_THRESHOLD_ENV, "250")
    assert _qr_zero_warn_threshold(FakeContext()) == 250

    monkeypatch.setenv(_QR_ZERO_WARN_THRESHOLD_ENV, "ikke-et-tal")
    assert _qr_zero_warn_threshold(FakeContext()) == _DEFAULT_QR_ZERO_WARN_THRESHOLD


@pytest.mark.unit
def test_incremental_batch_size_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_INCREMENTAL_BATCH_SIZE_ENV, raising=False)

    class FakeContext:
        class log:
            @staticmethod
            def warning(*args: object) -> None:
                pass

    assert _incremental_batch_size(FakeContext()) == _DEFAULT_INCREMENTAL_BATCH_SIZE


@pytest.mark.unit
def test_incremental_batch_size_custom(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_INCREMENTAL_BATCH_SIZE_ENV, "75000")

    class FakeContext:
        class log:
            @staticmethod
            def warning(*args: object) -> None:
                pass

    assert _incremental_batch_size(FakeContext()) == 75000


@pytest.mark.unit
def test_daily_sfi_refresh_shares_a_job_with_its_upstreams() -> None:
    """Refresh must wait for today's Observation/QR DSO sync.

    A dep on an asset that is not in the same job is treated as external — the
    ordering edge disappears — so the refresh and everything it waits for have
    to be selected by the same job.
    """
    from definitions import defs

    pipeline_nodes = {node.name for node in defs.resolve_job_def("ifdb_dfm_daily_job").nodes}
    recon_nodes = {
        node.name for node in defs.resolve_job_def("sfi_datapoint_reconciliation_job").nodes
    }

    assert "dso_refresh_sfi_datapoint_daily" in pipeline_nodes
    assert "dso_sync_fhir_observation_dfm" in pipeline_nodes
    assert "dso_sync_codesystem_sfi_concept" in pipeline_nodes
    assert "cs_sfi" in pipeline_nodes
    assert "dso_reconcile_sfi_datapoint_store" in recon_nodes


@pytest.mark.unit
def test_upstream_deps_exist_in_asset_graph() -> None:
    """A dep on a non-existent asset key becomes an external asset in Dagster
    and the ordering edge silently disappears — pin that every declared
    upstream key resolves to a registered asset."""
    from definitions import defs
    from DSO.load.refresh_sfi_datapoint import _SFI_UPSTREAM_DEPS
    from DSO.load.sync_codesystem import (
        dso_sync_codesystem_sfi_concept,
        dso_sync_codesystem_sfi_hierarchy,
        dso_sync_codesystem_sfi_properties,
    )

    graph = defs.resolve_asset_graph()
    declared = set(_SFI_UPSTREAM_DEPS)
    for cs_asset in (
        dso_sync_codesystem_sfi_concept,
        dso_sync_codesystem_sfi_properties,
        dso_sync_codesystem_sfi_hierarchy,
    ):
        for key in cs_asset.asset_deps[next(iter(cs_asset.keys))]:
            declared.add(key.to_user_string())

    missing = {d for d in declared if not graph.has(AssetKey.from_user_string(d))}
    assert not missing, f"Dangling upstream deps (not in asset graph): {sorted(missing)}"


@pytest.mark.unit
def test_inline_batch_apply_routed_variant_splits_by_empty_partition() -> None:
    """SWITCH-tilstand: byggeforespørgslen materialiseres én gang og deles mellem
    arbejdstabel (tomme partitioner) og store (fyldte); fire tællere returneres."""
    plain = _build_inline_batch_apply_sql(1, 100)
    routed = _build_inline_batch_apply_sql(1, 100, routed=True)
    assert "SFI_datapoint_store_backfill" not in plain
    assert "$PARTITION" not in plain
    assert "@deleted_rows AS deleted_rows, @inserted_rows AS inserted_rows" in plain
    assert "INTO #batch_rows" in routed
    assert "INSERT INTO [fhir].[SFI_datapoint_store_backfill]" in routed
    assert "INSERT INTO [fhir].[SFI_datapoint_store]" in routed
    assert "$PARTITION.pf_SFI_datapoint_store_Maaned(r.[occurred_at_dt])" in routed
    assert "#empty_partitions" in routed
    assert "@work_rows AS work_rows" in routed and "@store_rows AS store_rows" in routed
    # DELETE-trinnet er uændret i begge varianter — det er stadig ét seek pr. ressource.
    for sql in (plain, routed):
        assert "DELETE tgt" in sql
        assert "MERGE" not in sql


@pytest.mark.unit
def test_reconciliation_batch_routed_variant_reads_build_view_once() -> None:
    plain = _build_reconciliation_batch_sql(1, 10)
    routed = _build_reconciliation_batch_sql(1, 10, routed=True)
    assert plain.count("[fhir].[SFI_datapoint_build]") == 1
    assert routed.count("[fhir].[SFI_datapoint_build]") == 1
    assert "INTO #batch_rows" in routed and "INTO #batch_rows" not in plain
    assert "SFI_datapoint_store_backfill" in routed and "SFI_datapoint_store_backfill" not in plain


@pytest.mark.unit
def test_changed_resources_sql_caps_at_the_upper_bound() -> None:
    sql = _build_changed_resources_sql("2026-10-16 08:17:03", "2026-10-16 08:41:48")
    for alias in ("o", "qr", "qri"):
        assert f"{alias}.[dw_timestamp_effective] >= '2026-10-16 08:17:03'" in sql
        assert f"{alias}.[dw_timestamp_effective] <= '2026-10-16 08:41:48'" in sql


class _Dfm:
    """In-flight DSO-sync starts per source table; records checkpoint and run outcome."""

    def __init__(self, in_flight=None, checkpoint=None, consumers_absent_since=None):
        self.in_flight = in_flight or {}
        self.checkpoint = checkpoint
        self.consumers_absent_since = consumers_absent_since
        self.calls: list[tuple] = []

    def get_checkpoint(self, schema, table, *, scope="staging"):
        return self.consumers_absent_since if scope == "sfi_consumers" else None

    def safe_reader_checkpoint(self, started_at, *, writer=None, writer_scope=None):
        self.calls.append(("safe_reader_checkpoint", writer, writer_scope))
        held = self.in_flight.get(writer) if writer_scope == "dso" else None
        return min(started_at, held) if held else started_at

    def start_dfm_run(self, schema, table, **kwargs):
        self.calls.append(("start", schema, table, kwargs["scope"], kwargs["mode"]))
        return SimpleNamespace(log_id=7, checkpoint=self.checkpoint, started_at=None)

    def set_checkpoint(self, schema, table, **kwargs):
        self.calls.append(("set", schema, table, kwargs["scope"], kwargs["value"]))

    def complete_dfm_run(self, log_id, **kwargs):
        self.calls.append(("complete", log_id, kwargs["checkpoint_after"]))

    def fail_dfm_run(self, log_id, **kwargs):
        self.calls.append(("fail", log_id))


@pytest.mark.unit
def test_next_checkpoint_is_held_back_to_an_unfinished_source_sync() -> None:
    high_water = datetime(2026, 10, 22, 6, 0, 0)
    assert _next_checkpoint(_Dfm(), high_water) == high_water
    assert _next_checkpoint(_Dfm(), None) is None

    # The QR-item sync started before the high-water mark and has not committed: its rows
    # carry stamps below the mark that the scan could not see yet.
    dfm = _Dfm({("fhir", "QuestionnaireResponse_item"): "2026-10-22 05:41:12.0000000"})
    assert _next_checkpoint(dfm, high_water) == datetime(2026, 10, 22, 5, 41, 12)
    assert {(c[1], c[2]) for c in dfm.calls} == {
        (("fhir", "Observation"), "dso"),
        (("fhir", "QuestionnaireResponse"), "dso"),
        (("fhir", "QuestionnaireResponse_item"), "dso"),
    }


class _RefreshCursor:
    """Answers the incremental refresh's queries for a run that finds no changes."""

    def __init__(self, high_water):
        self.high_water = high_water
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchone(self):
        last = self.sql[-1]
        if "MAX(o.[dw_timestamp_effective])" in last:
            return self.high_water
        if "#changed_resources" in last:
            return (0,)
        return (17_982_923,)

    def close(self):
        pass


def _dso_with(cursor):
    from contextlib import nullcontext

    conn = SimpleNamespace(cursor=lambda: cursor, commit=lambda: None, rollback=lambda: None)
    return SimpleNamespace(get_connection=lambda: nullcontext(conn))


_LOG_CONTEXT = SimpleNamespace(
    run_id="run-1", log=SimpleNamespace(info=lambda *a: None, warning=lambda *a: None)
)


@pytest.mark.unit
def test_incremental_refresh_reads_from_the_checkpoint_up_to_the_source_high_water() -> None:
    cursor = _RefreshCursor(
        (
            datetime(2026, 10, 16, 8, 30),
            datetime(2026, 10, 16, 8, 41, 48, 700000),
            None,
            datetime(2026, 10, 16, 9, 0),
        )
    )
    next_checkpoint = _execute_incremental_refresh(
        _LOG_CONTEXT, _dso_with(cursor), _Dfm(), datetime(2026, 10, 16, 8, 17, 3)
    ).next_checkpoint

    assert next_checkpoint == datetime(2026, 10, 16, 8, 41, 48)
    changed_sql = next(s for s in cursor.sql if "INTO #changed_resources" in s)
    assert ">= '2026-10-16 08:17:03'" in changed_sql
    assert "<= '2026-10-16 08:41:48'" in changed_sql


@pytest.mark.unit
def test_store_refresh_keeps_its_checkpoint_in_dfm_and_leaves_it_on_failure(monkeypatch) -> None:
    import DSO.load.refresh_sfi_datapoint as refresh
    from core.dfm_config import RunModeConfig

    monkeypatch.setattr(refresh, "_ensure_store_table_exists", lambda *a: None)
    cursor = _RefreshCursor(
        (datetime(2026, 10, 22, 6, 0, 0), None, None, datetime(2026, 10, 22, 7))
    )
    dfm = _Dfm(checkpoint="2026-10-21 06:00:00")
    monkeypatch.setattr(refresh, "DfmManager", lambda *a: dfm)

    _refresh_sfi_datapoint_store(_LOG_CONTEXT, _dso_with(cursor), None, RunModeConfig())
    assert ("start", "fhir", "SFI_datapoint_store", "sfi_store", "delta") in dfm.calls
    assert ("set", "fhir", "SFI_datapoint_store", "sfi_store", "2026-10-22 06:00:00") in dfm.calls
    assert ("complete", 7, "2026-10-22 06:00:00") in dfm.calls

    def _broken(*_a):
        raise RuntimeError("build failed")

    monkeypatch.setattr(refresh, "_execute_incremental_refresh", _broken)
    dfm.calls.clear()
    with pytest.raises(RuntimeError, match="build failed"):
        _refresh_sfi_datapoint_store(_LOG_CONTEXT, _dso_with(cursor), None, RunModeConfig())
    assert ("fail", 7) in dfm.calls
    assert not [c for c in dfm.calls if c[0] in ("set", "complete")]


@pytest.mark.unit
def test_high_water_never_passes_dso_current_utc_time() -> None:
    """A stamp written in Danish local time before the switch lies up to two hours ahead."""
    from DSO.load.refresh_sfi_datapoint import _source_high_water

    now = datetime(2026, 10, 23, 8, 0, 0)
    local_stamp = datetime(2026, 10, 23, 9, 30, 0)
    assert _source_high_water(_RefreshCursor((local_stamp, None, None, now))) == now
    earlier = datetime(2026, 10, 23, 7, 15, 0)
    assert _source_high_water(_RefreshCursor((earlier, None, None, now))) == earlier
    assert _source_high_water(_RefreshCursor((None, None, None, now))) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("switched_on_since", "tag", "paused"),
    [
        ("2026-10-27 09:00:00", None, True),
        (None, "true", True),
        ("2026-10-27 09:00:00", "false", False),
        (None, None, False),
    ],
)
def test_a_paused_daily_refresh_touches_neither_store_nor_checkpoint(
    monkeypatch, switched_on_since, tag, paused
) -> None:
    import DSO.load.refresh_sfi_datapoint as refresh
    from core.dfm_config import RunModeConfig
    from utils.fhir import sfi_consumers

    tags = {sfi_consumers.CONSUMERS_ABSENT_TAG: tag} if tag else {}
    context = SimpleNamespace(run_id="run-1", run=SimpleNamespace(tags=tags), log=_LOG_CONTEXT.log)
    dfm = _Dfm(checkpoint="2026-10-21 06:00:00", consumers_absent_since=switched_on_since)
    monkeypatch.setattr(refresh, "DfmManager", lambda *a: dfm)
    monkeypatch.setattr(refresh, "_ensure_store_table_exists", lambda *a: None)
    ran = SimpleNamespace(metadata={})
    monkeypatch.setattr(
        refresh,
        "_execute_incremental_refresh",
        lambda *a: refresh._RefreshOutcome(ran, datetime(2026, 10, 22, 6, 0, 0), 0),
    )

    result = _refresh_sfi_datapoint_store(context, None, None, RunModeConfig())

    if paused:
        assert result.metadata["paused"].value is True
        assert dfm.calls == []
    else:
        assert result is ran
        assert ("set", "fhir", "SFI_datapoint_store", "sfi_store", "2026-10-22 06:00:00") in (
            dfm.calls
        )


@pytest.mark.unit
def test_the_rebuild_is_never_paused(monkeypatch) -> None:
    import DSO.load.refresh_sfi_datapoint as refresh
    from core.dfm_config import RunModeConfig

    monkeypatch.setattr(refresh, "_ensure_store_table_exists", lambda *a: None)
    monkeypatch.setattr(
        refresh,
        "DfmManager",
        lambda *a: _Dfm(checkpoint=None, consumers_absent_since="2026-10-27 09:00:00"),
    )
    rebuilt = SimpleNamespace(metadata={})
    monkeypatch.setattr(
        refresh,
        "_execute_full_reconciliation",
        lambda *a: refresh._RefreshOutcome(rebuilt, datetime(2026, 10, 22, 6, 0, 0), 0),
    )

    result = _refresh_sfi_datapoint_store(_LOG_CONTEXT, None, None, RunModeConfig(mode="full_load"))
    assert result is rebuilt


@pytest.mark.unit
@pytest.mark.parametrize(
    ("mode", "checkpoint"), [("full_load", "2026-10-21 06:00:00"), ("delta", None)]
)
def test_store_is_rebuilt_in_full_load_or_without_a_checkpoint(
    monkeypatch, mode, checkpoint
) -> None:
    import DSO.load.refresh_sfi_datapoint as refresh
    from core.dfm_config import RunModeConfig

    monkeypatch.setattr(refresh, "_ensure_store_table_exists", lambda *a: None)
    dfm = _Dfm(checkpoint=checkpoint)
    monkeypatch.setattr(refresh, "DfmManager", lambda *a: dfm)
    rebuilt = SimpleNamespace(metadata={})
    monkeypatch.setattr(
        refresh,
        "_execute_full_reconciliation",
        lambda *a: refresh._RefreshOutcome(rebuilt, datetime(2026, 10, 22, 6, 0, 0), 0),
    )

    def _incremental(*_a):
        raise AssertionError("incremental refresh must not run")

    monkeypatch.setattr(refresh, "_execute_incremental_refresh", _incremental)

    result = _refresh_sfi_datapoint_store(_LOG_CONTEXT, None, None, RunModeConfig(mode=mode))
    assert result is rebuilt
    assert ("set", "fhir", "SFI_datapoint_store", "sfi_store", "2026-10-22 06:00:00") in dfm.calls
