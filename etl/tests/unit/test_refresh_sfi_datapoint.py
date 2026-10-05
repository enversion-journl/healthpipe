# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SFI_datapoint store incremental refresh assets."""

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
    _WATERMARK_SCHEMA,
    _WATERMARK_TABLE_DAILY,
    _WATERMARK_TABLE_MONTHLY,
    _WATERMARK_TABLE_RECONCILIATION,
    _build_changed_resources_sql,
    _build_inline_batch_apply_sql,
    _build_number_changed_resources_sql,
    _build_qr_sfi_preflight_sql,
    _build_qr_zero_datapoint_count_sql,
    _build_reconciliation_batch_sql,
    _build_reconciliation_qr_gap_sql,
    _build_reconciliation_setup_sql,
    _build_sfi_props_pivot_sql,
    _incremental_batch_size,
    _is_store_missing_error,
    _qr_preflight_mode,
    _qr_zero_warn_threshold,
    _refresh_columns,
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
def test_watermark_constants() -> None:
    assert _WATERMARK_SCHEMA == "sfi_datapoint"
    assert _WATERMARK_TABLE_DAILY == "refresh_daily"
    assert _WATERMARK_TABLE_MONTHLY == "refresh_monthly"
    assert _WATERMARK_TABLE_RECONCILIATION == "refresh_reconciliation"


@pytest.mark.unit
def test_changed_resources_sql_with_watermark() -> None:
    """Incremental mode: watermark filter should appear in all three CTEs.

    The timestamp format passed here matches the canonical DSO watermark format
    ("%Y-%m-%d %H:%M:%S") used by SyncStateManager and sync_utils.
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
    sql = _build_changed_resources_sql(None)

    assert "CREATE CLUSTERED INDEX [IX_changed_resources_SourceResourceId]" in sql
    assert "ON #changed_resources ([source_resource], [source_resource_id])" in sql


@pytest.mark.unit
def test_changed_resources_sql_first_run() -> None:
    """First run (NULL watermark): no timestamp filter, but SFI filter still present."""
    sql = _build_changed_resources_sql(None)
    assert ">=" not in sql
    assert "element_system" in sql
    assert "urn:dk:ifdb:sfi" in sql
    assert "#changed_resources" in sql


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
def test_daily_sfi_refresh_runs_in_sof_dso_job_not_partitioned_daily() -> None:
    """Refresh must wait for today's Observation/QR DSO sync.

    The success sensor launches fhir_etl_ifdb_daily_job and
    fhir_ifdb_dfm_sof_dso_job in parallel. A dep on an asset that is not in
    the same job is treated as external — the ordering edge disappears.
    """
    from definitions import defs

    sof_dso_nodes = {node.name for node in defs.resolve_job_def("fhir_ifdb_dfm_sof_dso_job").nodes}
    daily_nodes = {node.name for node in defs.resolve_job_def("fhir_etl_ifdb_daily_job").nodes}
    monthly_nodes = {node.name for node in defs.resolve_job_def("fhir_etl_ifdb_monthly_job").nodes}
    recon_nodes = {
        node.name for node in defs.resolve_job_def("sfi_datapoint_reconciliation_job").nodes
    }

    assert "dso_refresh_sfi_datapoint_daily" in sof_dso_nodes
    assert "dso_sync_fhir_observation_dfm" in sof_dso_nodes
    assert "dso_sync_codesystem_sfi_concept" in sof_dso_nodes
    assert "cs_sfi" in sof_dso_nodes

    assert "dso_refresh_sfi_datapoint_daily" not in daily_nodes
    assert "dso_sync_codesystem_sfi_concept" not in daily_nodes
    assert "dso_refresh_sfi_datapoint_monthly" not in monthly_nodes
    assert "dso_refresh_sfi_datapoint_monthly" not in sof_dso_nodes
    assert "dso_refresh_sfi_datapoint_monthly" not in recon_nodes
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
