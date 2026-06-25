# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for FHIRDatabaseResource batch upsert fallback behavior.

Focus:
- Batch-level retry with backoff before falling back to per-row inserts.
- conn.rollback() resets the aborted PostgreSQL transaction before retries and fallback.
- Fallback to per-resource inserts only for configured database errors.
- Surface non-database errors immediately (no silent fallback, no retry).
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest
from core.fhir.direct_upsert import _BATCH_MAX_RETRIES
from core.resources import FHIRDatabaseResource
from fhir.resources.R4B.patient import Patient


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def _build_connection_and_cursor() -> tuple[MagicMock, MagicMock]:
    connection = MagicMock()
    cursor = MagicMock()

    cursor_cm = MagicMock()
    cursor_cm.__enter__.return_value = cursor
    cursor_cm.__exit__.return_value = False
    connection.cursor.return_value = cursor_cm

    cursor.fetchone.return_value = [100]
    return connection, cursor


@contextmanager
def _connection_ctx(connection: MagicMock):
    yield connection


def _sample_patients() -> list[Patient]:
    return [
        Patient(id="pat-1", identifier=[]),
        Patient(id="pat-2", identifier=[]),
    ]


def _sample_dict_resources() -> list[tuple[str, dict]]:
    return [
        ("obs-1", {"resourceType": "Observation", "id": "obs-1", "status": "final"}),
        ("obs-2", {"resourceType": "Observation", "id": "obs-2", "status": "final"}),
    ]


class TestBatchUpsertResourcesFallback:
    def test_persistent_error_retries_then_falls_back_to_individual(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep") as mock_sleep,
        ):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=_sample_patients(),
                resource_type="Patient",
                batch_size=500,
            )

        assert successes == 2
        assert failures == 0
        assert failed_ids == []
        assert cursor.executemany.call_count == 1 + _BATCH_MAX_RETRIES
        assert connection.rollback.call_count == 1 + _BATCH_MAX_RETRIES
        assert mock_sleep.call_count == _BATCH_MAX_RETRIES
        # 1 x SELECT max(txid) + 2 x fallback inserts
        assert cursor.execute.call_count == 3
        connection.commit.assert_called_once()

    def test_transient_error_succeeds_on_retry_without_fallback(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = [
            RuntimeError("transient db failure"),
            None,
        ]

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep") as mock_sleep,
        ):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=_sample_patients(),
                resource_type="Patient",
                batch_size=500,
            )

        assert successes == 2
        assert failures == 0
        assert failed_ids == []
        assert cursor.executemany.call_count == 2
        connection.rollback.assert_called_once()
        mock_sleep.assert_called_once()
        # Only SELECT max(txid), no per-row fallback inserts
        assert cursor.execute.call_count == 1
        connection.commit.assert_called_once()

    def test_rollback_called_before_each_retry_and_before_fallback(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("persistent failure")

        call_order: list[str] = []
        connection.rollback.side_effect = lambda: call_order.append("rollback")
        cursor.execute.side_effect = lambda *a, **kw: call_order.append("execute")
        cursor.fetchone.return_value = [100]

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep"),
        ):
            resource.batch_upsert_resources(
                resources=_sample_patients(),
                resource_type="Patient",
                batch_size=500,
            )

        # First call is SELECT max(txid), then rollback/retry/rollback/retry/rollback/fallback
        assert call_order[0] == "execute"  # SELECT max(txid)
        rollback_positions = [i for i, c in enumerate(call_order) if c == "rollback"]
        execute_positions = [i for i, c in enumerate(call_order) if c == "execute"]
        fallback_execute_positions = execute_positions[1:]  # skip SELECT max(txid)
        # All rollbacks happen before any fallback inserts
        assert all(r < min(fallback_execute_positions) for r in rollback_positions)

    def test_raises_non_database_error_without_fallback(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = ValueError("unexpected serialization error")

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            pytest.raises(ValueError, match="unexpected serialization error"),
        ):
            resource.batch_upsert_resources(
                resources=_sample_patients(),
                resource_type="Patient",
                batch_size=500,
            )

        assert cursor.executemany.call_count == 1
        # Only SELECT max(txid) should have been executed before the exception bubbles.
        assert cursor.execute.call_count == 1
        connection.rollback.assert_not_called()
        connection.commit.assert_not_called()


class TestBatchUpsertResourceDictsFallback:
    def test_persistent_error_retries_then_falls_back_to_individual(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep") as mock_sleep,
        ):
            successes, failures, failed_ids = resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        assert successes == 2
        assert failures == 0
        assert failed_ids == []
        assert cursor.executemany.call_count == 1 + _BATCH_MAX_RETRIES
        assert connection.rollback.call_count == 1 + _BATCH_MAX_RETRIES
        assert mock_sleep.call_count == _BATCH_MAX_RETRIES
        # 1 x SELECT max(txid) + 2 x fallback inserts
        assert cursor.execute.call_count == 3
        connection.commit.assert_called_once()

    def test_transient_error_succeeds_on_retry_without_fallback(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = [
            RuntimeError("transient db failure"),
            None,
        ]

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep") as mock_sleep,
        ):
            successes, failures, failed_ids = resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        assert successes == 2
        assert failures == 0
        assert failed_ids == []
        assert cursor.executemany.call_count == 2
        connection.rollback.assert_called_once()
        mock_sleep.assert_called_once()
        # Only SELECT max(txid), no per-row fallback inserts
        assert cursor.execute.call_count == 1
        connection.commit.assert_called_once()

    def test_dict_path_raises_non_database_error_without_fallback(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = ValueError("unexpected dict-path error")

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            pytest.raises(ValueError, match="unexpected dict-path error"),
        ):
            resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        assert cursor.executemany.call_count == 1
        # Only SELECT max(txid) should run before propagation.
        assert cursor.execute.call_count == 1
        connection.rollback.assert_not_called()
        connection.commit.assert_not_called()

    def test_unique_violation_on_one_row_does_not_poison_remaining_rows(self) -> None:
        """Savepoint isolation: one duplicate-key row must not abort the batch.

        Regression test for the 2026-06-09 Encounter incident: a concurrent
        asset had already created the resource under a different id, the
        fallback insert hit the identifier UNIQUE index, and without per-row
        subtransactions every subsequent insert failed with
        "current transaction is aborted".
        """
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        class FakeDiag:
            constraint_name = "idx_encounter_urn_source_ifdb_object_id_uniq"

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = FakeDiag()

        three_dicts = [
            ("obs-1", {"resourceType": "Observation", "id": "obs-1"}),
            ("obs-2", {"resourceType": "Observation", "id": "obs-2"}),
            ("obs-3", {"resourceType": "Observation", "id": "obs-3"}),
        ]
        # 1st execute = SELECT max(txid); then per-row fallback where the
        # FIRST row collides and the remaining two succeed.
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation("duplicate key value violates unique constraint"),
            None,
            None,
        ]

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep"),
        ):
            successes, failures, failed_ids = resource.batch_upsert_resource_dicts(
                resource_dicts=three_dicts,
                resource_type="Observation",
                batch_size=500,
            )

        assert successes == 2
        assert failures == 1
        assert len(failed_ids) == 1
        assert failed_ids[0].startswith("obs-1")
        # Each fallback row runs in its own subtransaction (SAVEPOINT).
        assert connection.transaction.call_count == 3
        connection.commit.assert_called_once()

    def test_backoff_delays_match_configured_values(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("persistent failure")

        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep") as mock_sleep,
        ):
            resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        assert mock_sleep.call_args_list == [call(0.5), call(1.0)]
