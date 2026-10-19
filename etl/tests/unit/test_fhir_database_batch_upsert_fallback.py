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
- Driver/serializer error text never reaches failed_ids or logs — it can
  embed resource content (for Patient the CPR in a unique-violation DETAIL).
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch

import pytest
from core.fhir.direct_upsert import _BATCH_MAX_RETRIES
from core.resources import FHIRDatabaseResource
from fhir.resources.R4B.patient import Patient

pytestmark = pytest.mark.unit


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


def _sample_patient_with_identifier() -> Patient:
    return Patient(
        id="incoming-pat",
        identifier=[
            {
                "system": "urn:source:ifdb:patient_id",
                "value": "PAT-1",
            }
        ],
    )


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

    def test_unique_identifier_collision_counts_as_success_when_resolved(self) -> None:
        """Concurrent create under another id is idempotent if identifier re-resolves."""
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        class FakeDiag:
            constraint_name = "idx_patient_urn_source_ifdb_patient_id_uniq"

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = FakeDiag()

        patient = _sample_patient_with_identifier()
        # 1st execute = SELECT max(txid); 2nd = fallback insert collision;
        # 3rd = conservative re-resolve by identifier; 4th = re-upsert.
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation("duplicate key value violates unique constraint"),
            None,
            None,
        ]
        cursor.fetchall.return_value = [("existing-pat",)]

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
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[patient],
                resource_type="Patient",
                batch_size=500,
            )

        assert successes == 1
        assert failures == 0
        assert failed_ids == []
        assert patient.id == "existing-pat"
        lookup_call = cursor.execute.call_args_list[-2]
        assert "'urn:source:ifdb:patient_id'" in lookup_call.args[0]
        assert lookup_call.args[1] == ("PAT-1",)
        retry_call = cursor.execute.call_args_list[-1]
        assert "ON CONFLICT (id) DO UPDATE" in retry_call.args[0]
        assert retry_call.args[1][0] == "existing-pat"
        assert '"existing-pat"' in retry_call.args[1][2]
        connection.commit.assert_called_once()

    def test_unique_identifier_collision_lookup_error_counts_as_failure(self) -> None:
        """A failing re-resolve lookup must not crash or poison the batch."""
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = None

        patient = _sample_patient_with_identifier()
        # 1st execute = SELECT max(txid); 2nd = fallback insert collision;
        # 3rd = re-resolve lookup, which itself fails.
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation("duplicate key value violates unique constraint"),
            RuntimeError("lookup failed"),
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
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[patient],
                resource_type="Patient",
                batch_size=500,
            )

        assert successes == 0
        assert failures == 1
        assert len(failed_ids) == 1
        connection.commit.assert_called_once()


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
        # Each fallback row runs in its own subtransaction (SAVEPOINT), and the
        # colliding row's identifier re-resolve lookup adds one more.
        assert connection.transaction.call_count == 4
        connection.commit.assert_called_once()

    def test_dict_unique_identifier_collision_counts_as_success_when_resolved(self) -> None:
        """Dict upserts use the same duplicate-identifier idempotency guard."""
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")

        class FakeDiag:
            constraint_name = "idx_observation_urn_source_ifdb_observation_id_uniq"

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = FakeDiag()

        rows = [
            (
                "incoming-obs",
                {
                    "resourceType": "Observation",
                    "id": "incoming-obs",
                    "identifier": [
                        {
                            "system": "urn:source:ifdb:observation_id",
                            "value": "OBS-1",
                        }
                    ],
                },
            )
        ]
        # 1st execute = SELECT max(txid); 2nd = fallback insert collision;
        # 3rd = conservative re-resolve by identifier; 4th = re-upsert.
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation("duplicate key value violates unique constraint"),
            None,
            None,
        ]
        cursor.fetchall.return_value = [("existing-obs",)]

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
                resource_dicts=rows,
                resource_type="Observation",
                batch_size=500,
            )

        assert successes == 1
        assert failures == 0
        assert failed_ids == []
        assert rows[0][1]["id"] == "existing-obs"
        lookup_call = cursor.execute.call_args_list[-2]
        assert "'urn:source:ifdb:observation_id'" in lookup_call.args[0]
        assert lookup_call.args[1] == ("OBS-1",)
        retry_call = cursor.execute.call_args_list[-1]
        assert "ON CONFLICT (id) DO UPDATE" in retry_call.args[0]
        assert retry_call.args[1][0] == "existing-obs"
        assert '"existing-obs"' in retry_call.args[1][2]
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


_CPR = "0101011234"
_UNIQUE_VIOLATION_TEXT = (
    'duplicate key value violates unique constraint "patient_urn_dk_cpr_uniq"\n'
    f"DETAIL:  Key (extract_identifier_value(resource, 'urn:dk:cpr'))=({_CPR}) "
    "already exists."
)


def _all_logged_text(logger: MagicMock) -> str:
    """Every positional arg and kwarg passed to any log method, as one string."""
    parts: list[str] = []
    for method in ("debug", "info", "warning", "error", "exception", "critical"):
        for logged in getattr(logger, method).call_args_list:
            parts.extend(str(arg) for arg in logged.args)
            parts.extend(f"{key}={value}" for key, value in logged.kwargs.items())
    return " | ".join(parts)


class TestFailureReportingCarriesNoErrorText:
    """GDPR: driver/serializer error text is of unbounded provenance and can
    embed resource content — for Patient resources the CPR, e.g. in a
    unique-violation DETAIL. failed_ids and logs may carry only the exception
    class and SQLSTATE, never the message, and never exc_info (the rendered
    traceback ends with the message)."""

    def test_resource_path_unique_violation_detail_never_reaches_failed_ids_or_logs(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        class FakeBatchUniqueViolation(RuntimeError):
            sqlstate = "23505"

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = None

        patient = Patient(
            id="incoming-pat",
            identifier=[{"system": "urn:dk:cpr", "value": _CPR}],
        )
        # Batch-level AND per-row error text both carry the CPR-bearing DETAIL.
        cursor.executemany.side_effect = FakeBatchUniqueViolation(_UNIQUE_VIOLATION_TEXT)
        # 1st execute = SELECT max(txid); 2nd = fallback insert collision;
        # 3rd = re-resolve lookup that finds nothing -> real failure.
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation(_UNIQUE_VIOLATION_TEXT),
            None,
        ]
        cursor.fetchall.return_value = []

        logger = MagicMock()
        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=logger),
            patch("core.fhir.direct_upsert.time.sleep"),
        ):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[patient],
                resource_type="Patient",
                batch_size=500,
            )

        assert (successes, failures) == (0, 1)
        assert failed_ids == ["incoming-pat: FakeUniqueViolation [sqlstate=23505]"]
        assert _CPR not in _all_logged_text(logger)

    def test_dict_path_unique_violation_detail_never_reaches_failed_ids_or_logs(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        class FakeBatchUniqueViolation(RuntimeError):
            sqlstate = "23505"

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"
            diag = None

        rows = [
            (
                "pat-dict",
                {
                    "resourceType": "Patient",
                    "id": "pat-dict",
                    "identifier": [{"system": "urn:dk:cpr", "value": _CPR}],
                },
            )
        ]
        cursor.executemany.side_effect = FakeBatchUniqueViolation(_UNIQUE_VIOLATION_TEXT)
        cursor.execute.side_effect = [
            None,
            FakeUniqueViolation(_UNIQUE_VIOLATION_TEXT),
            None,
        ]
        cursor.fetchall.return_value = []

        logger = MagicMock()
        with (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=logger),
            patch("core.fhir.direct_upsert.time.sleep"),
        ):
            successes, failures, failed_ids = resource.batch_upsert_resource_dicts(
                resource_dicts=rows,
                resource_type="Patient",
                batch_size=500,
            )

        assert (successes, failures) == (0, 1)
        assert failed_ids == ["pat-dict: FakeUniqueViolation [sqlstate=23505]"]
        assert _CPR not in _all_logged_text(logger)

    def test_serialization_failure_carries_only_exception_class(self) -> None:
        resource = _build_resource()

        class ExplodingResource:
            id = "boom-1"

            def json(self, **kwargs):
                raise ValueError(f"invalid identifier value {_CPR}")

        logger = MagicMock()
        with patch("core.resources.dg.get_dagster_logger", return_value=logger):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[ExplodingResource()],
                resource_type="Patient",
            )

        assert (successes, failures) == (0, 1)
        assert failed_ids == ["boom-1: ValueError"]
        assert _CPR not in _all_logged_text(logger)
        # exc_info renders the traceback, which ends with the raw message.
        assert all(c.kwargs.get("exc_info") is not True for c in logger.warning.call_args_list)

    def test_non_person_serialization_failure_keeps_full_message_and_traceback(self) -> None:
        """Plan (a) scoping: non-person types (here Observation) carry no CPR,
        so their full error message and traceback are kept for debugging —
        unlike Patient, which is reduced to the exception class. The distinction
        is made by resource_type in _error_label / the exc_info call sites."""
        resource = _build_resource()
        detail = "invalid coding system https://example.org/x"

        class ExplodingResource:
            id = "obs-boom"

            def json(self, **kwargs):
                raise ValueError(detail)

        logger = MagicMock()
        with patch("core.resources.dg.get_dagster_logger", return_value=logger):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[ExplodingResource()],
                resource_type="Observation",
            )

        assert (successes, failures) == (0, 1)
        # full driver/serializer message kept for a non-person type
        assert failed_ids == [f"obs-boom: {detail}"]
        assert detail in _all_logged_text(logger)
        # ... and the traceback is preserved for debugging
        assert any(c.kwargs.get("exc_info") is True for c in logger.warning.call_args_list)

    def test_dict_prepare_failure_carries_only_exception_class(self) -> None:
        resource = _build_resource()

        logger = MagicMock()
        with (
            patch("core.resources.dg.get_dagster_logger", return_value=logger),
            patch(
                "core.fhir.direct_upsert._transform_references_for_aidbox",
                side_effect=ValueError(f"bad reference for {_CPR}"),
            ),
        ):
            successes, failures, failed_ids = resource.batch_upsert_resource_dicts(
                resource_dicts=[("pat-prep", {"resourceType": "Patient", "id": "pat-prep"})],
                resource_type="Patient",
            )

        assert (successes, failures) == (0, 1)
        assert failed_ids == ["pat-prep: ValueError"]
        assert _CPR not in _all_logged_text(logger)
        assert all(c.kwargs.get("exc_info") is not True for c in logger.warning.call_args_list)

    def test_identifier_collision_warning_carries_system_and_count_not_value(self) -> None:
        from core.fhir.direct_upsert import _resolve_existing_id_for_identifier_collision

        connection, cursor = _build_connection_and_cursor()
        cursor.fetchall.return_value = [("row-a",), ("row-b",)]

        logger = MagicMock()
        with patch("core.resources.dg.get_dagster_logger", return_value=logger):
            resolved = _resolve_existing_id_for_identifier_collision(
                conn=connection,
                cur=cursor,
                table_name="patient",
                resource_json={"identifier": [{"system": "urn:dk:cpr", "value": _CPR}]},
            )

        assert resolved is None
        logged = _all_logged_text(logger)
        assert "matched 2 rows" in logged
        assert "urn:dk:cpr" in logged
        assert _CPR not in logged


class TestStaffCprIsNotLogged:
    """Practitioner carries the EMPLOYEE's CPR as its business identifier.

    The unique index is on extract_identifier_value(resource,'urn:dk:cpr'), so a
    unique violation renders that CPR in the Postgres DETAIL. Before
    Practitioner existed, "not Patient" implied "no CPR in the payload" — this
    pins the pair that broke that assumption.
    """

    @pytest.mark.parametrize("resource_type", ["Practitioner", "PractitionerRole"])
    def test_staff_cpr_stays_out_of_failed_ids_and_logs(self, resource_type: str) -> None:
        resource = _build_resource()

        class ExplodingResource:
            id = "prac-boom"

            def json(self, **kwargs):
                raise ValueError(f"duplicate key ... DETAIL: Key (...)=({_CPR}) already exists")

        logger = MagicMock()
        with patch("core.resources.dg.get_dagster_logger", return_value=logger):
            successes, failures, failed_ids = resource.batch_upsert_resources(
                resources=[ExplodingResource()],
                resource_type=resource_type,
            )

        assert (successes, failures) == (0, 1)
        assert failed_ids == ["prac-boom: ValueError"]
        assert _CPR not in _all_logged_text(logger)
        assert all(c.kwargs.get("exc_info") is not True for c in logger.warning.call_args_list)


class TestUniqueViolationRecoveryPolicy:
    """Én genoprettelsespolitik for objekt- og dict-stien — og nettet er synligt som tæller.

    Genoprettelse tælles som succes (idempotent slutresultat), men rapporteres
    i ``UpsertResult.recovered``: på en sti, der pre-resolver sine id'er, skal
    tallet være 0, så et tal over 0 afslører en manglende pre-resolve i stedet
    for at gemme sig som en langsom succes.
    """

    class _FakeDiag:
        constraint_name = "idx_uniq"

    def _unique_violation(self, text: str = "duplicate key value violates unique constraint"):
        diag = self._FakeDiag()

        class FakeUniqueViolation(Exception):
            sqlstate = "23505"

        exc = FakeUniqueViolation(text)
        exc.diag = diag  # type: ignore[attr-defined]
        return exc

    def _patches(self, connection: MagicMock, logger: MagicMock | None = None):
        return (
            patch("core.resources.BATCH_UPSERT_FALLBACK_ERROR", RuntimeError),
            patch.object(
                FHIRDatabaseResource,
                "get_connection",
                return_value=_connection_ctx(connection),
            ),
            patch("core.resources.dg.get_dagster_logger", return_value=logger or MagicMock()),
            patch("core.fhir.direct_upsert.time.sleep"),
        )

    def test_object_path_counts_recovered_row(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()
        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")
        # SELECT max(txid); insert kolliderer; opslag finder én række; re-upsert lykkes.
        cursor.execute.side_effect = [None, self._unique_violation(), None, None]
        cursor.fetchall.return_value = [("existing-pat",)]

        p1, p2, p3, p4 = self._patches(connection)
        with p1, p2, p3, p4:
            result = resource.batch_upsert_resources(
                resources=[_sample_patient_with_identifier()],
                resource_type="Patient",
                batch_size=500,
            )

        assert tuple(result) == (1, 0, [])
        assert result.recovered == 1

    def test_dict_path_counts_recovered_row(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()
        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")
        cursor.execute.side_effect = [None, self._unique_violation(), None, None]
        cursor.fetchall.return_value = [("existing-obs",)]
        rows = [
            (
                "incoming-obs",
                {
                    "resourceType": "Observation",
                    "id": "incoming-obs",
                    "identifier": [{"system": "urn:source:ifdb:observation_id", "value": "OBS-1"}],
                },
            )
        ]

        p1, p2, p3, p4 = self._patches(connection)
        with p1, p2, p3, p4:
            result = resource.batch_upsert_resource_dicts(
                resource_dicts=rows,
                resource_type="Observation",
                batch_size=500,
            )

        assert tuple(result) == (1, 0, [])
        assert result.recovered == 1
        assert rows[0][1]["id"] == "existing-obs"

    def test_recovered_is_zero_when_nothing_collides(self) -> None:
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()

        p1, p2, p3, p4 = self._patches(connection)
        with p1, p2, p3, p4:
            result = resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        # Stadig en almindelig tupel for de kaldesteder, der pakker tre værdier ud.
        successes, failures, failed_ids = result
        assert (successes, failures, failed_ids) == (2, 0, [])
        assert result == (2, 0, [])
        assert result.recovered == 0

    def test_dict_path_logs_generic_row_failure(self) -> None:
        """Dict-stien manglede den generiske fejl-warning; nu deler begge stier den."""
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()
        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")
        # SELECT max(txid); første række fejler af en anden grund end unique; anden lykkes.
        cursor.execute.side_effect = [None, RuntimeError("row exploded"), None]

        logger = MagicMock()
        p1, p2, p3, p4 = self._patches(connection, logger)
        with p1, p2, p3, p4:
            result = resource.batch_upsert_resource_dicts(
                resource_dicts=_sample_dict_resources(),
                resource_type="Observation",
                batch_size=500,
            )

        assert tuple(result) == (1, 1, ["obs-1: row exploded"])
        assert result.recovered == 0
        assert "Failed to insert into observation (id=obs-1): row exploded" in _all_logged_text(
            logger
        )

    def test_reupsert_failure_label_carries_no_error_text_for_patient(self) -> None:
        """Fejler re-upserten, må dens fejltekst (der kan bære CPR) heller ikke nå failed_ids."""
        resource = _build_resource()
        connection, cursor = _build_connection_and_cursor()
        cursor.executemany.side_effect = RuntimeError("simulated db batch failure")
        cursor.execute.side_effect = [
            None,
            self._unique_violation(_UNIQUE_VIOLATION_TEXT),
            None,
            RuntimeError(_UNIQUE_VIOLATION_TEXT),
        ]
        cursor.fetchall.return_value = [("existing-pat",)]

        logger = MagicMock()
        p1, p2, p3, p4 = self._patches(connection, logger)
        with p1, p2, p3, p4:
            result = resource.batch_upsert_resources(
                resources=[_sample_patient_with_identifier()],
                resource_type="Patient",
                batch_size=500,
            )

        assert tuple(result) == (0, 1, ["incoming-pat: RuntimeError"])
        assert result.recovered == 0
        assert _CPR not in _all_logged_text(logger)
