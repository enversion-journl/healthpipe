# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for FHIR posting utilities.

Tests the core posting functions:
- post_resources_direct()
- resolve_resource_ids_for_upsert()
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from core.fhir.direct_upsert import UpsertResult
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resource_dicts_direct,
    post_resources_direct,
    prepare_resources_for_upsert,
    resolve_resource_ids_for_upsert,
)
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient

pytestmark = pytest.mark.unit


class TestPostResourcesDirect:
    """Tests for post_resources_direct()."""

    def test_empty_resources_returns_zero_count(self):
        """Empty resource list returns MaterializeResult with zero count."""
        mock_context = MagicMock(spec=dg.AssetExecutionContext)
        mock_context.has_partition_key = True
        mock_context.partition_key = "2024-01-01"

        mock_fhir_database = MagicMock()

        result = post_resources_direct(
            context=mock_context,
            resources=[],
            fhir_database=mock_fhir_database,
            resource_type="Patient",
            source_name="test",
        )

        assert isinstance(result, dg.MaterializeResult)
        assert result.metadata["posted_count"].value == 0
        assert result.metadata["failed_count"].value == 0
        assert result.metadata["method"].value == "direct_postgres"

    def test_successful_posting(self):
        """Successful posting returns correct counts."""
        mock_context = MagicMock(spec=dg.AssetExecutionContext)
        mock_context.has_partition_key = True
        mock_context.partition_key = "2024-01-01"

        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (5, 0, [])

        patients = [Patient(id=f"patient-{i}", identifier=[]) for i in range(5)]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = post_resources_direct(
                context=mock_context,
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                source_name="test",
            )

        assert result.metadata["posted_count"].value == 5
        assert result.metadata["failed_count"].value == 0
        mock_fhir_database.batch_upsert_resources.assert_called_once()

    def test_partial_failure_raises(self):
        """Partial failure raises dg.Failure."""
        mock_context = MagicMock(spec=dg.AssetExecutionContext)
        mock_context.has_partition_key = True
        mock_context.partition_key = "2024-01-01"

        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (3, 2, ["id1", "id2"])

        patients = [Patient(id=f"patient-{i}", identifier=[]) for i in range(5)]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            with pytest.raises(dg.Failure) as exc_info:
                post_resources_direct(
                    context=mock_context,
                    resources=patients,
                    fhir_database=mock_fhir_database,
                    resource_type="Patient",
                    source_name="test",
                )

        assert "Failed to post 2 of 5" in str(exc_info.value.description)

    def test_invalidate_cache_default_true(self):
        """Default cache invalidation preserves existing non-chunk behaviour."""
        mock_context = MagicMock(spec=dg.AssetExecutionContext)
        mock_context.has_partition_key = True
        mock_context.partition_key = "2024-01-01"

        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (1, 0, [])
        patients = [Patient(id="patient-1", identifier=[])]

        post_resources_direct(
            context=mock_context,
            resources=patients,
            fhir_database=mock_fhir_database,
            resource_type="Patient",
            source_name="test",
        )

        _, kwargs = mock_fhir_database.batch_upsert_resources.call_args
        assert kwargs["invalidate_cache"] is True

    def test_invalidate_cache_false_threaded_to_upsert(self):
        """Chunked callers can defer cache invalidation until partition cleanup."""
        mock_context = MagicMock(spec=dg.AssetExecutionContext)
        mock_context.has_partition_key = True
        mock_context.partition_key = "2024-01-01"

        mock_fhir_database = MagicMock()
        mock_fhir_database.batch_upsert_resources.return_value = (1, 0, [])
        patients = [Patient(id="patient-1", identifier=[])]

        post_resources_direct(
            context=mock_context,
            resources=patients,
            fhir_database=mock_fhir_database,
            resource_type="Patient",
            source_name="test",
            invalidate_cache=False,
        )

        _, kwargs = mock_fhir_database.batch_upsert_resources.call_args
        assert kwargs["invalidate_cache"] is False


class TestResolveResourceIdsForUpsert:
    """Tests for resolve_resource_ids_for_upsert()."""

    def test_empty_resources_returns_empty_list(self):
        """Empty resource list returns empty list."""
        mock_fhir_database = MagicMock()

        result = resolve_resource_ids_for_upsert(
            resources=[],
            fhir_database=mock_fhir_database,
            resource_type="Patient",
            preferred_id_systems=["urn:test"],
        )

        assert result == []
        mock_fhir_database.resolve_resource_ids.assert_not_called()

    def test_resolves_ids_from_targeted_sql(self):
        """Resources get IDs attached via targeted SQL lookup."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {
            0: "uuid-1",
            1: "uuid-2",
        }

        patients = [
            Patient(identifier=[]),
            Patient(identifier=[]),
            Patient(identifier=[]),
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=["urn:test"],
            )

        assert result[0].id == "uuid-1"
        assert result[1].id == "uuid-2"
        assert result[2].id is None

        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="Patient",
            preferred_systems=["urn:test"],
            force_reload=False,
        )

    def test_calls_central_resource_id_resolver(self):
        """Helper delegates backend details to the central facade."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {}
        resources = [Patient(identifier=[])]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger"):
            resolve_resource_ids_for_upsert(
                resources=resources,
                fhir_database=mock_fhir_database,
                resource_type="Procedure",
                preferred_id_systems=["urn:p"],
            )

        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=resources,
            resource_type="Procedure",
            preferred_systems=["urn:p"],
            force_reload=False,
        )

    def test_force_reload_delegates_to_resolve_resource_ids(self):
        """force_reload=True is forwarded to resolve_resource_ids."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {0: "uuid-p1"}
        patients = [
            Patient(identifier=[Identifier(system="urn:cp", value="P1")]),
            Patient(identifier=[Identifier(system="urn:cp", value="P2")]),
            Patient(identifier=[Identifier(system="urn:other", value="skip")]),
        ]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger"):
            result = resolve_resource_ids_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="CarePlan",
                preferred_id_systems=["urn:cp"],
                force_reload=True,
            )

        assert result[0].id == "uuid-p1"
        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="CarePlan",
            preferred_systems=["urn:cp"],
            force_reload=True,
        )

    def test_prepare_resources_for_upsert_delegates_to_resolve_resource_ids(self):
        """prepare_resources_for_upsert delegates to resolve_resource_ids."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {0: "fhir-1"}
        reference_result = SimpleNamespace(
            total_resolved=0,
            total_unresolved=0,
            required_unresolved=0,
            nested_results={},
        )
        patients = [Patient(identifier=[Identifier(system="urn:cp", value="V1")])]

        with patch(
            "DSA.FHIR.utils.ifdb.fhir_posting.resolve_and_apply_all_references",
            return_value=reference_result,
        ):
            result = prepare_resources_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="CarePlan",
                preferred_id_systems=["urn:cp"],
                reference_configs=[MagicMock()],
                force_reload=True,
            )

        assert result[0].id == "fhir-1"
        mock_fhir_database.resolve_resource_ids.assert_called_once()

    def test_prepare_resources_for_upsert_keeps_default_paths(self):
        """All paths delegate to resolve_resource_ids (targeted SQL)."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {}
        reference_result = SimpleNamespace(
            total_resolved=0,
            total_unresolved=0,
            required_unresolved=0,
            nested_results={},
        )
        patients = [Patient(identifier=[])]

        with patch(
            "DSA.FHIR.utils.ifdb.fhir_posting.resolve_and_apply_all_references",
            return_value=reference_result,
        ):
            prepare_resources_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="CarePlan",
                preferred_id_systems=["urn:cp"],
                reference_configs=[MagicMock()],
            )

        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="CarePlan",
            preferred_systems=["urn:cp"],
            force_reload=False,
        )

    def test_prepare_resources_for_upsert_can_resolve_self_ids_only(self):
        """Resources without references use the same clean upsert prep entry point."""
        mock_fhir_database = MagicMock()
        mock_fhir_database.resolve_resource_ids.return_value = {0: "patient-1"}
        patients = [Patient(identifier=[])]

        with patch(
            "DSA.FHIR.utils.ifdb.fhir_posting.resolve_and_apply_all_references"
        ) as resolve_refs:
            result = prepare_resources_for_upsert(
                resources=patients,
                fhir_database=mock_fhir_database,
                resource_type="Patient",
                preferred_id_systems=["urn:patient"],
            )

        assert result[0].id == "patient-1"
        resolve_refs.assert_not_called()
        mock_fhir_database.resolve_resource_ids.assert_called_once_with(
            resources=patients,
            resource_type="Patient",
            preferred_systems=["urn:patient"],
            force_reload=False,
        )


class TestRecoveryCounterInMetadata:
    """Recovery-tælleren fra upserten skal nå materialiserings-metadata på begge stier."""

    def _context(self) -> MagicMock:
        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2024-01-01"
        return context

    def test_resource_path_surfaces_recovered_rows(self):
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = UpsertResult(5, 0, [], recovered=2)
        patients = [Patient(id=f"patient-{i}", identifier=[]) for i in range(5)]

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            logger = MagicMock()
            mock_logger.return_value = logger
            result = post_resources_direct(
                context=self._context(),
                resources=patients,
                fhir_database=fhir_database,
                resource_type="Patient",
            )

        assert result.metadata["posted_count"].value == 5
        assert result.metadata["unique_violation_recovered_count"].value == 2
        # Nettet må ikke være tavst: en genoprettelse er en advarsel, ikke en succes i stilhed.
        assert logger.warning.call_count == 1

    def test_dict_path_surfaces_recovered_rows(self):
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resource_dicts.return_value = UpsertResult(2, 0, [], recovered=1)

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            mock_logger.return_value = MagicMock()
            result = post_resource_dicts_direct(
                context=self._context(),
                resource_dicts=[("a", {"resourceType": "Observation"}), ("b", {})],
                fhir_database=fhir_database,
                resource_type="Observation",
            )

        assert result.metadata["posted_count"].value == 2
        assert result.metadata["unique_violation_recovered_count"].value == 1

    def test_plain_tuple_counts_as_zero_recovered(self):
        """Attrapper (og ældre kaldere) returnerer en bar tupel — det er 0 genoprettede."""
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = (1, 0, [])

        with patch("DSA.FHIR.utils.ifdb.fhir_posting.dg.get_dagster_logger") as mock_logger:
            logger = MagicMock()
            mock_logger.return_value = logger
            result = post_resources_direct(
                context=self._context(),
                resources=[Patient(id="p", identifier=[])],
                fhir_database=fhir_database,
                resource_type="Patient",
            )

        assert result.metadata["unique_violation_recovered_count"].value == 0
        logger.warning.assert_not_called()
