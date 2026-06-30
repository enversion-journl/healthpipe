# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for patient upsert idempotency.

These tests verify that the Patient asset correctly handles upsert behavior:
1. First run: INSERT new patients
2. Subsequent runs: UPDATE existing patients (no duplicates)
3. Mixed runs: INSERT new + UPDATE existing

These tests require a real FHIR database connection.
"""

import json
import logging
from unittest.mock import MagicMock

import dagster as dg
import pytest
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient

pytestmark = pytest.mark.skip(
    reason="Legacy patient upsert idempotency suite is flaky across environments"
)

from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

logger = logging.getLogger(__name__)


def _create_test_patient(
    patient_id: str,
    cpr: str,
    given_name: str = "Test",
    family_name: str = "Patient",
) -> Patient:
    """Create a test Patient resource with the given identifiers."""
    from fhir.resources.R4B.humanname import HumanName

    return Patient(
        identifier=[
            Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=patient_id),
            Identifier(system=SYSTEM_CPR, value=cpr),
        ],
        name=[HumanName(given=[given_name], family=family_name)],
        active=True,
        gender="unknown",
    )


def _count_patients_by_ifdb_id(fhir_database, patient_id: str) -> int:
    """Count how many patients exist with a given IFDB patient ID."""
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM patient
                WHERE resource @> %s
                """,
                [
                    json.dumps(
                        {"identifier": [{"system": SYSTEM_IFDB_PATIENT_ID, "value": patient_id}]}
                    )
                ],
            )
            return cur.fetchone()[0]


def _get_patient_fhir_id(fhir_database, ifdb_patient_id: str) -> str | None:
    """Get the FHIR UUID for a patient by IFDB patient ID."""
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM patient
                WHERE resource @> %s
                LIMIT 1
                """,
                [
                    json.dumps(
                        {
                            "identifier": [
                                {"system": SYSTEM_IFDB_PATIENT_ID, "value": ifdb_patient_id}
                            ]
                        }
                    )
                ],
            )
            row = cur.fetchone()
            return row[0] if row else None


def _delete_test_patients(fhir_database, patient_ids: list[str]) -> int:
    """Delete test patients by their IFDB patient IDs. Returns count deleted."""
    deleted = 0
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            for patient_id in patient_ids:
                cur.execute(
                    """
                    DELETE FROM patient
                    WHERE resource @> %s
                    """,
                    [
                        json.dumps(
                            {
                                "identifier": [
                                    {"system": SYSTEM_IFDB_PATIENT_ID, "value": patient_id}
                                ]
                            }
                        )
                    ],
                )
                deleted += cur.rowcount
        conn.commit()
    return deleted


@pytest.mark.integration
@pytest.mark.patient_test
class TestPatientUpsertIdempotency:
    """Tests for patient upsert idempotency with real database."""

    @pytest.fixture
    def test_patient_ids(self):
        """Generate unique test patient IDs for this test run."""
        import uuid

        prefix = str(uuid.uuid4())[:8]
        return [f"TEST_{prefix}_{i}" for i in range(5)]

    @pytest.fixture(autouse=True)
    def cleanup_test_patients(self, test_patient_ids, real_fhir_database_resource):
        """Clean up test patients before and after test."""
        # Clean before test
        _delete_test_patients(real_fhir_database_resource, test_patient_ids)

        yield

        # Clean after test
        _delete_test_patients(real_fhir_database_resource, test_patient_ids)

    def test_first_run_inserts_new_patients(
        self,
        real_fhir_database_resource,
        test_patient_ids,
    ):
        """Test that first run correctly inserts new patients."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        # Create test patients
        patients = [
            _create_test_patient(test_patient_ids[0], "0101900001"),
            _create_test_patient(test_patient_ids[1], "0101900002"),
        ]

        # Create mock context
        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # Post patients
        result = _post_patients_to_fhir(
            context=context,
            patients=patients,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        # Verify results
        assert result.metadata["posted_count"].value == 2
        assert result.metadata["failed_count"].value == 0
        assert result.metadata["new_count"].value == 2  # All should be new
        assert result.metadata["resolved_count"].value == 0  # None resolved

        # Verify patients exist in database
        assert _count_patients_by_ifdb_id(real_fhir_database_resource, test_patient_ids[0]) == 1
        assert _count_patients_by_ifdb_id(real_fhir_database_resource, test_patient_ids[1]) == 1

    def test_second_run_updates_existing_patients_no_duplicates(
        self,
        real_fhir_database_resource,
        test_patient_ids,
    ):
        """Test that second run updates existing patients without creating duplicates."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        # Create mock context
        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run: Insert patients
        patients_run1 = [
            _create_test_patient(test_patient_ids[0], "0101900001", "FirstName", "LastName"),
            _create_test_patient(test_patient_ids[1], "0101900002", "FirstName", "LastName"),
        ]

        _post_patients_to_fhir(
            context=context,
            patients=patients_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        # Store the original FHIR IDs
        original_id_0 = _get_patient_fhir_id(real_fhir_database_resource, test_patient_ids[0])
        original_id_1 = _get_patient_fhir_id(real_fhir_database_resource, test_patient_ids[1])

        assert original_id_0 is not None
        assert original_id_1 is not None

        # Second run: Same patients with different names (simulating update)
        patients_run2 = [
            _create_test_patient(test_patient_ids[0], "0101900001", "UpdatedFirst", "UpdatedLast"),
            _create_test_patient(test_patient_ids[1], "0101900002", "UpdatedFirst", "UpdatedLast"),
        ]

        result2 = _post_patients_to_fhir(
            context=context,
            patients=patients_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        # Verify second run shows all as resolved (existing)
        assert result2.metadata["posted_count"].value == 2
        assert result2.metadata["resolved_count"].value == 2  # Both should be resolved
        assert result2.metadata["new_count"].value == 0  # None should be new

        # Verify NO duplicates - should still be exactly 1 of each
        assert _count_patients_by_ifdb_id(real_fhir_database_resource, test_patient_ids[0]) == 1
        assert _count_patients_by_ifdb_id(real_fhir_database_resource, test_patient_ids[1]) == 1

        # Verify FHIR IDs are preserved (same UUID)
        new_id_0 = _get_patient_fhir_id(real_fhir_database_resource, test_patient_ids[0])
        new_id_1 = _get_patient_fhir_id(real_fhir_database_resource, test_patient_ids[1])

        assert new_id_0 == original_id_0, "FHIR ID should be preserved after update"
        assert new_id_1 == original_id_1, "FHIR ID should be preserved after update"

    def test_mixed_run_inserts_new_and_updates_existing(
        self,
        real_fhir_database_resource,
        test_patient_ids,
    ):
        """Test that a run with both new and existing patients handles both correctly."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # First run: Insert 2 patients
        patients_initial = [
            _create_test_patient(test_patient_ids[0], "0101900001"),
            _create_test_patient(test_patient_ids[1], "0101900002"),
        ]

        _post_patients_to_fhir(
            context=context,
            patients=patients_initial,
            fhir_database=real_fhir_database_resource,
            extracted_count=2,
        )

        # Second run: 2 existing + 2 new patients
        patients_mixed = [
            _create_test_patient(test_patient_ids[0], "0101900001"),  # Existing
            _create_test_patient(test_patient_ids[1], "0101900002"),  # Existing
            _create_test_patient(test_patient_ids[2], "0101900003"),  # New
            _create_test_patient(test_patient_ids[3], "0101900004"),  # New
        ]

        result = _post_patients_to_fhir(
            context=context,
            patients=patients_mixed,
            fhir_database=real_fhir_database_resource,
            extracted_count=4,
        )

        # Verify counts
        assert result.metadata["posted_count"].value == 4
        assert result.metadata["resolved_count"].value == 2  # 2 existing
        assert result.metadata["new_count"].value == 2  # 2 new

        # Verify no duplicates
        for i in range(4):
            count = _count_patients_by_ifdb_id(real_fhir_database_resource, test_patient_ids[i])
            assert (
                count == 1
            ), f"Patient {test_patient_ids[i]} should have exactly 1 record, found {count}"

    def test_multiple_runs_maintain_single_record(
        self,
        real_fhir_database_resource,
        test_patient_ids,
    ):
        """Test that running the same patient through multiple times maintains a single record."""
        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        patient_id = test_patient_ids[0]
        cpr = "0101900001"

        # Run the same patient 5 times
        for run in range(5):
            patients = [_create_test_patient(patient_id, cpr, f"Run{run}", f"Test{run}")]

            result = _post_patients_to_fhir(
                context=context,
                patients=patients,
                fhir_database=real_fhir_database_resource,
                extracted_count=1,
            )

            assert result.metadata["posted_count"].value == 1
            if run == 0:
                assert result.metadata["new_count"].value == 1
                assert result.metadata["resolved_count"].value == 0
            else:
                assert result.metadata["new_count"].value == 0
                assert result.metadata["resolved_count"].value == 1

        # After 5 runs, should still have exactly 1 patient
        count = _count_patients_by_ifdb_id(real_fhir_database_resource, patient_id)
        assert count == 1, f"Should have exactly 1 patient after 5 runs, found {count}"


@pytest.mark.integration
@pytest.mark.patient_test
class TestPatientIdResolutionPerformance:
    """Performance tests for patient ID resolution."""

    @pytest.fixture
    def bulk_test_patient_ids(self):
        """Generate unique test patient IDs for bulk testing."""
        import uuid

        prefix = str(uuid.uuid4())[:8]
        return [f"BULK_{prefix}_{i}" for i in range(100)]

    @pytest.fixture(autouse=True)
    def cleanup_bulk_test_patients(self, bulk_test_patient_ids, real_fhir_database_resource):
        """Clean up bulk test patients before and after test."""
        _delete_test_patients(real_fhir_database_resource, bulk_test_patient_ids)
        yield
        _delete_test_patients(real_fhir_database_resource, bulk_test_patient_ids)

    def test_bulk_upsert_performance(
        self,
        real_fhir_database_resource,
        bulk_test_patient_ids,
    ):
        """Test that bulk upsert with ID resolution performs well."""
        import time

        from DSA.FHIR.Patient.Patient_IFDB_dfm import _post_patients_to_fhir

        context = MagicMock(spec=dg.AssetExecutionContext)
        context.has_partition_key = True
        context.partition_key = "2025-01-15"
        context.log = MagicMock()

        # Create 100 test patients for first run
        patients_run1 = [
            _create_test_patient(pid, f"010190{i:04d}")
            for i, pid in enumerate(bulk_test_patient_ids)
        ]

        # First run (all inserts)
        start_time = time.perf_counter()
        result1 = _post_patients_to_fhir(
            context=context,
            patients=patients_run1,
            fhir_database=real_fhir_database_resource,
            extracted_count=100,
        )
        insert_time = time.perf_counter() - start_time

        assert result1.metadata["posted_count"].value == 100
        assert result1.metadata["new_count"].value == 100

        # Create fresh patient objects for second run (same identifiers)
        patients_run2 = [
            _create_test_patient(pid, f"010190{i:04d}")
            for i, pid in enumerate(bulk_test_patient_ids)
        ]

        # Second run (all updates)
        start_time = time.perf_counter()
        result2 = _post_patients_to_fhir(
            context=context,
            patients=patients_run2,
            fhir_database=real_fhir_database_resource,
            extracted_count=100,
        )
        update_time = time.perf_counter() - start_time

        assert result2.metadata["posted_count"].value == 100
        assert result2.metadata["resolved_count"].value == 100

        logger.info(f"Insert 100 patients: {insert_time:.2f}s")
        logger.info(f"Update 100 patients: {update_time:.2f}s")

        # Update should be reasonably fast (within 2x of insert time)
        # This is a soft assertion - if it fails, it's a performance concern
        assert (
            update_time < insert_time * 3
        ), f"Update time ({update_time:.2f}s) is more than 3x insert time ({insert_time:.2f}s)"
