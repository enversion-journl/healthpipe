# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for current IFDB Condition transforms and posting paths.

The legacy HEALTHISSUE-based Condition pipeline is decommissioned; these tests
cover the active DIAGNOSE and PAS_DIAGNOSE flows.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose import (
    _post_diagnose_to_fhir,
    _transform_diagnose_to_fhir,
)
from DSA.FHIR.Condition.Condition_IFDB_PasDiagnose import (
    _post_pas_diagnose_to_fhir,
    _transform_pas_diagnose_to_fhir,
)
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
)


@pytest.fixture
def mock_dagster_context() -> MagicMock:
    """Mock Dagster context for transform/post tests."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.partition_key = "2026-01-15"
    context.has_partition_key = True
    context.log = MagicMock()
    return context


@pytest.fixture
def sample_diagnose_rows() -> list[dict]:
    """Minimal valid rows for DIAGNOSE -> Condition transform."""
    return [
        {
            "OBJEKT_ID": "D-1",
            "PATIENT_ID": "P-1",
            "DIAGNOSE_KODE": "DA123",
            "DIAGNOSE_KODETEKST": "Aktiv diagnose",
            "SLETTET": 0,
            "BESLUT_TIDSPUNKT": datetime(2026, 1, 15, 9, 0, 0),
            "OPRET_TIDSPUNKT": datetime(2026, 1, 15, 8, 0, 0),
            "AFSLUT_TIDSPUNKT": None,
        },
        {
            "OBJEKT_ID": "D-2",
            "PATIENT_ID": "P-2",
            "DIAGNOSE_KODE": "DB456",
            "DIAGNOSE_KODETEKST": "Afsluttet diagnose",
            "SLETTET": 0,
            "BESLUT_TIDSPUNKT": datetime(2026, 1, 15, 10, 0, 0),
            "OPRET_TIDSPUNKT": datetime(2026, 1, 15, 7, 0, 0),
            "AFSLUT_TIDSPUNKT": datetime(2026, 1, 15, 11, 0, 0),
        },
    ]


@pytest.fixture
def sample_pas_row() -> dict:
    """Minimal valid row for PAS_DIAGNOSE -> Condition transform."""
    return {
        "OBJEKT_ID": "PAS-1",
        "PATIENT_ID": "P-10",
        "DIAGNOSE_KODE": "DZ038",
        "DIAGNOSE_KODETEKST": "Observation for mistanke",
        "SLETTET": 0,
        "DIAGNOSETYPE": "B",
        "LATER_DISPROVED": 0,
        "OPDATERET_TID": datetime(2026, 1, 15, 12, 0, 0),
    }


class TestDiagnoseTransform:
    def test_transforms_rows_to_condition_instances(
        self,
        sample_diagnose_rows: list[dict],
        mock_dagster_context: MagicMock,
    ) -> None:
        conditions = _transform_diagnose_to_fhir(sample_diagnose_rows, mock_dagster_context)
        assert len(conditions) == 2
        assert all(isinstance(cond, Condition) for cond in conditions)

    def test_sets_identifier_and_subject_systems(
        self,
        sample_diagnose_rows: list[dict],
        mock_dagster_context: MagicMock,
    ) -> None:
        conditions = _transform_diagnose_to_fhir(sample_diagnose_rows, mock_dagster_context)
        first = conditions[0]
        assert first.identifier[0].system == SYSTEM_IFDB_DIAGNOSE
        assert first.identifier[0].value == "D-1"
        assert first.subject.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert first.subject.identifier.value == "P-1"

    def test_maps_clinical_status_from_afslut_tidspunkt(
        self,
        sample_diagnose_rows: list[dict],
        mock_dagster_context: MagicMock,
    ) -> None:
        conditions = _transform_diagnose_to_fhir(sample_diagnose_rows, mock_dagster_context)
        assert conditions[0].clinicalStatus.coding[0].code == "active"
        assert conditions[1].clinicalStatus.coding[0].code == "resolved"

    def test_deleted_row_sets_entered_in_error_and_no_clinical_status(
        self,
        sample_diagnose_rows: list[dict],
        mock_dagster_context: MagicMock,
    ) -> None:
        deleted = dict(sample_diagnose_rows[0])
        deleted["SLETTET"] = 1
        conditions = _transform_diagnose_to_fhir([deleted], mock_dagster_context)
        cond = conditions[0]
        assert cond.verificationStatus.coding[0].code == "entered-in-error"
        assert cond.clinicalStatus is None

    def test_diagnosis_code_is_canonicalised_to_sks_d_form(
        self,
        sample_diagnose_rows: list[dict],
        mock_dagster_context: MagicMock,
    ) -> None:
        # Single codebase-wide rule (to_sks_diagnosis_code): bare ICD-10 codes
        # are D-prefixed (I10 -> DI10); SKS prefixes are stripped first; codes
        # already in SKS-D form are kept unchanged.
        bare = dict(sample_diagnose_rows[0])
        bare["DIAGNOSE_KODE"] = "I10"
        prefixed = dict(sample_diagnose_rows[1])
        prefixed["DIAGNOSE_KODE"] = "SKS.a.diaDA01"
        conditions = _transform_diagnose_to_fhir([bare, prefixed], mock_dagster_context)
        assert conditions[0].code.coding[0].code == "DI10"
        assert conditions[1].code.coding[0].code == "DA01"

    def test_empty_input_returns_empty_list(self, mock_dagster_context: MagicMock) -> None:
        assert _transform_diagnose_to_fhir([], mock_dagster_context) == []


class TestPasDiagnoseTransform:
    def test_sets_pas_identifier_system(
        self,
        sample_pas_row: dict,
        mock_dagster_context: MagicMock,
    ) -> None:
        cond = _transform_pas_diagnose_to_fhir([sample_pas_row], mock_dagster_context)[0]
        assert cond.identifier[0].system == SYSTEM_IFDB_PAS_DIAGNOSE
        assert cond.identifier[0].value == "PAS-1"

    def test_maps_refuted_when_later_disproved(
        self,
        sample_pas_row: dict,
        mock_dagster_context: MagicMock,
    ) -> None:
        row = dict(sample_pas_row)
        row["LATER_DISPROVED"] = 1
        cond = _transform_pas_diagnose_to_fhir([row], mock_dagster_context)[0]
        assert cond.verificationStatus.coding[0].code == "refuted"
        assert cond.clinicalStatus is None

    def test_applies_encounter_lookup_identifier(
        self,
        sample_pas_row: dict,
        mock_dagster_context: MagicMock,
    ) -> None:
        encounter_refs = {"PAS-1": "ENC-100"}
        cond = _transform_pas_diagnose_to_fhir(
            [sample_pas_row],
            mock_dagster_context,
            encounter_refs=encounter_refs,
        )[0]
        assert cond.encounter is not None
        assert cond.encounter.identifier.system == SYSTEM_IFDB_OBJECT_ID
        assert cond.encounter.identifier.value == "ENC-100"


def _condition_with_identifiers(
    condition_system: str,
    condition_value: str,
    patient_id: str,
    encounter_id: str | None = None,
) -> Condition:
    condition = Condition.construct(
        identifier=[Identifier.construct(system=condition_system, value=condition_value)],
        subject=Reference.construct(
            identifier=Identifier.construct(system=SYSTEM_IFDB_PATIENT_ID, value=patient_id)
        ),
    )
    if encounter_id:
        condition.encounter = Reference.construct(
            identifier=Identifier.construct(system=SYSTEM_IFDB_OBJECT_ID, value=encounter_id)
        )
    return condition


class TestDiagnosePosting:
    @patch("DSA.FHIR.Condition.Condition_IFDB_Diagnose.post_resources_direct")
    @patch("DSA.FHIR.Condition.Condition_IFDB_Diagnose.prepare_resources_for_upsert")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_patients")
    def test_post_resolves_patients_and_uses_diagnose_identifier_system(
        self,
        mock_resolve_missing_patients: MagicMock,
        mock_prepare: MagicMock,
        mock_post: MagicMock,
        mock_dagster_context: MagicMock,
    ) -> None:
        conditions = [
            _condition_with_identifiers(SYSTEM_IFDB_DIAGNOSE, "D-1", "P-1"),
            _condition_with_identifiers(SYSTEM_IFDB_DIAGNOSE, "D-2", "P-2"),
        ]
        mock_prepare.return_value = conditions
        mock_post.return_value = "posted"

        fhir_database = MagicMock()
        ifdb_oracle = MagicMock()

        result = _post_diagnose_to_fhir(
            context=mock_dagster_context,
            conditions=conditions,
            fhir_database=fhir_database,
            extracted_count=2,
            ifdb_oracle=ifdb_oracle,
        )

        assert result == "posted"
        mock_resolve_missing_patients.assert_called_once_with(
            patient_ids={"P-1", "P-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=mock_dagster_context,
        )
        assert mock_prepare.call_args.kwargs["resource_type"] == "Condition"
        assert mock_prepare.call_args.kwargs["preferred_id_systems"] == [SYSTEM_IFDB_DIAGNOSE]


class TestPasDiagnosePosting:
    @patch("DSA.FHIR.Condition.Condition_IFDB_PasDiagnose.post_resources_direct")
    @patch("DSA.FHIR.Condition.Condition_IFDB_PasDiagnose.prepare_resources_for_upsert")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_patients")
    @patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters")
    def test_post_resolves_patient_and_encounter_dependencies(
        self,
        mock_resolve_missing_encounters: MagicMock,
        mock_resolve_missing_patients: MagicMock,
        mock_prepare: MagicMock,
        mock_post: MagicMock,
        mock_dagster_context: MagicMock,
    ) -> None:
        conditions = [
            _condition_with_identifiers(
                SYSTEM_IFDB_PAS_DIAGNOSE,
                "PAS-1",
                "P-10",
                encounter_id="ENC-1",
            ),
            _condition_with_identifiers(
                SYSTEM_IFDB_PAS_DIAGNOSE,
                "PAS-2",
                "P-11",
                encounter_id="ENC-2",
            ),
        ]
        mock_prepare.return_value = conditions
        mock_post.return_value = "posted"

        fhir_database = MagicMock()
        ifdb_oracle = MagicMock()

        result = _post_pas_diagnose_to_fhir(
            context=mock_dagster_context,
            conditions=conditions,
            fhir_database=fhir_database,
            extracted_count=2,
            ifdb_oracle=ifdb_oracle,
        )

        assert result == "posted"
        mock_resolve_missing_patients.assert_called_once_with(
            patient_ids={"P-10", "P-11"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=mock_dagster_context,
        )
        mock_resolve_missing_encounters.assert_called_once_with(
            encounter_ids={"ENC-1", "ENC-2"},
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=mock_dagster_context,
        )
        assert mock_prepare.call_args.kwargs["resource_type"] == "Condition"
        assert mock_prepare.call_args.kwargs["preferred_id_systems"] == [SYSTEM_IFDB_PAS_DIAGNOSE]
