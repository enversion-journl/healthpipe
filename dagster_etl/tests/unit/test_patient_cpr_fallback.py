# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the patient-id → CPR reference-resolution fallback.

Covers:
- ``lookup_cprs_for_patient_ids``: staging translation PATIENT_ID → CPR.
- ``build_patient_cpr_fallback``: self-filtering, fallback-hit, fallback-miss.
- Integration with the shared reference resolver: a reference to a deleted
  PATIENT_ID whose CPR twin exists resolves; an unbridgeable required reference
  still raises (no silent data loss); the fallback is a no-op for direct hits.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import (
    build_patient_cpr_fallback,
    lookup_cprs_for_patient_ids,
)
from DSA.FHIR.utils.reference_resolver import (
    ReferenceFieldConfig,
    resolve_and_apply_all_references,
)
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

_MODULE = "DSA.FHIR.utils.ifdb.patient_cpr_fallback"


def _mock_mssql(rows: list[tuple], *, columns=("PATIENT_ID", "CPR")) -> MagicMock:
    """Build an MSSQLResource whose cursor yields ``rows``."""
    cursor = MagicMock()
    cursor.description = [(c,) for c in columns]
    cursor.fetchall.return_value = rows

    conn = MagicMock()
    conn.cursor.return_value = cursor

    mssql = MagicMock()

    @contextmanager
    def _get_connection(*args, **kwargs):
        yield conn

    mssql.get_connection.side_effect = _get_connection
    mssql._cursor = cursor  # exposed for assertions
    return mssql


def _patient_config(required: bool = True) -> ReferenceFieldConfig:
    return ReferenceFieldConfig(
        field_name="patient",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=required,
    )


def _eoc(patient_id: str) -> EpisodeOfCare:
    return EpisodeOfCare(
        identifier=[Identifier(system="urn:source:ifdb:episode_of_care_id", value="eoc-1")],
        status="active",
        patient=Reference(identifier=Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=patient_id)),
    )


# =============================================================================
# lookup_cprs_for_patient_ids
# =============================================================================


class TestLookupCprsForPatientIds:
    def test_returns_pid_to_cpr_for_rows_with_cpr(self):
        mssql = _mock_mssql([("3283517527", "0108589995"), ("119802175933", "0108589995")])
        result = lookup_cprs_for_patient_ids(["3283517527", "119802175933"], mssql)
        assert result == {
            "3283517527": "0108589995",
            "119802175933": "0108589995",
        }

    def test_skips_rows_with_empty_cpr(self):
        mssql = _mock_mssql([("p1", "0108589995"), ("p2", None), ("p3", "  ")])
        result = lookup_cprs_for_patient_ids(["p1", "p2", "p3"], mssql)
        assert result == {"p1": "0108589995"}

    def test_empty_input_short_circuits_without_query(self):
        mssql = _mock_mssql([])
        result = lookup_cprs_for_patient_ids([], mssql)
        assert result == {}
        mssql.get_connection.assert_not_called()

    def test_blank_ids_are_filtered_out(self):
        mssql = _mock_mssql([])
        result = lookup_cprs_for_patient_ids([None, "", "  "], mssql)
        assert result == {}
        mssql.get_connection.assert_not_called()


# =============================================================================
# build_patient_cpr_fallback
# =============================================================================


class TestBuildPatientCprFallback:
    def test_noop_for_non_patient_resource(self):
        mssql = _mock_mssql([])
        fallback = build_patient_cpr_fallback(mssql, MagicMock())
        result = fallback(
            resource_type="Organization",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=["x"],
        )
        assert result == {}
        mssql.get_connection.assert_not_called()

    def test_noop_for_non_patient_id_system(self):
        mssql = _mock_mssql([])
        fallback = build_patient_cpr_fallback(mssql, MagicMock())
        result = fallback(
            resource_type="Patient",
            identifier_system=SYSTEM_CPR,
            values=["x"],
        )
        assert result == {}
        mssql.get_connection.assert_not_called()

    @patch(f"{_MODULE}.batch_resolve_references")
    def test_fallback_hit_bridges_deleted_pid_via_cpr(self, mock_batch_resolve):
        # Deleted PATIENT_ID translates to a CPR whose active twin exists in FHIR.
        mssql = _mock_mssql([("3283517527", "0108589995")])
        mock_batch_resolve.return_value = {"0108589995": "fhir-active-patient"}

        fallback = build_patient_cpr_fallback(mssql, MagicMock())
        result = fallback(
            resource_type="Patient",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=["3283517527"],
        )

        assert result == {"3283517527": "fhir-active-patient"}
        # Resolution went through the CPR identifier system.
        _, kwargs = mock_batch_resolve.call_args
        assert kwargs["identifier_system"] == SYSTEM_CPR
        assert set(kwargs["identifiers"]) == {"0108589995"}

    @patch(f"{_MODULE}.batch_resolve_references")
    def test_fallback_miss_when_no_cpr_in_staging(self, mock_batch_resolve):
        mssql = _mock_mssql([])  # no rows → no CPR
        fallback = build_patient_cpr_fallback(mssql, MagicMock())
        result = fallback(
            resource_type="Patient",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=["unknown-pid"],
        )
        assert result == {}
        mock_batch_resolve.assert_not_called()

    @patch(f"{_MODULE}.batch_resolve_references")
    def test_fallback_miss_when_cpr_not_in_fhir(self, mock_batch_resolve):
        mssql = _mock_mssql([("p1", "0108589995")])
        mock_batch_resolve.return_value = {}  # CPR not found in FHIR
        fallback = build_patient_cpr_fallback(mssql, MagicMock())
        result = fallback(
            resource_type="Patient",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=["p1"],
        )
        assert result == {}


# =============================================================================
# Integration with the shared reference resolver
# =============================================================================


class TestResolverIntegration:
    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_deleted_pid_resolves_via_fallback(self, mock_batch_resolve):
        # Primary lookup finds nothing (deleted PATIENT_ID not in FHIR).
        mock_batch_resolve.return_value = {}

        episodes = [_eoc("3283517527")]
        fallback_calls: list[list[str]] = []

        def fallback(*, resource_type, identifier_system, values):
            fallback_calls.append(list(values))
            return {"3283517527": "fhir-active-patient"}

        result = resolve_and_apply_all_references(
            resources=episodes,
            reference_configs=[_patient_config(required=True)],
            fhir_database=MagicMock(),
            fallback_resolve_fn=fallback,
        )

        assert result.required_unresolved == 0
        assert episodes[0].patient.id == "fhir-active-patient"
        assert episodes[0].patient.identifier is None
        assert fallback_calls == [["3283517527"]]

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_unbridgeable_required_reference_still_raises(self, mock_batch_resolve):
        mock_batch_resolve.return_value = {}

        def fallback(*, resource_type, identifier_system, values):
            return {}  # cannot bridge

        with pytest.raises(ValueError, match="Patient not found"):
            resolve_and_apply_all_references(
                resources=[_eoc("3283517527")],
                reference_configs=[_patient_config(required=True)],
                fhir_database=MagicMock(),
                fallback_resolve_fn=fallback,
            )

    @patch("DSA.FHIR.utils.reference_resolver.batch_resolve_references")
    def test_direct_hit_does_not_invoke_fallback(self, mock_batch_resolve):
        mock_batch_resolve.return_value = {"119802175933": "fhir-direct"}

        episodes = [_eoc("119802175933")]
        fallback = MagicMock()

        resolve_and_apply_all_references(
            resources=episodes,
            reference_configs=[_patient_config(required=True)],
            fhir_database=MagicMock(),
            fallback_resolve_fn=fallback,
        )

        assert episodes[0].patient.id == "fhir-direct"
        fallback.assert_not_called()
