# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Asset-level tests: the patient-id → CPR fallback is wired into the EoC quality checks.

The quality checks re-transform archive rows and re-resolve the (required)
patient reference before upserting the FULL resource. An episode that
references a soft-deleted PATIENT_ID only ever reached FHIR via the CPR
fallback, so the checks re-encounter exactly those references. Without
``reference_fallback_fn`` wired into their ``prepare_resources_for_upsert``
calls, resolution raises ``Patient not found …`` and the whole check crashes.

Bug each test catches: dropping ``reference_fallback_fn=patient_cpr_fallback``
from the respective call site in ``EpisodeOfCare_QualityCheck``.

The fallback mechanics themselves (hit/miss/no-op/raise) are covered in
``test_patient_cpr_fallback.py``.
"""

from contextlib import ExitStack, contextmanager
from unittest.mock import MagicMock, patch

import dagster as dg
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_QualityCheck import (
    fhir_quality_check_eoc_healthissue_code,
    fhir_quality_check_eoc_previous_references,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

# A soft-deleted PATIENT_ID whose CPR twin is the active FHIR patient.
_DELETED_PID = "3283517527"
_CPR = "0108589995"
_ACTIVE_FHIR_PATIENT = "fhir-active-patient"

_ARCHIVE_COLUMNS = ("OBJECT_ID", "PATIENT_ID", "STATUS", "PREVIOUS_EOC_ID")


def _mssql_dispatching(archive_rows: list[tuple]) -> MagicMock:
    """MSSQL mock serving the detect query, the archive fetch and the CPR lookup.

    Dispatches on the executed SQL: the patient archive (rendered bracketed,
    ``[arc].[ifdb_patient]``) → the fallback's PATIENT_ID → CPR translation;
    anything joining ``fhir.EpisodeOfCare`` → detection candidates; otherwise
    the ids-mode archive fetch.
    """
    cursor = MagicMock()

    def _execute(sql: str, *args, **kwargs):
        if "ifdb_patient" in sql:
            cursor.description = [("PATIENT_ID",), ("CPR",)]
            cursor.fetchall.return_value = [(_DELETED_PID, _CPR)]
        elif "fhir.EpisodeOfCare" in sql:
            cursor.description = [("OBJECT_ID",)]
            cursor.fetchall.return_value = [(row[0],) for row in archive_rows]
        else:
            cursor.description = [(c,) for c in _ARCHIVE_COLUMNS]
            cursor.fetchall.return_value = archive_rows

    cursor.execute.side_effect = _execute
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mssql = MagicMock()

    @contextmanager
    def _get_connection(*args, **kwargs):
        yield conn

    mssql.get_connection.side_effect = _get_connection
    return mssql


def _fhir_database() -> MagicMock:
    """FHIR database mock: the episode itself already exists (this is a re-upsert)."""
    fhir_db = MagicMock()
    fhir_db.resolve_resource_ids.return_value = {0: "fhir-eoc-1"}
    return fhir_db


def _dispatching_batch_resolve(
    *, identifiers, resource_type, identifier_system, fhir_database, **kwargs
):
    """Shared stand-in for every ``batch_resolve_references`` alias.

    The deleted PATIENT_ID has no FHIR Patient (primary miss); its CPR resolves
    to the active twin (fallback hit); EpisodeOfCare self-references resolve
    directly.
    """
    if identifier_system == SYSTEM_IFDB_PATIENT_ID:
        return {}
    if identifier_system == SYSTEM_CPR:
        return {_CPR: _ACTIVE_FHIR_PATIENT}
    if identifier_system == SYSTEM_IFDB_EPISODE_OF_CARE_ID:
        return {value: f"fhir-{value}" for value in identifiers}
    return {}


# ``batch_resolve_references`` is imported into each consuming module's
# namespace, so all three aliases must point at the dispatcher.
_RESOLVE_ALIASES = (
    "DSA.FHIR.utils.reference_resolver.batch_resolve_references",
    "DSA.FHIR.utils.ifdb.patient_cpr_fallback.batch_resolve_references",
    "DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm.batch_resolve_references",
)


@contextmanager
def _patched_resolution():
    with ExitStack() as stack:
        for alias in _RESOLVE_ALIASES:
            stack.enter_context(patch(alias, side_effect=_dispatching_batch_resolve))
        yield


def _single_upserted_episode(fhir_db: MagicMock):
    (call,) = fhir_db.batch_upsert_resources.call_args_list
    (episode,) = call.kwargs["resources"]
    return episode


class TestPreviousReferencesCheckUsesCprFallback:
    def test_episode_with_deleted_pid_is_fixed_not_crashed(self):
        # The candidate's previous reference is fixable (plain-integer id, cf.
        # _is_resolvable_eoc_id), but its patient reference points at the
        # deleted PATIENT_ID.
        archive_rows = [("1001", _DELETED_PID, "active", "1000")]
        mssql = _mssql_dispatching(archive_rows)
        fhir_db = _fhir_database()

        with _patched_resolution():
            result = fhir_quality_check_eoc_previous_references(
                dg.build_asset_context(),
                mssql_target_staging=mssql,
                fhir_database=fhir_db,
            )

        assert result.metadata["eoc_references_fixed"] == 1
        episode = _single_upserted_episode(fhir_db)
        assert episode.patient.id == _ACTIVE_FHIR_PATIENT
        assert episode.patient.identifier is None


class TestHealthissueCheckUsesCprFallback:
    def test_episode_with_deleted_pid_is_fixed_not_crashed(self):
        # healthissue drift on an episode whose patient reference points at
        # the deleted PATIENT_ID (no previous reference involved).
        archive_rows = [("1001", _DELETED_PID, "active", None)]
        mssql = _mssql_dispatching(archive_rows)
        fhir_db = _fhir_database()

        with _patched_resolution():
            result = fhir_quality_check_eoc_healthissue_code(
                dg.build_asset_context(),
                mssql_target_staging=mssql,
                fhir_database=fhir_db,
            )

        assert result.metadata["eoc_healthissue_code_fixed"] == 1
        episode = _single_upserted_episode(fhir_db)
        assert episode.patient.id == _ACTIVE_FHIR_PATIENT
        assert episode.patient.identifier is None
