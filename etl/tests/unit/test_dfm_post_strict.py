# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for strict-mode posting in the DFM FHIR post helpers.

A resource that genuinely fails to persist (batch_upsert_resources reports it in
`failures`) must abort the run so the DFM checkpoint is NOT advanced — otherwise
the failed rows sit behind the checkpoint and are never retried.

Crucially for EpisodeOfCare: this gates only on **Pass 1** (the actual resource
persistence). An unresolvable previousEpisodeOfCare is NOT a Pass-1 failure (the
episode is posted without the reference, reconciled later by the quality-check
asset), and Pass-2 reference-update failures are tolerated. These tests pin that
distinction so the strict gate never breaks the previousEoC tolerance.
"""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.EpisodeOfCare import EpisodeOfCare_IFDB_dfm as emod
from DSA.FHIR.Patient import Patient_IFDB_dfm as pmod

pytestmark = pytest.mark.unit


def _ctx():
    ctx = MagicMock()
    ctx.has_partition_key = False
    ctx.log = MagicMock()
    return ctx


def _passthrough_upsert(resources, **kwargs):
    """Stand-in for prepare_resources_for_upsert: returns input unchanged."""
    return resources


# ===========================================================================
# Patient — strict on any post failure
# ===========================================================================


class TestPatientPostStrict:
    def test_raises_when_batch_reports_failures(self):
        fhir_db = MagicMock()
        fhir_db.batch_upsert_resources.return_value = (8, 2, ["p1: err", "p2: err"])
        patients = [MagicMock(id=None) for _ in range(10)]

        with patch.object(pmod, "prepare_resources_for_upsert", _passthrough_upsert):
            with pytest.raises(dg.Failure, match="Failed to post 2 of 10 Patient"):
                pmod._post_patients_to_fhir(
                    context=_ctx(),
                    patients=patients,
                    fhir_database=fhir_db,
                    extracted_count=10,
                )

    def test_no_raise_when_all_succeed(self):
        fhir_db = MagicMock()
        fhir_db.batch_upsert_resources.return_value = (10, 0, [])
        patients = [MagicMock(id=None) for _ in range(10)]

        with patch.object(pmod, "prepare_resources_for_upsert", _passthrough_upsert):
            result = pmod._post_patients_to_fhir(
                context=_ctx(),
                patients=patients,
                fhir_database=fhir_db,
                extracted_count=10,
            )

        assert result.metadata["posted_count"].value == 10
        assert result.metadata["failed_count"].value == 0


# ===========================================================================
# EpisodeOfCare — strict on Pass-1, tolerant of Pass-2 / previousEoC
# ===========================================================================


class TestEpisodePostStrict:
    def test_raises_on_pass1_failure(self):
        fhir_db = MagicMock()
        # Pass 1 reports a persistence failure.
        fhir_db.batch_upsert_resources.return_value = (4, 1, ["e1: err"])
        episodes = [MagicMock(id=None, patient=None, extension=None) for _ in range(5)]

        with patch.object(emod, "prepare_resources_for_upsert", _passthrough_upsert):
            with pytest.raises(dg.Failure, match="pass 1"):
                emod._post_episodes_to_fhir(
                    context=_ctx(),
                    episodes=episodes,
                    fhir_database=fhir_db,
                    extracted_count=5,
                    resolve_missing_dependencies=False,
                )

    def test_tolerates_pass2_reference_failures(self):
        """Pass-1 succeeds → episode persisted. Pass-2 (previous-ref update)
        reporting failures must NOT abort: the episode exists and the
        quality-check asset reconciles the missing reference later."""
        fhir_db = MagicMock()
        # Pass 1 ok (0 failures); Pass 2 reports failures.
        fhir_db.batch_upsert_resources.side_effect = [
            (2, 0, []),  # Pass 1
            (0, 1, ["e: x"]),  # Pass 2 (reference update)
        ]

        # ep1 carries a *resolved* previous-ref so it lands in episodes_with_refs
        # (triggering Pass 2); ep2 has no extension.
        ext = MagicMock()
        ext.url = emod.EXTENSION_PREVIOUS_EPISODE_OF_CARE
        ext.valueReference = MagicMock()  # .id is truthy by default → "resolved"
        ep1 = MagicMock(id=None, patient=None, extension=[ext])
        ep2 = MagicMock(id=None, patient=None, extension=None)
        episodes = [ep1, ep2]

        with (
            patch.object(emod, "prepare_resources_for_upsert", _passthrough_upsert),
            patch.object(emod, "_resolve_self_references_with_retry") as mock_resolve,
        ):
            result = emod._post_episodes_to_fhir(
                context=_ctx(),
                episodes=episodes,
                fhir_database=fhir_db,
                extracted_count=2,
                resolve_missing_dependencies=False,
            )

        # Did not raise → previousEoC tolerance preserved.
        mock_resolve.assert_called_once()
        assert fhir_db.batch_upsert_resources.call_count == 2  # Pass 1 + Pass 2
        assert result.metadata["posted_count"].value == 2

    def test_no_raise_when_all_succeed_no_refs(self):
        fhir_db = MagicMock()
        fhir_db.batch_upsert_resources.return_value = (3, 0, [])
        episodes = [MagicMock(id=None, patient=None, extension=None) for _ in range(3)]

        with (
            patch.object(emod, "prepare_resources_for_upsert", _passthrough_upsert),
            patch.object(emod, "_resolve_self_references_with_retry"),
        ):
            result = emod._post_episodes_to_fhir(
                context=_ctx(),
                episodes=episodes,
                fhir_database=fhir_db,
                extracted_count=3,
                resolve_missing_dependencies=False,
            )

        # No resolved refs → Pass 2 skipped → single upsert call.
        assert fhir_db.batch_upsert_resources.call_count == 1
        assert result.metadata["posted_count"].value == 3
