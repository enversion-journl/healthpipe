# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for EpisodeOfCare previousEpisodeOfCare robustness (A + B + D).

Covers:
  - _collect_previous_eoc_identifiers (B helper)
  - _has_resolved_previous (D helper)
  - _DETECT_SQL shape (D detection query)
"""

import pytest
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import EXTENSION_PREVIOUS_EPISODE_OF_CARE
from utils.fhir.identifier_systems import SYSTEM_IFDB_EPISODE_OF_CARE_ID


def _make_eoc(
    object_id: str,
    prev_id: str | None = None,
    prev_resolved_fhir_id: str | None = None,
) -> EpisodeOfCare:
    """Build a minimal EpisodeOfCare with optional previous-reference extension."""
    extensions = []
    if prev_id:
        ref = Reference(
            identifier=Identifier(system=SYSTEM_IFDB_EPISODE_OF_CARE_ID, value=prev_id),
        )
        if prev_resolved_fhir_id:
            ref.id = prev_resolved_fhir_id
        extensions.append(
            Extension(
                url=EXTENSION_PREVIOUS_EPISODE_OF_CARE,
                valueReference=ref,
            )
        )
    return EpisodeOfCare(
        status="active",
        patient=Reference(
            identifier=Identifier(system="urn:source:ifdb:patient_id", value="PAT-1")
        ),
        identifier=[Identifier(system=SYSTEM_IFDB_EPISODE_OF_CARE_ID, value=object_id)],
        extension=extensions or None,
    )


# ===========================================================================
# B: _collect_previous_eoc_identifiers
# ===========================================================================


class TestCollectPreviousEocIdentifiers:
    """Tests for _collect_previous_eoc_identifiers."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import (
            _collect_previous_eoc_identifiers,
        )

        self.collect = _collect_previous_eoc_identifiers

    def test_no_extensions_returns_empty(self):
        eoc = _make_eoc("100")
        assert self.collect([eoc]) == set()

    def test_single_previous_collected(self):
        eoc = _make_eoc("200", prev_id="100")
        assert self.collect([eoc]) == {"100"}

    def test_multiple_episodes_aggregated(self):
        batch = [
            _make_eoc("200", prev_id="100"),
            _make_eoc("300", prev_id="200"),
            _make_eoc("400"),
        ]
        assert self.collect(batch) == {"100", "200"}

    def test_duplicates_deduplicated(self):
        batch = [
            _make_eoc("200", prev_id="100"),
            _make_eoc("300", prev_id="100"),
        ]
        assert self.collect(batch) == {"100"}

    def test_already_resolved_still_collected(self):
        eoc = _make_eoc("200", prev_id="100", prev_resolved_fhir_id="fhir-abc")
        result = self.collect([eoc])
        assert "100" in result

    def test_empty_batch_returns_empty(self):
        assert self.collect([]) == set()


# ===========================================================================
# D: _has_resolved_previous
# ===========================================================================


class TestHasResolvedPrevious:
    """Tests for _has_resolved_previous."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_QualityCheck import (
            _has_resolved_previous,
        )

        self.has_resolved = _has_resolved_previous

    def test_no_extensions(self):
        eoc = _make_eoc("100")
        assert not self.has_resolved(eoc)

    def test_unresolved_previous(self):
        eoc = _make_eoc("200", prev_id="100")
        assert not self.has_resolved(eoc)

    def test_resolved_previous(self):
        eoc = _make_eoc("200", prev_id="100", prev_resolved_fhir_id="fhir-abc")
        assert self.has_resolved(eoc)

    def test_resolved_checks_id_not_identifier(self):
        """The check looks for .id (FHIR server ID), not .identifier."""
        eoc = _make_eoc("200", prev_id="100")
        assert not self.has_resolved(eoc)
        eoc.extension[0].valueReference.id = "fhir-123"
        assert self.has_resolved(eoc)


# ===========================================================================
# D: _DETECT_SQL validation
# ===========================================================================


class TestDetectSql:
    """Verify the detection query has expected structure."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_QualityCheck import _DETECT_SQL

        self.sql = _DETECT_SQL

    def test_joins_archive_to_fhir(self):
        assert "arc.episode_of_care_ifdb" in self.sql
        assert "fhir.EpisodeOfCare" in self.sql

    def test_filters_non_deleted_archive_rows(self):
        assert "a.[DW_IsDeleted] = 0" in self.sql

    def test_detects_null_previous_in_fhir(self):
        assert "f.previous_episode_of_care_id IS NULL" in self.sql

    def test_requires_archive_has_previous(self):
        assert "a.[PREVIOUS_EOC_ID] IS NOT NULL" in self.sql

    def test_exists_guard_for_resolvable_target(self):
        """The EXISTS subquery ensures the referenced previous episode is
        actually present and non-deleted in the archive — excluding dangling
        or hashed references that can never be resolved."""
        assert "EXISTS" in self.sql
        assert "p.[OBJECT_ID]   = a.[PREVIOUS_EOC_ID]" in self.sql
        assert "p.[DW_IsDeleted] = 0" in self.sql
