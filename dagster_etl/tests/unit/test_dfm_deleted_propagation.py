# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests: soft-deleted IFDB resources must still reach the FHIR DFM transforms.

Regression guard for the bug where the FHIR DFM query filtered `DW_IsDeleted = 0`
by default, so a soft-deleted Encounter/EpisodeOfCare (DELETED=1 → archive
DW_IsDeleted=1) fell out of the delta and was never tomb-stoned in Aidbox —
leaving it active forever. The archive query for these entities must include
deleted rows so the transform can set the isDeleted extension (and map status
to entered-in-error).

Patient already sets include_deleted=True (see Patient_IFDB_dfm); these tests
pin the same for Encounter and EpisodeOfCare.
"""

from core.dfm_config import RunModeConfig
from DSA.FHIR.Encounter.Encounter_IFDB_dfm import _build_encounter_query
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _build_eoc_query


def _cfg(mode: str = "delta", **kwargs) -> RunModeConfig:
    return RunModeConfig(mode=mode, **kwargs)


def _unbracket(sql: str) -> str:
    return sql.replace("[", "").replace("]", "")


class TestEncounterQueryIncludesDeleted:
    def test_no_dw_isdeleted_filter(self):
        sql = _build_encounter_query("2025-06-01 00:00:00", _cfg("delta"))
        assert "DW_IsDeleted" not in sql
        assert "arc.encounter_ifdb" in _unbracket(sql)

    def test_full_load_includes_deleted(self):
        sql = _build_encounter_query(None, _cfg("full_load"))
        assert "DW_IsDeleted" not in sql


class TestEpisodeQueryIncludesDeleted:
    def test_no_dw_isdeleted_filter(self):
        sql = _build_eoc_query("2025-06-01 00:00:00", _cfg("delta"))
        assert "DW_IsDeleted" not in sql
        assert "arc.episode_of_care_ifdb" in _unbracket(sql)

    def test_ordered_by_object_id(self):
        # safeguard A: previous (lower OBJECT_ID) processed first
        sql = _build_eoc_query(None, _cfg("full_load"))
        assert "ORDER BY [OBJECT_ID]" in sql
        assert "DW_IsDeleted" not in sql
