# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for EpisodeOfCare healthissue_code extension.

Covers:
  - _transform_episodes_to_fhir with healthissue_code_map (code found → valueCoding emitted)
  - _transform_episodes_to_fhir with healthissue_code_map miss (extension omitted)
  - _transform_episodes_to_fhir with healthissue_code_map=None (default, no extension)
  - _DETECT_HEALTHISSUE_CODE_SQL structural contracts
"""

from datetime import datetime

import pytest
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import _transform_episodes_to_fhir
from utils.fhir.fhir_extensions import EXTENSION_IFDB_HEALTHISSUE_CODE
from utils.fhir.identifier_systems import SYSTEM_IFDB_HEALTHISSUE_CODE, SYSTEM_IFDB_HEALTHISSUE_ID

pytestmark = pytest.mark.unit


class _MockLog:
    def __init__(self):
        self.warnings = []

    def info(self, msg):
        pass

    def warning(self, msg):
        self.warnings.append(msg)

    def debug(self, msg):
        pass


class _MockContext:
    def __init__(self):
        self.log = _MockLog()


def _base_row(object_id="EOC100", healthissue_id="500"):
    return {
        "OBJECT_ID": object_id,
        "PATIENT_ID": "PAT1",
        "STATUS": "active",
        "PERIOD_START": datetime(2025, 3, 1),
        "PERIOD_END": None,
        "PREVIOUS_EOC_ID": None,
        "PREVIOUS_EOC_REF_TYPE_CODE": None,
        "HEALTHISSUE_ID": healthissue_id,
        "MANAGING_ORGANIZATION_SORCODE": None,
        "DISCHARGE_DISPOSITION_CODE": None,
        "DELETED": 0,
        "IFDB_MODIFICATION_DATETIME": datetime(2025, 3, 2),
    }


# ===========================================================================
# Extension emission when healthissue_code_map is provided
# ===========================================================================


class TestHealthissueCodeExtensionEmission:
    def test_code_found_emits_value_coding(self):
        """When the map contains the HEALTHISSUE_ID, a valueCoding extension is emitted."""
        ctx = _MockContext()
        code_map = {
            "500": ("HealthIssueCodes.Codes.HealthIssueCodes.Kraeft.KraeftBryst", "Kræft i bryst")
        }
        episodes = _transform_episodes_to_fhir([_base_row()], ctx, healthissue_code_map=code_map)

        eoc = episodes[0]
        hi_exts = [e for e in eoc.extension if e.url == EXTENSION_IFDB_HEALTHISSUE_CODE]
        assert len(hi_exts) == 1
        ext = hi_exts[0]
        assert ext.valueCoding is not None
        assert ext.valueCoding.system == SYSTEM_IFDB_HEALTHISSUE_CODE
        assert ext.valueCoding.code == "HealthIssueCodes.Codes.HealthIssueCodes.Kraeft.KraeftBryst"
        assert ext.valueCoding.display == "Kræft i bryst"

    def test_code_not_in_map_omits_extension(self):
        """When HEALTHISSUE_ID has no entry in the map, the code extension is not emitted."""
        ctx = _MockContext()
        code_map = {"999": ("SomeCode", "SomeText")}
        episodes = _transform_episodes_to_fhir([_base_row()], ctx, healthissue_code_map=code_map)

        eoc = episodes[0]
        hi_code_exts = [
            e for e in (eoc.extension or []) if e.url == EXTENSION_IFDB_HEALTHISSUE_CODE
        ]
        assert hi_code_exts == []

    def test_none_map_omits_extension(self):
        """Default (healthissue_code_map=None) does not emit the code extension."""
        ctx = _MockContext()
        episodes = _transform_episodes_to_fhir([_base_row()], ctx, healthissue_code_map=None)

        eoc = episodes[0]
        hi_code_exts = [
            e for e in (eoc.extension or []) if e.url == EXTENSION_IFDB_HEALTHISSUE_CODE
        ]
        assert hi_code_exts == []

    def test_healthissue_id_extension_still_emitted(self):
        """The bare healthissue_id extension is always emitted regardless of code map."""
        ctx = _MockContext()
        episodes = _transform_episodes_to_fhir([_base_row()], ctx, healthissue_code_map=None)

        eoc = episodes[0]
        hi_id_exts = [e for e in (eoc.extension or []) if e.url == SYSTEM_IFDB_HEALTHISSUE_ID]
        assert len(hi_id_exts) == 1
        assert hi_id_exts[0].valueString == "500"

    def test_null_code_in_map_omits_extension(self):
        """If the map entry has code=None, the extension is not emitted.

        This is also the set→NULL direction the quality check detects: when a source
        code is removed, the re-transformed resource must carry no code extension, so
        the upsert clears fhir.EpisodeOfCare.healthissue_code on the next projection.
        """
        ctx = _MockContext()
        code_map = {"500": (None, "SomeText")}
        episodes = _transform_episodes_to_fhir([_base_row()], ctx, healthissue_code_map=code_map)

        eoc = episodes[0]
        hi_code_exts = [
            e for e in (eoc.extension or []) if e.url == EXTENSION_IFDB_HEALTHISSUE_CODE
        ]
        assert hi_code_exts == []

    def test_no_healthissue_id_no_extension(self):
        """Row with HEALTHISSUE_ID=None does not emit any healthissue extensions."""
        ctx = _MockContext()
        row = _base_row()
        row["HEALTHISSUE_ID"] = None
        code_map = {"500": ("Code", "Text")}
        episodes = _transform_episodes_to_fhir([row], ctx, healthissue_code_map=code_map)

        eoc = episodes[0]
        hi_exts = [
            e
            for e in (eoc.extension or [])
            if e.url in (EXTENSION_IFDB_HEALTHISSUE_CODE, SYSTEM_IFDB_HEALTHISSUE_ID)
        ]
        assert hi_exts == []


# ===========================================================================
# Detection SQL structural validation (quality-check)
# ===========================================================================


class TestDetectHealthissueCodeSql:
    @pytest.fixture(autouse=True)
    def _import(self):
        from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_QualityCheck import (
            _DETECT_HEALTHISSUE_CODE_SQL,
        )

        self.sql = _DETECT_HEALTHISSUE_CODE_SQL

    def test_joins_eoc_archive_to_healthissue_archive(self):
        assert "arc.ifdb_episode_of_care" in self.sql
        assert "arc.ifdb_healthissue" in self.sql

    def test_joins_to_fhir_table(self):
        assert "fhir.EpisodeOfCare" in self.sql

    def test_filters_non_deleted_archive_rows(self):
        assert "a.[dw_is_deleted] = 0" in self.sql

    def test_detects_code_mismatch_symmetrically(self):
        """NULL-safe comparison, so NULL→set, set→set' and set→NULL all register."""
        assert "ISNULL(f.healthissue_code, '') <> ISNULL(h.[HEALTHISSUE_CODE], '')" in self.sql

    def test_detects_text_mismatch(self):
        assert "ISNULL(f.healthissue_text, '') <> ISNULL(h.[HEALTHISSUE_TEXT], '')" in self.sql

    def test_guards_against_both_sides_null(self):
        """A NULL source code flattens to NULL in FHIR, so the both-NULL case must be
        excluded up front — otherwise it is re-detected as drift on every run and never
        converges."""
        assert "NOT (h.[HEALTHISSUE_CODE] IS NULL AND f.healthissue_code IS NULL)" in self.sql

    def test_does_not_guard_on_source_side_alone(self):
        """Regression: a bare source-side guard would also skip the set→NULL direction,
        leaving a removed code standing in fhir.EpisodeOfCare, invisible to the check."""
        assert "h.[HEALTHISSUE_CODE] IS NOT NULL" not in self.sql
