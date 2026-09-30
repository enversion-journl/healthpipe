# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests: QuestionnaireResponse canonical/version computed inline in MERGE.

The nightly measurement (2026-08-26) showed the post-merge normalization
UPDATE spending 933s touching 197k rows — every inserted/updated row needed
it because the MERGE wrote [questionnaire] but never the derived columns.
The fix computes canonical/version inside the MERGE (same CASE expressions,
shared single source of truth); the normalization UPDATE stays as a safety
net and now matches ~0 rows. End state is identical by construction.
"""

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    _build_questionnaire_response_key_normalization_sql,
    _questionnaire_canonical_expressions,
    build_fhir_merge_sql,
)

pytestmark = pytest.mark.unit

QR_COLUMNS = [
    "qr_id",
    "questionnaire",
    "status",
    "subject_patient_id",
    "authored",
    "last_updated",
]


class TestInlineComputedColumns:
    def test_qr_merge_computes_canonical_in_update_and_insert(self):
        sql = build_fhir_merge_sql(
            "QuestionnaireResponse",
            QR_COLUMNS,
            ["qr_id"],
            source_table="##stage",
            computed_columns=_questionnaire_canonical_expressions("src"),
        )
        # UPDATE SET normalizes both derived columns from src.questionnaire.
        assert "tgt.[questionnaire_canonical] = CASE WHEN src.[questionnaire] IS NULL" in sql
        assert "tgt.[questionnaire_version] = CASE WHEN src.[questionnaire] IS NULL" in sql
        # INSERT covers them too, so new rows are born normalized.
        assert "[questionnaire_canonical]" in sql.split("INSERT")[1]
        assert "[questionnaire_version]" in sql.split("INSERT")[1]

    def test_computed_columns_not_in_diff_predicate(self):
        sql = build_fhir_merge_sql(
            "QuestionnaireResponse",
            QR_COLUMNS,
            ["qr_id"],
            source_table="##stage",
            computed_columns=_questionnaire_canonical_expressions("src"),
        )
        diff = sql.split("WHEN MATCHED AND")[1].split("THEN")[0]
        assert "questionnaire_canonical" not in diff

    def test_other_tables_unchanged(self):
        sql = build_fhir_merge_sql(
            "Patient",
            ["patient_id", "name_given"],
            ["patient_id"],
            source_table="##stage",
        )
        assert "questionnaire_canonical" not in sql

    def test_normalization_uses_shared_expressions(self):
        exprs = _questionnaire_canonical_expressions("qr")
        sql = _build_questionnaire_response_key_normalization_sql("##stage")
        assert exprs["questionnaire_canonical"] in sql
        assert exprs["questionnaire_version"] in sql
        # Safety-net scope unchanged: joined to the stage table, guarded on
        # non-null questionnaire, drift-only predicate.
        assert "INNER JOIN ##stage" in sql
        assert "qr.[questionnaire] IS NOT NULL" in sql
        assert "qr.[questionnaire_canonical] IS NULL" in sql


class TestExpressionEquivalence:
    """The shared expressions must derive exactly what the old inline SQL did."""

    def test_canonical_splits_on_pipe(self):
        exprs = _questionnaire_canonical_expressions("src")
        canonical = exprs["questionnaire_canonical"]
        version = exprs["questionnaire_version"]
        assert "CHARINDEX(N'|', src.[questionnaire])" in canonical
        assert "LEFT(src.[questionnaire], CHARINDEX(N'|', src.[questionnaire]) - 1)" in canonical
        assert "SUBSTRING(src.[questionnaire], CHARINDEX(N'|', src.[questionnaire]) + 1" in version
        # No pipe: canonical = whole value, version = NULL.
        assert canonical.rstrip().endswith("ELSE src.[questionnaire] END")
        assert version.rstrip().endswith("ELSE NULL END")
