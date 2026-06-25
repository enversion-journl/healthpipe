# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from pathlib import Path

import pytest
from DSA.FHIR.utils.ifdb.validation import (
    FHIRAttributeValidator,
    ValidationResult,
)

# Check if fhir.ifdb.yml exists (used by FHIRAttributeValidator)
_YAML_PATH = Path(__file__).resolve().parents[2] / "model" / "fhir.ifdb.yml"
requires_fhir_ifdb_yaml = pytest.mark.skipif(
    not _YAML_PATH.exists(),
    reason="fhir.ifdb.yml not present on this branch",
)


@requires_fhir_ifdb_yaml
def test_extract_columns_from_sql():
    sql_source = """
def foo():
    query = \"\"\"SELECT
        c.PATIENT_ID,
        TRUNC(c.ADMISSION_DATE) AS ADMISSION_DATE,
        OTHER_COL
    FROM table c\"\"\"
"""
    v = FHIRAttributeValidator()
    cols = v._extract_columns_from_sql(sql_source)
    assert {"patient_id", "admission_date", "other_col"} <= cols


@requires_fhir_ifdb_yaml
def test_extract_columns_from_transform():
    transform_source = """
def transform(row):
    patient_id = row.get("patient_id")
    encounter_id = row["encounter_id"]
    extra = get_column_value(row, "extra_col")
    return patient_id, encounter_id, extra
"""
    v = FHIRAttributeValidator()
    extracted = {"patient_id", "encounter_id", "extra_col"}
    mapped = v._extract_columns_from_transform(transform_source, extracted)
    assert mapped == extracted


@requires_fhir_ifdb_yaml
def test_generate_summary_counts():
    v = FHIRAttributeValidator()
    res1 = ValidationResult("patient")
    res1.has_extraction_function = True
    res1.has_transform_function = True
    res1.yaml_columns = {"a", "b"}
    res1.missing_extraction = {"a"}
    res1.missing_mapping = {"b"}

    res2 = ValidationResult("encounter")
    res2.has_posting_function = True
    res2.uses_yaml_approach = True
    res2.yaml_columns = {"c"}

    v.results = {"patient": res1, "encounter": res2}
    summary = v._generate_summary()

    assert summary["total_resources"] == 2
    assert summary["resources_with_extraction"] == 1
    assert summary["resources_with_transform"] == 1
    assert summary["resources_with_posting"] == 1
    assert summary["resources_using_yaml"] == 1
    assert summary["total_yaml_columns"] == 3
    assert summary["total_missing_extraction"] == 1
    assert summary["total_missing_mapping"] == 1
