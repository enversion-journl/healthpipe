# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils.ifdb import yaml_loader as yl


def test_parse_column_from_path():
    table, column = yl.parse_column_from_path("IFDB.ENCOUNTER.OBJECT_ID")
    assert table == "IFDB.ENCOUNTER"
    assert column == "OBJECT_ID"

    with pytest.raises(ValueError):
        yl.parse_column_from_path("BADPATH")


def test_is_reference_and_is_derived():
    assert yl.is_reference_column({"ref": "Patient"}) is True
    assert yl.is_reference_column({"from": "IFDB.COL"}) is False

    assert yl.is_derived_column({"from": "Derived"}) is True
    assert yl.is_derived_column({"from": "IFDB.COL"}) is False


def test_get_partition_column_and_load(monkeypatch, tmp_path):
    # Prepare minimal YAML content
    yaml_content = """
fhir:
  canonical:
    patient:
      resource: Patient
      columns:
        last_updated:
          from: IFDB.PATIENT.LAST_UPDATED
          type: timestamp
        id_col:
          from: IFDB.PATIENT.ID
          type: string
    encounter:
      resource: Encounter
      columns:
        modified_at:
          from: IFDB.ENCOUNTER.MODIFICATION_DATE
          type: timestamp
    derived_col:
      resource: Observation
      columns:
        derived_field:
          from: Derived
          type: string
"""
    fake_yaml = tmp_path / "fhir.ifdb.yml"
    fake_yaml.write_text(yaml_content)

    # Patch path resolver and clear cache
    monkeypatch.setattr(yl, "_get_yaml_path", lambda: fake_yaml)
    yl.load_fhir_ifdb_yaml.cache_clear()

    data = yl.load_fhir_ifdb_yaml()
    assert "canonical" in data

    # Partition column should pick timestamp with update keyword
    assert yl.get_partition_column("encounter") == "MODIFICATION_DATE"
    assert yl.get_partition_column("patient") == "LAST_UPDATED"
    # Derived column should be ignored
    assert yl.get_partition_column("derived_col") is None
