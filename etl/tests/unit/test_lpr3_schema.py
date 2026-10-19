# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 metadata harvesting: no data-row parsing and no silent schema loss."""

import csv
import io
import tarfile

import pytest
from DSI.csv_files._lpr3_control import (
    EXPORT_COLUMNS,
    MAX_CONTROL_BYTES,
    SchemaHarvestError,
    harvest_members,
)
from scripts.lpr3_schema import harvest_schema
from tests.fixtures.lpr3 import csv_bytes

pytestmark = pytest.mark.unit


class UnreadableData:
    def read(self, *_):
        pytest.fail("The schema harvester attempted to read a data row")


def _members(tables=("DimKontakt_1083", "DimDato"), cutoff="20260910"):
    metadata = [
        [
            "column_ordinal",
            "column_name",
            "system_type_name",
            "is_nullable",
            "max_length",
            "precision",
            "scale",
            "collation_name",
        ],
        ["1", "DW_SK_Kontakt", "bigint", "0", "8", "19", "0", ""],
        ["2", "LPR_Entity_ID", "uniqueidentifier", "1", "16", "0", "0", ""],
        ["3", "Beskrivelse", "nvarchar(510)", "1", "1020", "0", "0", ""],
    ]
    names = [f"{table}_{cutoff}.csv" for table in tables]
    members = [(f"Metadata_{name}", io.BytesIO(csv_bytes(metadata))) for name in names]
    entries = [list(EXPORT_COLUMNS)]
    for name in names:
        entries.extend(
            [
                ["Metadata_" + name, "3", "256"],
                [name, "123", "4567"],
            ]
        )
    members.append((f"SDP_ExportLog_{cutoff}.csv", io.BytesIO(csv_bytes(entries))))
    members.extend((name, UnreadableData()) for name in names)
    return members


def test_preserves_source_order_types_and_region_without_reading_data():
    result = harvest_members(_members(), expected_tables=2)
    assert list(result) == ["DimDato", "DimKontakt"]
    kontakt = result["DimKontakt"]
    assert kontakt["regional"] is True
    assert result["DimDato"]["regional"] is False
    assert kontakt["rows_exportlog"] == "123"
    assert kontakt["bytes_exportlog"] == "4567"
    assert kontakt["datafile"] == "DimKontakt_1083_20260910.csv"
    assert kontakt["columns"][1] == [
        "2",
        "LPR_Entity_ID",
        "uniqueidentifier",
        "1",
        "16",
        "0",
        "0",
        "",
    ]
    assert kontakt["columns"][2][4] == "1020"  # Preserve SDS bytes, do not guess char widths.


def test_bz2_stream_with_actual_sds_member_order_can_harvest_47_metadata_files(tmp_path):
    # Invented table names exercise the envelope, not the actual LPR3 dataset spec.
    members = _members(tuple(f"DimTest{i}" for i in range(47)))
    # SDS order: data, then Metadata, then ExportLog.
    members = members[-47:] + members[:-47]
    archive = tmp_path / "Region1083_LPRGrunddata_M_20260915120000.tar.bz2"
    with tarfile.open(archive, "w:bz2") as tar:
        for name, handle in members:
            data = b"not parsed as CSV" if isinstance(handle, UnreadableData) else handle.read()
            info = tarfile.TarInfo("1083/" + name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    result = harvest_schema(archive)
    assert len(result) == 47
    assert result["DimTest46"]["rows_exportlog"] == "123"


def test_rejects_missing_metadata_without_parsing_data():
    members = _members()[1:]
    with pytest.raises(SchemaHarvestError, match="metadata=1"):
        harvest_members(members, expected_tables=2)


def test_rejects_duplicate_metadata_instead_of_overwriting():
    members = _members()
    members.insert(1, (members[0][0], io.BytesIO(b"")))
    with pytest.raises(SchemaHarvestError, match="duplicate table metadata"):
        harvest_members(members, expected_tables=2)


def test_rejects_duplicate_export_entries_instead_of_overwriting():
    members = _members()
    name, handle = members[2]
    rows = list(csv.reader(io.StringIO(handle.read().decode("cp1252")), delimiter=";"))
    rows.append(rows[-1])
    members[2] = (name, io.BytesIO(csv_bytes(rows)))
    with pytest.raises(SchemaHarvestError, match="duplicate filename"):
        harvest_members(members, expected_tables=2)


def test_export_log_columns_are_read_by_name():
    members = _members()
    name, handle = members[2]
    rows = list(csv.reader(io.StringIO(handle.read().decode("cp1252")), delimiter=";"))
    members[2] = (name, io.BytesIO(csv_bytes([list(reversed(row)) for row in rows])))
    result = harvest_members(members, expected_tables=2)
    assert result["DimKontakt"]["rows_exportlog"] == "123"
    assert result["DimKontakt"]["bytes_exportlog"] == "4567"


def test_export_log_allows_additional_fields_without_losing_totals():
    members = _members()
    name, handle = members[2]
    rows = list(csv.reader(io.StringIO(handle.read().decode("cp1252")), delimiter=";"))
    rows[0].append("Oprettet")
    for row in rows[1:]:
        row.append("2026-09-15 12:00:00")
    members[2] = (name, io.BytesIO(csv_bytes(rows)))
    assert harvest_members(members, expected_tables=2)["DimDato"]["rows_exportlog"] == "123"


@pytest.mark.parametrize(
    "mutation", ["region", "cutoff", "missing_log", "missing_data", "duplicate_data"]
)
def test_rejects_inconsistent_delivery(mutation):
    members = _members()
    if mutation == "region":
        name, handle = members[0]
        members[0] = (name.replace("_1083", ""), handle)
    elif mutation == "cutoff":
        name, handle = members[0]
        members[0] = (name.replace("20260910", "20260906"), handle)
    elif mutation == "missing_log":
        del members[2]
    elif mutation == "missing_data":
        members.pop()
    else:
        members.append(members[-1])
    with pytest.raises(SchemaHarvestError):
        harvest_members(members, expected_tables=2)


def test_does_not_echo_invalid_export_value():
    members = _members()
    name, handle = members[2]
    data = handle.read().replace(b'"123"', b'"sensitive-source-value"')
    members[2] = (name, io.BytesIO(data))
    with pytest.raises(SchemaHarvestError) as error:
        harvest_members(members, expected_tables=2)
    assert "sensitive-source-value" not in str(error.value)
    assert error.value.__suppress_context__


def test_rejects_oversized_metadata():
    members = _members()
    members[0] = (members[0][0], io.BytesIO(b"x" * (MAX_CONTROL_BYTES + 1)))
    with pytest.raises(SchemaHarvestError, match="size limit"):
        harvest_members(members, expected_tables=2)


def test_rejects_ragged_metadata():
    members = _members()
    members[0] = (members[0][0], io.BytesIO(csv_bytes([["name", "type"], ["incomplete"]])))
    with pytest.raises(SchemaHarvestError, match="column count"):
        harvest_members(members, expected_tables=2)
