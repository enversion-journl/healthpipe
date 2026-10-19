# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Failure boundaries of streaming LPR3 ingestion, using complete miniature tars."""

import io
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from DSI.csv_files import lpr3
from DSI.csv_files.lpr3_datasets import SOURCES_BY_PREFIX
from tests.fixtures.lpr3 import build_delivery, csv_bytes, source_row

pytestmark = pytest.mark.unit


def test_empty_nontext_fields_obey_metadata_nullability():
    def field(source, name):
        return next(c for c in SOURCES_BY_PREFIX[source].columns if c.name == name)

    assert not lpr3.valid_format(field("DimKontakt", "DW_SK_Kontakt"), "")
    assert lpr3.valid_format(field("DimResultatIndberetning", "LPR_Entity_ID"), "")
    assert lpr3.valid_format(field("DimAdresse", "Type"), "")


@pytest.mark.parametrize(
    "value,expected",
    [
        ("ab", True),
        ("0123456789ab", True),
        ("aB" * 16, True),
        ("ab" * 17, False),
        ("abc", False),
        ("0x0123456789ab", False),
        ("0123456789gh", False),
    ],
)
def test_varbinary_metadata_limits_maximum_bytes_not_exact_length(value, expected):
    column = next(
        c
        for c in SOURCES_BY_PREFIX["DimResultatIndberetning"].columns
        if c.name == "ResultatIndberetning_ID"
    )
    assert lpr3.valid_format(column, value) is expected


def plan_for(path):
    archive = next(a for a in lpr3.discover_archives(str(path.parent)) if a.path == str(path))
    plan = lpr3.preflight(archive, None)
    plan.update(run_id="test-run", advance_checkpoint=True)
    return plan


def test_one_delivery_track_uses_timestamp_and_strict_checkpoint(tmp_path):
    build_delivery(tmp_path, stamp="20260915195603", cadence="M")
    build_delivery(tmp_path, stamp="20260916190000", cadence="U")
    archives = lpr3.discover_archives(str(tmp_path))
    assert lpr3.select_delivery(archives, None).kadence == "U"
    assert lpr3.select_delivery(archives, "2026-09-16T19:00:00") is None
    assert lpr3.select_delivery(archives, "2026-09-15T19:56:03").identity == "2026-09-16T19:00:00"
    assert lpr3.select_delivery(archives, "2026-09-16T19:00:00", date(2026, 9, 15)).kadence == "M"


def test_older_cutoff_stops_before_full_preflight(tmp_path, monkeypatch):
    path = build_delivery(tmp_path)
    controls = MagicMock(side_effect=AssertionError("should not scan whole archive"))
    monkeypatch.setattr(lpr3, "delivery_controls", controls)
    with pytest.raises(ValueError, match="older"):
        lpr3.preflight(lpr3.discover_archives(str(path.parent))[0], "2026-09-11")
    controls.assert_not_called()


def test_preflight_checks_all_47_and_reports_missing_without_reuse(tmp_path):
    path = build_delivery(tmp_path, omit=("DimKontakt",))
    plan = plan_for(path)
    assert len(plan["available"]) == 46
    assert plan["missing"] == ["DimKontakt"]
    assert plan["cutoff"] == "2026-09-10"


def test_metadata_drift_rejected_before_any_sql(tmp_path):
    def drift(members):
        name = next(n for n in members if n.startswith("Metadata_DimAdresse_"))
        members[name] = members[name].replace(b'"bigint"', b'"int"', 1)

    path = build_delivery(tmp_path, alter_members=drift)
    with pytest.raises(ValueError, match="Metadata differs"):
        plan_for(path)


def test_all_47_streams_keep_source_columns_stamps_and_counts(tmp_path):
    plan = plan_for(build_delivery(tmp_path))
    writers = {prefix: MagicMock() for prefix in plan["available"]}
    stats = {}
    for name, handle in lpr3.iter_members(plan["path"]):
        match = lpr3.MEMBER_RE.fullmatch(name)
        if match and not match["metadata"]:
            prefix = match["table"]
            stats[prefix] = lpr3.stream_member(
                handle, SOURCES_BY_PREFIX[prefix], writers[prefix], plan
            )
    assert len(stats) == 47
    assert stats["DimSundhedsklynge"]["audit_min"] == 1
    assert stats["DimSundhedsklynge"]["audit_distinct"] == 1
    for prefix, item in stats.items():
        assert item["rows"] == 1
        assert item["bytes"] == plan["available"][prefix]["bytes"]
        row = writers[prefix].add.call_args.args[0]
        assert row[:3] == ("M", "2026-09-15 19:56:03", "2026-09-10")
        assert row[3:] == tuple(source_row(SOURCES_BY_PREFIX[prefix]))


@pytest.mark.parametrize(
    "column,value",
    [("LPR_Entity_ID", "private-invalid-uuid"), ("DW_SK_Kontakt", "9223372036854775808")],
)
def test_invalid_values_fail_without_leaking_source_values(column, value):
    dataset = SOURCES_BY_PREFIX["DimKontakt"]
    data = csv_bytes([dataset.source_columns, source_row(dataset, **{column: value})])
    with pytest.raises(ValueError, match="invalid format/width") as error:
        lpr3.stream_member(
            io.BytesIO(data),
            dataset,
            MagicMock(),
            {
                "encoding": "cp1252",
                "identity": "2026-09-15T19:56:03",
                "kadence": "M",
                "cutoff": "2026-09-10",
            },
        )
    assert value not in str(error.value)


@pytest.mark.parametrize("bad_total", ["rows", "bytes"])
def test_control_mismatch_discards_pending_buffers(tmp_path, monkeypatch, bad_total):
    prefix = "DimKontakt"
    path = build_delivery(
        tmp_path,
        **{("row_count_delta" if bad_total == "rows" else "byte_count_delta"): {prefix: 1}},
    )
    plan = plan_for(path)
    writer = MagicMock()
    monkeypatch.setattr(lpr3, "_ExtWriter", lambda *args: writer)
    conn = MagicMock()
    with pytest.raises(
        ValueError, match=f"{('row' if bad_total == 'rows' else 'byte')} counts differ"
    ):
        lpr3.load_group(conn, MagicMock(), plan, (prefix,))
    writer.discard.assert_called_once()
    writer.close.assert_called_once()
    writer.flush.assert_not_called()


def test_manifest_cannot_publish_partial_or_overlapping_groups(tmp_path):
    plan = plan_for(build_delivery(tmp_path))
    conn = MagicMock()
    with pytest.raises(ValueError, match="incomplete"):
        lpr3.write_manifest(conn, plan, ({},))
    with pytest.raises(ValueError, match="overlap"):
        lpr3.write_manifest(conn, plan, ({"DimKontakt": {}}, {"DimKontakt": {}}))
    conn.cursor.assert_not_called()


def test_writer_suppresses_database_values_on_failure():
    conn = MagicMock()
    conn.cursor.return_value.bulkcopy.side_effect = RuntimeError("private-source-data")
    writer = lpr3._ExtWriter(
        conn, SOURCES_BY_PREFIX["DimKontakt"], SimpleNamespace(log=MagicMock())
    )
    writer.add(
        tuple(["M", "2026-09-15", "2026-09-10", *source_row(SOURCES_BY_PREFIX["DimKontakt"])])
    )
    with pytest.raises(ValueError, match="source values suppressed") as error:
        writer.flush()
    assert "private-source-data" not in str(error.value)
    writer.discard()
