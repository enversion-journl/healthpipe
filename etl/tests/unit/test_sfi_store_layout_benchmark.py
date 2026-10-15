# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Målebænk for storens layout: rene funktioner og status-indsamling med attrapper.

Fejl disse tests fanger:
- et layout mangler et af de indeks, SWITCH-testen forudsætter identiske på begge sider
- pladsvogteren regner forkert eller kan omgås
- statusasset'et rapporterer forkert på en upartitioneret eller manglende store
"""

from __future__ import annotations

import pytest
from DSO.load import sfi_store_partitioning as p
from infrastructure.assets import sfi_store_layout_benchmark as b

pytestmark = pytest.mark.unit


def _norm(text: str) -> str:
    return " ".join(text.split())


class FakeCursor:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sql: list[str] = []
        self._rows: list = []
        self.description = None

    def execute(self, sql, params=None):
        text = _norm(sql)
        self.sql.append(text)
        self._rows = []
        self.description = None
        for key, rows in self.responses:
            if key in text:
                self._rows = list(rows)
                self.description = [("c",)]
                break

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def nextset(self):
        return False


def test_variants_are_validated_and_deduplicated():
    assert b.validate_variants(["rowstore", "rowstore", "baseline"]) == ["rowstore", "baseline"]
    with pytest.raises(ValueError, match="Ukendte"):
        b.validate_variants(["heap"])
    with pytest.raises(ValueError, match="Mindst"):
        b.validate_variants([])


def test_footprint_estimate_scales_with_rows_and_layouts():
    one = b.estimate_footprint_gb(1_000_000, 4000.0, ["baseline"])
    assert one == pytest.approx(1_000_000 * 4000 * 1.25 / 1024**3)
    assert b.estimate_footprint_gb(1_000_000, 4000.0, ["baseline", "columnstore"]) > one
    assert b.estimate_footprint_gb(2_000_000, 4000.0, ["baseline"]) == pytest.approx(2 * one)


def test_layout_statements_per_variant():
    table = "[fhir].[SFI_datapoint_store_bench_x]"
    baseline = b.layout_statements("baseline", table)
    assert baseline[0].startswith("ALTER TABLE [fhir].[SFI_datapoint_store_bench_x] ADD CONSTRAINT")
    assert "INCLUDE ([datapoint_key])" in baseline[-1], "dagens SourceResource har INCLUDE"
    assert all(
        "DATA_COMPRESSION" not in s for s in baseline[1:]
    ), "dagens sekundære indeks er ukomprimerede"

    rowstore = b.layout_statements("rowstore", table)
    assert rowstore[0].startswith(
        "CREATE UNIQUE CLUSTERED INDEX [UQ_SFI_datapoint_store_bench_x_IdOccurred]"
    )
    assert [s.split("[")[1].split("]")[0] for s in rowstore[1:]] == [
        ix.name for ix in b.ROWSTORE_INDEXES
    ]
    assert len(b.ROWSTORE_INDEXES) == 5 and sum(not ix.aligned for ix in b.ROWSTORE_INDEXES) == 1

    columnstore = b.layout_statements("columnstore", table)
    # heap -> partitioneret rowstore -> columnstore (DROP_EXISTING, samme navn) -> B-træer
    assert columnstore[0].startswith(
        "CREATE CLUSTERED INDEX [CCI_FHIR_SFI_datapoint_store_bench_x]"
    )
    assert columnstore[1].startswith(
        "CREATE CLUSTERED COLUMNSTORE INDEX [CCI_FHIR_SFI_datapoint_store_bench_x]"
    )
    assert "DROP_EXISTING = ON" in columnstore[1]
    for statement in columnstore[:2]:
        assert f"ON [{p.PARTITION_SCHEME}]([{p.PARTITION_COLUMN}])" in statement
    names = [s.split("[")[1].split("]")[0] for s in columnstore[2:]]
    assert names == [ix.name for ix in p.STORE_INDEXES], "columnstore-varianten ER basis-DDL'en"
    assert b.secondary_index_names("columnstore") == names
    assert all(f"ON [{p.PARTITION_SCHEME}]([{p.PARTITION_COLUMN}])" in s for s in columnstore[2:])
    assert (
        len(b.secondary_index_names("baseline")) == 5
        and len(b.secondary_index_names("rowstore")) == 5
    )
    with pytest.raises(ValueError):
        b.layout_statements("heap", table)


def test_bench_tables_live_in_fhir_schema_with_prefix():
    assert b.bench_table("rowstore") == "[fhir].[SFI_datapoint_store_bench_rowstore]"
    assert b.bench_work_table("rowstore") == "[fhir].[SFI_datapoint_store_bench_rowstore_work]"


def test_preflight_refuses_when_space_is_short():
    cfg = b.SfiStoreLayoutBenchmarkConfig(source_rows=5_000_000, min_free_gb=60.0)
    cur = FakeCursor(
        [
            ("SERVERPROPERTY('Edition')", [("Developer Edition (64-bit)", "16.0.4255.1", 3)]),
            (
                "sys.dm_db_partition_stats AS ps WHERE ps.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')",
                [(200_000.0, 60_000.0, 53_000_000)],
            ),
            ("dm_os_volume_stats", [(70.0,)]),
        ]
    )
    log = type("L", (), {"info": lambda *a, **k: None})()
    import dagster as dg

    with pytest.raises(dg.Failure, match="For lidt plads"):
        b.preflight(cur, cfg, log)
    cur.responses[-1] = ("dm_os_volume_stats", [(400.0,)])
    info = b.preflight(cur, cfg, log)
    assert info["store_rows"] == 53_000_000 and info["variants"] == [
        "baseline",
        "rowstore",
        "columnstore",
    ]
    assert info["estimate_gb"] > 0 and info["free_gb_before"] == 400.0


def test_summary_table_renders_one_row_per_variant():
    results = {
        "rowstore": {
            "size": {"rows": 10, "total_mb": 1.0, "index_mb": 0.5},
            "load": {"first_batch_us_per_row": 1.0, "last_batch_us_per_row": 2.0},
            "index_build_ms": {"a": 1000},
            "repost": {"us_per_row": 3.0},
            "switch": {
                "ok": True,
                "switch_out_ms": 5,
                "switch_in_ms": 6,
                "non_aligned_rebuild_ms": 2000,
            },
            "queries": {"patient": {"second_ms": 7}},
        },
        "baseline": {"switch": {"skipped": "upartitioneret layout"}},
    }
    table = b.summary_table(results)
    lines = table.splitlines()
    assert len(lines) == 4
    assert (
        "| rowstore | 10 | 1.0 | 0.5 | 1.0→2.0 | 1.0 | 3.0 | -/- | 5/6 | 2.0 | 7 | - | - | - |"
        in lines[2]
    )
    assert "upartitioneret layout" in lines[3]


def test_collect_partition_status_on_partitioned_store():
    cur = FakeCursor(
        [
            ("OBJECT_ID(N'[fhir].[SFI_datapoint_store]', N'U') IS NULL", [(1,)]),
            ("sys.partition_schemes ps ON ps.data_space_id", [(1,)]),
            ("OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]', N'U') IS NULL", [(1,)]),
            (
                "sys.dm_db_partition_stats AS ps WHERE ps.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')",
                [(1000.0, 400.0, 53)],
            ),
            (
                "FROM sys.indexes i WHERE i.object_id = OBJECT_ID(N'[fhir].[SFI_datapoint_store]')",
                [("IX_FHIR_SFI_datapoint_store_SourceResource", 1)],
            ),
            (
                "sys.partition_range_values",
                [(13, 40, "2022-10-01T00:00:00"), (14, 13, "2022-11-01T00:00:00")],
            ),
            (
                "OBJECT_ID(N'[fhir].[SFI_datapoint_store_backfill]') AND ps.index_id IN (0, 1) AND ps.row_count > 0",
                [(13, 7)],
            ),
            ("sys.sequences", [(12345,)]),
        ]
    )
    status = b.collect_partition_status(cur)
    assert status["store_partitioned"] is True and "old_copy_exists" not in status
    assert status["disabled_indexes"] == ["IX_FHIR_SFI_datapoint_store_SourceResource"]
    assert [x["partition"] for x in status["populated_partitions"]] == [13, 14]
    assert status["months_of_boundary_headroom"] == 172 - 14
    assert status["work_rows"] == 7 and status["sequence_current_value"] == 12345


def test_collect_partition_status_on_missing_store():
    cur = FakeCursor([("sys.sequences", [])])
    status = b.collect_partition_status(cur)
    assert status["store_exists"] is False and status["store_partitioned"] is False
    assert "populated_partitions" not in status and status["sequence_current_value"] is None


def test_select_into_columns_strips_identity_but_keeps_not_null():
    """Kopien laves med SELECT INTO; en IDENTITY på kilden må ikke følge med, for
    eksplicitte id'er indsættes bagefter (målt fejl: 'Cannot insert explicit value
    for identity column ... IDENTITY_INSERT is set to OFF')."""
    names = ["sfi_datapoint_id", "datapoint_key", "occurred_at_dt"]
    assert b.column_list(names) == "[sfi_datapoint_id], [datapoint_key], [occurred_at_dt]"
    assert b.select_into_columns(names) == (
        "ISNULL(CAST([sfi_datapoint_id] AS BIGINT), 0) AS [sfi_datapoint_id], "
        "[datapoint_key], [occurred_at_dt]"
    )


def test_qualified_column_list_prefixes_every_column():
    assert (
        b.qualified_column_list(["a", "source_resource_id"], "s") == "s.[a], s.[source_resource_id]"
    )
