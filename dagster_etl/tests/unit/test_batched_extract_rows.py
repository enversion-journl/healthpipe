# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for core.extract.BatchedExtractRows (lazy pickle-sidecar view)."""

from __future__ import annotations

import json
import pickle
from pathlib import Path

import pytest
from core.extract import BatchedExtractRows


def _make_sidecar_dir(
    tmp_path: Path,
    batches: list[list[dict]],
) -> Path:
    """Write a valid sidecar directory with manifest and batch pickle files."""
    sidecar_dir = tmp_path / "sidecars"
    sidecar_dir.mkdir()

    batch_files = []
    row_count = 0
    for idx, batch in enumerate(batches, start=1):
        filename = f"batch-{idx:06d}.pickle"
        batch_files.append(filename)
        with open(sidecar_dir / filename, "wb") as f:
            pickle.dump(batch, f)
        row_count += len(batch)

    manifest = {
        "row_count": row_count,
        "batch_count": len(batches),
        "batch_files": batch_files,
    }
    (sidecar_dir / "manifest.json").write_text(json.dumps(manifest))
    return sidecar_dir


@pytest.mark.unit
class TestBatchedExtractRowsLen:
    def test_len_returns_total_row_count(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"a": 1}, {"a": 2}], [{"a": 3}]])
        rows = BatchedExtractRows(sidecar)
        assert len(rows) == 3

    def test_len_empty(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [])
        rows = BatchedExtractRows(sidecar)
        assert len(rows) == 0


@pytest.mark.unit
class TestBatchedExtractRowsIter:
    def test_iter_yields_all_rows_in_order(self, tmp_path):
        batches = [[{"id": 1}, {"id": 2}], [{"id": 3}], [{"id": 4}, {"id": 5}]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        assert list(rows) == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]

    def test_iter_empty_batches(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [])
        rows = BatchedExtractRows(sidecar)
        assert list(rows) == []


@pytest.mark.unit
class TestBatchedExtractRowsGetItem:
    def test_positive_index(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"v": "a"}, {"v": "b"}], [{"v": "c"}]])
        rows = BatchedExtractRows(sidecar)
        assert rows[0] == {"v": "a"}
        assert rows[1] == {"v": "b"}
        assert rows[2] == {"v": "c"}

    def test_negative_index(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"v": "a"}, {"v": "b"}], [{"v": "c"}]])
        rows = BatchedExtractRows(sidecar)
        assert rows[-1] == {"v": "c"}
        assert rows[-3] == {"v": "a"}

    def test_index_out_of_range(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"v": "a"}]])
        rows = BatchedExtractRows(sidecar)
        with pytest.raises(IndexError):
            rows[5]

    def test_slice(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"i": 0}, {"i": 1}], [{"i": 2}, {"i": 3}]])
        rows = BatchedExtractRows(sidecar)
        assert rows[1:3] == [{"i": 1}, {"i": 2}]

    def test_slice_across_batch_boundary(self, tmp_path):
        sidecar = _make_sidecar_dir(
            tmp_path, [[{"i": 0}, {"i": 1}], [{"i": 2}], [{"i": 3}, {"i": 4}]]
        )
        rows = BatchedExtractRows(sidecar)
        assert rows[1:4] == [{"i": 1}, {"i": 2}, {"i": 3}]


@pytest.mark.unit
class TestBatchedExtractRowsIterBatches:
    def test_no_chunk_size_yields_original_batches(self, tmp_path):
        batches = [[{"x": 1}, {"x": 2}], [{"x": 3}]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        result = list(rows.iter_batches())
        assert result == batches

    def test_chunk_size_coalesces(self, tmp_path):
        batches = [[{"x": 1}], [{"x": 2}], [{"x": 3}], [{"x": 4}], [{"x": 5}]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        result = list(rows.iter_batches(chunk_size=2))
        assert result == [[{"x": 1}, {"x": 2}], [{"x": 3}, {"x": 4}], [{"x": 5}]]

    def test_chunk_size_splits_large_batches(self, tmp_path):
        batches = [[{"x": i} for i in range(7)]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        result = list(rows.iter_batches(chunk_size=3))
        assert result == [
            [{"x": 0}, {"x": 1}, {"x": 2}],
            [{"x": 3}, {"x": 4}, {"x": 5}],
            [{"x": 6}],
        ]

    def test_chunk_size_zero_raises(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"x": 1}]])
        rows = BatchedExtractRows(sidecar)
        with pytest.raises(ValueError, match="chunk_size must be >= 1"):
            list(rows.iter_batches(chunk_size=0))

    def test_chunk_size_equals_total_rows(self, tmp_path):
        batches = [[{"x": 1}, {"x": 2}], [{"x": 3}]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        result = list(rows.iter_batches(chunk_size=3))
        assert result == [[{"x": 1}, {"x": 2}, {"x": 3}]]


@pytest.mark.unit
class TestBatchedExtractRowsEquality:
    def test_equals_plain_list(self, tmp_path):
        batches = [[{"a": 1}, {"a": 2}], [{"a": 3}]]
        sidecar = _make_sidecar_dir(tmp_path, batches)
        rows = BatchedExtractRows(sidecar)
        assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]

    def test_not_equals_wrong_list(self, tmp_path):
        sidecar = _make_sidecar_dir(tmp_path, [[{"a": 1}]])
        rows = BatchedExtractRows(sidecar)
        assert rows != [{"a": 99}]
