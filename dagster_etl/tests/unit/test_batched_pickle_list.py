# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import json
import pickle
from pathlib import Path

import core.extract.pickle_list as pickle_list
import pytest
from core.extract import write_batches_to_pickle_list


def test_write_batches_to_pickle_list_loads_as_plain_list(tmp_path):
    batches = [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}],
    ]
    seen_progress: list[tuple[int, int]] = []

    result = write_batches_to_pickle_list(
        batches,
        temp_dir=tmp_path,
        on_batch=lambda batch_count, row_count: seen_progress.append((batch_count, row_count)),
        write_batch_sidecars=True,
    )

    with open(result.path, "rb") as file:
        loaded = pickle.load(file)

    assert type(loaded) is list
    assert loaded == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert result.row_count == 3
    assert result.batch_count == 2
    assert result.batch_dir is not None
    manifest = json.loads((Path(result.batch_dir) / "manifest.json").read_text())
    assert manifest["row_count"] == 3
    assert manifest["batch_count"] == 2
    assert manifest["batch_files"] == ["batch-000001.pickle", "batch-000002.pickle"]
    assert seen_progress == [(1, 2), (2, 3)]


def test_write_batches_to_pickle_list_handles_empty_source(tmp_path):
    result = write_batches_to_pickle_list(
        [],
        temp_dir=tmp_path,
        write_batch_sidecars=True,
    )

    with open(result.path, "rb") as file:
        loaded = pickle.load(file)

    assert loaded == []
    assert result.row_count == 0
    assert result.batch_count == 0
    assert result.batch_dir is not None
    manifest = json.loads((Path(result.batch_dir) / "manifest.json").read_text())
    assert manifest["row_count"] == 0
    assert manifest["batch_files"] == []


def test_write_batches_to_pickle_list_can_skip_batch_sidecars(tmp_path):
    result = write_batches_to_pickle_list(
        [[{"id": 1}]],
        temp_dir=tmp_path,
        write_batch_sidecars=False,
    )

    assert result.batch_dir is None
    with open(result.path, "rb") as file:
        assert pickle.load(file) == [{"id": 1}]


def test_write_batches_to_pickle_list_removes_temp_file_on_failure(tmp_path):
    def _broken_batches():
        yield [{"id": 1}]
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        write_batches_to_pickle_list(_broken_batches(), temp_dir=tmp_path)

    assert list(tmp_path.glob("batched-extract-*.pickle")) == []
    assert list(tmp_path.glob("batched-extract-batches-*")) == []


def test_write_batches_to_pickle_list_disables_pickler_memo(monkeypatch, tmp_path):
    class _TrackingPickler(pickle.Pickler):
        instances: list[_TrackingPickler] = []

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.instances.append(self)

    monkeypatch.setattr(pickle_list.pickle, "Pickler", _TrackingPickler)

    result = write_batches_to_pickle_list(
        [
            [{"id": 1}],
            [{"id": 2}],
            [{"id": 3}],
        ],
        temp_dir=tmp_path,
    )

    with open(result.path, "rb") as file:
        assert pickle.load(file) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert bool(_TrackingPickler.instances[0].fast) is True
