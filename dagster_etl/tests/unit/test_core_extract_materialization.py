# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import pickle

import pytest
from core.extract import ExtractBatchingPolicy, materialize_row_batches_to_pickle_artifact


class _FakeLog:
    def __init__(self) -> None:
        self.infos: list[tuple] = []

    def info(self, *args) -> None:
        self.infos.append(args)


class _FakeContext:
    has_partition_key = True
    partition_key = "2026-04"

    def __init__(self) -> None:
        self.log = _FakeLog()
        self.metadata = {}

    def add_output_metadata(self, metadata) -> None:
        self.metadata.update(metadata)


class _FakeBatchSource:
    column_names = ("ID", "VALUE")

    def __init__(self, batches) -> None:
        self._batches = batches
        self.close_count = 0

    def iter_batches(self):
        yield from self._batches

    def close(self) -> None:
        self.close_count += 1


def test_materialize_row_batches_to_pickle_artifact_writes_plain_list(tmp_path):
    context = _FakeContext()
    source = _FakeBatchSource(
        [
            [{"ID": 1, "VALUE": "a"}],
            [{"ID": 2, "VALUE": "b"}],
        ]
    )
    policy = ExtractBatchingPolicy(
        enabled=True,
        fetch_size=1,
        env_prefix="TEST",
        log_every_batches=1,
    )

    artifact = materialize_row_batches_to_pickle_artifact(
        batch_source=source,
        policy=policy,
        context=context,
        source_label="TEST.TABLE",
        temp_dir=str(tmp_path),
    )

    with open(artifact.temp_path, "rb") as file:
        loaded = pickle.load(file)

    assert loaded == [{"ID": 1, "VALUE": "a"}, {"ID": 2, "VALUE": "b"}]
    assert artifact.row_count == 2
    assert artifact.batch_count == 2
    assert artifact.batch_dir is None
    assert artifact.column_names == ("ID", "VALUE")
    assert source.close_count == 1
    assert "row_count" in context.metadata
    assert "batch_count" in context.metadata
    assert context.metadata["streamable_batches"].value is False


def test_materialize_row_batches_can_write_sidecars_when_policy_opts_in(tmp_path):
    context = _FakeContext()
    source = _FakeBatchSource(
        [
            [{"ID": 1, "VALUE": "a"}],
            [{"ID": 2, "VALUE": "b"}],
        ]
    )
    policy = ExtractBatchingPolicy(
        enabled=True,
        fetch_size=1,
        env_prefix="TEST",
        log_every_batches=1,
        write_batch_sidecars=True,
    )

    artifact = materialize_row_batches_to_pickle_artifact(
        batch_source=source,
        policy=policy,
        context=context,
        source_label="TEST.TABLE",
        temp_dir=str(tmp_path),
    )

    assert artifact.batch_dir is not None
    assert context.metadata["streamable_batches"].value is True


def test_materialize_row_batches_closes_source_on_failure(tmp_path):
    context = _FakeContext()

    def _broken_batches():
        yield [{"ID": 1}]
        raise RuntimeError("boom")

    source = _FakeBatchSource(_broken_batches())
    policy = ExtractBatchingPolicy(enabled=True, fetch_size=1, env_prefix="TEST")

    with pytest.raises(RuntimeError, match="boom"):
        materialize_row_batches_to_pickle_artifact(
            batch_source=source,
            policy=policy,
            context=context,
            source_label="TEST.TABLE",
            temp_dir=str(tmp_path),
        )

    assert source.close_count == 1
    assert list(tmp_path.glob("batched-extract-*.pickle")) == []
