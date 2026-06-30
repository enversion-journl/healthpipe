# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import errno
import pickle
from pathlib import Path

import core.io_managers as io_managers
import dagster as dg
import pytest
from core.extract import BatchedPickleListArtifact, write_batches_to_pickle_list
from core.io_managers import (
    BatchAwarePickledObjectFilesystemIOManager,
    _batch_sidecar_dir,
    _promote_temp_pickle,
)


def _artifact_from_batches(
    tmp_path,
    batches,
    *,
    write_batch_sidecars: bool = True,
) -> BatchedPickleListArtifact:
    write_result = write_batches_to_pickle_list(
        batches,
        temp_dir=tmp_path,
        write_batch_sidecars=write_batch_sidecars,
    )
    return BatchedPickleListArtifact(
        temp_path=write_result.path,
        batch_dir=write_result.batch_dir,
        row_count=write_result.row_count,
        batch_count=write_result.batch_count,
        column_names=("ID",),
        source_label="TEST.TABLE",
        batch_size=2,
    )


def test_dump_to_path_promotes_batched_artifact(tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    artifact = _artifact_from_batches(tmp_path, [[{"ID": 1}], [{"ID": 2}]])
    temp_path = Path(artifact.temp_path)
    final_path = tmp_path / "asset.pickle"

    manager.dump_to_path(dg.build_output_context(asset_key="asset"), artifact, final_path)

    assert not temp_path.exists()
    assert artifact.batch_dir is not None
    assert not Path(artifact.batch_dir).exists()
    loaded = manager.load_from_path(dg.build_input_context(asset_key="asset"), final_path)
    assert loaded == [
        {"ID": 1},
        {"ID": 2},
    ]
    assert type(loaded) is list
    assert (_batch_sidecar_dir(final_path) / "manifest.json").exists()


def test_streaming_input_manager_loads_batched_artifact_lazily(tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(
        base_dir=str(tmp_path / "storage"),
        stream_batched_inputs=True,
    )
    artifact = _artifact_from_batches(tmp_path, [[{"ID": 1}], [{"ID": 2}]])
    final_path = tmp_path / "asset.pickle"

    manager.dump_to_path(dg.build_output_context(asset_key="asset"), artifact, final_path)

    loaded = manager.load_from_path(dg.build_input_context(asset_key="asset"), final_path)
    assert loaded == [{"ID": 1}, {"ID": 2}]
    assert type(loaded) is not list
    assert len(loaded) == 2
    assert list(loaded.iter_batches()) == [[{"ID": 1}], [{"ID": 2}]]
    assert list(loaded.iter_batches(chunk_size=2)) == [[{"ID": 1}, {"ID": 2}]]


def test_failed_pickle_promotion_preserves_existing_sidecar(monkeypatch, tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    final_path = tmp_path / "asset.pickle"

    # Existing successful materialization with a sidecar.
    old_artifact = _artifact_from_batches(tmp_path, [[{"ID": "old"}]])
    manager.dump_to_path(dg.build_output_context(asset_key="asset"), old_artifact, final_path)
    old_sidecar = _batch_sidecar_dir(final_path)
    assert (old_sidecar / "manifest.json").exists()

    new_artifact = _artifact_from_batches(tmp_path, [[{"ID": "new"}]])

    real_replace = io_managers.os.replace
    failed_once = False

    def fail_staged_pickle_replace(src, dst):
        nonlocal failed_once
        if not failed_once and Path(dst) == final_path and Path(src).name.endswith(".tmp"):
            failed_once = True
            raise OSError("simulated pickle promotion failure")
        return real_replace(src, dst)

    monkeypatch.setattr(io_managers.os, "replace", fail_staged_pickle_replace)

    with pytest.raises(OSError, match="simulated pickle promotion failure"):
        manager.dump_to_path(
            dg.build_output_context(asset_key="asset"),
            new_artifact,
            final_path,
        )

    assert (old_sidecar / "manifest.json").exists()
    loaded = manager.load_from_path(dg.build_input_context(asset_key="asset"), final_path)
    assert loaded == [{"ID": "old"}]
    assert new_artifact.batch_dir is not None
    assert not Path(new_artifact.batch_dir).exists()


def test_failed_sidecar_promotion_preserves_matching_previous_artifact(
    monkeypatch,
    tmp_path,
):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    final_path = tmp_path / "asset.pickle"

    old_artifact = _artifact_from_batches(tmp_path, [[{"ID": "old"}]])
    manager.dump_to_path(dg.build_output_context(asset_key="asset"), old_artifact, final_path)
    old_sidecar = _batch_sidecar_dir(final_path)
    assert (old_sidecar / "manifest.json").exists()

    new_artifact = _artifact_from_batches(tmp_path, [[{"ID": "new"}]])
    real_replace = io_managers.os.replace

    def fail_final_sidecar_replace(src, dst):
        if Path(dst) == old_sidecar:
            raise OSError("simulated sidecar promotion failure")
        return real_replace(src, dst)

    monkeypatch.setattr(io_managers.os, "replace", fail_final_sidecar_replace)

    with pytest.raises(OSError, match="simulated sidecar promotion failure"):
        manager.dump_to_path(
            dg.build_output_context(asset_key="asset"),
            new_artifact,
            final_path,
        )

    loaded = manager.load_from_path(dg.build_input_context(asset_key="asset"), final_path)
    assert loaded == [{"ID": "old"}]
    streaming_manager = BatchAwarePickledObjectFilesystemIOManager(
        base_dir=str(tmp_path / "storage"),
        stream_batched_inputs=True,
    )
    streamed = streaming_manager.load_from_path(
        dg.build_input_context(asset_key="asset"),
        final_path,
    )
    assert list(streamed) == [{"ID": "old"}]


def test_non_streamable_artifact_removes_previous_sidecar(tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    final_path = tmp_path / "asset.pickle"

    old_artifact = _artifact_from_batches(
        tmp_path,
        [[{"ID": "old"}]],
        write_batch_sidecars=True,
    )
    manager.dump_to_path(dg.build_output_context(asset_key="asset"), old_artifact, final_path)
    sidecar = _batch_sidecar_dir(final_path)
    assert (sidecar / "manifest.json").exists()

    new_artifact = _artifact_from_batches(
        tmp_path,
        [[{"ID": "new"}]],
        write_batch_sidecars=False,
    )
    manager.dump_to_path(dg.build_output_context(asset_key="asset"), new_artifact, final_path)

    assert not sidecar.exists()
    assert manager.load_from_path(dg.build_input_context(asset_key="asset"), final_path) == [
        {"ID": "new"}
    ]


def test_legacy_list_output_removes_previous_sidecar_for_same_asset(tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    final_path = tmp_path / "asset.pickle"

    old_artifact = _artifact_from_batches(
        tmp_path,
        [[{"ID": "old"}]],
        write_batch_sidecars=True,
    )
    manager.dump_to_path(dg.build_output_context(asset_key="asset"), old_artifact, final_path)
    sidecar = _batch_sidecar_dir(final_path)
    assert (sidecar / "manifest.json").exists()

    manager.dump_to_path(
        dg.build_output_context(asset_key="asset"),
        [{"ID": "new"}],
        final_path,
    )

    assert not sidecar.exists()
    streaming_manager = BatchAwarePickledObjectFilesystemIOManager(
        base_dir=str(tmp_path / "storage"),
        stream_batched_inputs=True,
    )
    loaded = streaming_manager.load_from_path(
        dg.build_input_context(asset_key="asset"),
        final_path,
    )
    assert loaded == [{"ID": "new"}]
    assert type(loaded) is list


def test_dump_to_path_delegates_normal_objects(tmp_path):
    manager = BatchAwarePickledObjectFilesystemIOManager(base_dir=str(tmp_path / "storage"))
    final_path = tmp_path / "normal.pickle"

    manager.dump_to_path(dg.build_output_context(asset_key="normal"), {"ok": True}, final_path)

    with open(final_path, "rb") as file:
        assert pickle.load(file) == {"ok": True}


def test_promote_temp_pickle_uses_copy_then_replace_on_cross_device_move(
    monkeypatch,
    tmp_path,
):
    source = tmp_path / "source.pickle"
    source.write_bytes(b"payload")
    destination = tmp_path / "nested" / "final.pickle"
    real_replace = io_managers.os.replace
    calls: list[tuple[str, str]] = []

    def _fake_replace(src, dst):
        calls.append((str(src), str(dst)))
        if len(calls) == 1:
            raise OSError(errno.EXDEV, "cross-device link")
        return real_replace(src, dst)

    monkeypatch.setattr(io_managers.os, "replace", _fake_replace)

    _promote_temp_pickle(str(source), destination)

    assert destination.read_bytes() == b"payload"
    assert not source.exists()
    assert len(calls) == 2
