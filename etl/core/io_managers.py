# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Custom IO managers used by shared ETL infrastructure."""

from __future__ import annotations

import errno
import os
import shutil
import uuid
from pathlib import Path

import dagster as dg
from core.extract import BatchedExtractRows, BatchedPickleListArtifact
from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager


class BatchAwarePickledObjectFilesystemIOManager(PickledObjectFilesystemIOManager):
    """Filesystem IO manager that can promote pre-written pickle artifacts."""

    def __init__(
        self,
        *,
        base_dir: str | None = None,
        stream_batched_inputs: bool = False,
    ) -> None:
        super().__init__(base_dir=base_dir)
        self._stream_batched_inputs = stream_batched_inputs

    def dump_to_path(self, context: dg.OutputContext, obj, path):
        if isinstance(obj, BatchedPickleListArtifact):
            sidecar_destination = _batch_sidecar_dir(path)
            staged_pickle: Path | None = None
            staged_sidecar: Path | None = None
            try:
                staged_pickle = _stage_temp_file(obj.temp_path, path)
                if obj.batch_dir is not None:
                    staged_sidecar = _stage_temp_dir(obj.batch_dir, sidecar_destination)

                _replace_batched_artifact_from_stage(
                    staged_pickle=staged_pickle,
                    final_pickle=path,
                    staged_sidecar=staged_sidecar,
                    final_sidecar=sidecar_destination,
                )
                staged_pickle = None
                staged_sidecar = None
            except BaseException:
                if staged_pickle is not None:
                    staged_pickle.unlink(missing_ok=True)
                if staged_sidecar is not None:
                    shutil.rmtree(staged_sidecar, ignore_errors=True)
                raise
            return
        super().dump_to_path(context=context, obj=obj, path=path)
        shutil.rmtree(_batch_sidecar_dir(path), ignore_errors=True)

    def load_from_path(self, context: dg.InputContext, path):
        sidecar_dir = _batch_sidecar_dir(path)
        if self._stream_batched_inputs and (sidecar_dir / "manifest.json").exists():
            return BatchedExtractRows(sidecar_dir)
        return super().load_from_path(context=context, path=path)

    def get_metadata(self, context: dg.OutputContext, obj):
        metadata = super().get_metadata(context=context, obj=obj)
        if isinstance(obj, BatchedPickleListArtifact):
            metadata.update(
                {
                    "batched_extract": dg.MetadataValue.bool(True),
                    "streamable_batches": dg.MetadataValue.bool(obj.batch_dir is not None),
                    "row_count": dg.MetadataValue.int(obj.row_count),
                    "batch_count": dg.MetadataValue.int(obj.batch_count),
                    "batch_size": dg.MetadataValue.int(obj.batch_size),
                    "source_label": dg.MetadataValue.text(obj.source_label),
                    "column_count": dg.MetadataValue.int(len(obj.column_names)),
                }
            )
        return metadata


def _batch_sidecar_dir(path) -> Path:
    return Path(os.fspath(path)).with_name(f"{Path(os.fspath(path)).name}.batches")


def _promote_temp_dir(temp_dir: str, final_dir) -> None:
    staged = _stage_temp_dir(temp_dir, final_dir)
    try:
        _replace_path_from_stage(staged, final_dir)
    except BaseException:
        shutil.rmtree(staged, ignore_errors=True)
        raise


def _stage_temp_file(temp_path: str, final_path) -> Path:
    source = Path(temp_path)
    destination = Path(os.fspath(final_path))
    destination.parent.mkdir(parents=True, exist_ok=True)
    sibling_tmp = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        try:
            os.replace(source, sibling_tmp)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            shutil.copyfile(source, sibling_tmp)
            source.unlink(missing_ok=True)
        return sibling_tmp
    except BaseException:
        sibling_tmp.unlink(missing_ok=True)
        raise


def _stage_temp_dir(temp_dir: str, final_dir) -> Path:
    source = Path(temp_dir)
    destination = Path(os.fspath(final_dir))
    destination.parent.mkdir(parents=True, exist_ok=True)
    sibling_tmp = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")

    try:
        try:
            os.replace(source, sibling_tmp)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            shutil.copytree(source, sibling_tmp)
            shutil.rmtree(source, ignore_errors=True)
        return sibling_tmp
    except BaseException:
        shutil.rmtree(sibling_tmp, ignore_errors=True)
        raise


def _remove_path(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _replace_path_from_stage(staged_path: Path, final_path) -> None:
    destination = Path(os.fspath(final_path))
    backup = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.bak")
    backup_exists = False

    try:
        if destination.exists():
            os.replace(destination, backup)
            backup_exists = True
        os.replace(staged_path, destination)
    except BaseException:
        _remove_path(destination)
        if backup_exists and backup.exists():
            os.replace(backup, destination)
        raise
    else:
        if backup_exists:
            _remove_path(backup)


def _replace_batched_artifact_from_stage(
    *,
    staged_pickle: Path,
    final_pickle,
    staged_sidecar: Path | None,
    final_sidecar: Path,
) -> None:
    pickle_destination = Path(os.fspath(final_pickle))
    sidecar_destination = Path(os.fspath(final_sidecar))
    pickle_backup = pickle_destination.with_name(
        f".{pickle_destination.name}.{uuid.uuid4().hex}.bak"
    )
    sidecar_backup = sidecar_destination.with_name(
        f".{sidecar_destination.name}.{uuid.uuid4().hex}.bak"
    )
    pickle_backup_exists = False
    sidecar_backup_exists = False

    try:
        if pickle_destination.exists():
            os.replace(pickle_destination, pickle_backup)
            pickle_backup_exists = True
        if sidecar_destination.exists():
            os.replace(sidecar_destination, sidecar_backup)
            sidecar_backup_exists = True

        os.replace(staged_pickle, pickle_destination)
        if staged_sidecar is not None:
            os.replace(staged_sidecar, sidecar_destination)
    except BaseException:
        _remove_path(pickle_destination)
        _remove_path(sidecar_destination)
        if pickle_backup_exists and pickle_backup.exists():
            os.replace(pickle_backup, pickle_destination)
        if sidecar_backup_exists and sidecar_backup.exists():
            os.replace(sidecar_backup, sidecar_destination)
        raise
    else:
        if pickle_backup_exists:
            _remove_path(pickle_backup)
        if sidecar_backup_exists:
            _remove_path(sidecar_backup)


def _promote_temp_pickle(temp_path: str, final_path) -> None:
    staged = _stage_temp_file(temp_path, final_path)
    try:
        _replace_path_from_stage(staged, final_path)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise


class BatchAwareFilesystemIOManager(
    dg.ConfigurableIOManagerFactory[BatchAwarePickledObjectFilesystemIOManager]
):
    """Dagster resource factory for batch-aware filesystem pickle storage."""

    base_dir: str | None = None
    stream_batched_inputs: bool = False

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def create_io_manager(
        self, context: dg.InitResourceContext
    ) -> BatchAwarePickledObjectFilesystemIOManager:
        base_dir = self.base_dir or context.instance.storage_directory()  # type: ignore[union-attr]
        return BatchAwarePickledObjectFilesystemIOManager(
            base_dir=base_dir,
            stream_batched_inputs=self.stream_batched_inputs,
        )
