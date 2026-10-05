# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Incremental pickle writer for list-shaped extract outputs."""

from __future__ import annotations

import json
import os
import pickle
import shutil
import tempfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.extract.protocols import RowBatch


@dataclass(frozen=True)
class PickleListWriteResult:
    """Stats for an incrementally written list pickle."""

    path: str
    batch_dir: str | None
    row_count: int
    batch_count: int


class _CountingRows:
    def __init__(
        self,
        batches: Iterable[RowBatch],
        *,
        on_batch: Callable[[int, int], None] | None = None,
        batch_dir: str | os.PathLike[str] | None = None,
    ) -> None:
        self._batches = batches
        self._on_batch = on_batch
        self._batch_dir = Path(batch_dir) if batch_dir is not None else None
        self._batch_files: list[str] = []
        self.row_count = 0
        self.batch_count = 0

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for batch in self._batches:
            self.batch_count += 1
            self.row_count += len(batch)
            if self._batch_dir is not None:
                self._write_batch_sidecar(self.batch_count, batch)
            if self._on_batch:
                self._on_batch(self.batch_count, self.row_count)
            yield from batch

    @property
    def batch_files(self) -> tuple[str, ...]:
        return tuple(self._batch_files)

    def _write_batch_sidecar(self, batch_count: int, batch: RowBatch) -> None:
        assert self._batch_dir is not None
        self._batch_dir.mkdir(parents=True, exist_ok=True)
        filename = f"batch-{batch_count:06d}.pickle"
        with open(self._batch_dir / filename, "wb") as file:
            pickle.dump(batch, file, protocol=pickle.HIGHEST_PROTOCOL)
        self._batch_files.append(filename)


class _StreamingList:
    """Pickle helper that loads back as a plain Python list."""

    def __init__(self, row_iter: Iterable[dict[str, Any]]) -> None:
        self._row_iter = iter(row_iter)

    def __reduce_ex__(self, _protocol):
        # Pickle constructs a real list, then consumes the iterator as list
        # items. This preserves the downstream ``pickle.load() -> list``
        # contract without requiring us to build that list before dumping.
        return (list, (), None, self._row_iter, None)


def write_batches_to_pickle_list(
    batches: Iterable[RowBatch],
    *,
    temp_dir: str | os.PathLike[str] | None = None,
    on_batch: Callable[[int, int], None] | None = None,
    write_batch_sidecars: bool = False,
) -> PickleListWriteResult:
    """Write row batches to a temp pickle that loads as ``list``.

    The caller owns promotion of the returned temp path to its final location.
    On any exception while writing, the temp file is removed.
    """
    fd, temp_path = tempfile.mkstemp(
        prefix="batched-extract-",
        suffix=".pickle",
        dir=temp_dir,
    )
    os.close(fd)

    batch_dir: str | None = None
    if write_batch_sidecars:
        batch_dir = tempfile.mkdtemp(
            prefix="batched-extract-batches-",
            dir=temp_dir,
        )

    counter = _CountingRows(batches, on_batch=on_batch, batch_dir=batch_dir)
    try:
        with open(temp_path, "wb") as file:
            pickler = pickle.Pickler(file, protocol=pickle.HIGHEST_PROTOCOL)
            # Extract rows are value data, not object graphs where identity or
            # cycles matter. Disabling the Pickler memo prevents the C pickler
            # from retaining every previously serialized row dict, key string,
            # and value for the entire multi-million-row dump.
            pickler.fast = True
            pickler.dump(_StreamingList(counter))

        if batch_dir is not None:
            manifest = {
                "version": 1,
                "row_count": counter.row_count,
                "batch_count": counter.batch_count,
                "batch_files": list(counter.batch_files),
            }
            with open(Path(batch_dir) / "manifest.json", "w") as file:
                json.dump(manifest, file)
    except BaseException:
        Path(temp_path).unlink(missing_ok=True)
        if batch_dir is not None:
            shutil.rmtree(batch_dir, ignore_errors=True)
        raise

    return PickleListWriteResult(
        path=temp_path,
        batch_dir=batch_dir,
        row_count=counter.row_count,
        batch_count=counter.batch_count,
    )
