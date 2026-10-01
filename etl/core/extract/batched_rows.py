# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Lazy row access for batched extract artifacts."""

from __future__ import annotations

import json
import pickle
from collections.abc import Iterator
from pathlib import Path
from typing import Any


class BatchedExtractRows(list):
    """List-compatible view over pickled extract batch sidecars.

    Dagster list type checks only need this object to behave like a list, but
    large downstream transforms can opt into ``iter_batches()`` to avoid loading
    the full partition into memory.
    """

    def __init__(self, sidecar_dir: str | Path) -> None:
        super().__init__()
        self.sidecar_dir = Path(sidecar_dir)
        manifest_path = self.sidecar_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        self.row_count = int(manifest["row_count"])
        self.batch_count = int(manifest["batch_count"])
        self.batch_files = tuple(str(name) for name in manifest["batch_files"])

    def __len__(self) -> int:
        return self.row_count

    def __iter__(self) -> Iterator[dict[str, Any]]:
        for batch in self.iter_batches():
            yield from batch

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(self.row_count)
            if step != 1:
                return list(self)[index]
            return list(self._iter_range(start, stop))

        if index < 0:
            index += self.row_count
        if index < 0 or index >= self.row_count:
            raise IndexError(index)
        for row in self._iter_range(index, index + 1):
            return row
        raise IndexError(index)

    def __eq__(self, other) -> bool:
        if isinstance(other, list):
            return list(self) == other
        return super().__eq__(other)

    def iter_batches(self, chunk_size: int | None = None) -> Iterator[list[dict[str, Any]]]:
        """Yield rows as lists, optionally coalesced to ``chunk_size``."""
        if chunk_size is not None and chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")

        if chunk_size is None:
            for batch_file in self.batch_files:
                yield self._load_batch(batch_file)
            return

        current: list[dict[str, Any]] = []
        for batch_file in self.batch_files:
            batch = self._load_batch(batch_file)
            offset = 0
            while offset < len(batch):
                space = chunk_size - len(current)
                current.extend(batch[offset : offset + space])
                offset += space
                if len(current) == chunk_size:
                    yield current
                    current = []

        if current:
            yield current

    def _iter_range(self, start: int, stop: int) -> Iterator[dict[str, Any]]:
        if start >= stop:
            return

        seen = 0
        for batch_file in self.batch_files:
            batch = self._load_batch(batch_file)
            batch_start = seen
            batch_end = seen + len(batch)
            seen = batch_end
            if batch_end <= start:
                continue
            if batch_start >= stop:
                break
            local_start = max(0, start - batch_start)
            local_stop = min(len(batch), stop - batch_start)
            yield from batch[local_start:local_stop]

    def _load_batch(self, batch_file: str) -> list[dict[str, Any]]:
        with open(self.sidecar_dir / batch_file, "rb") as file:
            return pickle.load(file)
