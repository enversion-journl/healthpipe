# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Encoding and tar primitives for file deliveries on a mounted share."""

import csv
import io
import os
import tarfile
from collections.abc import Generator
from typing import IO, Protocol

DEFAULT_ENCODING = "cp1252"
DELIMITER = ";"


class CsvReader(Protocol):
    """``csv.reader``'s structural surface: iteration plus ``line_num``, which
    the callers quote in their own field-count messages."""

    line_num: int

    def __iter__(self) -> "CsvReader": ...

    def __next__(self) -> list[str]: ...


# ---------------------------------------------------------------------------
# environment
# ---------------------------------------------------------------------------


def source_encoding(env_var: str, default: str = DEFAULT_ENCODING) -> str:
    """Per-source encoding from the environment; SDS delivers cp1252."""
    return os.getenv(env_var, "").strip() or default


# ---------------------------------------------------------------------------
# streaming out of the tar
# ---------------------------------------------------------------------------


class NonSeekableReader(io.RawIOBase):
    """RawIOBase adapter: tarfile's stream-mode file objects lack seekable(),
    which io.TextIOWrapper requires."""

    def __init__(self, fileobj):
        self._fileobj = fileobj

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        data = self._fileobj.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def open_member_csv(handle, encoding: str, delimiter: str = DELIMITER) -> CsvReader:
    """A csv reader over one member's byte stream, decoded as ``encoding``."""
    text = io.TextIOWrapper(
        io.BufferedReader(NonSeekableReader(handle)), encoding=encoding, newline=""
    )
    return csv.reader(text, delimiter=delimiter)


def iter_members(tar_path: str) -> Generator[tuple[str, IO[bytes]], None, None]:
    """Every regular file in the archive as ``(basename, handle)``, in tar order.

    FORWARD-ONLY STREAM. The tar is opened in ``r|bz2`` mode, which decompresses
    once and never seeks backwards. A handle is therefore valid ONLY until the
    loop advances: a member may be skipped entirely or read partially — tarfile
    winds forward through the decompressor for you — but it can never be
    revisited, and reading a handle after the next member was yielded raises
    ``tarfile.StreamError``. That is the whole point: a delivery of any size is
    read in a single pass without unpacking anything to disk.

    Skipping a member saves DECODING, not DECOMPRESSION. One bz2 stream is
    decompressed end to end whatever the caller reads, so a pass that wants only
    a subset of members costs the same wall-clock in the decompressor as a pass
    that reads them all. A source that splits a large delivery over several
    passes (LPR3 Grunddata: 47 members, ~22 GB compressed) pays the
    decompression once per pass — that is a property of the format, not
    something this module can amortise.

    Directories and members tarfile cannot open are skipped; routing, warnings
    and errors are the caller's, so this never decides what a member means.
    """
    with tarfile.open(tar_path, mode="r|bz2") as tar:
        for member in tar:
            if not member.isfile():
                continue
            handle = tar.extractfile(member)
            if handle is None:
                continue
            yield os.path.basename(member.name), handle
