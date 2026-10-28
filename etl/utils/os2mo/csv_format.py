# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Render an export set as the CSV file uploaded to OS2mo.

This file IS the delivery: it is uploaded through OS2mo's GraphQL
``upload_file`` mutation, and Magenta's import reads it from there.

The header is always the 11 fixed codesystem columns (``kode``, ``kode_parent``,
``gyldig_fra``, ``gyldig_til``, ``navn``, ``type``, ``niveau``, ``aktiv``,
``dw_is_deleted``, ``dw_timestamp_insert``, ``dw_timestamp_update``), regardless
of data — there is no dynamic map-column width.

``;`` separator and CRLF line endings, as in the phase-1 file. Every field is
quoted and an embedded quote is doubled (RFC 4180), so a name containing ``;``,
``"`` or a line break can never shift a column.

UTF-8, which carries Æ, Ø and Å — and any other character — unchanged. With a
BOM, so the file also opens correctly in Excel; a program reading it should
decode as ``utf-8-sig`` so the BOM does not end up in the first header. The
columns are named in Example Region's delivery vocabulary — see
:mod:`utils.os2mo.codesystem_export`.
"""

import csv
import io
from pathlib import Path
from typing import Any

from utils.os2mo.codesystem_export import (
    CodesystemExportSpec,
    export_columns,
)

__all__ = [
    "CSV_DELIMITER",
    "CSV_ENCODING",
    "CSV_LINE_TERMINATOR",
    "encode_csv",
    "render_csv",
    "write_csv",
]

CSV_DELIMITER = ";"
CSV_ENCODING = "utf-8-sig"
CSV_LINE_TERMINATOR = "\r\n"


def render_csv(spec: CodesystemExportSpec, export_rows: list[dict[str, Any]]) -> str:
    """Render the export rows as CSV text (without the BOM)."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=export_columns(),
        delimiter=CSV_DELIMITER,
        lineterminator=CSV_LINE_TERMINATOR,
        extrasaction="ignore",
        restval="",
        quoting=csv.QUOTE_ALL,
        doublequote=True,
    )
    writer.writeheader()
    writer.writerows(export_rows)
    return buffer.getvalue()


def encode_csv(text: str) -> bytes:
    """The exact bytes that are uploaded and written to disk: BOM + UTF-8."""
    return text.encode(CSV_ENCODING)


def write_csv(text: str, path: Path | str) -> Path:
    """Write rendered CSV text to ``path`` byte for byte and return it.

    Written as bytes, so no newline translation can turn the CRLF the writer
    produced into ``\\r\\r\\n`` on Windows. Parent directories are created; the
    file is replaced wholesale, because a full load has no partial state.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(encode_csv(text))
    return target
