# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Source-agnostic protocols for batched DSI extraction."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol

RowBatch = list[dict[str, Any]]


class RowBatchSource(Protocol):
    """Connector-provided source of row batches.

    Concrete source systems own query/page/file semantics. Core extract
    utilities consume only this protocol.
    """

    column_names: tuple[str, ...]

    def iter_batches(self) -> Iterable[RowBatch]:
        """Yield normalized row dictionaries in deterministic batch order."""


class ClosableRowBatchSource(RowBatchSource, Protocol):
    """Optional extension for batch sources that own cursors or handles."""

    def close(self) -> None:
        """Release source handles. Must be idempotent."""
