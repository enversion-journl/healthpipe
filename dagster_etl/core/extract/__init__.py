# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Source-agnostic batched extraction utilities."""

from core.extract.batched_rows import BatchedExtractRows
from core.extract.batching import (
    ExtractBatchingPolicy,
    iter_cursor_fetchmany,
    iter_sized_batches,
)
from core.extract.materialization import (
    BatchedPickleListArtifact,
    materialize_row_batches_to_pickle_artifact,
)
from core.extract.pickle_list import (
    PickleListWriteResult,
    write_batches_to_pickle_list,
)
from core.extract.protocols import ClosableRowBatchSource, RowBatch, RowBatchSource

__all__ = [
    "BatchedPickleListArtifact",
    "BatchedExtractRows",
    "ClosableRowBatchSource",
    "ExtractBatchingPolicy",
    "PickleListWriteResult",
    "RowBatch",
    "RowBatchSource",
    "iter_cursor_fetchmany",
    "iter_sized_batches",
    "materialize_row_batches_to_pickle_artifact",
    "write_batches_to_pickle_list",
]
