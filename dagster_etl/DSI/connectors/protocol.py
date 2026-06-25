# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ExtractionConfig Protocol for the DSI extraction layer.

Both TableExtractionConfig (IFDB) and TableConfig (ERP) satisfy this protocol
without modification — they already have `description` and `partition_column` fields.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class ExtractionConfig(Protocol):
    description: str
    partition_column: str | None
