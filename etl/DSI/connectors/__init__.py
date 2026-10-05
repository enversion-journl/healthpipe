# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI Connectors — shared abstractions and connector-specific implementations.

Provides:
- ExtractionConfig protocol
- SqlExtractor base class + ExtractResult
- Shared factory utilities
- Connector packages: mssql/, oracle/
"""

from DSI.connectors.base import ExtractResult, SqlExtractor
from DSI.connectors.factory import create_extraction_assets
from DSI.connectors.protocol import ExtractionConfig
from DSI.connectors.types import PartitionType, TableType

__all__ = [
    "ExtractionConfig",
    "ExtractResult",
    "PartitionType",
    "SqlExtractor",
    "TableType",
    "create_extraction_assets",
]
