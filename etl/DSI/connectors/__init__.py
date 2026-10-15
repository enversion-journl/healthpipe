# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI Connectors — shared abstractions and connector-specific implementations.

Provides:
- ExtractionConfig protocol
- SqlExtractor base class + ExtractResult
- Connector packages: mssql/, oracle/, csv_files/

The DFM extraction factory lives in ``DSI.connectors.factory``; import
``create_dfm_extraction_assets`` from there directly.
"""

from DSI.connectors.base import ExtractResult, SqlExtractor
from DSI.connectors.protocol import ExtractionConfig
from DSI.connectors.types import PartitionType, TableType

__all__ = [
    "ExtractionConfig",
    "ExtractResult",
    "PartitionType",
    "SqlExtractor",
    "TableType",
]
