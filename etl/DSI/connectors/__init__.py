# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI Connectors — shared abstractions and connector-specific implementations.

Provides:
- ExtractionConfig protocol (``DSI.connectors.protocol``)
- SqlExtractor base class + ExtractResult (``DSI.connectors.base``)
- Connector packages: mssql/, oracle/, csv_files/

The DFM extraction factory lives in ``DSI.connectors.factory``; import
``create_dfm_extraction_assets`` from there directly.
"""
