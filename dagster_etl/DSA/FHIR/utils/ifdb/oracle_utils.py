# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Oracle database utilities for FHIR ETL assets.

Re-exports fetch_rows_as_dicts from the canonical location in
DSI.connectors.oracle.models to preserve backward-compatible import paths.
"""

from DSI.connectors.oracle.models import fetch_rows_as_dicts  # noqa: F401

__all__ = ["fetch_rows_as_dicts"]
