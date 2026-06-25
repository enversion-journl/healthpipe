# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dev-only assets — isolated development/debugging pipelines.
"""

from .reset_faktura import erp_faktura_reset_tables

all_assets = [
    erp_faktura_reset_tables,
]

__all__ = [
    "erp_faktura_reset_tables",
    "all_assets",
]
