# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dm — the resolved datamart layer (source values + DW keys side by side)."""

from DSA.SQL.dm.haiba import haiba_dm_assets
from DSA.SQL.dm.haiba_kontakt import haiba_kontakt_assets

all_assets = [*haiba_dm_assets, *haiba_kontakt_assets]

__all__ = ["all_assets"]
