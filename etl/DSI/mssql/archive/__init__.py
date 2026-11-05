# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Archive merge assets for the ERP (ILS, MySupply, SydAX) and KMD UdData extract layers."""

from .arc_ils_afdlgrbeh import arc_ils_afdlgrbeh
from .arc_ils_lgrbeh import arc_ils_lgrbeh
from .ils_archive_configs import arc_ils_unspsc, arc_ils_vare, ils_dfm_archive_assets
from .kmd_uddata_archive_configs import kmd_uddata_archive_assets
from .mysupply_archive_configs import mysupply_archive_assets
from .sydax_archive_configs import sydax_archive_assets

all_archive_assets = [
    arc_ils_lgrbeh,
    arc_ils_afdlgrbeh,
    arc_ils_unspsc,
    arc_ils_vare,
    *kmd_uddata_archive_assets,
    *mysupply_archive_assets,
    *ils_dfm_archive_assets,
    *sydax_archive_assets,
]

__all__ = ["all_archive_assets"]
