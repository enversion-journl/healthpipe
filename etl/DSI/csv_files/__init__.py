# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DSI CSV file extraction assets."""

from DSI.csv_files.cpr import cpr_extract_assets
from DSI.csv_files.cpr_archive import cpr_archive_assets
from DSI.csv_files.drg import drg_extract_assets, drg_freshness_assets
from DSI.csv_files.drg_archive import drg_archive_assets
from DSI.csv_files.lpr3 import lpr3_extract_assets
from DSI.csv_files.lpr3_archive import lpr3_archive_assets
from DSI.csv_files.haiba import haiba_assets
from DSI.csv_files.haiba_archive import haiba_archive_assets
from DSI.csv_files.os2mo import os2mo_assets
from DSI.csv_files.shaksor_map import extract_shak_register
from DSI.csv_files.silkeborgdata_e26_pers import extract_silkeborgdata_e26_pers
from DSI.csv_files.silkeborgdata_e26_pers_archive import arc_silkeborgdata_e26_pers
from DSI.csv_files.sks_register import extract_sks_register
from DSI.csv_files.sor_archive import arc_sor_register
from DSI.csv_files.sor_register import extract_sor_register
from DSI.csv_files.udredningsret import udredningsret_extract_assets
from DSI.csv_files.udredningsret_archive import udredningsret_archive_assets

all_extract_assets = [
    *os2mo_assets,
    *cpr_extract_assets,
    *cpr_archive_assets,
    *haiba_assets,
    *lpr3_extract_assets,
    *lpr3_archive_assets,
    *haiba_archive_assets,
    *drg_extract_assets,
    *drg_freshness_assets,
    *drg_archive_assets,
    *udredningsret_extract_assets,
    *udredningsret_archive_assets,
    extract_sor_register,
    arc_sor_register,
    extract_sks_register,
    extract_shak_register,
    extract_silkeborgdata_e26_pers,
    arc_silkeborgdata_e26_pers,
]

__all__ = ["all_extract_assets"]
