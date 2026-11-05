# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Load Module

Builds ``all_dso_sync_assets`` from the modules that sync staging (DSI/DSA) to the DSO database.
Uses SQL Server Linked Server for optimal performance on large datasets.
"""

from DSO.load.sync_codesystem import all_codesystem_dso_sync_assets
from DSO.load.sync_conceptmap import all_conceptmap_dso_sync_assets
from DSO.load.sync_dim import all_dso_sync_dim_assets
from DSO.load.sync_drg import all_drg_dso_sync_assets
from DSO.load.sync_fct import all_dso_sync_fct_assets
from DSO.load.sync_haiba import all_haiba_dso_sync_assets
from DSO.load.sync_ref_erp import all_dso_sync_ref_erp_assets
from DSO.load.sync_security import all_dso_sync_sec_assets
from DSO.load.sync_udredningsret import all_udredningsret_dso_sync_assets

all_dso_sync_assets = (
    all_dso_sync_dim_assets
    + all_dso_sync_fct_assets
    + all_haiba_dso_sync_assets
    + all_drg_dso_sync_assets
    + all_udredningsret_dso_sync_assets
    + all_dso_sync_ref_erp_assets
    + all_codesystem_dso_sync_assets
    + all_conceptmap_dso_sync_assets
    + all_dso_sync_sec_assets
)

__all__ = ["all_dso_sync_assets"]
